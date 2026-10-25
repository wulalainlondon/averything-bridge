package core

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/identity"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/workitems"
	"everything-go/internal/workspace"
	"github.com/coder/websocket"
	"golang.org/x/text/unicode/norm"
)

type workspaceWire struct {
	Type          string                       `json:"type"`
	RequestID     string                       `json:"request_id,omitempty"`
	Authority     string                       `json:"authority"`
	PrincipalID   string                       `json:"principal_id,omitempty"`
	OK            bool                         `json:"ok"`
	Error         string                       `json:"error,omitempty"`
	Data          *workspace.Result            `json:"data,omitempty"`
	Projects      []workitems.ProjectRef       `json:"projects,omitempty"`
	Catalog       []workspace.Agent            `json:"catalog,omitempty"`
	Backends      []protocol.BackendDefinition `json:"backends,omitempty"`
	MutationState string                       `json:"mutation_state,omitempty"`
}

// PrivatePayload makes room messages, invite bearer codes and provider
// references ineligible for the generic frame-log payload preview.
func (workspaceWire) PrivatePayload() {}
func workspaceHash(v string) string   { b := sha256.Sum256([]byte(v)); return hex.EncodeToString(b[:]) }

func (h *Hub) WorkspaceAvailable() bool { return h.workspace != nil }

func (h *Hub) workspaceOwner(c *Client) (workspace.Actor, bool) {
	if h.workspace == nil || c == nil || !c.live() || c.enrollmentOnly || c.inventoryProbe || h.pairedTaskDevice(c) == "" {
		return workspace.Actor{}, false
	}
	p := c.readIdentity.Load()
	if p == nil || h.pairing == nil || !h.pairing.MatchesDevice(p.token, h.pairedTaskDevice(c)) {
		return workspace.Actor{}, false
	}
	return h.workspace.Owner(h.pairedTaskDevice(c), workspaceHash(p.token)), true
}
func (h *Hub) workspaceGrantCurrent(g workspace.Grant) bool {
	if h.workspace == nil || !g.Owner || g.PrincipalID != h.workspace.Owner("", "").ID || h.pairing == nil {
		return false
	}
	for _, binding := range h.pairing.DeviceBindings() {
		if binding.DeviceID == g.DeviceID && hmac.Equal([]byte(workspaceHash(binding.Token)), []byte(g.CredentialHash)) {
			return true
		}
	}
	return false
}
func (h *Hub) handleWorkspace(c *Client, cmd clientproto.Command) {
	out := workspaceWire{Type: "workspace_result", RequestID: cmd.RequestID, Authority: h.cfg.InstanceID, MutationState: "refused"}
	a, ok := h.workspaceOwner(c)
	if !ok {
		out.Error = "workspace_permission_denied"
		c.enqueueEventUnlogged(out)
		return
	}
	if cmd.Workspace == nil || cmd.RequestID == "" || len(cmd.RequestID) > 128 {
		out.Error = "workspace_request_invalid"
		c.enqueueEventUnlogged(out)
		return
	}
	out.PrincipalID = a.ID
	if cmd.Workspace.Action == "agent_catalog" {
		if cmd.Workspace.Authority != h.cfg.InstanceID {
			out.Error = "workspace_wrong_authority"
		} else {
			out.OK = true
			if h.work != nil {
				out.Projects, _ = h.work.ProjectRefs(c.ctx)
			}
			out.Catalog = h.workspaceAgentCatalog()
			out.Backends = h.client.BackendRegistryUpdated(h.workspaceCatalog(c.ctx)).Backends
		}
	} else {
		result, err := h.executeWorkspace(c.ctx, a, *cmd.Workspace)
		out.MutationState = workspaceMutationOutcome(*cmd.Workspace, result, err)
		if result.MutationID != "" {
			out.Data = &result
		}
		if err != nil {
			out.Error = err.Error()
		} else {
			out.OK = true
			out.Data = &result
		}
	}
	if current, valid := h.workspaceOwner(c); !valid || current.ID != a.ID || current.CredentialHash != a.CredentialHash {
		out.OK = false
		out.Data = nil
		out.Catalog = nil
		out.Backends = nil
		out.Projects = nil
		out.Error = "workspace_permission_revoked"
		out.MutationState = "unknown"
	}
	c.enqueueEventUnlogged(out)
}
func (h *Hub) workspaceAgentCatalog() []workspace.Agent {
	out := []workspace.Agent{}
	if h.partners == nil {
		return out
	}
	_, profiles := h.partners.Snapshot()
	for _, p := range profiles {
		s, ok := h.partnerSession(p.SessionID)
		if !ok || h.intakeWritePolicy(s) != nil {
			continue
		}
		if _, exclusive := h.workspace.SessionRoom(context.Background(), s.ID); exclusive {
			continue
		}
		snap := s.SettingsSnapshot()
		out = append(out, workspace.Agent{Ready: true, Name: p.Name, Description: p.Description, SessionID: s.ID, ThreadID: s.ResumeID(), Config: snap.ConfigRevision, RoleRevision: p.RoleRevision, RoleHash: workspaceHash(p.Description), ProfileID: p.ID, Backend: string(snap.Backend), Model: snap.Model, Sandbox: snap.Sandbox})
	}
	return out
}

type workspaceAgentInput struct {
	Name           string  `json:"name"`
	Description    *string `json:"description"`
	ProfileID      string  `json:"profile_id"`
	SessionID      string  `json:"session_id"`
	ExpectedThread *string `json:"expected_native_thread_id"`
	ExpectedConfig *uint64 `json:"expected_config_revision"`
	Backend        string  `json:"backend"`
	Model          string  `json:"model"`
	Disabled       bool    `json:"disabled"`
}

func (h *Hub) verifyWorkspaceAgent(ctx context.Context, a workspace.Actor, c workspace.Command) (workspace.Agent, error) {
	if !a.Owner {
		return workspace.Agent{}, errors.New("paired_owner_required")
	}
	if _, _, err := h.workspace.Authorize(ctx, a, c.RoomID, "manage"); err != nil {
		return workspace.Agent{}, err
	}
	var in workspaceAgentInput
	d := json.NewDecoder(strings.NewReader(string(c.Data)))
	d.DisallowUnknownFields()
	if d.Decode(&in) != nil {
		return workspace.Agent{}, errors.New("binding_payload_invalid")
	}
	if c.Action == "create_agent" {
		if in.SessionID != "" || in.ProfileID != "" || in.ExpectedThread != nil || in.ExpectedConfig != nil {
			return workspace.Agent{}, errors.New("fresh_room_binding_required")
		}
		if in.Backend != "codex" && in.Backend != "claude" {
			return workspace.Agent{}, errors.New("backend_unsupported")
		}
		if in.Model == "" {
			return workspace.Agent{}, errors.New("model_required")
		}
		known := false
		for _, definition := range h.workspaceCatalog(ctx) {
			if definition.ID == in.Backend {
				if definition.DefaultModel == in.Model {
					known = true
				}
				for _, m := range definition.Models {
					if m.ID == in.Model {
						known = true
					}
				}
			}
		}
		if !known {
			return workspace.Agent{}, errors.New("model_not_in_current_catalog")
		}
		id := h.workspace.RoomSessionID(a, c.RoomID, c.MutationID)
		return workspace.Agent{Ready: false, SetupReason: "preparing_original_session", Name: strings.TrimSpace(in.Name), Description: optionalRoomRole(in.Description), SessionID: id, Config: 0, RoleRevision: 1, RoleHash: workspaceHash(optionalRoomRole(in.Description)), Backend: in.Backend, Model: in.Model, Sandbox: "read-only", RoomExclusive: true}, nil
	}
	id := in.SessionID
	if strings.HasPrefix(id, "sk1:") {
		key, ok := identity.ParseSessionKey(id)
		if !ok || key.AuthorityInstanceID != h.cfg.InstanceID {
			return workspace.Agent{}, errors.New("workspace_wrong_authority")
		}
		id = key.SessionID
	}
	s, ok := h.partnerSession(id)
	if !ok || in.ExpectedThread == nil || in.ExpectedConfig == nil {
		return workspace.Agent{}, errors.New("original_binding_required")
	}
	if s.ResumeID() != *in.ExpectedThread || s.SettingsSnapshot().ConfigRevision != *in.ExpectedConfig {
		return workspace.Agent{}, errors.New("session_binding_changed")
	}
	if err := h.workspaceNativeWritePolicy(s); err != nil {
		return workspace.Agent{}, errors.New("original_session_permission_required")
	}
	var profileID, description, name string
	var roleRevision, sourceRoleRevision uint64
	if h.partners != nil {
		if p, found := h.partners.ForSession(s.ID); found {
			profileID, description, name, roleRevision = p.ID, p.Description, p.Name, p.RoleRevision
		}
	}
	sourceRoleRevision = roleRevision
	if in.ProfileID != "" && profileID != in.ProfileID {
		return workspace.Agent{}, errors.New("profile_binding_changed")
	}
	if in.Name != "" {
		name = in.Name
	}
	if in.Description != nil {
		description = *in.Description
	}
	if name == "" {
		name = s.Snapshot().Name
	}
	roomID, exclusive := h.workspace.SessionRoom(ctx, s.ID)
	if exclusive && roomID != c.RoomID {
		return workspace.Agent{}, errors.New("session_already_bound")
	}
	if c.Action == "update_agent" {
		old, err := h.workspace.Lookup(ctx, a, c.RoomID, c.EntityID)
		if err != nil {
			return workspace.Agent{}, err
		}
		var prior workspace.Agent
		if json.Unmarshal(old.Body, &prior) != nil || prior.SessionID != s.ID {
			return workspace.Agent{}, errors.New("binding_identity_immutable")
		}
		if err := h.workspaceAgentOriginalTarget(ctx, prior, s); err != nil {
			return workspace.Agent{}, err
		}
		roleRevision = prior.RoleRevision
		sourceRoleRevision = prior.SourceRoleRevision
		if prior.Description != description {
			roleRevision++
		}
	}
	settings := s.SettingsSnapshot()
	description = norm.NFC.String(strings.TrimSpace(description))
	return workspace.Agent{Ready: true, Name: strings.TrimSpace(name), Description: description, ProfileID: profileID, SessionID: s.ID, ThreadID: s.ResumeID(), Config: settings.ConfigRevision, RoleRevision: roleRevision, SourceRoleRevision: sourceRoleRevision, RoleHash: workspaceHash(description), Backend: string(settings.Backend), Model: settings.Model, Sandbox: settings.Sandbox, RoomExclusive: exclusive, Disabled: in.Disabled}, nil
}

func (h *Hub) workspaceCatalog(ctx context.Context) []backend.Definition {
	defs := h.cfg.Backends
	if provider, ok := h.exec.(interface {
		CatalogDefinitions(context.Context, []backend.Definition) []backend.Definition
	}); ok {
		catalogCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		defs = provider.CatalogDefinitions(catalogCtx, defs)
	}
	out := []backend.Definition{}
	for _, def := range defs {
		if def.ID == backend.Codex || def.ID == backend.Claude {
			out = append(out, def)
		}
	}
	return out
}
func (h *Hub) executeWorkspace(ctx context.Context, a workspace.Actor, c workspace.Command) (workspace.Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if h.workspace == nil {
		return workspace.Result{}, errors.New("workspace_unavailable")
	}
	if c.Authority != h.cfg.InstanceID {
		return workspace.Result{}, errors.New("workspace_wrong_authority")
	}
	if c.Action == "create_room" {
		var in struct {
			ProjectID string `json:"project_id"`
		}
		if json.Unmarshal(c.Data, &in) != nil {
			return workspace.Result{}, errors.New("room_payload_invalid")
		}
		if in.ProjectID != "" {
			if !a.Owner || h.work == nil {
				return workspace.Result{}, errors.New("original_project_unavailable")
			}
			project, err := h.work.GetProject(ctx, in.ProjectID)
			if err != nil || project.AuthorityInstanceID != h.cfg.InstanceID || project.ArchivedAt != nil {
				return workspace.Result{}, errors.New("original_project_unavailable")
			}
			c.VerifiedProjectID = project.ID
		}
	}
	if c.Action == "set_push_subscription" && h.workspacePush == nil {
		return workspace.Result{}, errors.New("workspace_push_not_configured")
	}
	if c.Action == "tool_accounts" {
		return h.workspaceToolAccount(a, c)
	}
	if c.Action == "tool_read" {
		return h.workspaceToolRead(ctx, a, c)
	}
	if c.Action == "agent_binding" {
		return h.workspaceAgentBinding(ctx, a, c)
	}
	if c.Action == "approve_effect" {
		if err := h.verifyWorkspaceEffect(ctx, a, &c); err != nil {
			return workspace.Result{}, err
		}
	}
	create := c.Action == "create_agent"
	if create || c.Action == "bind_agent" || c.Action == "update_agent" {
		agent, err := h.verifyWorkspaceAgent(ctx, a, c)
		if err != nil {
			return workspace.Result{}, err
		}
		c.VerifiedAgent = &agent
		if create {
			c.Action = "bind_agent"
		}
	}
	result, err := h.workspace.Apply(ctx, a, c)
	if err != nil {
		return result, err
	}
	if create && result.Entity != nil {
		var agent workspace.Agent
		if json.Unmarshal(result.Entity.Body, &agent) != nil {
			return workspace.Result{}, errors.New("binding_storage_invalid")
		}
		pending := workspace.PendingAgent{Entity: *result.Entity, Agent: agent, Grant: workspace.Grant{PrincipalID: a.ID, DeviceID: a.DeviceID, CredentialHash: a.CredentialHash, Owner: a.Owner}}
		if room, _, e := h.workspace.Authorize(ctx, a, c.RoomID, "manage"); e == nil {
			pending.Room = room
			pending.Grant.ACLRevision = room.ACLRevision
		}
		if ready, e := h.provisionWorkspaceAgent(ctx, pending); e == nil {
			result.Entity = &ready
		} else {
			_, _ = h.workspace.AgentReady(ctx, c.RoomID, result.Entity.ID, agent.SessionID, false, "native_session_setup_failed")
			return result, errors.New("native_session_setup_failed_lookup_original")
		}
	}
	if c.Action == "accept_task" && result.Entity != nil {
		_ = h.projectWorkspaceTask(ctx, c.RoomID, result.Entity.ID)
	}
	if c.MutationID != "" && !workspace.ReadAction(c.Action) {
		h.wakeWorkspace()
	}
	return result, nil
}
func (h *Hub) provisionWorkspaceAgent(ctx context.Context, p workspace.PendingAgent) (workspace.Entity, error) {
	if h.workspaceMCPURL == "" {
		return workspace.Entity{}, errors.New("room_tool_runtime_unavailable")
	}
	if !h.workspaceGrantCurrent(p.Grant) || p.Room.ID == "" || p.Room.Archived || (p.Room.ExpiresAt > 0 && p.Room.ExpiresAt <= time.Now().UnixMilli()) {
		return workspace.Entity{}, errors.New("original_room_grant_revoked")
	}
	a := h.workspace.Owner(p.Grant.DeviceID, p.Grant.CredentialHash)
	current, err := h.workspace.Lookup(ctx, a, p.Room.ID, p.Entity.ID)
	if err != nil {
		return workspace.Entity{}, err
	}
	var agent workspace.Agent
	if json.Unmarshal(current.Body, &agent) != nil || agent.Disabled || !agent.RoomExclusive || agent.SessionID != p.Agent.SessionID {
		return workspace.Entity{}, errors.New("agent_binding_changed")
	}
	if agent.Ready {
		return current, nil
	}
	cwd, err := workspaceProspectivePath(filepath.Join(h.cfg.DataDir, "collaboration-workspace-v1", "workdirs", p.Room.ID, agent.SessionID))
	if err != nil {
		return workspace.Entity{}, err
	}
	if !h.cwdInScope(cwd) {
		cwd, err = workspaceProspectivePath(filepath.Join(h.cfg.RootDir, ".bridge-workspaces", p.Room.ID, agent.SessionID))
		if err != nil {
			return workspace.Entity{}, err
		}
	}
	if !h.cwdInScope(cwd) {
		return workspace.Entity{}, errors.New("room_workdir_outside_root")
	}
	if err = os.MkdirAll(cwd, 0700); err != nil {
		return workspace.Entity{}, err
	}
	if !h.cwdInScope(cwd) {
		return workspace.Entity{}, errors.New("room_workdir_outside_root")
	}
	s, exists := h.registry.Get(agent.SessionID)
	if exists {
		v := s.SettingsSnapshot()
		if v.Cwd != cwd || string(v.Backend) != agent.Backend || v.Model != agent.Model || v.Sandbox != "read-only" || v.ConfigRevision != agent.Config || s.State() == session.Closed {
			return workspace.Entity{}, errors.New("reserved_session_changed")
		}
	} else {
		s = h.registry.Create(agent.SessionID, "頻道 · "+agent.Name, cwd, agent.Backend, agent.Model, "read-only", "")
		if s.SettingsSnapshot().ConfigRevision != agent.Config {
			return workspace.Entity{}, errors.New("initial_configuration_changed")
		}
	}
	if err = h.registry.PersistDurably(); err != nil {
		return workspace.Entity{}, err
	}
	h.runtimes.Ensure(s.ID, "idle", 0)
	ready, err := h.workspace.AgentReady(ctx, p.Room.ID, p.Entity.ID, agent.SessionID, true, "")
	if err == nil {
		h.workspaceInvalidation()
	}
	return ready, err
}
func (h *Hub) CloseWorkspace() error {
	if h.workspaceCancel != nil {
		h.workspaceCancel()
	}
	if h.workspaceMCP != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = h.workspaceMCP.Shutdown(ctx)
		cancel()
	}
	h.workspaceWorkers.Wait()
	if h.workspace != nil {
		return h.workspace.Close()
	}
	return nil
}
func (h *Hub) wakeWorkspace() {
	select {
	case h.workspaceEffectWake <- struct{}{}:
	default:
	}
	select {
	case h.workspaceWake <- struct{}{}:
	default:
	}
	h.workspaceInvalidation()
}
func (h *Hub) workspaceInvalidation() {
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()
	for _, c := range clients {
		if _, ok := h.workspaceOwner(c); ok {
			c.enqueueEventUnlogged(workspaceWire{Type: "workspace_changed", Authority: h.cfg.InstanceID, OK: true})
		}
	}
}

func (h *Hub) rejectRoomSessionCommand(c *Client, cmd clientproto.Command) bool {
	if cmd.SessionID == "" {
		return false
	}
	if h.workspace == nil {
		if !h.workspaceReservedSession(cmd.SessionID) {
			return false
		}
		c.enqueueEventUnlogged(h.client.Error("", cmd.RequestID, "workspace_unavailable"))
		return true
	}
	room, exclusive := h.workspace.SessionRoom(context.Background(), cmd.SessionID)
	if !exclusive {
		if strings.HasPrefix(cmd.SessionID, "s_cw_") {
			c.enqueueEventUnlogged(h.client.Error("", cmd.RequestID, "workspace_permission_denied"))
			return true
		}
		return false
	}
	a, ok := h.workspaceOwner(c)
	if !ok {
		c.enqueueEventUnlogged(h.client.Error("", cmd.RequestID, "workspace_permission_denied"))
		return true
	}
	if _, _, err := h.workspace.Authorize(context.Background(), a, room, "read"); err != nil {
		c.enqueueEventUnlogged(h.client.Error("", cmd.RequestID, "workspace_permission_denied"))
		return true
	}
	switch cmd.Kind {
	case "message", "steer_message", "new_session", "clear_session", "close_session", "fork_session", "switch_session_config", "set_effort", "codex_voice_start", "codex_voice_text", "voice_call_prepare", "voice_transfer_control":
		c.enqueueEventUnlogged(h.client.Error(cmd.SessionID, cmd.RequestID, "This AI is reserved for its shared room. Use the room composer and task controls to preserve its context."))
		return true
	}
	return false
}

// Guests never become Hub Clients. They cannot receive a legacy broadcast,
// offline replay, owner session catalog, original device key or LINE grant.
func (h *Hub) ServeWorkspaceGuest(w http.ResponseWriter, r *http.Request) {
	if h.workspace == nil {
		http.Error(w, "workspace_unavailable", http.StatusNotFound)
		return
	}
	if !directPrivateRequest(r) && !strings.HasPrefix(requestHTTPOrigin(r), "https://") {
		http.Error(w, "secure_transport_required", http.StatusForbidden)
		return
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		u, err := url.Parse(origin)
		if err != nil {
			http.Error(w, "origin_not_allowed", http.StatusForbidden)
			return
		}
		host := u.Hostname()
		ip := net.ParseIP(host)
		if u.Host == "" || (u.Host != r.Host && host != "localhost" && !(ip != nil && ip.IsLoopback())) {
			http.Error(w, "origin_not_allowed", http.StatusForbidden)
			return
		}
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true, CompressionMode: webSocketCompressionMode(r.UserAgent())})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(4096)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	helloCtx, stopHello := context.WithTimeout(ctx, 10*time.Second)
	_, data, err := conn.Read(helloCtx)
	stopHello()
	if err != nil {
		return
	}
	var hello struct {
		Type       string `json:"type"`
		Credential string `json:"credential"`
		InviteCode string `json:"invite_code"`
		DeviceID   string `json:"device_id"`
		Name       string `json:"name"`
	}
	d := json.NewDecoder(strings.NewReader(string(data)))
	d.DisallowUnknownFields()
	if d.Decode(&hello) != nil || hello.Type != "workspace_guest_hello" {
		_ = conn.Close(websocket.StatusPolicyViolation, "workspace_unauthorized")
		return
	}
	var actor workspace.Actor
	if hello.InviteCode != "" {
		actor, _, err = h.workspace.AcceptInvite(ctx, hello.InviteCode, hello.Credential, hello.DeviceID, hello.Name)
	} else {
		actor, err = h.workspace.Authenticate(ctx, hello.Credential)
	}
	if err != nil {
		_ = conn.Close(websocket.StatusPolicyViolation, "workspace_unauthorized")
		return
	}
	credential := hello.Credential
	hello.Credential = ""
	hello.InviteCode = ""
	write := func(v workspaceWire) error {
		b, e := json.Marshal(v)
		if e != nil {
			return e
		}
		writeCtx, stop := context.WithTimeout(ctx, 10*time.Second)
		defer stop()
		return conn.Write(writeCtx, websocket.MessageText, b)
	}
	if write(workspaceWire{Type: "workspace_guest_ready", Authority: h.cfg.InstanceID, PrincipalID: actor.ID, OK: true}) != nil {
		return
	}
	conn.SetReadLimit(16 * 1024 * 1024)
	var last time.Time
	var burst int
	for {
		readCtx, stop := context.WithTimeout(ctx, 90*time.Second)
		typ, data, e := conn.Read(readCtx)
		stop()
		if e != nil {
			return
		}
		if typ != websocket.MessageText {
			return
		}
		if time.Since(last) > time.Second {
			last = time.Now()
			burst = 0
		}
		burst++
		if burst > 30 {
			_ = conn.Close(websocket.StatusPolicyViolation, "request_rate_limit")
			return
		}
		current, e := h.workspace.Authenticate(ctx, credential)
		if e != nil || current.ID != actor.ID {
			_ = conn.Close(websocket.StatusPolicyViolation, "workspace_unauthorized")
			return
		}
		actor = current
		var envelope struct {
			Type      string             `json:"type"`
			RequestID string             `json:"request_id"`
			Workspace *workspace.Command `json:"workspace"`
		}
		decoder := json.NewDecoder(strings.NewReader(string(data)))
		decoder.DisallowUnknownFields()
		out := workspaceWire{Type: "workspace_result", Authority: h.cfg.InstanceID, PrincipalID: actor.ID, MutationState: "refused"}
		if decoder.Decode(&envelope) != nil || len(envelope.RequestID) > 128 || envelope.RequestID == "" {
			out.Error = "workspace_request_invalid"
		} else if envelope.Type == "ping" {
			out.Type = "workspace_pong"
			out.OK = true
		} else if envelope.Type != "workspace_request" || envelope.Workspace == nil {
			out.Error = "isolated_workspace_transport_only"
		} else {
			out.RequestID = envelope.RequestID
			result, e := h.executeWorkspace(ctx, actor, *envelope.Workspace)
			out.MutationState = workspaceMutationOutcome(*envelope.Workspace, result, e)
			if result.MutationID != "" {
				out.Data = &result
			}
			if e != nil {
				out.Error = e.Error()
			} else {
				out.OK = true
				out.Data = &result
			}
		}
		if write(out) != nil {
			return
		}
	}
}

func workspaceMutationOutcome(c workspace.Command, result workspace.Result, err error) string {
	if workspace.ReadAction(c.Action) || c.Action == "agent_catalog" {
		return ""
	}
	if result.MutationID != "" {
		return "committed"
	}
	var uncertain *workspace.CommitUncertain
	if errors.As(err, &uncertain) {
		return "unknown"
	}
	return "refused"
}

func optionalRoomRole(v *string) string {
	if v == nil {
		return ""
	}
	return norm.NFC.String(strings.TrimSpace(*v))
}

// Resolve the existing ancestor before joining generated, nonexistent suffixes.
// This handles macOS /var -> /private/var without treating a symlinked escape
// as an in-root directory merely because its final leaf does not exist yet.
func workspaceProspectivePath(path string) (string, error) {
	p, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	suffix := []string{}
	for {
		if _, err := os.Lstat(p); err == nil {
			base, err := filepath.EvalSymlinks(p)
			if err != nil {
				return "", err
			}
			for i := len(suffix) - 1; i >= 0; i-- {
				base = filepath.Join(base, suffix[i])
			}
			return base, nil
		} else if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(p)
		if parent == p {
			return "", errors.New("room_workdir_ancestor_unavailable")
		}
		suffix = append(suffix, filepath.Base(p))
		p = parent
	}
}

// Native events remain original-session events, but a paired owner who has left
// the room cannot recover room content through the legacy stream or replay.
func (h *Hub) workspaceNativeFrameAllowed(c *Client, data []byte) bool {
	var value any
	if json.Unmarshal(data, &value) != nil {
		return false
	}
	var allowed func(any) bool
	allowed = func(v any) bool {
		switch node := v.(type) {
		case []any:
			for _, child := range node {
				if !allowed(child) {
					return false
				}
			}
		case map[string]any:
			if id, ok := node["session_id"].(string); ok && strings.HasPrefix(identity.WireSessionID(id), "s_cw_") {
				if h.workspace == nil {
					return false
				}
				room, exclusive := h.workspace.SessionRoom(context.Background(), identity.WireSessionID(id))
				if !exclusive {
					return false
				}
				a, ok := h.workspaceOwner(c)
				if !ok {
					return false
				}
				if _, _, err := h.workspace.Authorize(context.Background(), a, room, "read"); err != nil {
					return false
				}
			}
			for _, child := range node {
				if !allowed(child) {
					return false
				}
			}
		}
		return true
	}
	return allowed(value)
}

func (h *Hub) workspaceRoomNativeEvent(event any) bool {
	data, err := json.Marshal(event)
	if err != nil {
		return false
	}
	var header struct {
		SessionID string `json:"session_id"`
	}
	return json.Unmarshal(data, &header) == nil && strings.HasPrefix(identity.WireSessionID(header.SessionID), "s_cw_")
}

// Check structural identities only; text containing JSON is still ordinary text.
func workspaceFrameHasReservedSession(data []byte) bool {
	var value any
	if json.Unmarshal(data, &value) != nil {
		return false
	}
	var contains func(any) bool
	contains = func(v any) bool {
		switch node := v.(type) {
		case []any:
			for _, child := range node {
				if contains(child) {
					return true
				}
			}
		case map[string]any:
			if id, ok := node["session_id"].(string); ok && strings.HasPrefix(identity.WireSessionID(id), "s_cw_") {
				return true
			}
			for _, child := range node {
				if contains(child) {
					return true
				}
			}
		}
		return false
	}
	return contains(value)
}
