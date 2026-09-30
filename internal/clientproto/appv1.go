// Package clientproto adapts the core session gateway to a concrete client
// wire protocol. AppV1 is the current React/Capacitor bridge protocol.
package clientproto

import (
	"everything-go/internal/backend"
	"everything-go/internal/coordination"
	"everything-go/internal/eventinbox"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/toolenv"
	"everything-go/internal/workitems"
)

// AppV1 builds outbound frames for the mobile app's current JSON protocol.
// Keeping these constructors behind one type lets the core move toward
// transport/session intent without scattering app-specific frame names.
type AppV1 struct{}

func NewAppV1() AppV1 { return AppV1{} }

// Command is the protocol-neutral shape the core router consumes.
type Command struct {
	PM              *coordination.Command
	Collaboration   *coordination.CollaborationCommand
	ToolEnvironment *toolenv.Request
	Kind            string

	SessionID string
	RequestID string

	DeviceID        string
	DeviceName      string
	ClientSurface   string
	AuthToken       string
	ReplayAck       bool
	ProtocolVersion int
	ClientInfo      *protocol.ClientInfo
	ConnectionProbe bool
	BatchID         string
	Revision        uint64
	Read            bool

	Name            string
	Cwd             string
	Backend         string
	Model           string
	ModelSet        bool
	Sandbox         string
	ResumeClaudeID  string
	Content         string
	UploadRequestID string
	UploadID        string
	UploadKind      string
	MediaType       string
	SizeBytes       int64

	Effort                 string
	EffortSet              bool
	ExpectedConfigRevision *uint64
	ConfigScope            string
	ServiceTier            *string
	CollaborationMode      *string
	Personality            *string
	Pinned                 *bool
	Hidden                 *bool
	Objective              string
	GoalStatus             string
	TokenBudget            *int

	Limit           int
	KnownLast       string
	Mode            string
	Before          string
	IncludeThinking bool

	ShellID string
	Data    string
	ID      string
	PID     int
	Force   bool

	Path                    string
	Paths                   []string
	ClientHash              string
	ExpectedModified        *int64
	Token                   string
	Platform                string
	NotificationPreferences *protocol.NotificationPreferences

	Query            string
	Offset           int
	Filters          *protocol.SearchFilters
	Cursor           string
	ProjectDir       string
	IncludeHidden    bool
	IncludeSubagents bool
	MsgUUID          string
	Around           int

	SDP           string
	Candidate     string
	SDPMid        string
	SDPMLineIndex *int

	Answers   map[string]any
	Cancelled *bool
	Decision  string

	FileID string

	ForkAfterMessageID string

	FeedID         string
	EventID        string
	Title          string
	HTML           string
	Source         string
	URL            string
	ClientDedupKey string
	ContentType    string

	ProjectID             string
	WorkItemID            string
	MutationID            string
	ExpectedVersion       uint64
	ExpectedRevision      *uint64
	Lifecycle             string
	Priority              string
	SortKey               int64
	Description           string
	Outcome               string
	NextStep              string
	AcceptanceCriteria    string
	WorkspacePath         string
	ThreadID              string
	Role                  string
	DependsOnID           string
	WorkLinkID            string
	CommentID             string
	WorkAttachmentID      string
	AttachmentID          string
	RunID                 string
	RunKind               string
	Body                  string
	BlockedReasonCode     string
	BlockedNote           string
	Assignee              string
	DueAt                 *int64
	Labels                []string
	AutomationMode        string
	WorkflowID            string
	WorkflowNodeID        string
	WorkflowName          string
	WorkflowDescription   string
	WorkflowDefinition    workitems.WorkflowDefinition
	ProjectContext        string
	DeliveredRevision     uint64
	AckedRevision         uint64
	Fields                []string
	BootstrapID           string
	CurrentState          string
	SessionIDs            []string
	SelectedSuggestionIDs []string

	Images []backend.ImageAttachment
	Files  []backend.FileAttachment
}

func (AppV1) ParseCommand(in protocol.Inbound) Command {
	return Command{
		ToolEnvironment: in.ToolEnvironment,
		PM:              in.PM,
		Collaboration:   in.Collaboration,
		Kind:            in.Type,
		SessionID:       in.SessionID,
		RequestID:       in.RequestID,

		DeviceID:        in.DeviceID,
		DeviceName:      in.DeviceName,
		ClientSurface:   in.ClientSurface,
		AuthToken:       in.AuthToken,
		ReplayAck:       in.ReplayAck,
		ProtocolVersion: in.ProtocolVersion,
		ClientInfo:      in.ClientInfo,
		ConnectionProbe: in.ConnectionProbe,
		BatchID:         in.BatchID,
		Revision:        in.Revision,
		Read:            in.Read,

		Name:            in.Name,
		Cwd:             in.Cwd,
		Backend:         in.Backend,
		Model:           in.Model,
		ModelSet:        in.ModelSet,
		Sandbox:         in.Sandbox,
		ResumeClaudeID:  in.ResumeClaudeID,
		Content:         in.Content,
		UploadRequestID: in.UploadRequestID,
		UploadID:        in.UploadID,
		UploadKind:      in.UploadKind,
		MediaType:       in.MediaType,
		SizeBytes:       in.SizeBytes,

		Effort: func() string {
			if in.Effort != nil {
				return *in.Effort
			}
			return ""
		}(),
		EffortSet:              in.Effort != nil,
		ExpectedConfigRevision: in.ExpectedConfigRevision,
		ConfigScope:            in.ConfigScope,
		ServiceTier:            in.ServiceTier,
		CollaborationMode:      in.CollaborationMode,
		Personality:            in.Personality,
		Pinned:                 in.Pinned,
		Hidden:                 in.Hidden,
		Objective:              in.Objective,
		GoalStatus:             in.Status,
		TokenBudget:            in.TokenBudget,

		Limit:           in.Limit,
		KnownLast:       in.KnownLast,
		Mode:            in.Mode,
		Before:          in.Before,
		IncludeThinking: in.IncludeThinking,

		ShellID: in.ShellID,
		Data:    in.Data,
		ID:      in.ID,
		PID:     in.PID,
		Force:   in.Force,

		Path:                    in.Path,
		Paths:                   in.Paths,
		ClientHash:              in.ClientHash,
		ExpectedModified:        in.ExpectedModified,
		Token:                   in.Token,
		Platform:                in.Platform,
		NotificationPreferences: in.NotificationPreferences,

		Query:            in.Query,
		Offset:           in.Offset,
		Filters:          in.Filters,
		Cursor:           in.Cursor,
		ProjectDir:       in.ProjectDir,
		IncludeHidden:    in.IncludeHidden,
		IncludeSubagents: in.IncludeSubagents,
		MsgUUID:          in.MsgUUID,
		Around:           in.Around,

		SDP:           in.SDP,
		Candidate:     in.Candidate,
		SDPMid:        in.SDPMid,
		SDPMLineIndex: in.SDPMLineIndex,

		Answers:   in.Answers,
		Cancelled: in.Cancelled,
		Decision:  in.Decision,

		FileID: in.FileID,

		ForkAfterMessageID: in.ForkAfterMessageID,

		FeedID:         in.FeedID,
		EventID:        in.EventID,
		Title:          in.Title,
		HTML:           in.HTML,
		Source:         in.Source,
		URL:            in.URL,
		ClientDedupKey: in.ClientDedupKey,
		ContentType:    in.ContentType,

		ProjectID:             in.ProjectID,
		WorkItemID:            in.WorkItemID,
		MutationID:            in.MutationID,
		ExpectedVersion:       in.ExpectedVersion,
		ExpectedRevision:      in.ExpectedRevision,
		Lifecycle:             in.Lifecycle,
		Priority:              in.Priority,
		SortKey:               in.SortKey,
		Description:           in.Description,
		Outcome:               in.Outcome,
		NextStep:              in.NextStep,
		AcceptanceCriteria:    in.AcceptanceCriteria,
		WorkspacePath:         in.WorkspacePath,
		ThreadID:              in.ThreadID,
		Role:                  in.Role,
		DependsOnID:           in.DependsOnID,
		WorkLinkID:            in.WorkLinkID,
		CommentID:             in.CommentID,
		WorkAttachmentID:      in.WorkAttachmentID,
		AttachmentID:          in.AttachmentID,
		RunID:                 in.RunID,
		RunKind:               in.RunKind,
		Body:                  in.Body,
		BlockedReasonCode:     in.BlockedReasonCode,
		BlockedNote:           in.BlockedNote,
		Assignee:              in.Assignee,
		DueAt:                 in.DueAt,
		Labels:                in.Labels,
		AutomationMode:        in.AutomationMode,
		WorkflowID:            in.WorkflowID,
		WorkflowNodeID:        in.WorkflowNodeID,
		WorkflowName:          in.WorkflowName,
		WorkflowDescription:   in.WorkflowDescription,
		WorkflowDefinition:    in.WorkflowDefinition,
		ProjectContext:        in.ProjectContext,
		DeliveredRevision:     in.DeliveredRevision,
		AckedRevision:         in.AckedRevision,
		Fields:                append([]string(nil), in.Fields...),
		BootstrapID:           in.BootstrapID,
		CurrentState:          in.CurrentState,
		SessionIDs:            append([]string(nil), in.SessionIDs...),
		SelectedSuggestionIDs: append([]string(nil), in.SelectedSuggestionIDs...),

		Images: inboundImagesToBackend(in.Images),
		Files:  inboundFilesToBackend(in.Files),
	}
}

func inboundImagesToBackend(images []protocol.InboundImage) []backend.ImageAttachment {
	if len(images) == 0 {
		return nil
	}
	out := make([]backend.ImageAttachment, 0, len(images))
	for _, img := range images {
		out = append(out, backend.ImageAttachment{
			Data:      img.Data,
			MediaType: img.MediaType,
		})
	}
	return out
}

func inboundFilesToBackend(files []protocol.InboundFile) []backend.FileAttachment {
	if len(files) == 0 {
		return nil
	}
	out := make([]backend.FileAttachment, 0, len(files))
	for _, f := range files {
		out = append(out, backend.FileAttachment{
			Name:         f.Name,
			Content:      f.Content,
			MediaType:    f.MediaType,
			RemotePath:   f.RemotePath,
			AttachmentID: f.AttachmentID,
			SizeBytes:    f.SizeBytes,
		})
	}
	return out
}

type HelloInput struct {
	ClientID     string
	DeviceID     string
	DeviceName   string
	InstanceID   string
	Gen          string
	IsLocked     bool
	LockedToMe   bool
	PairingOpen  bool
	InstanceName string
	RootDir      string
	DataDir      string
	LanIP        string
	TunnelURL    string
	Backends     []backend.Definition
	Capabilities []string
}

func (AppV1) HelloAck(in HelloInput) protocol.HelloAck {
	return protocol.HelloAck{
		Type:            "hello_ack",
		ProtocolVersion: protocol.ProtocolVersion,
		ClientID:        in.ClientID,
		DeviceID:        in.DeviceID,
		DeviceName:      in.DeviceName,
		InstanceID:      in.InstanceID,
		Gen:             in.Gen,
		IsLocked:        in.IsLocked,
		LockedToMe:      in.LockedToMe,
		PairingOpen:     in.PairingOpen,
		InstanceName:    in.InstanceName,
		RootDir:         in.RootDir,
		DataDir:         in.DataDir,
		LanIP:           in.LanIP,
		TunnelURL:       in.TunnelURL,
		Backends:        backendDefinitionsToWire(in.Backends),
		Capabilities:    append([]string(nil), in.Capabilities...),
	}
}

func (AppV1) UsageReport(rep backend.UsageReport) protocol.UsageReport {
	out := protocol.NewUsageReport(
		usageWindowToWire(rep.FiveHour),
		usageWindowToWire(rep.SevenDay),
		usageWindowToWire(rep.SevenDaySonnet),
	)
	out.BackendID, out.Source, out.CollectedAt = rep.BackendID, rep.Source, rep.CollectedAt
	return out
}

func usageWindowToWire(w *backend.UsageWindow) *protocol.UsageWindow {
	if w == nil {
		return nil
	}
	return &protocol.UsageWindow{
		Utilization: w.Utilization,
		ResetsAt:    w.ResetsAt,
		Unit:        w.Unit, Label: w.Label, DurationMinutes: w.DurationMinutes,
	}
}

func backendDefinitionsToWire(defs []backend.Definition) []protocol.BackendDefinition {
	if len(defs) == 0 {
		return nil
	}
	out := make([]protocol.BackendDefinition, 0, len(defs))
	for _, d := range defs {
		models := make([]protocol.ModelDefinition, 0, len(d.Models))
		for _, m := range d.Models {
			tiers := make([]protocol.ServiceTierDefinition, 0, len(m.ServiceTiers))
			for _, tier := range m.ServiceTiers {
				tiers = append(tiers, protocol.ServiceTierDefinition{ID: tier.ID, Name: tier.Name, Description: tier.Description})
			}
			models = append(models, protocol.ModelDefinition{
				ID: m.ID, Label: m.Label, Description: m.Description,
				SupportedReasoningEfforts: m.SupportedReasoningEfforts,
				DefaultReasoningEffort:    m.DefaultReasoningEffort,
				InputModalities:           m.InputModalities, SupportsPersonality: m.SupportsPersonality,
				ServiceTiers: tiers, DefaultServiceTier: m.DefaultServiceTier, IsDefault: m.IsDefault,
			})
		}
		modes := make([]protocol.CollaborationModeDefinition, 0, len(d.CollaborationModes))
		for _, mode := range d.CollaborationModes {
			modes = append(modes, protocol.CollaborationModeDefinition{Name: mode.Name, Mode: mode.Mode, Model: mode.Model, ReasoningEffort: mode.ReasoningEffort})
		}
		out = append(out, protocol.BackendDefinition{
			ID:                 d.ID,
			Label:              d.Label,
			DefaultModel:       d.DefaultModel,
			Models:             models,
			CollaborationModes: modes,
			Capabilities: protocol.BackendCapabilities{
				History:      d.Capabilities.History,
				Usage:        d.Capabilities.Usage,
				Interactions: d.Capabilities.Interactions,
				Sandbox:      d.Capabilities.Sandbox,
				Images:       d.Capabilities.Images,
				Files:        d.Capabilities.Files,
				Remote:       d.Capabilities.Remote,
				Steering:     d.Capabilities.Steering,
			},
		})
	}
	return out
}

func (AppV1) BackendRegistryUpdated(defs []backend.Definition) protocol.BackendRegistryUpdated {
	return protocol.NewBackendRegistryUpdated(backendDefinitionsToWire(defs))
}

func (AppV1) SessionsList(sessions []protocol.SessionSummary) protocol.SessionsList {
	return protocol.NewSessionsList(sessions)
}

func (AppV1) Pong() protocol.Pong {
	return protocol.NewPong()
}

func (AppV1) Error(sessionID, code, message string) protocol.Error {
	return protocol.NewError(sessionID, code, message)
}

func (AppV1) ClaimAck() protocol.ClaimAck {
	return protocol.NewClaimAck()
}

func (AppV1) UnclaimAck(isLocked bool) protocol.UnclaimAck {
	return protocol.NewUnclaimAck(isLocked)
}

func (AppV1) PairingWindowAck(expiresAt int64) protocol.PairingWindowAck {
	return protocol.NewPairingWindowAck(expiresAt)
}

func (AppV1) SessionClosed(sessionID string) protocol.SessionClosed {
	return protocol.NewSessionClosed(sessionID)
}

func (AppV1) SessionRenamed(sessionID, name, authorityInstanceID, mutationID string, revision uint64, updatedAt int64, updatedBy string) protocol.SessionRenamed {
	return protocol.NewSessionRenamed(sessionID, name, authorityInstanceID, mutationID, revision, updatedAt, updatedBy)
}

func (AppV1) SessionRenameRejected(sessionID, authorityInstanceID, mutationID, reason, currentName string, currentRevision uint64) protocol.SessionRenameRejected {
	return protocol.NewSessionRenameRejected(sessionID, authorityInstanceID, mutationID, reason, currentName, currentRevision)
}

func (AppV1) SessionMetaUpdated(sessionID string, pinned, hidden *bool) protocol.SessionMetaUpdated {
	return protocol.SessionMetaUpdated{
		Type:      "session_meta_updated",
		SessionID: sessionID,
		Pinned:    pinned,
		Hidden:    hidden,
	}
}

func (AppV1) SessionConfigResult(sessionID, mutationID string, accepted bool, reason string, snap session.Snapshot) protocol.SessionConfigResult {
	sandbox := snap.Sandbox
	if sandbox == "" {
		sandbox = "danger-full-access"
	}
	result := protocol.NewSessionConfigResult(
		sessionID, mutationID, accepted, reason,
		snap.Backend, snap.Model, snap.Effort, sandbox,
		snap.ServiceTier, snap.CollaborationMode, snap.Personality,
	)
	result.ConfigRevision = snap.ConfigRevision
	return result
}

func (AppV1) ForkError(sessionID, reason string) protocol.ForkError {
	return protocol.NewForkError(sessionID, reason)
}

func (AppV1) SessionForked(sessionID, parentID, name string, createdAt float64) protocol.SessionForked {
	return protocol.NewSessionForked(sessionID, parentID, name, createdAt)
}

func (AppV1) HistorySnapshot(sessionID string, messages []map[string]any, sourceCount int, hasMoreBefore, knownIDFound bool, reason string) protocol.HistorySnapshot {
	return protocol.HistorySnapshot{
		Type:           "history_snapshot",
		SessionID:      sessionID,
		Messages:       messages,
		SourceCount:    sourceCount,
		HasMoreBefore:  hasMoreBefore,
		KnownIDFound:   knownIDFound,
		SnapshotReason: reason,
	}
}

func (AppV1) HistoryDelta(sessionID, afterSourceMessageID string, messages []map[string]any, sourceCount int) protocol.HistoryDelta {
	return protocol.HistoryDelta{
		Type:                 "history_delta",
		SessionID:            sessionID,
		AfterSourceMessageID: afterSourceMessageID,
		Messages:             messages,
		SourceCount:          sourceCount,
	}
}

func (AppV1) ResumableSessions(sessions any) protocol.ResumableSessions {
	return protocol.NewResumableSessions(sessions)
}

func (AppV1) ShellCreated(shellID string) protocol.ShellCreated {
	return protocol.NewShellCreated(shellID)
}

func (AppV1) TasksList(tasks []protocol.Task) protocol.TasksList {
	return protocol.NewTasksList(tasks)
}

func (AppV1) TaskKilled(id string, ok bool) protocol.TaskKilled {
	return protocol.NewTaskKilled(id, ok)
}

func (AppV1) ProcessesList(procs []protocol.Process) protocol.ProcessesList {
	return protocol.NewProcessesList(procs)
}

func (AppV1) ProcessKilled(pid int, ok bool, message string) protocol.ProcessKilled {
	return protocol.NewProcessKilled(pid, ok, message)
}

func (AppV1) GitDiffResult(sessionID, diff, errCode string, initialized bool) protocol.GitDiffResult {
	return protocol.NewGitDiffResult(sessionID, diff, errCode, initialized)
}

func (AppV1) InstancesList() protocol.InstancesList {
	return protocol.NewInstancesList()
}

func (AppV1) InboxListItems(items []protocol.InboxItem) protocol.InboxList {
	return protocol.NewInboxListItems(items)
}

func (AppV1) FeedList(items any) protocol.FeedList {
	return protocol.NewFeedList(items)
}

func (AppV1) FeedAck(feedID string) protocol.FeedAck {
	return protocol.NewFeedAck(feedID)
}

func (AppV1) FeedNew(item any) protocol.FeedNew {
	return protocol.NewFeedNew(item)
}

func (AppV1) FeedDetail(feedID, html, contentType string) protocol.FeedDetail {
	return protocol.NewFeedDetail(feedID, html, contentType)
}

func (AppV1) FeedUpdated(feedID string, read, deleted bool) protocol.FeedUpdated {
	return protocol.NewFeedUpdated(feedID, read, deleted)
}

func (AppV1) ExternalEventSnapshot(snapshot eventinbox.Snapshot) protocol.ExternalEventSnapshot {
	return protocol.NewExternalEventSnapshot(snapshot)
}

func (AppV1) ExternalEventUpsert(item eventinbox.View) protocol.ExternalEventUpsert {
	return protocol.NewExternalEventUpsert(item)
}

func (AppV1) ExternalEventState(item eventinbox.View) protocol.ExternalEventState {
	return protocol.NewExternalEventState(item)
}

func (AppV1) PendingInteractionsList(items []backend.UserInputPayload) protocol.PendingInteractionsList {
	return protocol.NewPendingInteractionsList(items)
}

func (AppV1) WebRTCReady() protocol.WebRTCReady {
	return protocol.NewWebRTCReady()
}

func (AppV1) WebRTCAnswer(sdp string) protocol.WebRTCAnswer {
	return protocol.NewWebRTCAnswer(sdp)
}

type SessionCreatedInput struct {
	ID        string
	Name      string
	CreatedAt float64
	Cwd       string
	Backend   string
	Model     string
	Sandbox   string
}

type WebRTCOfferInput struct {
	SDP string
}

func (c Command) WebRTCOffer() WebRTCOfferInput {
	return WebRTCOfferInput{SDP: c.SDP}
}

type WebRTCICEInput struct {
	Candidate     string
	SDPMid        string
	SDPMLineIndex *int
}

func (c Command) WebRTCICE() WebRTCICEInput {
	return WebRTCICEInput{
		Candidate:     c.Candidate,
		SDPMid:        c.SDPMid,
		SDPMLineIndex: c.SDPMLineIndex,
	}
}

func (AppV1) SessionCreated(s SessionCreatedInput) protocol.SessionCreated {
	return protocol.SessionCreated{
		Type:      "session_created",
		SessionID: s.ID,
		Name:      s.Name,
		CreatedAt: s.CreatedAt,
		Cwd:       s.Cwd,
		Backend:   s.Backend,
		Model:     s.Model,
		Sandbox:   s.Sandbox,
	}
}
