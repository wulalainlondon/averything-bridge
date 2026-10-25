package linebridge

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"regexp"
)

// VoiceDispatchAccount is explicit operator intent in the original private Go
// configuration. It is never accepted as a model/HTTP request and grants no
// account, pairing or sharing administration. Binding changes retire the policy.
type VoiceDispatchAccount struct {
	Account string `json:"account_ref"`
	Binding uint64 `json:"binding_revision"`
}

func (v *VoiceDispatchAccount) UnmarshalJSON(raw []byte) error {
	scan := json.NewDecoder(bytes.NewReader(raw))
	token, err := scan.Token()
	if err != nil || token != json.Delim('{') {
		return Fail("line_voice_dispatch_configuration_invalid")
	}
	fields := map[string]bool{}
	for scan.More() {
		token, err = scan.Token()
		key, ok := token.(string)
		if err != nil || !ok || fields[key] {
			return Fail("line_voice_dispatch_configuration_invalid")
		}
		fields[key] = true
		var value json.RawMessage
		if scan.Decode(&value) != nil {
			return Fail("line_voice_dispatch_configuration_invalid")
		}
	}
	type plain VoiceDispatchAccount
	var out plain
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&out) != nil || d.Decode(new(any)) != io.EOF {
		return Fail("line_voice_dispatch_configuration_invalid")
	}
	*v = VoiceDispatchAccount(out)
	return nil
}

var voiceDispatchAccountRef = regexp.MustCompile(`^ln_[a-f0-9]{32}$`)

func ValidateVoiceDispatchAccounts(items []VoiceDispatchAccount) error {
	if len(items) > 16 {
		return Fail("line_voice_dispatch_configuration_invalid")
	}
	seen := map[string]bool{}
	for _, item := range items {
		if !voiceDispatchAccountRef.MatchString(item.Account) || item.Binding == 0 || seen[item.Account] {
			return Fail("line_voice_dispatch_configuration_invalid")
		}
		seen[item.Account] = true
	}
	return nil
}

type VoiceDispatchAccess struct {
	Enabled bool     `json:"enabled"`
	Scopes  []string `json:"scopes"`
}

func (s *Service) voiceDispatchAllowed(ctx context.Context, c Caller, id, scope string) bool {
	if s.config.SubjectAllowed == nil || !s.config.SubjectAllowed(c.Subject) || s.config.VoiceDispatchSubjectAllowed == nil || !s.config.VoiceDispatchSubjectAllowed(c.Subject) || s.config.BlockedScopes[scope] {
		return false
	}
	valid := false
	for _, allowed := range scopes {
		if scope == allowed {
			valid = true
			break
		}
	}
	if !valid {
		return false
	}
	for _, item := range s.config.VoiceDispatchAccounts {
		if item.Account != id || item.Binding == 0 {
			continue
		}
		a, err := s.store.Account(ctx, id)
		return err == nil && a.Enabled && a.Principal != "" && a.Revision == item.Binding
	}
	return false
}
func (s *Service) voiceDispatchAccess(ctx context.Context, c Caller, id string) VoiceDispatchAccess {
	out := VoiceDispatchAccess{Scopes: []string{}}
	for _, scope := range scopes {
		if s.voiceDispatchAllowed(ctx, c, id, scope) {
			out.Scopes = append(out.Scopes, scope)
		}
	}
	out.Enabled = len(out.Scopes) > 0
	return out
}
