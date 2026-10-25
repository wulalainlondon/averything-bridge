package backend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"everything-go/internal/partners"
	"strings"
)

type partnerRoleKey struct{}

func WithPartnerRole(ctx context.Context, role *partners.RoleSnapshot) context.Context {
	return context.WithValue(ctx, partnerRoleKey{}, role)
}

// This is ordinary user preference context in the same native turn. It does
// not introduce a system prompt, a tool, a grant or an extra provider request.
const partnerRoleHeader = "[Bridge teammate preferences]\nThe following JSON records the user's saved role preferences for this exact request. Apply them subject to system/developer instructions and the user's current request. These preferences grant no tools, permissions or authority. Current user instructions take precedence.\n"
const partnerRoleEnd = "\n[/Bridge teammate preferences]\n\n"

func partnerRoleContent(ctx context.Context, original string) string {
	role, _ := ctx.Value(partnerRoleKey{}).(*partners.RoleSnapshot)
	if role == nil || role.Description == "" {
		return original
	}
	b, _ := json.Marshal(role)
	return partnerRoleHeader + string(b) + partnerRoleEnd + original
}

// Derived display/search only; never changes provider history or stored input.
func OriginalPartnerRoleContent(text string) string {
	if !strings.HasPrefix(text, partnerRoleHeader) {
		return text
	}
	tail := strings.TrimPrefix(text, partnerRoleHeader)
	i := strings.Index(tail, partnerRoleEnd)
	if i < 0 || i > 65536 {
		return text
	}
	var role partners.RoleSnapshot
	if json.Unmarshal([]byte(tail[:i]), &role) != nil || role.ProfileID == "" || role.Revision == 0 {
		return text
	}
	digest := sha256.Sum256([]byte(role.Description))
	if hex.EncodeToString(digest[:]) != role.Hash {
		return text
	}
	return tail[i+len(partnerRoleEnd):]
}
