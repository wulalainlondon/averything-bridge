package goexec

import (
	"encoding/json"
	"everything-go/internal/backend"
	"strings"
)

// Preserve the upstream classification; older histories only retain text.
func codexErrorCode(info json.RawMessage, message string) string {
	var code string
	_ = json.Unmarshal(info, &code)
	if code == "misalignment_policy_violation" || code == "misalignmentPolicyViolation" ||
		strings.Contains(strings.ToLower(message), "blocked by our safety systems") {
		return "misalignment_policy_violation"
	}
	return backend.ErrTurn
}
