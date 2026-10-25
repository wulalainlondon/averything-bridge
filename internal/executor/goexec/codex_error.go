package goexec

import (
	"encoding/json"
	"everything-go/internal/recovery"
)

// Preserve the upstream classification; older histories only retain text.
func codexErrorCode(info json.RawMessage, message string) string {
	return recovery.ClassifyCodex(info, message).ErrorCode()
}
