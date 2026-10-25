// Package recovery separates error diagnosis from permission to replay input.
// Nothing in this package dispatches work or restarts an executor.
package recovery

import (
	"encoding/json"
	"strings"
)

type Category string

const (
	Unknown                Category = "unknown"
	ModelCapacity          Category = "model_capacity"
	TemporaryRateLimit     Category = "temporary_rate_limit"
	UsageExhausted         Category = "usage_exhausted"
	TransportUncertain     Category = "transport_uncertain"
	AuthenticationRequired Category = "authentication_required"
	PermissionRevoked      Category = "application_permission_revoked"
	PolicyBlocked          Category = "policy_blocked"
	OwnershipConflict      Category = "ownership_conflict"
	ConfigurationError     Category = "tool_or_configuration_error"
)

// Failure contains only allowlisted diagnostic values, never provider messages,
// credentials, input, additionalDetails, or arbitrary error data.
type Failure struct {
	Category     Category `json:"category"`
	Source       string   `json:"source"`
	UpstreamCode string   `json:"upstream_code,omitempty"`
	HTTPStatus   int      `json:"http_status,omitempty"`
}

func normalizeCode(code string) string {
	return strings.ToLower(strings.ReplaceAll(code, "_", ""))
}

func categoryForCode(code string) (Category, bool) {
	switch code {
	case "serveroverloaded", "flexunavailable":
		return ModelCapacity, true
	case "ratelimitexceeded":
		return TemporaryRateLimit, true
	case "usagelimitexceeded", "sessionbudgetexceeded":
		return UsageExhausted, true
	case "unauthorized":
		return AuthenticationRequired, true
	case "misalignmentpolicyviolation", "cyberpolicy", "toomanydenials":
		return PolicyBlocked, true
	case "contextwindowexceeded", "badrequest", "sandboxerror", "threadrollbackfailed":
		return ConfigurationError, true
	case "activeturnnotsteerable":
		return OwnershipConflict, true
	case "httpconnectionfailed", "responsestreamconnectionfailed", "responsestreamdisconnected", "responsetoomanyfailedattempts":
		return TransportUncertain, true
	case "internalservererror", "other":
		return Unknown, true
	default:
		return Unknown, false
	}
}

// ClassifyCodex supports the string and externally tagged object variants of
// CodexErrorInfo. HTTP 429 alone cannot distinguish rate limits from quota.
// Text fallbacks are diagnostic evidence only; they never establish rejection.
func ClassifyCodex(info json.RawMessage, message string) Failure {
	f := Failure{Category: Unknown, Source: "unknown"}
	var code string
	if json.Unmarshal(info, &code) != nil {
		var variants map[string]json.RawMessage
		if json.Unmarshal(info, &variants) == nil && len(variants) == 1 {
			for key, value := range variants {
				switch normalizeCode(key) {
				case "httpconnectionfailed", "responsestreamconnectionfailed", "responsestreamdisconnected", "responsetoomanyfailedattempts", "activeturnnotsteerable":
					code = key
				default:
					continue
				}
				var detail struct {
					HTTPStatus *int `json:"httpStatusCode"`
				}
				if json.Unmarshal(value, &detail) == nil && detail.HTTPStatus != nil && *detail.HTTPStatus >= 100 && *detail.HTTPStatus <= 599 {
					f.HTTPStatus = *detail.HTTPStatus
				}
			}
		}
	}
	if category, known := categoryForCode(normalizeCode(code)); known {
		f.Category, f.Source, f.UpstreamCode = category, "codex_error_info", normalizeCode(code)
		if f.HTTPStatus == 401 {
			f.Category = AuthenticationRequired
		}
	} else {
		// Unknown variant payloads are neither persisted nor trusted for policy.
		f.HTTPStatus = 0
	}
	text := strings.ToLower(strings.TrimSpace(message))
	// Explicit permission/policy denials take precedence over transient hints.
	if text == "fatal error: application network permission was revoked" || text == "application network permission was revoked" {
		f.Category, f.Source = PermissionRevoked, "known_message"
	} else if f.Category == PolicyBlocked {
		return f
	} else if strings.Contains(text, "blocked by our safety systems") {
		f.Category, f.Source = PolicyBlocked, "known_message"
	} else if f.Category == Unknown && strings.Contains(text, "selected model is at capacity") {
		f.Category, f.Source = ModelCapacity, "known_message"
	}
	return f
}

// ClassifyIngress recognizes only the documented app-server ingress rejection.
// An HTTP/server failure occurring after turn acceptance is a different event.
func ClassifyIngress(code int, message string) (Failure, bool) {
	if code == -32001 && message == "Server overloaded; retry later." {
		return Failure{Category: ModelCapacity, Source: "codex_ingress", UpstreamCode: "ingress_overloaded"}, true
	}
	return Failure{Category: Unknown, Source: "jsonrpc_error"}, false
}

// ErrorCode preserves the existing policy code used by older clients.
func (f Failure) ErrorCode() string {
	if f.Category == PolicyBlocked {
		return "misalignment_policy_violation"
	}
	if f.Category == Unknown || f.Category == "" {
		return "turn_error"
	}
	return string(f.Category)
}

// PublicMessage avoids leaking upstream diagnostics for known classifications.
// Unknown errors retain the caller's existing error presentation contract.
func (f Failure) PublicMessage(fallback string) string {
	switch f.Category {
	case ModelCapacity:
		return "模型服務目前滿載，此次工作未能完成。"
	case TemporaryRateLimit:
		return "模型服務暫時受到速率限制，此次工作未能完成。"
	case UsageExhausted:
		return "模型帳號或工作額度已用完，需等待重置或處理方案。"
	case AuthenticationRequired:
		return "模型服務需要重新登入或確認授權。"
	case PermissionRevoked:
		return "執行端的應用程式網路權限已撤銷，需先恢復權限；Bridge 不會自動重送。"
	case PolicyBlocked:
		return "此請求已被上游政策阻擋。"
	case OwnershipConflict:
		return "原生對話的控制權或執行狀態不允許此操作。"
	case TransportUncertain:
		return "模型連線中斷，原工作結果尚待核對；Bridge 不會自動重送。"
	default:
		return fallback
	}
}
