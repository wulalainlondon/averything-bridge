package recovery

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCodexClassificationAndSanitization(t *testing.T) {
	for _, tc := range []struct {
		name, info, message string
		category            Category
		source              string
		http                int
	}{
		{"camel_capacity", `"serverOverloaded"`, "provider secret", ModelCapacity, "codex_error_info", 0},
		{"snake_capacity", `"server_overloaded"`, "provider secret", ModelCapacity, "codex_error_info", 0},
		{"temporary_rate", `"rateLimitExceeded"`, "", TemporaryRateLimit, "codex_error_info", 0},
		{"usage_is_not_rate", `"usageLimitExceeded"`, "", UsageExhausted, "codex_error_info", 0},
		{"budget", `"sessionBudgetExceeded"`, "", UsageExhausted, "codex_error_info", 0},
		{"permission_incident", `"other"`, "Fatal error: application network permission was revoked", PermissionRevoked, "known_message", 0},
		{"permission_overrides_capacity", `"serverOverloaded"`, "Fatal error: application network permission was revoked", PermissionRevoked, "known_message", 0},
		{"auth", `"unauthorized"`, "", AuthenticationRequired, "codex_error_info", 0},
		{"safety", `"misalignment_policy_violation"`, "", PolicyBlocked, "codex_error_info", 0},
		{"cyber", `"cyberPolicy"`, "", PolicyBlocked, "codex_error_info", 0},
		{"bad_request", `"badRequest"`, "", ConfigurationError, "codex_error_info", 0},
		{"ownership", `{"activeTurnNotSteerable":{"turnKind":"review"}}`, "", OwnershipConflict, "codex_error_info", 0},
		{"object_503_not_capacity", `{"httpConnectionFailed":{"httpStatusCode":503}}`, "", TransportUncertain, "codex_error_info", 503},
		{"object_429_not_rate", `{"responseStreamConnectionFailed":{"httpStatusCode":429}}`, "", TransportUncertain, "codex_error_info", 429},
		{"object_auth", `{"responseStreamDisconnected":{"httpStatusCode":401}}`, "", AuthenticationRequired, "codex_error_info", 401},
		{"nullable_http", `{"responseTooManyFailedAttempts":{"httpStatusCode":null}}`, "", TransportUncertain, "codex_error_info", 0},
		{"invalid_http", `{"httpConnectionFailed":{"httpStatusCode":999}}`, "", TransportUncertain, "codex_error_info", 0},
		{"unknown_variant", `{"secret_variant":{"httpStatusCode":503,"token":"provider secret"}}`, "timeout HTTP 429 503", Unknown, "unknown", 0},
		{"malformed", `{`, "timeout HTTP 429 503", Unknown, "unknown", 0},
		{"multiple_variants", `{"serverOverloaded":{},"unauthorized":{}}`, "", Unknown, "unknown", 0},
		{"non_object_variant", `{"serverOverloaded":{}}`, "", Unknown, "unknown", 0},
		{"text_diagnosis", `"other"`, "The selected model is at capacity; provider secret", ModelCapacity, "known_message", 0},
		{"policy_text", `null`, "This request was blocked by our safety systems.", PolicyBlocked, "known_message", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := ClassifyCodex(json.RawMessage(tc.info), tc.message)
			if f.Category != tc.category || f.Source != tc.source || f.HTTPStatus != tc.http {
				t.Fatalf("classification: %+v", f)
			}
			b, _ := json.Marshal(f)
			if strings.Contains(string(b), "provider secret") || strings.Contains(string(b), "secret_variant") {
				t.Fatal("untrusted diagnostic data persisted", string(b))
			}
			if f.Category != Unknown && f.Category != ConfigurationError && strings.Contains(f.PublicMessage(tc.message), "provider secret") {
				t.Fatal("raw provider message exposed")
			}
		})
	}
}

func TestIngressProofRequiresExactDocumentedContract(t *testing.T) {
	for _, tc := range []struct {
		code     int
		message  string
		rejected bool
	}{
		{-32001, "Server overloaded; retry later.", true},
		{-32001, "Unknown method", false},
		{-32000, "Server overloaded; retry later.", false},
		{503, "Server overloaded; retry later.", false},
	} {
		f, rejected := ClassifyIngress(tc.code, tc.message)
		if rejected != tc.rejected || (rejected && f.Category != ModelCapacity) {
			t.Fatalf("wrong rejection evidence: %+v %v", f, rejected)
		}
	}
}
