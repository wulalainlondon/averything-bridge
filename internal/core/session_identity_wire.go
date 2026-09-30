package core

import (
	"encoding/json"
	"strings"

	"everything-go/internal/identity"
	"everything-go/internal/protocol"
)

func scopeSessionIDsJSON(data []byte, authorityInstanceID string) ([]byte, error) {
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	scopeSessionValue(value, authorityInstanceID, "")
	return json.Marshal(value)
}

func scopeSessionValue(value any, authorityInstanceID, parentType string) bool {
	switch typed := value.(type) {
	case []any:
		changed := false
		for _, item := range typed {
			changed = scopeSessionValue(item, authorityInstanceID, parentType) || changed
		}
		return changed
	case map[string]any:
		typeName, _ := typed["type"].(string)
		changed := false
		if versionedPayload(typeName) {
			if typed["schema_version"] != float64(protocol.PayloadVersions[payloadDomain(typeName)]) {
				typed["schema_version"] = protocol.PayloadVersions[payloadDomain(typeName)]
				changed = true
			}
		}
		for key, item := range typed {
			if number, ok := item.(float64); ok && isLegacyTimestampField(key) {
				canonicalKey := key + "_unix_ms"
				if _, exists := typed[canonicalKey]; !exists {
					if number > -100_000_000_000 && number < 100_000_000_000 {
						number *= 1000
					}
					typed[canonicalKey] = number
					changed = true
				}
			}
			if text, ok := item.(string); ok && isSessionIDField(key) && text != "" {
				if scoped, err := identity.MakeSessionKey(authorityInstanceID, identity.WireSessionID(text)); err == nil {
					typed[key] = scoped
					changed = true
				}
				continue
			}
			if key == "session_ids" {
				if list, ok := item.([]any); ok {
					for index, entry := range list {
						if text, ok := entry.(string); ok && text != "" {
							if scoped, err := identity.MakeSessionKey(authorityInstanceID, identity.WireSessionID(text)); err == nil {
								list[index] = scoped
								changed = true
							}
						}
					}
				}
				continue
			}
			if key == "id" && (parentType == "sessions_list" || parentType == "sessions_list_append") {
				if text, ok := item.(string); ok && text != "" {
					if scoped, err := identity.MakeSessionKey(authorityInstanceID, identity.WireSessionID(text)); err == nil {
						typed[key] = scoped
						changed = true
					}
				}
				continue
			}
			childParent := ""
			if key == "sessions" && (typeName == "sessions_list" || typeName == "sessions_list_append") {
				childParent = typeName
			}
			changed = scopeSessionValue(item, authorityInstanceID, childParent) || changed
		}
		if changed && typeName != "" {
			if _, exists := typed["authority_instance_id"]; !exists {
				typed["authority_instance_id"] = authorityInstanceID
			}
		}
		return changed
	default:
		return false
	}
}

func payloadDomain(typeName string) string {
	switch {
	case strings.HasPrefix(typeName, "session_runtime"):
		return "runtime"
	case strings.HasPrefix(typeName, "work_"):
		return "work"
	case strings.HasPrefix(typeName, "external_event"):
		return "external_event"
	case typeName == "media" || typeName == "document" || strings.HasPrefix(typeName, "attachment_"):
		return "attachment"
	default:
		return ""
	}
}

func versionedPayload(typeName string) bool { return payloadDomain(typeName) != "" }

func isSessionIDField(key string) bool {
	return key == "session_id" || strings.HasSuffix(key, "_session_id")
}

func isLegacyTimestampField(key string) bool {
	return !strings.HasSuffix(key, "_unix_ms") &&
		(strings.HasSuffix(key, "_at") || key == "timestamp" || key == "last_activity" || key == "last_used")
}
