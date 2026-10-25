package core

import (
	"encoding/json"
	"net/http"

	observationcontract "everything-go/contracts/internal-agent-observation/v1"
	metadatacontract "everything-go/contracts/plugin-metadata/v1"
	taskcontract "everything-go/contracts/task-api/v1"
)

// This is the Hub process's read-only discovery projection. Native tool
// registration, provider metadata and a standalone process cannot prove a
// Claude/Codex plugin registry installation or host load.
func pluginMetadataStatus() map[string]any {
	return map[string]any{
		"type": "plugin_metadata_status", "schema": metadatacontract.Version,
		"host":           map[string]string{"installed": "unknown", "enabled": "unknown", "loaded": "unknown", "discovery": "unknown"},
		"process_loaded": true,
		"discovery": map[string]any{
			"scope":  "authenticated_hub_metadata_only",
			"http":   []string{"/api/plugin-metadata/v1/status", "/api/internal-agents/v1/observation"},
			"socket": []string{"request_plugin_metadata_status", "request_internal_agent_observation"},
		},
		"contracts": map[string]any{
			"task_api":    map[string]string{"version": taskcontract.Version, "sha256": taskcontract.Hash()},
			"observation": map[string]string{"version": observationcontract.Version, "sha256": observationcontract.Hash()},
		},
		"supported_mode": map[string]any{
			"provider_metadata":       "existing_codex_daemon_only",
			"coverage":                []string{"partial_provider_child_cache", "partial_selected_native_root"},
			"root_inventory_callback": "unsupported",
			"canonical_qa":            "unsupported_original_relation_required",
			"report_navigation":       "unsupported_original_owner_required",
			"business_auth":           "original_permissions_preserved",
		},
	}
}

func (h *Hub) ServePluginMetadataStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !h.HTTPAuthorized(r) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if r.URL.RawQuery != "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	_ = json.NewEncoder(w).Encode(pluginMetadataStatus())
}
