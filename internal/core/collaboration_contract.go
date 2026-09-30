package core

import "everything-go/internal/coordination"

// CollaborationSchemas derives the public wire and input schemas from the same
// Go types used by handlers and tool registration. Private storage fields never
// appear in the public snapshot schema.
func CollaborationSchemas() map[string]any {
	command := coordination.StructSchema(coordination.CollaborationCommand{})
	props := command["properties"].(map[string]any)
	props["action"].(map[string]any)["enum"] = []string{"snapshot", "read_source", "create", "upgrade", "approve", "dispatch", "take_over", "prepare_handback", "return_to_pm", "accept", "rework", "reopen", "continue", "pause", "resume", "cancel", "human_answer", "human_answer_continue", "dispose", "decision", "approve_assistance", "adopt_assistance", "approve_rule", "revoke_rule"}
	command["required"] = []string{"action", "mutation_id", "authority_instance_id"}
	snapshot := coordination.StructSchema(collaborationEvent{})
	sp := snapshot["properties"].(map[string]any)
	sp["type"] = map[string]any{"type": "string", "const": "human_ai_collaboration_snapshot"}
	data := sp["collaboration"].(map[string]any)["properties"].(map[string]any)
	delete(data, "receipts")
	artifacts := data["artifacts"].(map[string]any)["additionalProperties"].(map[string]any)["properties"].(map[string]any)
	for _, name := range []string{"locator", "source_path", "source_root", "content"} {
		delete(artifacts, name)
	}
	addCollaborationTimestampAliases(snapshot)
	return map[string]any{"$schema": "https://json-schema.org/draft/2020-12/schema", "title": "Bridge human and AI collaboration v2", "x-capability": coordination.CollaborationCapability, "$defs": map[string]any{"command": command, "snapshot": snapshot, "source": coordination.StructSchema(coordination.SourceRef{}), "report": coordination.StructSchema(coordination.ReportInput{}), "coverage": coordination.StructSchema(coordination.Coverage{})}}
}

func addCollaborationTimestampAliases(schema map[string]any) {
	if props, ok := schema["properties"].(map[string]any); ok {
		for _, key := range []string{"created_at", "updated_at", "started_at", "finished_at"} {
			if _, ok := props[key]; ok {
				props[key+"_unix_ms"] = map[string]any{"type": "integer"}
			}
		}
		for _, value := range props {
			if child, ok := value.(map[string]any); ok {
				addCollaborationTimestampAliases(child)
			}
		}
	}
	for _, key := range []string{"items", "additionalProperties"} {
		if child, ok := schema[key].(map[string]any); ok {
			addCollaborationTimestampAliases(child)
		}
	}
}
