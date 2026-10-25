package goexec

import (
	"context"
	"everything-go/internal/taskapi"
)

type ClaudeBackgroundTaskAPIVerifier struct{ Version string }

func (v ClaudeBackgroundTaskAPIVerifier) Verify(context.Context, taskapi.Invocation) (taskapi.VerifiedContext, error) {
	return taskapi.VerifiedContext{}, taskapi.Failure("unsupported", "known_none", "read_capabilities")
}
func ClaudeBackgroundTaskAPIInterfaceCapability(version string) taskapi.ProviderCapability {
	return taskapi.UnloadedCapability("claude_background", version, "No verified public BG run/session/native consumption/final and invocation binding; Codex RPC is not a substitute")
}
