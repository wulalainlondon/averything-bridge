package taskapi

// SupportedNativeProviderVersion is an exact transport qualification list.
// It does not establish account access, tool loading, caller identity or grants.
// Every invocation still passes its existing authority/lease/scope verifier.
func SupportedNativeProviderVersion(provider, version string) bool {
	switch provider {
	case "codex":
		return version == "0.160.0" || version == "0.162.1"
	case "claude":
		return version == "2.1.280" || version == "2.1.291" || version == "2.1.296"
	default:
		return false
	}
}
