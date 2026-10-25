package taskapi

import "testing"

func TestNativeProviderVersionExactQualification(t *testing.T) {
	for _, pair := range [][2]string{{"codex", "0.160.0"}, {"codex", "0.162.1"}, {"claude", "2.1.280"}, {"claude", "2.1.291"}, {"claude", "2.1.296"}} {
		if !SupportedNativeProviderVersion(pair[0], pair[1]) {
			t.Fatalf("qualified transport rejected: %v", pair)
		}
	}
	for _, pair := range [][2]string{{"codex", ""}, {"codex", "unknown"}, {"codex", "0.162.0"}, {"codex", "0.163.0"}, {"codex", "0.162.1-unverified"}, {"claude", "2.1.295"}, {"claude", "2.1.297"}, {"ollama", "0.162.1"}} {
		if SupportedNativeProviderVersion(pair[0], pair[1]) {
			t.Fatalf("unqualified transport accepted: %v", pair)
		}
	}
}
