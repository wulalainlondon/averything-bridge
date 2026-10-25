package core

import (
	"net/http/httptest"
	"testing"
)

func TestInboxDownloadOriginUsesReachableTransport(t *testing.T) {
	for _, tc := range []struct{ target, remote, forwarded, want string }{
		{"http://100.77.166.27:8766", "100.70.64.26:50888", "", "http://100.77.166.27:8766"},
		{"http://[fd7a::1234]:8766", "[fd7a::2]:50888", "", "http://[fd7a::1234]:8766"},
		{"https://bridge.example.test", "192.0.2.1:3456", "", "https://bridge.example.test"},
		{"http://bridge.trycloudflare.com", "127.0.0.1:3456", "https", "https://bridge.trycloudflare.com"},
		{"http://bridge.trycloudflare.com", "[::1]:3456", "https", "https://bridge.trycloudflare.com"},
		{"http://100.77.166.27:8766", "192.0.2.1:3456", "https", "http://100.77.166.27:8766"},
	} {
		r := httptest.NewRequest("GET", tc.target, nil)
		r.RemoteAddr = tc.remote
		r.Header.Set("X-Forwarded-Proto", tc.forwarded)
		if got := requestHTTPOrigin(r); got != tc.want {
			t.Fatalf("origin=%s want=%s", got, tc.want)
		}
	}
}
