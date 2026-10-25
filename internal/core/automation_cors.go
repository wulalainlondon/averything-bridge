package core

import (
	"net/http"
	"strings"
)

// Native WebViews have a local application origin while their paired Bridge
// lives on LAN/Tailscale. CORS only makes the existing authenticated API
// readable to those application origins; it does not authorize the request.
func automationAppCORS(w http.ResponseWriter, r *http.Request) bool {
	w.Header().Add("Vary", "Origin")
	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
	}
	allowed := false
	switch origin {
	case "http://localhost", "https://localhost", "capacitor://localhost",
		"tauri://localhost", "http://tauri.localhost", "https://tauri.localhost":
		allowed = true
	}
	if !allowed {
		if r.Method == http.MethodOptions {
			http.Error(w, "application origin not allowed", http.StatusForbidden)
			return true
		}
		// Preserve existing non-CORS clients and their normal authorization.
		return false
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	if r.Method != http.MethodOptions {
		return false
	}
	w.Header().Add("Vary", "Access-Control-Request-Method")
	w.Header().Add("Vary", "Access-Control-Request-Headers")
	method := r.Header.Get("Access-Control-Request-Method")
	if method != http.MethodGet && method != http.MethodPost {
		http.Error(w, "preflight method not allowed", http.StatusForbidden)
		return true
	}
	for _, header := range strings.Split(r.Header.Get("Access-Control-Request-Headers"), ",") {
		switch strings.ToLower(strings.TrimSpace(header)) {
		case "", "authorization", "content-type", "x-bridge-device-id":
		default:
			http.Error(w, "preflight header not allowed", http.StatusForbidden)
			return true
		}
	}
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Bridge-Device-ID")
	w.WriteHeader(http.StatusNoContent)
	return true
}
