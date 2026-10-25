package linebridge

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

type HTTPAuth func(*http.Request) (Caller, bool)

func Handler(service *Service, authorize HTTPAuth) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		// Token-only API: no ambient cookies or credentialed CORS.
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Bridge-Device-ID")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		caller, ok := authorize(r)
		if !ok || caller.Subject == "" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if service == nil {
			http.Error(w, "LINE service unavailable", http.StatusServiceUnavailable)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		bytes, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 65536))
		if err != nil {
			http.Error(w, "request limit", http.StatusRequestEntityTooLarge)
			return
		}
		request, err := DecodeRequest(bytes)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "code": SafeCode(err)})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
		defer cancel()
		result, err := service.Execute(ctx, caller, request)
		if err != nil {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "code": SafeCode(err)})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": result})
	})
}
