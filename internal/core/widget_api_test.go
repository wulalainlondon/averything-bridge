package core

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWidgetReadGrantScopeRevocationAndCompletionLedger(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	if err := h.pairing.Claim("full-private-credential", "phone"); err != nil {
		t.Fatal(err)
	}
	call := func(method, path, authorization, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		r.Header.Set("Authorization", authorization)
		w := httptest.NewRecorder()
		h.ServeWidgetAPI(w, r)
		return w
	}
	if w := call("POST", "/api/widgets/v1/access", "", `{"deviceId":"phone"}`); w.Code != 401 {
		t.Fatalf("unauthed mint: %d", w.Code)
	}
	issued := call("POST", "/api/widgets/v1/access", "Bearer full-private-credential", `{"deviceId":"phone"}`)
	if issued.Code != 200 {
		t.Fatalf("mint: %d %s", issued.Code, issued.Body.String())
	}
	var access struct {
		ReadToken string `json:"readToken"`
	}
	_ = json.Unmarshal(issued.Body.Bytes(), &access)
	if access.ReadToken == "" || strings.Contains(issued.Body.String(), "full-private-credential") {
		t.Fatal("bad read credential")
	}
	request := httptest.NewRequest("GET", "/files", nil)
	request.Header.Set("Authorization", "Bearer "+access.ReadToken)
	if h.HTTPAuthorized(request) {
		t.Fatal("read grant authorized general API")
	}
	if w := call("POST", "/api/widgets/v1/access", "Bearer "+access.ReadToken, `{"deviceId":"phone"}`); w.Code != 401 {
		t.Fatal("read grant minted another credential")
	}
	if w := call("POST", "/api/widgets/v1/snapshot", "Widget "+access.ReadToken, ""); w.Code != 405 {
		t.Fatal("snapshot accepted mutation")
	}
	h.registry.Create("s1", "QA", t.TempDir(), "codex", "", "", "")
	h.runtimes.Update("s1", "completed", "E", 0, "completed", "")
	h.runtimes.Update("s1", "running", "F", 0, "", "")
	h.runtimes.Update("s1", "completed", "F", 0, "completed", "")
	response := call("GET", "/api/widgets/v1/snapshot", "Widget "+access.ReadToken, "")
	if response.Code != 200 || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("snapshot: %d", response.Code)
	}
	var snapshot struct {
		Completions []struct {
			RequestID string `json:"requestId"`
		} `json:"completions"`
		Bridges []struct {
			ID    string `json:"id"`
			Tasks []struct {
				RequestID string `json:"requestId"`
			} `json:"tasks"`
		} `json:"bridges"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Bridges) != 1 || snapshot.Bridges[0].ID != "i1" || snapshot.Bridges[0].Tasks[0].RequestID != "F" || len(snapshot.Completions) != 2 {
		t.Fatalf("lost request projection: %s", response.Body.String())
	}
	if err := h.pairing.Unclaim("full-private-credential"); err != nil {
		t.Fatal(err)
	}
	if w := call("GET", "/api/widgets/v1/snapshot", "Widget "+access.ReadToken, ""); w.Code != 401 {
		t.Fatal("revoked device still has widget access")
	}
}
