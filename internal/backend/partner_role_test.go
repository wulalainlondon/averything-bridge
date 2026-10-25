package backend

import (
	"context"
	"everything-go/internal/partners"
	"strings"
	"testing"
)

func TestPartnerRoleInOriginalProviderContent(t *testing.T) {
	p := partners.Profile{ID: "p", RoleRevision: 3, Description: "Use traditional Chinese"}
	ctx := WithPartnerRole(WithVisualPresentation(context.Background(), "auto-v1"), p.Role())
	effective := PresentationContent(ctx, "current explicit request")
	if !strings.Contains(effective, "Use traditional Chinese") || !strings.Contains(effective, "Current user instructions take precedence") || !strings.HasSuffix(effective, "current explicit request") {
		t.Fatal("role context missing")
	}
	if OriginalPresentationContent(effective) != "current explicit request" {
		t.Fatal("role projection did not retain original user content")
	}
	if PresentationContent(ctx, "/compact") != "/compact" {
		t.Fatal("command was modified")
	}
	if PresentationContent(context.Background(), "original") != "original" {
		t.Fatal("ordinary content changed")
	}
}
