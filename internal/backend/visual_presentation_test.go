package backend

import (
	"context"
	"strings"
	"testing"
)

func TestVisualPresentationExactOriginalAndControl(t *testing.T) {
	raw := "測 e\u0301\n original"
	ctx := WithVisualPresentation(context.Background(), "auto-v1")
	if PresentationContent(context.Background(), raw) != raw {
		t.Fatal("legacy changed")
	}
	if !strings.HasSuffix(PresentationContent(ctx, raw), raw) {
		t.Fatal("original changed")
	}
	if !strings.Contains(PresentationContent(ctx, raw), "conversation-visual.v1") {
		t.Fatal("missing format")
	}
	if PresentationContent(ctx, "/compact") != "/compact" {
		t.Fatal("control changed")
	}
	a, b := PresentationHashes(ctx, raw)
	if a == b || len(a) != 64 || len(b) != 64 {
		t.Fatal("invalid audit hashes")
	}
	if ValidVisualPresentation("arbitrary") || !ValidVisualPresentation("text-v1") {
		t.Fatal("invalid modes")
	}
	if !strings.Contains(PresentationContent(WithVisualPresentation(context.Background(), "text-v1"), raw), "Use a visual block ONLY when the user explicitly requests") {
		t.Fatal("text mode ignored")
	}
}
