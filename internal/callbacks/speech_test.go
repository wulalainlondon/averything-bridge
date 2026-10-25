package callbacks

import (
	"strings"
	"testing"
)

func TestSpeechIsBoundedSealedPresentationWithoutInstructionsOrURLs(t *testing.T) {
	out := Speech("task", "review_ready", "# 完成\n已修復 [報告](https://example.invalid/report)\n```sh\nrm -rf example\n```\n"+strings.Repeat("結果", 2000))
	if !strings.Contains(out, "等你驗收") || strings.Contains(out, "https://") || strings.Contains(out, "rm -rf") || len([]rune(out)) > 1500 {
		t.Fatal(out)
	}
}

func TestSpokenVerificationDoesNotEraseNumericMeaning(t *testing.T) {
	for _, pair := range [][2]string{{"1.5", "15"}, {"-2", "2"}, {"10%", "10"}, {"1/2", "12"}, {"$20", "20"}} {
		if SpokenKey(pair[0]) == SpokenKey(pair[1]) {
			t.Fatal("numeric meaning erased", pair)
		}
	}
	if SpokenKey("S10，測試。") != SpokenKey("s10測試") {
		t.Fatal("sentence formatting changed content")
	}
}
