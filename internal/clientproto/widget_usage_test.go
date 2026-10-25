package clientproto

import (
	"everything-go/internal/backend"
	"testing"
)

func TestUsageCarriesExplicitIdentityAndUnitForWidgets(t *testing.T) {
	used := 0.25
	report := backend.UsageReport{BackendID: "claude", Source: "provider_quota", CollectedAt: 1234567890000, FiveHour: &backend.UsageWindow{Utilization: &used, Unit: "fraction", Label: "5 hours", DurationMinutes: 300}}
	wire := (AppV1{}).UsageReport(report)
	if wire.BackendID != "claude" || wire.Source != "provider_quota" || wire.CollectedAt != report.CollectedAt {
		t.Fatal("usage identity dropped")
	}
	if wire.FiveHour.Unit != "fraction" || wire.FiveHour.DurationMinutes != 300 {
		t.Fatal("usage unit dropped")
	}
	legacy := (AppV1{}).UsageReport(backend.NewUsageReport(nil, nil, nil))
	if legacy.BackendID != "" || legacy.Source != "" {
		t.Fatal("legacy identity must not be guessed")
	}
}
