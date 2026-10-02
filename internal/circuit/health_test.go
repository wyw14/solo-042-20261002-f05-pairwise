package circuit

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestHealthReportsCodesSeverityAndChangeTime(t *testing.T) {
	base := time.Date(2026, 10, 2, 10, 0, 0, 0, time.UTC)
	now := base
	clock := func() time.Time { return now }

	registry := NewRegistry(clock,
		Circuit{ID: "C-OK", Zone: "north", ValveID: "V-A", Enabled: true},
		Circuit{ID: "C-OFF", Zone: "south", ValveID: "V-B", Enabled: false, UpdatedAt: base.Add(-time.Minute)},
	)
	manager := NewManager("C-OK", "C-OFF")
	service := NewHealthService(registry, manager, clock)

	healthy := service.Check("C-OK")
	if !healthy.Available || healthy.Severity != SeverityOK || healthy.Code != ReasonAvailable {
		t.Fatalf("healthy circuit misreported: %+v", healthy)
	}
	if healthy.Zone != "north" {
		t.Fatalf("zone missing: %+v", healthy)
	}

	disabled := service.Check("C-OFF")
	if disabled.Available || disabled.Code != ReasonDisabled || disabled.Severity != SeverityDegraded {
		t.Fatalf("disabled circuit misreported: %+v", disabled)
	}
	if !disabled.ChangedAt.Equal(base.Add(-time.Minute)) {
		t.Fatalf("disabled changed_at = %v, want %v", disabled.ChangedAt, base.Add(-time.Minute))
	}

	missing := service.Check("C-GHOST")
	if missing.Available || missing.Code != ReasonNotConfigured || missing.Severity != SeverityDegraded {
		t.Fatalf("missing circuit misreported: %+v", missing)
	}

	sessionID := uuid.New()
	reservedAt := base.Add(time.Minute)
	now = reservedAt
	if _, err := manager.Reserve("C-OK", "V-01", sessionID, reservedAt); err != nil {
		t.Fatal(err)
	}
	reserved := service.Check("C-OK")
	if reserved.Available || reserved.Code != ReasonReserved || reserved.Severity != SeverityBusy {
		t.Fatalf("reserved circuit misreported: %+v", reserved)
	}
	if !reserved.ChangedAt.Equal(reservedAt) {
		t.Fatalf("reserved changed_at = %v, want %v", reserved.ChangedAt, reservedAt)
	}

	faultAt := reservedAt.Add(2 * time.Minute)
	now = faultAt
	if err := manager.RetainFault("C-OK", sessionID, faultAt); err != nil {
		t.Fatal(err)
	}
	faulted := service.Check("C-OK")
	if faulted.Available || faulted.Code != ReasonFaultRetention || faulted.Severity != SeverityCritical {
		t.Fatalf("faulted circuit misreported: %+v", faulted)
	}
	if !faulted.ChangedAt.Equal(faultAt) {
		t.Fatalf("fault changed_at = %v, want %v", faulted.ChangedAt, faultAt)
	}
}
