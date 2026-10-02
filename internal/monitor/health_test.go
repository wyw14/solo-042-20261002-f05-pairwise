package monitor

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/health"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/probe"
	"github.com/wyw14/cry-98/internal/telemetry"
	"github.com/wyw14/cry-98/internal/vessel"
)

const (
	staleAfter = 2 * time.Minute
	startGrace = 30 * time.Second
)

type fixture struct {
	now      time.Time
	vessels  *vessel.Registry
	probes   *probe.Registry
	ingestor *telemetry.Ingestor
	monitor  *ProbeMonitor
	circuits *circuit.Registry
	manager  *circuit.Manager
	health   *HealthService
}

func setup(t *testing.T) *fixture {
	t.Helper()
	base := time.Date(2026, 10, 2, 10, 0, 0, 0, time.UTC)
	fx := &fixture{now: base}
	clock := func() time.Time { return fx.now }

	fx.vessels = vessel.NewRegistry()
	for _, seed := range []struct{ id, name, zone string }{
		{"V-01", "Vessel 01", "north"},
		{"V-02", "Vessel 02", "south"},
		{"V-03", "Vessel 03", "east"},
	} {
		v, err := model.NewVessel(seed.id, seed.name, seed.zone, clock())
		if err != nil {
			t.Fatal(err)
		}
		if err := fx.vessels.Register(v); err != nil {
			t.Fatal(err)
		}
	}

	fx.probes = probe.NewRegistry(5)
	for _, seed := range []struct {
		id, vesselID string
		kind         model.ProbeKind
	}{
		{"P-01", "V-01", model.ProbeLevel},
		{"P-02", "V-02", model.ProbePressure},
		{"P-03", "V-03", model.ProbeTemperature},
	} {
		p, err := model.NewProbe(seed.id, seed.kind, seed.vesselID, clock())
		if err != nil {
			t.Fatal(err)
		}
		if err := fx.probes.Register(p); err != nil {
			t.Fatal(err)
		}
	}

	state := vessel.NewStateService(fx.vessels, clock)
	snapshots := telemetry.NewSnapshotStore()
	fx.ingestor = telemetry.NewIngestor(fx.probes, state, snapshots, clock)
	fx.monitor = NewProbeMonitor(fx.probes, fx.vessels, staleAfter, startGrace, clock)
	if err := fx.ingestor.AddObserver(fx.monitor.Observe); err != nil {
		t.Fatal(err)
	}

	fx.circuits = circuit.NewRegistry(clock,
		circuit.Circuit{ID: "C-A", Zone: "north", ValveID: "VALVE-A", Enabled: true},
		circuit.Circuit{ID: "C-B", Zone: "south", ValveID: "VALVE-B", Enabled: true},
	)
	fx.manager = circuit.NewManager("C-A", "C-B")
	circuitHealth := circuit.NewHealthService(fx.circuits, fx.manager, clock)
	fx.health = NewHealthService(fx.ingestor, circuitHealth, fx.monitor, clock)
	return fx
}

func (fx *fixture) ingest(t *testing.T, probeID string, value float64, at time.Time) {
	t.Helper()
	probeState, ok := fx.probes.Get(probeID)
	if !ok {
		t.Fatalf("probe %s not registered", probeID)
	}
	if err := fx.ingestor.Ingest(probeState.NewSample(value, at)); err != nil {
		t.Fatal(err)
	}
}

func (fx *fixture) zone(t *testing.T, report HealthReport, zone string) ZoneHealth {
	t.Helper()
	for _, z := range report.Zones {
		if z.Zone == zone {
			return z
		}
	}
	t.Fatalf("zone %s missing from report: %+v", zone, report.Zones)
	return ZoneHealth{}
}

func TestStartupWindowIsStartingNotDegraded(t *testing.T) {
	fx := setup(t)

	report := fx.health.Report()

	// New machine-readable verdict: nothing has ever sampled -> starting.
	if report.Severity != health.SeverityStarting {
		t.Fatalf("fresh report severity = %s, want starting", report.Severity)
	}
	if report.Telemetry.Code != telemetry.ReasonAwaitingSamples {
		t.Fatalf("telemetry code = %q, want %q", report.Telemetry.Code, telemetry.ReasonAwaitingSamples)
	}

	north := fx.zone(t, report, "north")
	if north.Severity != health.SeverityStarting {
		t.Fatalf("north severity = %s, want starting", north.Severity)
	}
	if north.Telemetry.Code != telemetry.ReasonAwaitingSamples {
		t.Fatalf("north telemetry code = %q", north.Telemetry.Code)
	}
	if len(north.ProbeIssues) != 1 || north.ProbeIssues[0].Code != ReasonAwaitingSample {
		t.Fatalf("north probe issues = %+v, want one awaiting-sample probe", north.ProbeIssues)
	}
	if len(north.UnavailableCircuits) != 0 {
		t.Fatalf("north circuits = %+v, want none unavailable", north.UnavailableCircuits)
	}

	// Legacy compatibility: old consumers still see status=degraded because a
	// never-sampled probe was listed in probe_issues, and the old flat
	// telemetry/circuits/probe_issues fields are still populated.
	if report.Status != "degraded" {
		t.Fatalf("legacy status = %q, want degraded", report.Status)
	}
	if len(report.Circuits) != 2 || len(report.ProbeIssues) != 3 {
		t.Fatalf("legacy fields changed shape: circuits=%d probe_issues=%d", len(report.Circuits), len(report.ProbeIssues))
	}
	if !report.Telemetry.Healthy {
		t.Fatal("legacy telemetry.healthy should be true with no drops")
	}
}

func TestHealthyZoneRecoversToOK(t *testing.T) {
	fx := setup(t)

	// North samples repeatedly while still inside every other zone's startup
	// grace window.
	at := fx.now
	for i := 0; i < 3; i++ {
		at = at.Add(5 * time.Second)
		fx.now = at
		fx.ingest(t, "P-01", float64(50+i), at)
	}

	report := fx.health.Report()
	north := fx.zone(t, report, "north")
	if north.Severity != health.SeverityOK || north.Telemetry.Code != "" {
		t.Fatalf("north after samples = %+v code %q, want ok", north.Severity, north.Telemetry.Code)
	}
	if north.Telemetry.Accepted != 3 || north.Telemetry.LastSampleAt.IsZero() {
		t.Fatalf("north telemetry counters wrong: %+v", north.Telemetry)
	}
	if len(north.ProbeIssues) != 0 {
		t.Fatalf("north probe issues = %+v, want none", north.ProbeIssues)
	}
	if south := fx.zone(t, report, "south"); south.Severity != health.SeverityStarting {
		t.Fatalf("unsampled south severity = %s, want starting", south.Severity)
	}
	if report.Severity != health.SeverityStarting {
		t.Fatalf("overall severity = %s, want starting", report.Severity)
	}
	// Legacy status stays degraded: the old rule counted never-sampled probes
	// in probe_issues even during startup. The new severity field is what
	// separates startup from a real fault.
	if report.Status != "degraded" {
		t.Fatalf("legacy status = %q, want degraded while probes await samples", report.Status)
	}
}

func TestGraceElapsedSilenceIsCriticalFault(t *testing.T) {
	fx := setup(t)
	start := fx.now

	// North was healthy, then nothing for the rest of the run. Advance past
	// the startup grace for never-sampled probes (south/east) but keep
	// north's last sample inside the stale window.
	healthyAt := start.Add(10 * time.Second)
	fx.now = healthyAt
	fx.ingest(t, "P-01", 50, healthyAt)

	fx.now = healthyAt.Add(startGrace + time.Second)
	report := fx.health.Report()

	if report.Severity != health.SeverityCritical {
		t.Fatalf("severity = %s, want critical after grace elapsed", report.Severity)
	}
	south := fx.zone(t, report, "south")
	if south.Severity != health.SeverityCritical {
		t.Fatalf("south severity = %s, want critical", south.Severity)
	}
	if south.ProbeIssues[0].Code != ReasonSilent {
		t.Fatalf("south probe code = %q, want %q", south.ProbeIssues[0].Code, ReasonSilent)
	}
	if !south.ProbeIssues[0].ChangedAt.Equal(start.Add(startGrace)) {
		t.Fatalf("silent probe changed_at = %v, want %v", south.ProbeIssues[0].ChangedAt, start.Add(startGrace))
	}
	// North's probe last sampled recently: it must still be ok.
	north := fx.zone(t, report, "north")
	if north.Severity != health.SeverityOK {
		t.Fatalf("north severity = %s, want ok (recent sample)", north.Severity)
	}
}

func TestPreviouslySamplingProbeStaleIsDegraded(t *testing.T) {
	fx := setup(t)

	last := fx.now.Add(5 * time.Second)
	fx.now = last
	fx.ingest(t, "P-01", 50, last)

	fx.now = last.Add(staleAfter + time.Second)
	probeHealth, ok := fx.monitor.Health("P-01")
	if !ok {
		t.Fatal("probe missing")
	}
	if probeHealth.Code != ReasonStale || probeHealth.Severity != health.SeverityDegraded {
		t.Fatalf("stale probe = code %q severity %q", probeHealth.Code, probeHealth.Severity)
	}
	if !probeHealth.ChangedAt.Equal(last.Add(staleAfter)) {
		t.Fatalf("stale changed_at = %v, want %v", probeHealth.ChangedAt, last.Add(staleAfter))
	}

	report := fx.health.Report()
	north := fx.zone(t, report, "north")
	if north.Severity != health.SeverityDegraded {
		t.Fatalf("north severity = %s, want degraded", north.Severity)
	}
	if report.Status != "degraded" {
		t.Fatalf("legacy status = %q, want degraded", report.Status)
	}
}

func TestDisconnectedProbeIsCritical(t *testing.T) {
	fx := setup(t)

	disconnectedAt := fx.now.Add(5 * time.Second)
	fx.now = disconnectedAt
	lifecycle := probe.NewLifecycle(fx.probes, func() time.Time { return fx.now })
	if _, err := lifecycle.Disconnect("P-02"); err != nil {
		t.Fatal(err)
	}

	probeHealth, ok := fx.monitor.Health("P-02")
	if !ok {
		t.Fatal("probe missing")
	}
	if probeHealth.Code != ReasonDisconnected || probeHealth.Severity != health.SeverityCritical {
		t.Fatalf("disconnected probe = code %q severity %q", probeHealth.Code, probeHealth.Severity)
	}
	if !probeHealth.ChangedAt.Equal(disconnectedAt) {
		t.Fatalf("changed_at = %v, want %v", probeHealth.ChangedAt, disconnectedAt)
	}
}

func TestBacklogOverWatermarkDegradesZone(t *testing.T) {
	fx := setup(t)

	// Get north flowing first so the zone is past startup.
	at := fx.now.Add(5 * time.Second)
	fx.now = at
	fx.ingest(t, "P-01", 50, at)

	// Queue 4 of 5 samples without consuming the subscription: 80% full.
	probeState, _ := fx.probes.Get("P-01")
	subscription, ok := fx.probes.Subscription("P-01")
	if !ok {
		t.Fatal("subscription missing")
	}
	for i := 0; i < 4; i++ {
		if !subscription.Send(probeState.NewSample(float64(i), at.Add(time.Duration(i)*time.Second))) {
			t.Fatal("sample queue unexpectedly full")
		}
	}

	report := fx.health.Report()
	north := fx.zone(t, report, "north")
	if north.Telemetry.Backlog != 4 || north.Telemetry.BacklogCapacity != 5 {
		t.Fatalf("backlog = %d/%d, want 4/5", north.Telemetry.Backlog, north.Telemetry.BacklogCapacity)
	}
	if north.Telemetry.Code != telemetry.ReasonBacklog || north.Severity != health.SeverityDegraded {
		t.Fatalf("backlogged zone = code %q severity %q", north.Telemetry.Code, north.Severity)
	}
	if report.Telemetry.Backlog != 4 {
		t.Fatalf("global backlog = %d, want 4", report.Telemetry.Backlog)
	}
}

func TestReservedCircuitIsBusyButNotLegacyDegraded(t *testing.T) {
	fx := setup(t)

	// Keep every probe fresh so legacy status has no other reason to degrade.
	at := fx.now.Add(5 * time.Second)
	fx.now = at
	for _, id := range []string{"P-01", "P-02", "P-03"} {
		fx.ingest(t, id, 50, at)
	}

	sessionID := uuid.New()
	if _, err := fx.manager.Reserve("C-A", "V-01", sessionID, at); err != nil {
		t.Fatal(err)
	}

	report := fx.health.Report()
	north := fx.zone(t, report, "north")
	if north.Severity != health.SeverityBusy {
		t.Fatalf("north severity = %s, want busy", north.Severity)
	}
	if len(north.UnavailableCircuits) != 1 || north.UnavailableCircuits[0].Code != circuit.ReasonReserved {
		t.Fatalf("reserved circuit not listed: %+v", north.UnavailableCircuits)
	}
	if report.Severity != health.SeverityBusy {
		t.Fatalf("overall severity = %s, want busy", report.Severity)
	}
	if report.Status != "ok" {
		t.Fatalf("legacy status = %q, reservation must not degrade it", report.Status)
	}

	faultAt := at.Add(time.Minute)
	fx.now = faultAt
	if err := fx.manager.RetainFault("C-A", sessionID, faultAt); err != nil {
		t.Fatal(err)
	}
	report = fx.health.Report()
	if report.Severity != health.SeverityCritical || report.Status != "degraded" {
		t.Fatalf("after fault severity=%s status=%s, want critical/degraded", report.Severity, report.Status)
	}
}
