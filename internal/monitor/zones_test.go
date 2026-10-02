package monitor

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/probe"
	"github.com/wyw14/cry-98/internal/telemetry"
	"github.com/wyw14/cry-98/internal/vessel"
)

const testStaleAfter = 2 * time.Minute

type zoneFixture struct {
	service  *HealthService
	probes   *probe.Registry
	circuits *circuit.Registry
	manager  *circuit.Manager
	ingestor *telemetry.Ingestor
	now      func() time.Time
	advance  func(time.Duration)
}

func newZoneFixture(t *testing.T) *zoneFixture {
	t.Helper()
	current := time.Unix(1_000_000, 0).UTC()
	now := func() time.Time { return current }
	vessels := vessel.NewRegistry()
	for _, seed := range []struct{ id, zone string }{{"V-01", "north"}, {"V-02", "south"}} {
		item, err := model.NewVessel(seed.id, "Vessel "+seed.id, seed.zone, now())
		if err != nil {
			t.Fatal(err)
		}
		if err := vessels.Register(item); err != nil {
			t.Fatal(err)
		}
	}
	probes := probe.NewRegistry(8)
	for _, seed := range []struct{ id, vesselID string }{{"P-01", "V-01"}, {"P-02", "V-02"}} {
		item, err := model.NewProbe(seed.id, model.ProbeLevel, seed.vesselID, now())
		if err != nil {
			t.Fatal(err)
		}
		if err := probes.Register(item); err != nil {
			t.Fatal(err)
		}
	}
	circuits := circuit.NewRegistry(now,
		circuit.Circuit{ID: "C-N", Zone: "north", ValveID: "VALVE-N", Enabled: true},
		circuit.Circuit{ID: "C-S", Zone: "south", ValveID: "VALVE-S", Enabled: true},
	)
	manager := circuit.NewManager("C-N", "C-S")
	circuitHealth := circuit.NewHealthService(circuits, manager, now)
	snapshots := telemetry.NewSnapshotStore()
	snapshots.Replace(vessels.List(), now())
	ingestor := telemetry.NewIngestor(probes, vessel.NewStateService(vessels, now), snapshots, now)
	monitor := NewProbeMonitor(probes, testStaleAfter, now)
	if err := ingestor.AddObserver(monitor.Observe); err != nil {
		t.Fatal(err)
	}
	return &zoneFixture{
		service:  NewHealthService(ingestor, circuitHealth, monitor, vessels, now),
		probes:   probes,
		circuits: circuits,
		manager:  manager,
		ingestor: ingestor,
		now:      now,
		advance:  func(d time.Duration) { current = current.Add(d) },
	}
}

func (f *zoneFixture) ingestSample(t *testing.T, probeID string, value float64) {
	t.Helper()
	probeState, ok := f.probes.Get(probeID)
	if !ok {
		t.Fatalf("probe %q not registered", probeID)
	}
	if err := f.ingestor.Ingest(probeState.NewSample(value, f.now())); err != nil {
		t.Fatal(err)
	}
}

// dropSamples ingests count obsolete-generation samples for probeID so the
// ingestor records them as dropped.
func (f *zoneFixture) dropSamples(t *testing.T, probeID string, count int) {
	t.Helper()
	probeState, ok := f.probes.Get(probeID)
	if !ok {
		t.Fatalf("probe %q not registered", probeID)
	}
	stale := probeState.NewSample(1, f.now())
	lifecycle := probe.NewLifecycle(f.probes, f.now)
	if _, err := lifecycle.Rebind(probeID, probeState.VesselID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		if err := f.ingestor.Ingest(stale); err == nil {
			t.Fatal("obsolete sample was accepted")
		}
	}
}

func findZone(t *testing.T, zones []ZoneHealth, name string) ZoneHealth {
	t.Helper()
	for _, zone := range zones {
		if zone.Zone == name {
			return zone
		}
	}
	t.Fatalf("zone %q missing from report", name)
	return ZoneHealth{}
}

func requireTime(t *testing.T, got *time.Time, want time.Time) {
	t.Helper()
	if got == nil {
		t.Fatalf("timestamp is nil, want %v", want)
	}
	if !got.Equal(want) {
		t.Fatalf("timestamp = %v, want %v", got, want)
	}
}

func TestZoneReportStartupGraceIsNotAFailure(t *testing.T) {
	f := newZoneFixture(t)
	boot := f.now()
	report := f.service.Report()

	// Legacy fields stay compatible: probes without samples are still
	// listed and the legacy status stays degraded.
	if report.Status != "degraded" {
		t.Fatalf("legacy status = %q, want degraded", report.Status)
	}
	if len(report.ProbeIssues) != 2 {
		t.Fatalf("probe issues = %d, want 2", len(report.ProbeIssues))
	}

	// The refined view marks the boot window as starting, not failed.
	if report.WorstStatus != StatusStarting {
		t.Fatalf("worst status = %q, want %q", report.WorstStatus, StatusStarting)
	}
	for _, name := range []string{"north", "south"} {
		zone := findZone(t, report.Zones, name)
		if zone.Status != StatusStarting {
			t.Fatalf("zone %s status = %q, want %q", name, zone.Status, StatusStarting)
		}
		if len(zone.Probes) != 1 {
			t.Fatalf("zone %s probe issues = %d, want 1", name, len(zone.Probes))
		}
		issue := zone.Probes[0]
		if issue.Code != ReasonProbeNoSamples {
			t.Fatalf("zone %s probe code = %q, want %q", name, issue.Code, ReasonProbeNoSamples)
		}
		requireTime(t, issue.Since, boot)
		requireTime(t, zone.LastChangedAt, boot)
		if zone.Telemetry.Backlogged {
			t.Fatalf("zone %s telemetry should not be backlogged", name)
		}
	}
}

func TestZoneReportSilentProbeAfterGrace(t *testing.T) {
	f := newZoneFixture(t)
	boot := f.now()
	f.advance(testStaleAfter + time.Second)
	report := f.service.Report()

	if report.WorstStatus != StatusDegraded {
		t.Fatalf("worst status = %q, want %q", report.WorstStatus, StatusDegraded)
	}
	north := findZone(t, report.Zones, "north")
	if north.Status != StatusDegraded {
		t.Fatalf("north status = %q, want %q", north.Status, StatusDegraded)
	}
	issue := north.Probes[0]
	if issue.Code != ReasonProbeSilent {
		t.Fatalf("probe code = %q, want %q", issue.Code, ReasonProbeSilent)
	}
	requireTime(t, issue.Since, boot.Add(testStaleAfter))
}

func TestZoneReportStaleProbeAfterSampling(t *testing.T) {
	f := newZoneFixture(t)
	f.ingestSample(t, "P-01", 42)
	sampledAt := f.now()
	f.advance(testStaleAfter + time.Second)
	report := f.service.Report()

	north := findZone(t, report.Zones, "north")
	if north.Status != StatusDegraded {
		t.Fatalf("north status = %q, want %q", north.Status, StatusDegraded)
	}
	var stale *ProbeIssue
	for i := range north.Probes {
		if north.Probes[i].ProbeID == "P-01" {
			stale = &north.Probes[i]
		}
	}
	if stale == nil {
		t.Fatal("P-01 missing from north probe issues")
	}
	if stale.Code != ReasonProbeStale {
		t.Fatalf("probe code = %q, want %q", stale.Code, ReasonProbeStale)
	}
	requireTime(t, stale.Since, sampledAt.Add(testStaleAfter))
}

func TestZoneReportDisconnectedProbe(t *testing.T) {
	f := newZoneFixture(t)
	f.advance(time.Minute)
	lifecycle := probe.NewLifecycle(f.probes, f.now)
	if _, err := lifecycle.Disconnect("P-01"); err != nil {
		t.Fatal(err)
	}
	disconnectedAt := f.now()
	report := f.service.Report()

	north := findZone(t, report.Zones, "north")
	if north.Status != StatusDegraded {
		t.Fatalf("north status = %q, want %q", north.Status, StatusDegraded)
	}
	if len(north.Probes) != 1 {
		t.Fatalf("north probe issues = %d, want 1", len(north.Probes))
	}
	issue := north.Probes[0]
	if issue.Code != ReasonProbeDisconnected {
		t.Fatalf("probe code = %q, want %q", issue.Code, ReasonProbeDisconnected)
	}
	requireTime(t, issue.Since, disconnectedAt)
}

func TestZoneReportCircuitIssues(t *testing.T) {
	f := newZoneFixture(t)
	// Keep probes healthy so only circuits drive the zone statuses.
	f.ingestSample(t, "P-01", 50)
	f.ingestSample(t, "P-02", 50)

	// A reserved circuit is listed but does not degrade its zone.
	if _, err := f.manager.Reserve("C-S", "V-02", uuid.New(), f.now()); err != nil {
		t.Fatal(err)
	}
	report := f.service.Report()
	south := findZone(t, report.Zones, "south")
	if south.Status != StatusOK {
		t.Fatalf("south status = %q, want %q", south.Status, StatusOK)
	}
	if len(south.Circuits) != 1 || south.Circuits[0].Code != circuit.CodeCircuitReserved {
		t.Fatalf("south circuits = %+v, want one %q entry", south.Circuits, circuit.CodeCircuitReserved)
	}
	if report.Status != "ok" {
		t.Fatalf("legacy status = %q, want ok", report.Status)
	}

	// A faulted reservation degrades the zone and the overall report.
	if err := f.manager.RetainFault("C-S", f.managerReservation(t, "C-S")); err != nil {
		t.Fatal(err)
	}
	// A disabled circuit degrades its zone with the disable timestamp.
	f.advance(time.Minute)
	if err := f.circuits.SetEnabled("C-N", false); err != nil {
		t.Fatal(err)
	}
	disabledAt := f.now()

	report = f.service.Report()
	if report.WorstStatus != StatusDegraded {
		t.Fatalf("worst status = %q, want %q", report.WorstStatus, StatusDegraded)
	}
	if report.Status != "degraded" {
		t.Fatalf("legacy status = %q, want degraded", report.Status)
	}
	north := findZone(t, report.Zones, "north")
	if north.Status != StatusDegraded {
		t.Fatalf("north status = %q, want %q", north.Status, StatusDegraded)
	}
	if len(north.Circuits) != 1 || north.Circuits[0].Code != circuit.CodeCircuitDisabled {
		t.Fatalf("north circuits = %+v, want one %q entry", north.Circuits, circuit.CodeCircuitDisabled)
	}
	requireTime(t, north.Circuits[0].Since, disabledAt)
	south = findZone(t, report.Zones, "south")
	if south.Status != StatusDegraded {
		t.Fatalf("south status = %q, want %q", south.Status, StatusDegraded)
	}
	if len(south.Circuits) != 1 || south.Circuits[0].Code != circuit.CodeCircuitFaultRetained {
		t.Fatalf("south circuits = %+v, want one %q entry", south.Circuits, circuit.CodeCircuitFaultRetained)
	}
}

func (f *zoneFixture) managerReservation(t *testing.T, circuitID string) uuid.UUID {
	t.Helper()
	reservation, ok := f.manager.Get(circuitID)
	if !ok {
		t.Fatalf("circuit %q is not reserved", circuitID)
	}
	return reservation.SessionID
}

func TestZoneReportTelemetryBacklog(t *testing.T) {
	f := newZoneFixture(t)
	f.ingestSample(t, "P-02", 60)
	f.advance(time.Second)
	droppedAt := f.now()
	f.dropSamples(t, "P-01", 10)
	report := f.service.Report()

	north := findZone(t, report.Zones, "north")
	if !north.Telemetry.Backlogged {
		t.Fatal("north telemetry should be backlogged")
	}
	if north.Telemetry.Code != ReasonCodeTelemetryBacklog {
		t.Fatalf("north telemetry code = %q, want %q", north.Telemetry.Code, ReasonCodeTelemetryBacklog)
	}
	if north.Telemetry.Dropped != 10 {
		t.Fatalf("north dropped = %d, want 10", north.Telemetry.Dropped)
	}
	requireTime(t, north.Telemetry.LastDropAt, droppedAt)
	if north.Status != StatusDegraded {
		t.Fatalf("north status = %q, want %q", north.Status, StatusDegraded)
	}

	south := findZone(t, report.Zones, "south")
	if south.Telemetry.Backlogged {
		t.Fatal("south telemetry should not be backlogged")
	}
	if south.Telemetry.Accepted != 1 || south.Telemetry.Dropped != 0 {
		t.Fatalf("south telemetry = %+v, want accepted=1 dropped=0", south.Telemetry)
	}
}

func TestZoneReportHealthy(t *testing.T) {
	f := newZoneFixture(t)
	f.ingestSample(t, "P-01", 50)
	f.ingestSample(t, "P-02", 50)
	report := f.service.Report()

	if report.Status != "ok" || report.WorstStatus != StatusOK {
		t.Fatalf("status = %q/%q, want ok/ok", report.Status, report.WorstStatus)
	}
	if len(report.ProbeIssues) != 0 {
		t.Fatalf("probe issues = %d, want 0", len(report.ProbeIssues))
	}
	if len(report.Zones) != 2 {
		t.Fatalf("zones = %d, want 2", len(report.Zones))
	}
	for _, zone := range report.Zones {
		if zone.Status != StatusOK {
			t.Fatalf("zone %s status = %q, want %q", zone.Zone, zone.Status, StatusOK)
		}
		if len(zone.Circuits) != 0 || len(zone.Probes) != 0 {
			t.Fatalf("zone %s should have no issues, got %+v", zone.Zone, zone)
		}
	}
}
