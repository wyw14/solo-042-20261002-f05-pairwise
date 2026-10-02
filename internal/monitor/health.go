package monitor

import (
	"sort"
	"time"

	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/health"
	"github.com/wyw14/cry-98/internal/telemetry"
)

// TelemetryZoneHealth is the telemetry view for one zone: the raw counters
// from the ingestor plus a machine-readable verdict.
type TelemetryZoneHealth struct {
	telemetry.ZoneStats
	Code      telemetry.ReasonCode `json:"code,omitempty"`
	Severity  health.Severity      `json:"severity"`
	ChangedAt time.Time            `json:"changed_at"`
}

// ZoneHealth lists everything that limits one zone: unavailable circuits,
// probes that are disconnected/stale/silent (or still awaiting their first
// sample), and the zone's telemetry counters with backlog depth.
type ZoneHealth struct {
	Zone                string              `json:"zone"`
	Severity            health.Severity     `json:"severity"`
	ChangedAt           time.Time           `json:"changed_at"`
	UnavailableCircuits []circuit.Health    `json:"unavailable_circuits"`
	ProbeIssues         []ProbeHealth       `json:"probe_issues"`
	Telemetry           TelemetryZoneHealth `json:"telemetry"`
}

// HealthReport stays wire-compatible with the original ok/degraded report:
// status, checked_at, telemetry, circuits and probe_issues keep their old
// shape. severity, changed_at and zones are additive.
type HealthReport struct {
	Status      string           `json:"status"`
	Severity    health.Severity  `json:"severity"`
	CheckedAt   time.Time        `json:"checked_at"`
	ChangedAt   time.Time        `json:"changed_at"`
	Telemetry   telemetry.Health `json:"telemetry"`
	Circuits    []circuit.Health `json:"circuits"`
	ProbeIssues []ProbeHealth    `json:"probe_issues"`
	Zones       []ZoneHealth     `json:"zones"`
}

type HealthService struct {
	telemetry *telemetry.Ingestor
	circuits  *circuit.HealthService
	probes    *ProbeMonitor
	now       func() time.Time
}

func NewHealthService(ingestor *telemetry.Ingestor, circuits *circuit.HealthService, probes *ProbeMonitor, now func() time.Time) *HealthService {
	if now == nil {
		now = time.Now
	}
	return &HealthService{telemetry: ingestor, circuits: circuits, probes: probes, now: now}
}

func latestTime(current, candidate time.Time) time.Time {
	if candidate.After(current) {
		return candidate
	}
	return current
}

// assessTelemetryZone turns raw per-zone counters into a severity verdict.
// The drop threshold mirrors the legacy ingestor.Healthy rule
// (dropped < accepted+10); zero traffic is "starting", never a fault.
func assessTelemetryZone(stats telemetry.ZoneStats, startedAt time.Time) TelemetryZoneHealth {
	assessment := TelemetryZoneHealth{
		ZoneStats: stats,
		Severity:  health.SeverityOK,
		ChangedAt: latestTime(startedAt, stats.LastSampleAt),
	}
	switch {
	case stats.Dropped >= stats.Accepted+10:
		assessment.Code = telemetry.ReasonDrops
		assessment.Severity = health.SeverityDegraded
	case stats.Accepted == 0:
		assessment.Code = telemetry.ReasonAwaitingSamples
		assessment.Severity = health.SeverityStarting
	case stats.BacklogCapacity > 0 && float64(stats.Backlog)/float64(stats.BacklogCapacity) >= telemetry.BacklogWatermark:
		assessment.Code = telemetry.ReasonBacklog
		assessment.Severity = health.SeverityDegraded
	}
	return assessment
}

func (s *HealthService) Report() HealthReport {
	checkedAt := s.now().UTC()
	telemetryHealth := s.telemetry.Health()
	circuitHealth := s.circuits.All()
	probeHealth := s.probes.All()

	zoneNames := make(map[string]struct{})
	zoneCircuits := make(map[string][]circuit.Health)
	for _, item := range circuitHealth {
		if item.Zone == "" {
			continue
		}
		zoneNames[item.Zone] = struct{}{}
		if !item.Available {
			zoneCircuits[item.Zone] = append(zoneCircuits[item.Zone], item)
		}
	}

	zoneProbes := make(map[string][]ProbeHealth)
	for _, item := range probeHealth {
		zoneNames[item.Zone] = struct{}{}
		if item.Severity != health.SeverityOK {
			zoneProbes[item.Zone] = append(zoneProbes[item.Zone], item)
		}
	}

	zoneTelemetry := make(map[string]telemetry.ZoneStats)
	for _, stats := range s.telemetry.ZoneStatsByZone() {
		zoneTelemetry[stats.Zone] = stats
		zoneNames[stats.Zone] = struct{}{}
	}

	names := make([]string, 0, len(zoneNames))
	for name := range zoneNames {
		names = append(names, name)
	}
	sort.Strings(names)

	zones := make([]ZoneHealth, 0, len(names))
	overallSeverity := telemetryHealth.Severity
	overallChanged := telemetryHealth.ChangedAt

	for _, name := range names {
		stats, ok := zoneTelemetry[name]
		if !ok {
			stats = telemetry.ZoneStats{Zone: name}
		}
		zoneTelemetryHealth := assessTelemetryZone(stats, telemetryHealth.StartedAt)

		zone := ZoneHealth{
			Zone:                name,
			Severity:            zoneTelemetryHealth.Severity,
			ChangedAt:           zoneTelemetryHealth.ChangedAt,
			UnavailableCircuits: zoneCircuits[name],
			ProbeIssues:         zoneProbes[name],
			Telemetry:           zoneTelemetryHealth,
		}
		if zone.UnavailableCircuits == nil {
			zone.UnavailableCircuits = []circuit.Health{}
		}
		if zone.ProbeIssues == nil {
			zone.ProbeIssues = []ProbeHealth{}
		}
		for _, item := range zoneCircuits[name] {
			zone.Severity = health.Worst(zone.Severity, item.Severity)
			zone.ChangedAt = latestTime(zone.ChangedAt, item.ChangedAt)
		}
		for _, item := range zoneProbes[name] {
			zone.Severity = health.Worst(zone.Severity, item.Severity)
			zone.ChangedAt = latestTime(zone.ChangedAt, item.ChangedAt)
		}
		zones = append(zones, zone)

		overallSeverity = health.Worst(overallSeverity, zone.Severity)
		overallChanged = latestTime(overallChanged, zone.ChangedAt)
	}

	report := HealthReport{
		Status:      "ok",
		Severity:    overallSeverity,
		CheckedAt:   checkedAt,
		ChangedAt:   overallChanged,
		Telemetry:   telemetryHealth,
		Circuits:    circuitHealth,
		ProbeIssues: s.probes.Disconnected(),
		Zones:       zones,
	}
	// Legacy status preserves the exact pre-expansion ok/degraded rule:
	// unhealthy drop counters, any disconnected/stale probe (including probes
	// still awaiting their first sample in the startup window), or any
	// unavailable circuit other than an expected reservation. New consumers
	// should read Severity and Zones, where startup ("starting") is distinct
	// from a real fault ("degraded"/"critical").
	if !report.Telemetry.Healthy || len(report.ProbeIssues) > 0 {
		report.Status = "degraded"
	}
	for _, item := range report.Circuits {
		if !item.Available && item.Code != circuit.ReasonReserved {
			report.Status = "degraded"
		}
	}
	return report
}
