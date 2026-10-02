package monitor

import (
	"time"

	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/telemetry"
	"github.com/wyw14/cry-98/internal/vessel"
)

type HealthReport struct {
	Status      string           `json:"status"`
	CheckedAt   time.Time        `json:"checked_at"`
	Telemetry   telemetry.Health `json:"telemetry"`
	Circuits    []circuit.Health `json:"circuits"`
	ProbeIssues []ProbeHealth    `json:"probe_issues"`
	// WorstStatus is the most severe per-zone status (ok, starting, or
	// degraded). Unlike Status it reflects the startup grace period, so
	// "starting" distinguishes a sample-less boot from a real failure.
	WorstStatus string `json:"worst_status"`
	// Zones breaks unavailable circuits, silent probes, and telemetry
	// backlog down by zone with machine-readable reason codes.
	Zones []ZoneHealth `json:"zones"`
}

type HealthService struct {
	telemetry *telemetry.Ingestor
	circuits  *circuit.HealthService
	probes    *ProbeMonitor
	vessels   *vessel.Registry
	now       func() time.Time
}

func NewHealthService(ingestor *telemetry.Ingestor, circuits *circuit.HealthService, probes *ProbeMonitor, vessels *vessel.Registry, now func() time.Time) *HealthService {
	if now == nil {
		now = time.Now
	}
	return &HealthService{telemetry: ingestor, circuits: circuits, probes: probes, vessels: vessels, now: now}
}

func (s *HealthService) Report() HealthReport {
	circuits := s.circuits.All()
	probes := s.probes.All()
	vesselZones := make(map[string]string)
	for _, item := range s.vessels.List() {
		vesselZones[item.ID] = item.Zone
	}
	zones := buildZones(circuits, probes, s.telemetry.ProbeStats(), vesselZones)
	issues := make([]ProbeHealth, 0)
	for _, health := range probes {
		if !health.Connected || health.Stale {
			issues = append(issues, health)
		}
	}
	report := HealthReport{
		Status:      "ok",
		CheckedAt:   s.now().UTC(),
		Telemetry:   s.telemetry.Health(),
		Circuits:    circuits,
		ProbeIssues: issues,
		WorstStatus: worstStatus(zones),
		Zones:       zones,
	}
	if !report.Telemetry.Healthy || len(report.ProbeIssues) > 0 {
		report.Status = "degraded"
	}
	for _, health := range report.Circuits {
		if !health.Available && health.Code != circuit.CodeCircuitReserved {
			report.Status = "degraded"
		}
	}
	return report
}
