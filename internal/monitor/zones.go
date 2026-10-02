package monitor

import (
	"sort"
	"time"

	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/telemetry"
)

// Zone-level health statuses, ordered by severity: ok < starting < degraded.
const (
	StatusOK       = "ok"
	StatusStarting = "starting"
	StatusDegraded = "degraded"
)

// ReasonCodeTelemetryBacklog marks a zone whose telemetry drop counters
// exceed the tolerated slack (see telemetry.CountsHealthy).
const ReasonCodeTelemetryBacklog = "telemetry_backlog"

// CircuitIssue is an unavailable circuit attributed to a zone.
type CircuitIssue struct {
	CircuitID string     `json:"circuit_id"`
	Code      string     `json:"code"`
	Since     *time.Time `json:"since,omitempty"`
	Detail    string     `json:"detail,omitempty"`
}

// ProbeIssue is a disconnected, stale, or not-yet-reporting probe.
type ProbeIssue struct {
	ProbeID  string     `json:"probe_id"`
	VesselID string     `json:"vessel_id"`
	Code     string     `json:"code"`
	Since    *time.Time `json:"since,omitempty"`
}

// ZoneBacklog aggregates the telemetry ingest counters of a zone's probes.
type ZoneBacklog struct {
	Accepted   uint64     `json:"accepted"`
	Dropped    uint64     `json:"dropped"`
	Backlogged bool       `json:"backlogged"`
	Code       string     `json:"code,omitempty"`
	LastDropAt *time.Time `json:"last_drop_at,omitempty"`
}

// ZoneHealth is the per-zone breakdown of the health report.
type ZoneHealth struct {
	Zone          string         `json:"zone"`
	Status        string         `json:"status"`
	LastChangedAt *time.Time     `json:"last_changed_at,omitempty"`
	Circuits      []CircuitIssue `json:"circuits,omitempty"`
	Probes        []ProbeIssue   `json:"probes,omitempty"`
	Telemetry     ZoneBacklog    `json:"telemetry"`
}

func statusRank(status string) int {
	switch status {
	case StatusDegraded:
		return 2
	case StatusStarting:
		return 1
	default:
		return 0
	}
}

// worstStatus returns the most severe status across zones ("ok" when empty).
func worstStatus(zones []ZoneHealth) string {
	worst := StatusOK
	for _, zone := range zones {
		if statusRank(zone.Status) > statusRank(worst) {
			worst = zone.Status
		}
	}
	return worst
}

// circuitIssueSeverity maps a circuit reason code to a zone status. A
// reserved circuit is listed but does not degrade the zone, matching the
// top-level report's treatment of reservations.
func circuitIssueSeverity(code string) string {
	switch code {
	case circuit.CodeCircuitReserved:
		return StatusOK
	default:
		return StatusDegraded
	}
}

// probeIssueSeverity maps a probe reason code to a zone status. Probes
// still inside their startup grace window only mark the zone as starting.
func probeIssueSeverity(reason string) string {
	if reason == ReasonProbeNoSamples {
		return StatusStarting
	}
	return StatusDegraded
}

// zoneAccumulator tracks a zone's issues while the report is built.
type zoneAccumulator struct {
	health      ZoneHealth
	severity    string
	lastChanged time.Time
	lastDrop    time.Time
}

func (acc *zoneAccumulator) raise(severity string, since *time.Time) {
	if statusRank(severity) > statusRank(acc.severity) {
		acc.severity = severity
	}
	if since != nil && since.After(acc.lastChanged) {
		acc.lastChanged = *since
	}
}

// buildZones groups circuit, probe, and telemetry signals by zone. The
// vesselZones map resolves probe/vessel membership; probes whose vessel is
// unknown fall into the empty zone so they stay visible.
func buildZones(circuits []circuit.Health, probes []ProbeHealth, stats map[string]telemetry.ProbeStat, vesselZones map[string]string) []ZoneHealth {
	zones := make(map[string]*zoneAccumulator)
	zone := func(name string) *zoneAccumulator {
		acc, ok := zones[name]
		if !ok {
			acc = &zoneAccumulator{health: ZoneHealth{Zone: name, Status: StatusOK}, severity: StatusOK}
			zones[name] = acc
		}
		return acc
	}
	for _, vesselZone := range vesselZones {
		zone(vesselZone)
	}
	for _, health := range circuits {
		acc := zone(health.Zone)
		if health.Available {
			continue
		}
		acc.health.Circuits = append(acc.health.Circuits, CircuitIssue{
			CircuitID: health.CircuitID,
			Code:      health.Code,
			Since:     health.Since,
			Detail:    health.Reason,
		})
		acc.raise(circuitIssueSeverity(health.Code), health.Since)
	}
	probeZones := make(map[string]string, len(probes))
	for _, health := range probes {
		probeZone := vesselZones[health.VesselID]
		probeZones[health.ProbeID] = probeZone
		if health.Reason == "" {
			continue
		}
		acc := zone(probeZone)
		acc.health.Probes = append(acc.health.Probes, ProbeIssue{
			ProbeID:  health.ProbeID,
			VesselID: health.VesselID,
			Code:     health.Reason,
			Since:    health.Since,
		})
		acc.raise(probeIssueSeverity(health.Reason), health.Since)
	}
	for probeID, stat := range stats {
		probeZone, known := probeZones[probeID]
		if !known {
			// Stat without a live probe record: keep it visible rather
			// than dropping it silently.
			probeZone = ""
		}
		acc := zone(probeZone)
		acc.health.Telemetry.Accepted += stat.Accepted
		acc.health.Telemetry.Dropped += stat.Dropped
		if stat.LastDropAt.After(acc.lastDrop) {
			acc.lastDrop = stat.LastDropAt
		}
	}
	result := make([]ZoneHealth, 0, len(zones))
	for _, acc := range zones {
		backlog := &acc.health.Telemetry
		if !telemetry.CountsHealthy(backlog.Accepted, backlog.Dropped) {
			backlog.Backlogged = true
			backlog.Code = ReasonCodeTelemetryBacklog
			acc.raise(StatusDegraded, timePtr(acc.lastDrop))
		}
		if !acc.lastDrop.IsZero() {
			backlog.LastDropAt = timePtr(acc.lastDrop)
		}
		if !acc.lastChanged.IsZero() {
			acc.health.LastChangedAt = timePtr(acc.lastChanged)
		}
		acc.health.Status = acc.severity
		result = append(result, acc.health)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Zone < result[j].Zone })
	return result
}
