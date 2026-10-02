package telemetry

import (
	"time"

	"github.com/wyw14/cry-98/internal/health"
)

// ReasonCode is a machine-readable identifier for the telemetry health state.
type ReasonCode string

const (
	ReasonAwaitingSamples ReasonCode = "TELEMETRY_AWAITING_SAMPLES"
	ReasonDrops           ReasonCode = "TELEMETRY_SAMPLE_DROPS"
	ReasonBacklog         ReasonCode = "TELEMETRY_BACKLOG"
)

// BacklogWatermark is the fraction of subscription buffer capacity that
// counts as a backlog worth alerting on.
const BacklogWatermark = 0.8

type Health struct {
	Accepted        uint64          `json:"accepted"`
	Dropped         uint64          `json:"dropped"`
	UpdatedAt       time.Time       `json:"updated_at"`
	Healthy         bool            `json:"healthy"`
	StartedAt       time.Time       `json:"started_at"`
	Backlog         int             `json:"backlog"`
	BacklogCapacity int             `json:"backlog_capacity"`
	Code            ReasonCode      `json:"code,omitempty"`
	Severity        health.Severity `json:"severity"`
	ChangedAt       time.Time       `json:"changed_at"`
}

func (i *Ingestor) Health() Health {
	accepted, dropped := i.Stats()
	updated := i.LastUpdated()

	backlog, capacity := 0, 0
	for _, totals := range i.BacklogByZone() {
		backlog += totals.Depth
		capacity += totals.Capacity
	}

	result := Health{
		Accepted:        accepted,
		Dropped:         dropped,
		UpdatedAt:       updated,
		Healthy:         dropped < accepted+10,
		StartedAt:       i.startedAt,
		Backlog:         backlog,
		BacklogCapacity: capacity,
		Severity:        health.SeverityOK,
	}

	// Drops always take priority: they prove traffic is flowing but failing.
	if !result.Healthy {
		result.Code = ReasonDrops
		result.Severity = health.SeverityDegraded
	} else if accepted == 0 {
		// No sample has ever been ingested: expected during the startup window,
		// even if a handful of obsolete samples were rejected. Distinct from a
		// live pipeline that later starts losing samples.
		result.Code = ReasonAwaitingSamples
		result.Severity = health.SeverityStarting
	} else if capacity > 0 && float64(backlog)/float64(capacity) >= BacklogWatermark {
		result.Code = ReasonBacklog
		result.Severity = health.SeverityDegraded
	}

	if !updated.IsZero() {
		result.ChangedAt = updated
	} else {
		result.ChangedAt = i.startedAt
	}
	return result
}
