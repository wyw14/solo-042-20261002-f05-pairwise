package monitor

import (
	"sort"
	"sync"
	"time"

	"github.com/wyw14/cry-98/internal/health"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/probe"
	"github.com/wyw14/cry-98/internal/vessel"
)

// UnknownZone groups probes whose vessel cannot be resolved to a configured
// zone.
const UnknownZone = "unknown"

// ReasonCode is a machine-readable identifier for a probe's condition.
type ReasonCode string

const (
	ReasonAwaitingSample ReasonCode = "PROBE_AWAITING_SAMPLE"
	ReasonDisconnected   ReasonCode = "PROBE_DISCONNECTED"
	ReasonStale          ReasonCode = "PROBE_STALE"
	ReasonSilent         ReasonCode = "PROBE_SILENT"
)

type ProbeHealth struct {
	ProbeID    string          `json:"probe_id"`
	VesselID   string          `json:"vessel_id"`
	Zone       string          `json:"zone,omitempty"`
	Connected  bool            `json:"connected"`
	LastSample time.Time       `json:"last_sample,omitempty"`
	Stale      bool            `json:"stale"`
	Reason     string          `json:"reason,omitempty"`
	Code       ReasonCode      `json:"code,omitempty"`
	Severity   health.Severity `json:"severity"`
	CheckedAt  time.Time       `json:"checked_at"`
	ChangedAt  time.Time       `json:"changed_at,omitempty"`
}

type ProbeMonitor struct {
	registry     *probe.Registry
	vessels      *vessel.Registry
	mu           sync.RWMutex
	last         map[string]time.Time
	staleAfter   time.Duration
	startupGrace time.Duration
	now          func() time.Time
}

func NewProbeMonitor(registry *probe.Registry, vessels *vessel.Registry, staleAfter, startupGrace time.Duration, now func() time.Time) *ProbeMonitor {
	if now == nil {
		now = time.Now
	}
	return &ProbeMonitor{
		registry:     registry,
		vessels:      vessels,
		last:         make(map[string]time.Time),
		staleAfter:   staleAfter,
		startupGrace: startupGrace,
		now:          now,
	}
}

func (m *ProbeMonitor) Observe(sample model.Sample) {
	m.mu.Lock()
	if last, exists := m.last[sample.ProbeID]; !exists || sample.ObservedAt.After(last) {
		m.last[sample.ProbeID] = sample.ObservedAt.UTC()
	}
	m.mu.Unlock()
}

// zoneOf resolves the zone for a probe's vessel.
func (m *ProbeMonitor) zoneOf(vesselID string) string {
	if m.vessels != nil {
		if vesselState, ok := m.vessels.Get(vesselID); ok {
			return vesselState.Zone
		}
	}
	return UnknownZone
}

func (m *ProbeMonitor) Health(probeID string) (ProbeHealth, bool) {
	probeState, ok := m.registry.Get(probeID)
	if !ok {
		return ProbeHealth{}, false
	}
	m.mu.RLock()
	last := m.last[probeID]
	m.mu.RUnlock()
	now := m.now()

	result := ProbeHealth{
		ProbeID:    probeID,
		VesselID:   probeState.VesselID,
		Zone:       m.zoneOf(probeState.VesselID),
		Connected:  probeState.Connected,
		LastSample: last,
		Severity:   health.SeverityOK,
		CheckedAt:  now.UTC(),
		ChangedAt:  last,
	}

	switch {
	case !probeState.Connected:
		result.Code = ReasonDisconnected
		result.Severity = health.SeverityCritical
		result.Reason = "probe is disconnected"
		result.Stale = last.IsZero() || now.Sub(last) > m.staleAfter
		result.ChangedAt = probeState.UpdatedAt
	case last.IsZero():
		// Bound but has never delivered a sample. Inside the startup grace
		// window this is expected ("starting"); once it elapses it is a real
		// fault distinct from a probe that previously reported and went quiet.
		graceDeadline := probeState.UpdatedAt.Add(m.startupGrace)
		result.Stale = true
		if now.Before(graceDeadline) {
			result.Code = ReasonAwaitingSample
			result.Severity = health.SeverityStarting
			result.Reason = "probe has not reported since startup"
			result.ChangedAt = probeState.UpdatedAt
		} else {
			result.Code = ReasonSilent
			result.Severity = health.SeverityCritical
			result.Reason = "probe has never reported and the startup grace window has elapsed"
			result.ChangedAt = graceDeadline
		}
	case now.Sub(last) > m.staleAfter:
		result.Code = ReasonStale
		result.Severity = health.SeverityDegraded
		result.Reason = "probe telemetry is stale"
		result.Stale = true
		result.ChangedAt = last.Add(m.staleAfter)
	}
	return result, true
}

func (m *ProbeMonitor) All() []ProbeHealth {
	probes := m.registry.List()
	result := make([]ProbeHealth, 0, len(probes))
	for _, probeState := range probes {
		if probeHealth, ok := m.Health(probeState.ID); ok {
			result = append(result, probeHealth)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ProbeID < result[j].ProbeID })
	return result
}

// Disconnected preserves the legacy view: every probe that is disconnected or
// not producing fresh data, including probes still inside their startup grace.
// New consumers should prefer All and filter on Severity/Code.
func (m *ProbeMonitor) Disconnected() []ProbeHealth {
	all := m.All()
	result := make([]ProbeHealth, 0)
	for _, probeHealth := range all {
		if !probeHealth.Connected || probeHealth.Stale {
			result = append(result, probeHealth)
		}
	}
	return result
}
