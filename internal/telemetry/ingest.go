package telemetry

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/probe"
	"github.com/wyw14/cry-98/internal/vessel"
)

type Ingestor struct {
	probes    *probe.Registry
	vessels   *vessel.StateService
	snapshots *SnapshotStore
	mu        sync.RWMutex
	accepted  uint64
	dropped   uint64
	observers []func(model.Sample)
	startedAt time.Time
	zoneStats map[string]*zoneCounter
}

type zoneCounter struct {
	accepted     uint64
	dropped      uint64
	lastSampleAt time.Time
}

// ZoneStats is the machine-readable telemetry view for one zone.
type ZoneStats struct {
	Zone            string    `json:"zone"`
	Accepted        uint64    `json:"accepted"`
	Dropped         uint64    `json:"dropped"`
	Backlog         int       `json:"backlog"`
	BacklogCapacity int       `json:"backlog_capacity"`
	LastSampleAt    time.Time `json:"last_sample_at,omitempty"`
}

// UnknownZone collects activity that cannot be resolved to a configured zone.
const UnknownZone = "unknown"

func (i *Ingestor) AddObserver(observer func(model.Sample)) error {
	if observer == nil {
		return errors.New("telemetry observer is required")
	}
	i.mu.Lock()
	i.observers = append(i.observers, observer)
	i.mu.Unlock()
	return nil
}

func NewIngestor(probes *probe.Registry, vessels *vessel.StateService, snapshots *SnapshotStore, now func() time.Time) *Ingestor {
	if now == nil {
		now = time.Now
	}
	return &Ingestor{
		probes:    probes,
		vessels:   vessels,
		snapshots: snapshots,
		startedAt: now().UTC(),
		zoneStats: make(map[string]*zoneCounter),
	}
}

func (i *Ingestor) zoneForVessel(vesselID string) string {
	if vesselState, ok := i.vessels.Registry().Get(vesselID); ok {
		return vesselState.Zone
	}
	return UnknownZone
}

func (i *Ingestor) recordZone(zone string, accepted bool, observedAt time.Time) {
	stats, ok := i.zoneStats[zone]
	if !ok {
		stats = &zoneCounter{}
		i.zoneStats[zone] = stats
	}
	if accepted {
		stats.accepted++
		if observedAt.After(stats.lastSampleAt) {
			stats.lastSampleAt = observedAt.UTC()
		}
	} else {
		stats.dropped++
	}
}

func (i *Ingestor) Ingest(sample model.Sample) error {
	probeState, ok := i.probes.Get(sample.ProbeID)
	if !ok {
		return errors.New("sample probe is not registered")
	}
	if !probeState.Connected || sample.CalibrationEpoch != probeState.CalibrationEpoch || sample.BindingGeneration != probeState.BindingGeneration {
		i.mu.Lock()
		i.dropped++
		i.recordZone(i.zoneForVessel(probeState.VesselID), false, sample.ObservedAt)
		i.mu.Unlock()
		return errors.New("sample belongs to an obsolete probe generation")
	}
	vesselState, ok := i.vessels.Registry().Get(probeState.VesselID)
	if !ok {
		return errors.New("sample vessel is not registered")
	}
	level, pressure, temperature := vesselState.LevelMM, vesselState.PressureKPa, vesselState.TemperatureC
	switch sample.Kind {
	case model.ProbeLevel:
		level = sample.Value
	case model.ProbePressure:
		pressure = sample.Value
	case model.ProbeTemperature:
		temperature = sample.Value
	default:
		return errors.New("unsupported sample kind")
	}
	updated, err := i.vessels.ApplyTelemetry(probeState.VesselID, level, pressure, temperature, sample.ObservedAt)
	if err != nil {
		return err
	}
	i.snapshots.Apply(updated)
	i.mu.Lock()
	i.accepted++
	i.recordZone(vesselState.Zone, true, sample.ObservedAt)
	observers := append([]func(model.Sample){}, i.observers...)
	i.mu.Unlock()
	for _, observer := range observers {
		observer(sample)
	}
	return nil
}

func (i *Ingestor) Stats() (accepted, dropped uint64) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.accepted, i.dropped
}

func (i *Ingestor) Snapshot() Snapshot {
	return i.snapshots.StableSnapshot()
}

func (i *Ingestor) LastUpdated() time.Time {
	return i.snapshots.StableSnapshot().UpdatedAt
}

// StartedAt reports when the ingestor was constructed. Health checks use it to
// tell the no-sample startup window apart from a real outage.
func (i *Ingestor) StartedAt() time.Time {
	return i.startedAt
}

// Zones returns the zones that have seen accepted or dropped samples so far.
func (i *Ingestor) Zones() []string {
	i.mu.RLock()
	defer i.mu.RUnlock()
	zones := make([]string, 0, len(i.zoneStats))
	for zone := range i.zoneStats {
		zones = append(zones, zone)
	}
	sort.Strings(zones)
	return zones
}

// BacklogByZone sums buffered-but-unconsumed samples for each zone's live
// probe subscriptions, along with total buffer capacity. Probes that no longer
// have a subscription (detached) do not contribute.
func (i *Ingestor) BacklogByZone() map[string]struct{ Depth, Capacity int } {
	depths := i.probes.QueueDepths()
	totals := make(map[string]struct{ Depth, Capacity int })
	for probeID, depth := range depths {
		probeState, ok := i.probes.Get(probeID)
		if !ok {
			continue
		}
		zone := i.zoneForVessel(probeState.VesselID)
		entry := totals[zone]
		entry.Depth += depth
		_, capacity, known := i.probes.QueueDepth(probeID)
		if known {
			entry.Capacity += capacity
		}
		totals[zone] = entry
	}
	return totals
}

// ZoneStatsByZone returns per-zone accepted/dropped/backlog counters. Zones
// with configured probes but no activity yet are included with zero counters
// so callers can distinguish startup from silence.
func (i *Ingestor) ZoneStatsByZone() []ZoneStats {
	i.mu.RLock()
	stats := make(map[string]ZoneStats, len(i.zoneStats))
	for zone, counter := range i.zoneStats {
		stats[zone] = ZoneStats{
			Zone:         zone,
			Accepted:     counter.accepted,
			Dropped:      counter.dropped,
			LastSampleAt: counter.lastSampleAt,
		}
	}
	i.mu.RUnlock()
	for zone, backlog := range i.BacklogByZone() {
		entry := stats[zone]
		entry.Zone = zone
		entry.Backlog = backlog.Depth
		entry.BacklogCapacity = backlog.Capacity
		stats[zone] = entry
	}
	result := make([]ZoneStats, 0, len(stats))
	for _, entry := range stats {
		result = append(result, entry)
	}
	sort.Slice(result, func(a, b int) bool { return result[a].Zone < result[b].Zone })
	return result
}
