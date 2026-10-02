package telemetry

import (
	"errors"
	"sync"
	"time"

	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/probe"
	"github.com/wyw14/cry-98/internal/vessel"
)

// ProbeStat captures per-probe ingest counters for zone-level backlog reporting.
type ProbeStat struct {
	Accepted   uint64    `json:"accepted"`
	Dropped    uint64    `json:"dropped"`
	LastDropAt time.Time `json:"last_drop_at,omitempty"`
}

type Ingestor struct {
	probes          *probe.Registry
	vessels         *vessel.StateService
	snapshots       *SnapshotStore
	now             func() time.Time
	mu              sync.RWMutex
	accepted        uint64
	dropped         uint64
	acceptedByProbe map[string]uint64
	droppedByProbe  map[string]uint64
	lastDropAt      map[string]time.Time
	observers       []func(model.Sample)
}

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
		probes:          probes,
		vessels:         vessels,
		snapshots:       snapshots,
		now:             now,
		acceptedByProbe: make(map[string]uint64),
		droppedByProbe:  make(map[string]uint64),
		lastDropAt:      make(map[string]time.Time),
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
		i.droppedByProbe[sample.ProbeID]++
		i.lastDropAt[sample.ProbeID] = i.now().UTC()
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
	i.acceptedByProbe[sample.ProbeID]++
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

// ProbeStats returns a copy of the per-probe ingest counters keyed by probe id.
func (i *Ingestor) ProbeStats() map[string]ProbeStat {
	i.mu.RLock()
	defer i.mu.RUnlock()
	ids := make(map[string]struct{}, len(i.acceptedByProbe)+len(i.droppedByProbe))
	for id := range i.acceptedByProbe {
		ids[id] = struct{}{}
	}
	for id := range i.droppedByProbe {
		ids[id] = struct{}{}
	}
	stats := make(map[string]ProbeStat, len(ids))
	for id := range ids {
		stats[id] = ProbeStat{
			Accepted:   i.acceptedByProbe[id],
			Dropped:    i.droppedByProbe[id],
			LastDropAt: i.lastDropAt[id],
		}
	}
	return stats
}

func (i *Ingestor) Snapshot() Snapshot {
	return i.snapshots.StableSnapshot()
}

func (i *Ingestor) LastUpdated() time.Time {
	return i.snapshots.StableSnapshot().UpdatedAt
}
