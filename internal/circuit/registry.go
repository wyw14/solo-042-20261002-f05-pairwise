package circuit

import (
	"errors"
	"sort"
	"sync"
	"time"
)

type Circuit struct {
	ID        string    `json:"id"`
	Zone      string    `json:"zone"`
	ValveID   string    `json:"valve_id"`
	Enabled   bool      `json:"enabled"`
	Pressure  float64   `json:"pressure_kpa"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Registry struct {
	mu       sync.RWMutex
	circuits map[string]Circuit
	now      func() time.Time
}

func NewRegistry(now func() time.Time, circuits ...Circuit) *Registry {
	if now == nil {
		now = time.Now
	}
	r := &Registry{circuits: make(map[string]Circuit), now: now}
	for _, circuit := range circuits {
		if circuit.ID != "" {
			if circuit.UpdatedAt.IsZero() {
				circuit.UpdatedAt = now().UTC()
			}
			r.circuits[circuit.ID] = circuit
		}
	}
	return r
}

func (r *Registry) Get(id string) (Circuit, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	circuit, ok := r.circuits[id]
	return circuit, ok
}

func (r *Registry) List() []Circuit {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]Circuit, 0, len(r.circuits))
	for _, circuit := range r.circuits {
		result = append(result, circuit)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (r *Registry) UpdatePressure(id string, pressure float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	circuit, ok := r.circuits[id]
	if !ok {
		return errors.New("circuit not found")
	}
	circuit.Pressure = pressure
	r.circuits[id] = circuit
	return nil
}

func (r *Registry) SetEnabled(id string, enabled bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	circuit, ok := r.circuits[id]
	if !ok {
		return errors.New("circuit not found")
	}
	circuit.Enabled = enabled
	circuit.UpdatedAt = r.now().UTC()
	r.circuits[id] = circuit
	return nil
}
