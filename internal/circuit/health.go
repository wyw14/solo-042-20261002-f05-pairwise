package circuit

import "time"

// Machine-readable reason codes for circuit health. The human-readable
// Reason strings are kept for display; consumers should switch on Code.
const (
	CodeCircuitNotConfigured = "circuit_not_configured"
	CodeCircuitDisabled      = "circuit_disabled"
	CodeCircuitReserved      = "circuit_reserved"
	CodeCircuitFaultRetained = "circuit_fault_retained"
)

type Health struct {
	CircuitID string     `json:"circuit_id"`
	Zone      string     `json:"zone"`
	Available bool       `json:"available"`
	Reason    string     `json:"reason,omitempty"`
	Code      string     `json:"code,omitempty"`
	Since     *time.Time `json:"since,omitempty"`
	CheckedAt time.Time  `json:"checked_at"`
}

type HealthService struct {
	registry *Registry
	manager  *Manager
	now      func() time.Time
}

func NewHealthService(registry *Registry, manager *Manager, now func() time.Time) *HealthService {
	if now == nil {
		now = time.Now
	}
	return &HealthService{registry: registry, manager: manager, now: now}
}

func (s *HealthService) Check(id string) Health {
	result := Health{CircuitID: id, CheckedAt: s.now().UTC()}
	circuit, ok := s.registry.Get(id)
	if !ok {
		result.Reason = "circuit is not configured"
		result.Code = CodeCircuitNotConfigured
		return result
	}
	result.Zone = circuit.Zone
	if !circuit.Enabled {
		result.Reason = "circuit is disabled"
		result.Code = CodeCircuitDisabled
		result.Since = timePtr(circuit.UpdatedAt)
		return result
	}
	if reservation, occupied := s.manager.Get(id); occupied {
		if reservation.Faulted {
			result.Reason = "circuit is retained after an isolation failure"
			result.Code = CodeCircuitFaultRetained
		} else {
			result.Reason = "circuit is reserved"
			result.Code = CodeCircuitReserved
		}
		result.Since = timePtr(reservation.ReservedAt)
		return result
	}
	result.Available = true
	return result
}

// timePtr returns nil for zero timestamps so they are omitted from JSON
// instead of serializing as the zero time.
func timePtr(at time.Time) *time.Time {
	if at.IsZero() {
		return nil
	}
	return &at
}

func (s *HealthService) All() []Health {
	circuits := s.registry.List()
	result := make([]Health, 0, len(circuits))
	for _, circuit := range circuits {
		result = append(result, s.Check(circuit.ID))
	}
	return result
}
