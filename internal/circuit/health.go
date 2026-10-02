package circuit

import (
	"time"

	"github.com/wyw14/cry-98/internal/health"
)

// ReasonCode is a machine-readable identifier for why a circuit is unavailable.
type ReasonCode string

const (
	ReasonNotConfigured  ReasonCode = "CIRCUIT_NOT_CONFIGURED"
	ReasonDisabled       ReasonCode = "CIRCUIT_DISABLED"
	ReasonReserved       ReasonCode = "CIRCUIT_RESERVED"
	ReasonFaultRetention ReasonCode = "CIRCUIT_FAULT_RETENTION"
	ReasonAvailable      ReasonCode = ""
)

// Severity is re-exported from the shared health vocabulary.
type Severity = health.Severity

// Re-exported severities so existing circuit callers keep working.
var (
	SeverityOK       = health.SeverityOK
	SeverityBusy     = health.SeverityBusy
	SeverityDegraded = health.SeverityDegraded
	SeverityCritical = health.SeverityCritical
)

type Health struct {
	CircuitID string     `json:"circuit_id"`
	Zone      string     `json:"zone,omitempty"`
	Available bool       `json:"available"`
	Reason    string     `json:"reason,omitempty"`
	Code      ReasonCode `json:"code,omitempty"`
	Severity  Severity   `json:"severity"`
	CheckedAt time.Time  `json:"checked_at"`
	ChangedAt time.Time  `json:"changed_at,omitempty"`
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
	result := Health{CircuitID: id, Severity: health.SeverityOK, CheckedAt: s.now().UTC()}
	circuit, ok := s.registry.Get(id)
	if !ok {
		result.Code = ReasonNotConfigured
		result.Severity = health.SeverityDegraded
		result.Reason = "circuit is not configured"
		return result
	}
	result.Zone = circuit.Zone
	if !circuit.Enabled {
		result.Code = ReasonDisabled
		result.Severity = health.SeverityDegraded
		result.Reason = "circuit is disabled"
		result.ChangedAt = circuit.UpdatedAt
		return result
	}
	if reservation, occupied := s.manager.Get(id); occupied {
		if reservation.Faulted {
			result.Code = ReasonFaultRetention
			result.Severity = health.SeverityCritical
			result.Reason = "circuit is retained after an isolation failure"
			result.ChangedAt = reservation.FaultedAt
		} else {
			result.Code = ReasonReserved
			result.Severity = health.SeverityBusy
			result.Reason = "circuit is reserved"
			result.ChangedAt = reservation.ReservedAt
		}
		return result
	}
	result.Available = true
	return result
}

func (s *HealthService) All() []Health {
	circuits := s.registry.List()
	result := make([]Health, 0, len(circuits))
	for _, circuit := range circuits {
		result = append(result, s.Check(circuit.ID))
	}
	return result
}
