package telemetry

import (
	"time"
)

type Health struct {
	Accepted  uint64    `json:"accepted"`
	Dropped   uint64    `json:"dropped"`
	UpdatedAt time.Time `json:"updated_at"`
	Healthy   bool      `json:"healthy"`
}

// dropSlack is the number of dropped samples tolerated on top of the
// accepted count before telemetry is considered unhealthy.
const dropSlack = 10

// CountsHealthy reports whether the given accepted/dropped counters are
// within the tolerated drop slack. Zone-level backlog reporting reuses the
// same rule so per-zone and global telemetry health stay consistent.
func CountsHealthy(accepted, dropped uint64) bool {
	return dropped < accepted+dropSlack
}

func (i *Ingestor) Health() Health {
	accepted, dropped := i.Stats()
	return Health{
		Accepted:  accepted,
		Dropped:   dropped,
		UpdatedAt: i.LastUpdated(),
		Healthy:   CountsHealthy(accepted, dropped),
	}
}
