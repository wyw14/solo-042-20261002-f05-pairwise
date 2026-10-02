// Package health defines the shared severity vocabulary used by every
// component health report.
package health

import "strings"

// Severity ranks a single health finding. The ordering is total:
//
//	ok < starting < busy < degraded < critical
//
// "starting" marks the no-sample startup window so that automation can keep it
// apart from a genuine fault, while "busy" marks expected-but-limiting
// conditions such as a circuit reserved for an in-progress fill.
type Severity string

const (
	SeverityOK       Severity = "ok"
	SeverityStarting Severity = "starting"
	SeverityBusy     Severity = "busy"
	SeverityDegraded Severity = "degraded"
	SeverityCritical Severity = "critical"
)

// Rank returns the numeric rank of a severity; unknown values rank below ok.
func Rank(severity Severity) int {
	switch severity {
	case SeverityOK:
		return 0
	case SeverityStarting:
		return 1
	case SeverityBusy:
		return 2
	case SeverityDegraded:
		return 3
	case SeverityCritical:
		return 4
	default:
		return -1
	}
}

// Worst returns the more severe of the two inputs.
func Worst(a, b Severity) Severity {
	if Rank(a) >= Rank(b) {
		return a
	}
	return b
}

// Normalize lower-cases and trims a severity, mapping empty values to ok.
func Normalize(value string) Severity {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return SeverityOK
	}
	return Severity(value)
}
