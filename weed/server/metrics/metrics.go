package metrics

import "github.com/prometheus/client_golang/prometheus"

// Label constants.
const (
	LblUnretryable = "unretryable"
	LblReachMax    = "reach_max"
	LblOK          = "ok"
	LblError       = "error"
	LblRollback    = "rollback"
	LblType        = "type"
	LblResult      = "result"
	LblSQLType     = "sql_type"
	LblGeneral     = "general"
	LblInternal    = "internal"
)

var (
	// PanicCounter measures the count of panics.
	PanicCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "seaweedfs",
			Name:      "panic_total",
			Help:      "Counter of panic.",
		}, []string{LblType})
)
