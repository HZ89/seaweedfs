package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "seaweedfs"
	// MasterSubsystem - subsystem name used by master server of seaweedfs
	MasterSubsystem = "master_server"
	VolumeSubsystem = "volume_server"
)

var (
	MasterRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: MasterSubsystem,
			Name:      "request_count",
			Help:      "Counter of master server requests broken out for each verb, API resource, client, and HTTP response contentType and code.",
		},
		[]string{"verb", "from", "path"},
	)
	MasterRequestLatencies = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: MasterSubsystem,
			Name:      "request_latencies",
			Help:      "Response latency distribution in microseconds for each verb, resource and subresource.",
			// Use buckets ranging from 125 ms to 8 seconds.
			Buckets: prometheus.ExponentialBuckets(125000, 2.0, 7),
		},
		[]string{"verb", "from", "path"},
	)
	MasterRequestLatenciesSummary = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: namespace,
			Subsystem: MasterSubsystem,
			Name:      "request_latencies_summary",
			Help:      "Response latency summary in microseconds for each verb, resource and subresource.",
			// Make the sliding window of 5h.
			// TODO: The value for this should be based on our SLI definition (medium term).
			MaxAge: 5 * time.Hour,
		},
		[]string{"verb", "from", "path"},
	)
	MasterResponseSizes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: MasterSubsystem,
			Name:      "response_sizes",
			Help:      "Response size distribution in bytes for each verb, resource, subresource and scope (namespace/cluster).",
			// Use buckets ranging from 1000 bytes (1KB) to 10^9 bytes (1GB).
			Buckets: prometheus.ExponentialBuckets(1000, 10.0, 7),
		},
		[]string{"verb", "", "", ""},
	)
	// DroppedRequests is a number of requests dropped with 'Try again later' response"
	MasterDroppedRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: MasterSubsystem,
			Name:      "dropped_requests",
			Help:      "Number of requests dropped with 'Try again later' response",
		},
		[]string{"requestKind"},
	)
	DataCenterNumber = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: MasterSubsystem,
			Name:      "datacenter_numbers",
			Help:      "Number of collection created by admin",
		},
		[]string{"topo"},
	)
	RackNumber = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: MasterSubsystem,
			Name:      "rack_numbers",
			Help:      "Number of rack created by admin",
		},
		[]string{"topo", "datacenter"},
	)
	DataNodeNumber = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: MasterSubsystem,
			Name:      "datanode_numbers",
			Help:      "Number of data nodes created by admin",
		},
		[]string{"topo", "datacenter", "rack"},
	)
	AllVolumeNumber = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: MasterSubsystem,
			Name:      "volume_numbers",
			Help:      "Number of volume created by admin",
		},
		[]string{"topo", "datacenter", "rack", "datanode"},
	)
	ActiveVolumeNumber = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: MasterSubsystem,
			Name:      "active_volume_numbers",
			Help:      "Active number of volume created by admin",
		},
		[]string{"topo", "datacenter", "rack", "datanode"},
	)
	CollectionNumber = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: MasterSubsystem,
			Name:      "collection_numbers",
			Help:      "Number of collection created by admin",
		},
		[]string{"topo"},
	)
	metricsList = []prometheus.Collector{
		MasterRequestCounter,
		MasterRequestLatencies,
		MasterRequestLatenciesSummary,
		MasterDroppedRequests,
		PanicCounter,
		DataCenterNumber,
		RackNumber,
		DataNodeNumber,
		AllVolumeNumber,
		ActiveVolumeNumber,
		CollectionNumber,
	}
)

// MasterReset resets metrics
func MasterReset() {
	MasterRequestLatencies.Reset()
	MasterRequestLatenciesSummary.Reset()
}

var mRegisterMetrics sync.Once

// MasterRegisterMetrics registers the metrics which are ONLY used in Master server.
func MasterRegisterMetrics() {
	// Register the metrics.
	mRegisterMetrics.Do(func() {
		for _, metric := range metricsList {
			prometheus.MustRegister(metric)
		}
	})
}
