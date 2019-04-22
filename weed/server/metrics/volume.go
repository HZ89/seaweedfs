package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	FileNumber = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: VolumeSubsystem,
			Name:      "file_numbers",
			Help:      "Number of files stored",
		},
		[]string{"datacenter", "rack", "collection", "machine", "volumeId"},
	)
	FileSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: VolumeSubsystem,
			Name:      "file_size",
			Help:      "Size of all files stored",
		},
		[]string{"datacenter", "rack", "collection", "machine", "volumeId"},
	)
	volMetricsList = []prometheus.Collector{
		FileNumber,
		FileSize,
	}
)

// VolumeReset resets metrics
func VolumeReset() {

}

var registerMetrics sync.Once

// VolumeRegisterMetrics registers the metrics which are ONLY used in volume server.
func VolumeRegisterMetrics() {
	// Register the metrics.
	registerMetrics.Do(func() {
		for _, metric := range volMetricsList {
			prometheus.MustRegister(metric)
		}
	})
}
