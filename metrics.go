package firehosebatcher

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	namespace string = ""
)

var (
	BytesRead prometheus.Counter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "firehose_batcher",
			Name:      "bytes_read",
			Help:      "Number of bytes read from the input source",
		},
	)
	BytesBatched prometheus.Counter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "firehose_batcher",
			Name:      "bytes_batched",
			Help:      "Number of bytes successfully added into batches",
		},
	)
	BytesSent prometheus.Counter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "firehose_batcher",
			Name:      "bytes_sent",
			Help:      "Number of bytes successfully sent to AWS Firehose",
		},
	)

	// ---

	BatchesCreated prometheus.Counter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "firehose_batcher",
			Name:      "batches_created",
			Help:      "How many batches have been initalized",
		},
	)
	BatchesSent prometheus.Counter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "firehose_batcher",
			Name:      "batches_sent",
			Help:      "How many batches have been successfully sent",
		},
	)

	// ---

	BatchSize prometheus.Histogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "firehose_batcher",
			Name:      "batch_size",
			Help:      "Size of the batches in bytes",

			Buckets: prometheus.LinearBuckets(0, BATCH_SIZE_LIMIT/10, 10),
		},
	)
	BatchLength prometheus.Histogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "firehose_batcher",
			Name:      "batch_length",
			Help:      "Number of records per batch",

			Buckets: prometheus.LinearBuckets(0, BATCH_ITEM_LIMIT/10, 11),
		},
	)

	// ---

	BatchFillLatency prometheus.Histogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "firehose_batcher",
			Name:      "batch_fill_latency",
			Help:      "Time it takes to fill a batch in seconds ",

			// 5ms -> 5min
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 17),
		},
	)
	BatchSendLatency prometheus.Histogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "firehose_batcher",
			Name:      "batch_send_latency",
			Help:      "Time it takes to send a batch to AWS in seconds ",

			// 5ms -> 5min
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 17),
		},
	)
)

func RegisterMetrics(r prometheus.Registerer) error {
	metrics := []prometheus.Collector{
		BytesRead,
		BytesBatched,
		BytesSent,

		BatchesCreated,
		BatchesSent,

		BatchSize,
		BatchLength,

		BatchFillLatency,
		BatchSendLatency,
	}

	for _, m := range metrics {
		if err := r.Register(m); err != nil {
			return err
		}
	}

	return nil
}
