package aggregator

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	failedMessagesDesc = prometheus.NewDesc("bnpb_failed_messages", "Failed source messages in the control store, by source and status.",
		[]string{"source", "status"}, nil)
	failedMetricsOnce  sync.Once
	failedMetricsStore atomic.Pointer[Store]
)

// Every replica reads the same control store, so the dashboard takes the max across replicas.
type failedMessagesCollector struct{}

func registerFailedMessageMetrics(store *Store) {
	failedMetricsStore.Store(store)
	failedMetricsOnce.Do(func() { prometheus.MustRegister(failedMessagesCollector{}) })
}

func (failedMessagesCollector) Describe(descs chan<- *prometheus.Desc) { descs <- failedMessagesDesc }

func (failedMessagesCollector) Collect(metrics chan<- prometheus.Metric) {
	store := failedMetricsStore.Load()
	if store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rows, err := store.read.Query(ctx, "SELECT source, status, count(*) FROM failed_messages GROUP BY source, status")
	if err != nil {
		return
	}
	defer rows.Close()
	counts := map[[2]string]int64{}
	for rows.Next() {
		var source, status string
		var count int64
		if rows.Scan(&source, &status, &count) != nil {
			return
		}
		counts[[2]string{source, status}] = count
	}
	if rows.Err() != nil {
		return
	}
	// Zero rows still appear so a panel shows 0 instead of no data.
	for _, source := range []Source{BMKG, PVMBG, Satelit, SensorPantai} {
		for _, status := range []string{"retrying", "parked", "resolved"} {
			metrics <- prometheus.MustNewConstMetric(failedMessagesDesc, prometheus.GaugeValue,
				float64(counts[[2]string{string(source), status}]), string(source), status)
		}
	}
}
