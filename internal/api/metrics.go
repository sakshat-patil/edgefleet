package api

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics is a minimal Prometheus text-format exporter (no external deps).
type Metrics struct {
	mu         sync.Mutex
	requests   map[string]int64   // route|status -> count
	latencySum map[string]float64 // route -> seconds
	latencyCnt map[string]int64

	Heartbeats     atomic.Int64
	CommandsQueued atomic.Int64
	CommandsSent   atomic.Int64
	EventsIngested atomic.Int64
	ConfigsApplied atomic.Int64
}

// NewMetrics returns empty metrics.
func NewMetrics() *Metrics {
	return &Metrics{requests: map[string]int64{}, latencySum: map[string]float64{}, latencyCnt: map[string]int64{}}
}

// Observe records one HTTP request.
func (m *Metrics) Observe(route string, status int, d time.Duration) {
	m.mu.Lock()
	m.requests[fmt.Sprintf("%s|%d", route, status)]++
	m.latencySum[route] += d.Seconds()
	m.latencyCnt[route]++
	m.mu.Unlock()
}

// Write emits all metrics in Prometheus exposition format.
func (m *Metrics) Write(w io.Writer, gauges map[string]float64) {
	m.mu.Lock()
	keys := make([]string, 0, len(m.requests))
	for k := range m.requests {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Fprintln(w, "# TYPE edgefleet_http_requests_total counter")
	for _, k := range keys {
		var route string
		var status int
		for i := len(k) - 1; i >= 0; i-- {
			if k[i] == '|' {
				route = k[:i]
				fmt.Sscan(k[i+1:], &status)
				break
			}
		}
		fmt.Fprintf(w, "edgefleet_http_requests_total{route=%q,status=\"%d\"} %d\n", route, status, m.requests[k])
	}
	fmt.Fprintln(w, "# TYPE edgefleet_http_request_duration_seconds summary")
	routes := make([]string, 0, len(m.latencyCnt))
	for r := range m.latencyCnt {
		routes = append(routes, r)
	}
	sort.Strings(routes)
	for _, r := range routes {
		fmt.Fprintf(w, "edgefleet_http_request_duration_seconds_sum{route=%q} %f\n", r, m.latencySum[r])
		fmt.Fprintf(w, "edgefleet_http_request_duration_seconds_count{route=%q} %d\n", r, m.latencyCnt[r])
	}
	m.mu.Unlock()
	counters := []struct {
		name string
		v    int64
	}{
		{"edgefleet_heartbeats_total", m.Heartbeats.Load()},
		{"edgefleet_commands_queued_total", m.CommandsQueued.Load()},
		{"edgefleet_commands_delivered_total", m.CommandsSent.Load()},
		{"edgefleet_detection_events_ingested_total", m.EventsIngested.Load()},
	}
	for _, c := range counters {
		fmt.Fprintf(w, "# TYPE %s counter\n%s %d\n", c.name, c.name, c.v)
	}
	gk := make([]string, 0, len(gauges))
	for k := range gauges {
		gk = append(gk, k)
	}
	sort.Strings(gk)
	for _, k := range gk {
		fmt.Fprintf(w, "# TYPE %s gauge\n%s %g\n", k, k, gauges[k])
	}
}
