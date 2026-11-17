// Command loadgen measures the sustained detection-event throughput of the
// Kafka pipeline. It publishes events at a target rate from many simulated
// cameras and reports achieved events/sec and produce latency percentiles.
//
// Example:
//
//	loadgen -brokers kafka:9092 -devices 500 -rate 10000 -duration 5m
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/sakshat-patil/edgefleet/internal/model"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "Kafka brokers")
	topic := flag.String("topic", "detections", "topic")
	devices := flag.Int("devices", 500, "number of distinct device IDs")
	rate := flag.Int("rate", 10000, "target events/sec (0 = as fast as possible)")
	duration := flag.Duration("duration", time.Minute, "test duration")
	workers := flag.Int("workers", 16, "producer goroutines")
	batch := flag.Int("batch", 200, "messages per produce call")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()

	w := &kafka.Writer{
		Addr: kafka.TCP(strings.Split(*brokers, ",")...), Topic: *topic, Balancer: &kafka.Hash{},
		BatchSize: *batch, BatchTimeout: 5 * time.Millisecond, RequiredAcks: kafka.RequireOne,
		Compression: kafka.Lz4, AllowAutoTopicCreation: true,
	}
	defer w.Close()

	var sent, failed atomic.Int64
	var latMu sync.Mutex
	var lats []time.Duration

	// Token bucket: each worker takes batches of permits.
	perWorker := 0.0
	if *rate > 0 {
		perWorker = float64(*rate) / float64(*workers)
	}
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			var seq uint64
			msgs := make([]kafka.Message, *batch)
			next := time.Now()
			for ctx.Err() == nil {
				if perWorker > 0 {
					next = next.Add(time.Duration(float64(*batch) / perWorker * float64(time.Second)))
					if d := time.Until(next); d > 0 {
						select {
						case <-time.After(d):
						case <-ctx.Done():
							return
						}
					}
				}
				for j := range msgs {
					seq++
					dev := fmt.Sprintf("cam-%04d", rand.IntN(*devices))
					b, _ := json.Marshal(model.DetectionEvent{DeviceID: dev, Seq: seq, Label: "person", Confidence: 0.9, BBox: [4]int{10, 20, 64, 128}, FrameTS: time.Now()})
					msgs[j] = kafka.Message{Key: []byte(dev), Value: b}
				}
				t0 := time.Now()
				if err := w.WriteMessages(ctx, msgs...); err != nil {
					if ctx.Err() == nil {
						failed.Add(int64(len(msgs)))
					}
					continue
				}
				d := time.Since(t0)
				sent.Add(int64(len(msgs)))
				latMu.Lock()
				lats = append(lats, d)
				latMu.Unlock()
			}
		}(i)
	}

	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		var last int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s := sent.Load()
				fmt.Printf("[%5.0fs] %8.0f events/sec  total=%d failed=%d\n", time.Since(start).Seconds(), float64(s-last)/5, s, failed.Load())
				last = s
			}
		}
	}()
	wg.Wait()
	elapsed := time.Since(start)
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	pct := func(p float64) time.Duration {
		if len(lats) == 0 {
			return 0
		}
		return lats[int(p*float64(len(lats)-1))]
	}
	res := map[string]any{
		"target_rate": *rate, "devices": *devices, "duration_s": elapsed.Seconds(),
		"sent": sent.Load(), "failed": failed.Load(),
		"events_per_sec":       float64(sent.Load()) / elapsed.Seconds(),
		"batch_latency_p50_ms": pct(0.50).Seconds() * 1000,
		"batch_latency_p99_ms": pct(0.99).Seconds() * 1000,
	}
	out, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(out))
}
