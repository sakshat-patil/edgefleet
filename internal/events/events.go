// Package events consumes detection events from Kafka and keeps per-device
// detection counters in the store.
package events

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/sakshat-patil/edgefleet/internal/model"
)

// CounterSink receives aggregated per-device detection counts.
type CounterSink interface {
	AddDetections(ctx context.Context, counts map[string]int64) error
}

// Consume reads the detections topic as part of consumer group groupID and
// flushes per-device counts to sink once per second. Offsets are committed only
// after a successful flush, giving at-least-once counting.
func Consume(ctx context.Context, brokers []string, topic, groupID string, sink CounterSink, onEvents func(int), log *slog.Logger) error {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers, Topic: topic, GroupID: groupID,
		MinBytes: 1 << 10, MaxBytes: 10 << 20, MaxWait: 500 * time.Millisecond,
	})
	defer r.Close()

	var (
		mu      sync.Mutex
		counts  = map[string]int64{}
		pending []kafka.Message
	)
	flush := func() {
		mu.Lock()
		c, msgs := counts, pending
		counts, pending = map[string]int64{}, nil
		mu.Unlock()
		if len(msgs) == 0 {
			return
		}
		if err := sink.AddDetections(ctx, c); err != nil {
			log.Error("detection flush failed", "err", err)
			return
		}
		if err := r.CommitMessages(ctx, msgs...); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("kafka commit failed", "err", err)
		}
		if onEvents != nil {
			onEvents(len(msgs))
		}
	}
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				flush()
			}
		}
	}()
	log.Info("kafka consumer started", "brokers", brokers, "topic", topic, "group", groupID)
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				flush()
				return nil
			}
			return err
		}
		var ev model.DetectionEvent
		if err := json.Unmarshal(m.Value, &ev); err != nil || ev.DeviceID == "" {
			ev.DeviceID = string(m.Key)
		}
		mu.Lock()
		if ev.DeviceID != "" {
			counts[ev.DeviceID]++
		}
		pending = append(pending, m)
		mu.Unlock()
	}
}
