package queue

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/sakshat-patil/edgefleet/internal/model"
)

// Sink receives commands from the queue (the store's per-device pending list).
type Sink interface {
	PushCommand(ctx context.Context, c model.Command) error
}

// Dispatch consumes commands from q and moves them into each device's pending
// list in sink. A message is acked only after it is safely stored, so a crash
// redelivers it (at-least-once; devices dedupe on Command.ID).
func Dispatch(ctx context.Context, q Queue, sink Sink, log *slog.Logger) {
	for ctx.Err() == nil {
		msgs, err := q.Receive(ctx, 10)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Error("dispatch receive failed", "err", err)
			time.Sleep(time.Second)
			continue
		}
		var acks []string
		for _, m := range msgs {
			if err := sink.PushCommand(ctx, m.Command); err != nil {
				log.Error("dispatch store failed", "err", err, "device_id", m.Command.DeviceID, "command_id", m.Command.ID)
				continue
			}
			if m.Receipt != "" {
				acks = append(acks, m.Receipt)
			}
		}
		if len(acks) > 0 {
			if err := q.Ack(ctx, acks...); err != nil {
				log.Error("dispatch ack failed", "err", err)
			}
		}
	}
}
