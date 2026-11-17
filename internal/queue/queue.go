// Package queue carries device commands from the API to the dispatcher.
// AWS SQS gives durability, retries and a dead-letter queue; Memory is for tests.
package queue

import (
	"context"

	"github.com/sakshat-patil/edgefleet/internal/model"
)

// Message is a received command plus the handle needed to acknowledge it.
type Message struct {
	Command model.Command
	Receipt string
}

// Queue is a durable command queue.
type Queue interface {
	Send(ctx context.Context, cmds ...model.Command) error
	Receive(ctx context.Context, max int) ([]Message, error)
	Ack(ctx context.Context, receipts ...string) error
}

// Memory is a buffered-channel queue. Ack is a no-op.
type Memory struct{ ch chan model.Command }

// NewMemory returns a queue that buffers up to size commands.
func NewMemory(size int) *Memory { return &Memory{ch: make(chan model.Command, size)} }

func (m *Memory) Send(ctx context.Context, cmds ...model.Command) error {
	for _, c := range cmds {
		select {
		case m.ch <- c:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (m *Memory) Receive(ctx context.Context, max int) ([]Message, error) {
	var out []Message
	select {
	case c := <-m.ch:
		out = append(out, Message{Command: c})
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	for len(out) < max {
		select {
		case c := <-m.ch:
			out = append(out, Message{Command: c})
		default:
			return out, nil
		}
	}
	return out, nil
}

func (m *Memory) Ack(context.Context, ...string) error { return nil }
