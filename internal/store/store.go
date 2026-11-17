// Package store persists device state, pending commands and rollouts.
// Redis is used in production; Memory is used in tests and for quick local runs.
package store

import (
	"context"
	"errors"

	"github.com/sakshat-patil/edgefleet/internal/model"
)

// ErrNotFound is returned when a device or rollout does not exist.
var ErrNotFound = errors.New("not found")

// Store is the persistence interface used by the control plane.
type Store interface {
	PutDevice(ctx context.Context, d *model.Device) error
	GetDevice(ctx context.Context, id string) (*model.Device, error)
	ListDevices(ctx context.Context, group string) ([]*model.Device, error)
	DeleteDevice(ctx context.Context, id string) error
	Groups(ctx context.Context) (map[string]int, error)
	// UpdateDevice applies fn to the stored device atomically.
	UpdateDevice(ctx context.Context, id string, fn func(*model.Device) error) (*model.Device, error)

	PushCommand(ctx context.Context, c model.Command) error
	PopCommands(ctx context.Context, deviceID string, max int) ([]model.Command, error)

	AddDetections(ctx context.Context, counts map[string]int64) error

	PutRollout(ctx context.Context, r *model.Rollout) error
	GetRollout(ctx context.Context, id string) (*model.Rollout, error)
	ListRollouts(ctx context.Context) ([]*model.Rollout, error)

	Ping(ctx context.Context) error
}
