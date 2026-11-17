package store

import (
	"context"
	"encoding/json"
	"sort"
	"sync"

	"github.com/sakshat-patil/edgefleet/internal/model"
)

// Memory is an in-process Store. Values are deep-copied via JSON so callers
// cannot mutate stored state, matching Redis semantics.
type Memory struct {
	mu       sync.Mutex
	devices  map[string][]byte
	cmds     map[string][]model.Command
	rollouts map[string][]byte
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{devices: map[string][]byte{}, cmds: map[string][]model.Command{}, rollouts: map[string][]byte{}}
}

func clone[T any](b []byte) (*T, error) {
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

func (m *Memory) PutDevice(_ context.Context, d *model.Device) error {
	b, err := json.Marshal(d)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.devices[d.ID] = b
	m.mu.Unlock()
	return nil
}

func (m *Memory) GetDevice(_ context.Context, id string) (*model.Device, error) {
	m.mu.Lock()
	b, ok := m.devices[id]
	m.mu.Unlock()
	if !ok {
		return nil, ErrNotFound
	}
	return clone[model.Device](b)
}

func (m *Memory) ListDevices(_ context.Context, group string) ([]*model.Device, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []*model.Device{}
	for _, b := range m.devices {
		d, err := clone[model.Device](b)
		if err != nil {
			return nil, err
		}
		if group == "" || d.Group == group {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (m *Memory) DeleteDevice(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.devices[id]; !ok {
		return ErrNotFound
	}
	delete(m.devices, id)
	delete(m.cmds, id)
	return nil
}

func (m *Memory) Groups(ctx context.Context) (map[string]int, error) {
	ds, err := m.ListDevices(ctx, "")
	if err != nil {
		return nil, err
	}
	g := map[string]int{}
	for _, d := range ds {
		g[d.Group]++
	}
	return g, nil
}

func (m *Memory) UpdateDevice(_ context.Context, id string, fn func(*model.Device) error) (*model.Device, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.devices[id]
	if !ok {
		return nil, ErrNotFound
	}
	d, err := clone[model.Device](b)
	if err != nil {
		return nil, err
	}
	if err := fn(d); err != nil {
		return nil, err
	}
	nb, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}
	m.devices[id] = nb
	return d, nil
}

func (m *Memory) PushCommand(_ context.Context, c model.Command) error {
	m.mu.Lock()
	m.cmds[c.DeviceID] = append(m.cmds[c.DeviceID], c)
	m.mu.Unlock()
	return nil
}

func (m *Memory) PopCommands(_ context.Context, deviceID string, max int) ([]model.Command, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	q := m.cmds[deviceID]
	if len(q) > max {
		m.cmds[deviceID] = q[max:]
		return q[:max], nil
	}
	delete(m.cmds, deviceID)
	return q, nil
}

func (m *Memory) AddDetections(ctx context.Context, counts map[string]int64) error {
	for id, n := range counts {
		_, err := m.UpdateDevice(ctx, id, func(d *model.Device) error { d.Detections += n; return nil })
		if err != nil && err != ErrNotFound {
			return err
		}
	}
	return nil
}

func (m *Memory) PutRollout(_ context.Context, r *model.Rollout) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.rollouts[r.ID] = b
	m.mu.Unlock()
	return nil
}

func (m *Memory) GetRollout(_ context.Context, id string) (*model.Rollout, error) {
	m.mu.Lock()
	b, ok := m.rollouts[id]
	m.mu.Unlock()
	if !ok {
		return nil, ErrNotFound
	}
	return clone[model.Rollout](b)
}

func (m *Memory) ListRollouts(_ context.Context) ([]*model.Rollout, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []*model.Rollout{}
	for _, b := range m.rollouts {
		r, err := clone[model.Rollout](b)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (m *Memory) Ping(context.Context) error { return nil }
