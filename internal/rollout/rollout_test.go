package rollout

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/sakshat-patil/edgefleet/internal/model"
	"github.com/sakshat-patil/edgefleet/internal/queue"
	"github.com/sakshat-patil/edgefleet/internal/store"
)

type fixture struct {
	ctx context.Context
	s   *store.Memory
	q   *queue.Memory
	e   *Engine
	now time.Time
}

func newFixture(t *testing.T, n int) *fixture {
	t.Helper()
	f := &fixture{ctx: context.Background(), s: store.NewMemory(), q: queue.NewMemory(10000), now: time.Unix(1_700_000_000, 0)}
	f.e = New(f.s, f.q, slog.New(slog.NewTextHandler(io.Discard, nil)))
	f.e.Now = func() time.Time { return f.now }
	for i := 0; i < n; i++ {
		_ = f.s.PutDevice(f.ctx, &model.Device{ID: fmt.Sprintf("cam-%03d", i), Group: "lobby", FirmwareVersion: "1.0.0", FirmwareState: model.FirmwareOK, LastSeen: f.now})
	}
	return f
}

// drain returns queued commands and applies them like a well-behaved device
// would: failing firmware updates for any device in fail.
func (f *fixture) drain(t *testing.T, fail map[string]bool) []model.Command {
	t.Helper()
	var out []model.Command
	for {
		ctx, cancel := context.WithTimeout(f.ctx, 10*time.Millisecond)
		msgs, err := f.q.Receive(ctx, 100)
		cancel()
		if err != nil {
			return out
		}
		for _, m := range msgs {
			c := m.Command
			out = append(out, c)
			_, _ = f.s.UpdateDevice(f.ctx, c.DeviceID, func(d *model.Device) error {
				d.LastSeen = f.now
				switch c.Type {
				case model.CmdUpdateFirmware:
					if fail[c.DeviceID] {
						d.FirmwareState = model.FirmwareFailed
					} else {
						d.FirmwareVersion, d.FirmwareState = c.Payload["version"], model.FirmwareOK
					}
				case model.CmdRollbackFirmware:
					d.FirmwareVersion, d.FirmwareState = c.Payload["version"], model.FirmwareOK
				}
				return nil
			})
		}
	}
}

func (f *fixture) tick(d time.Duration) {
	f.now = f.now.Add(d)
	ds, _ := f.s.ListDevices(f.ctx, "")
	for _, dev := range ds {
		_, _ = f.s.UpdateDevice(f.ctx, dev.ID, func(x *model.Device) error { x.LastSeen = f.now; return nil })
	}
}

func TestStagedRolloutCompletes(t *testing.T) {
	f := newFixture(t, 100)
	r, err := f.e.Create(f.ctx, Request{Group: "lobby", TargetVersion: "1.1.0", Stages: []int{5, 25, 100}, SoakSeconds: 10})
	if err != nil {
		t.Fatal(err)
	}
	wantStage := []int{5, 20, 75}
	for stage, want := range wantStage {
		if got := len(f.drain(t, nil)); got != want {
			t.Fatalf("stage %d: got %d update commands, want %d", stage, got, want)
		}
		// Not promoted before the soak period ends.
		r, _ = f.e.Reconcile(f.ctx, r.ID)
		if r.CurrentStage != stage {
			t.Fatalf("promoted before soak: stage %d", r.CurrentStage)
		}
		f.tick(11 * time.Second)
		r, err = f.e.Reconcile(f.ctx, r.ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	if r.Status != model.RolloutCompleted {
		t.Fatalf("status %s, want completed", r.Status)
	}
	ds, _ := f.s.ListDevices(f.ctx, "lobby")
	for _, d := range ds {
		if d.FirmwareVersion != "1.1.0" {
			t.Fatalf("%s on %s", d.ID, d.FirmwareVersion)
		}
	}
}

func TestAutoRollbackOnCanaryFailure(t *testing.T) {
	f := newFixture(t, 100)
	r, err := f.e.Create(f.ctx, Request{Group: "lobby", TargetVersion: "2.0.0-bad", Stages: []int{10, 100}, MaxFailureRate: 0.2})
	if err != nil {
		t.Fatal(err)
	}
	// 3 of the 10 canaries fail -> 30% > 20%.
	f.drain(t, map[string]bool{"cam-000": true, "cam-001": true, "cam-002": true})
	r, err = f.e.Reconcile(f.ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != model.RolloutRolledBack {
		t.Fatalf("status %s, want rolled_back", r.Status)
	}
	if len(r.Failed) != 3 {
		t.Fatalf("failed=%d want 3", len(r.Failed))
	}
	cmds := f.drain(t, nil)
	if len(cmds) != 10 {
		t.Fatalf("got %d rollback commands, want 10", len(cmds))
	}
	ds, _ := f.s.ListDevices(f.ctx, "lobby")
	for _, d := range ds {
		if d.FirmwareVersion != "1.0.0" {
			t.Fatalf("%s left on %s after rollback", d.ID, d.FirmwareVersion)
		}
	}
}

func TestPausedRolloutDoesNotAdvance(t *testing.T) {
	f := newFixture(t, 20)
	r, _ := f.e.Create(f.ctx, Request{Group: "lobby", TargetVersion: "1.1.0", Stages: []int{50, 100}, SoakSeconds: 1})
	f.drain(t, nil)
	if _, err := f.e.Pause(f.ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	f.tick(time.Minute)
	r, _ = f.e.Reconcile(f.ctx, r.ID)
	if r.CurrentStage != 0 || r.Status != model.RolloutPaused {
		t.Fatalf("paused rollout moved: stage=%d status=%s", r.CurrentStage, r.Status)
	}
}

func TestValidation(t *testing.T) {
	f := newFixture(t, 1)
	bad := []Request{
		{Group: "lobby"},
		{Group: "lobby", TargetVersion: "x", Stages: []int{50, 40, 100}},
		{Group: "lobby", TargetVersion: "x", Stages: []int{10, 50}},
		{Group: "empty", TargetVersion: "x"},
		{Group: "lobby", TargetVersion: "1.0.0"},
	}
	for i, req := range bad {
		if _, err := f.e.Create(f.ctx, req); err == nil {
			t.Fatalf("case %d: expected error", i)
		}
	}
}
