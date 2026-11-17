// Package rollout implements staged (canary) firmware rollouts with automatic
// health-gated promotion and automatic rollback.
//
// A rollout targets one device group. Devices are ordered healthy-first and
// released in cumulative stages (default 5% -> 25% -> 100%). After a stage is
// released the reconciler waits until every device in it has reported the new
// version (or failed), then waits SoakSeconds more. If at any point the share
// of failed devices exceeds MaxFailureRate, every device touched so far is sent
// a rollback_firmware command and the rollout is marked rolled_back.
package rollout

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"time"

	"github.com/sakshat-patil/edgefleet/internal/health"
	"github.com/sakshat-patil/edgefleet/internal/model"
	"github.com/sakshat-patil/edgefleet/internal/queue"
	"github.com/sakshat-patil/edgefleet/internal/store"
)

// Errors returned to API callers.
var (
	ErrInvalid   = errors.New("invalid rollout request")
	ErrNoDevices = errors.New("no devices in group need this version")
	ErrState     = errors.New("rollout is not in a state that allows this")
)

// Request creates a rollout.
type Request struct {
	Group               string  `json:"group"`
	TargetVersion       string  `json:"target_version"`
	FirmwareURL         string  `json:"firmware_url"`
	Stages              []int   `json:"stages,omitempty"`
	MaxFailureRate      float64 `json:"max_failure_rate,omitempty"`
	SoakSeconds         int     `json:"soak_seconds,omitempty"`
	StageTimeoutSeconds int     `json:"stage_timeout_seconds,omitempty"`
}

// Engine creates and drives rollouts.
type Engine struct {
	Store        store.Store
	Queue        queue.Queue
	Log          *slog.Logger
	Thresholds   health.Thresholds
	StageTimeout time.Duration
	Now          func() time.Time
}

// New returns an Engine with sane defaults.
func New(s store.Store, q queue.Queue, log *slog.Logger) *Engine {
	return &Engine{Store: s, Queue: q, Log: log, Thresholds: health.DefaultThresholds, StageTimeout: 10 * time.Minute, Now: time.Now}
}

func validateStages(st []int) error {
	if len(st) == 0 {
		return fmt.Errorf("%w: stages must not be empty", ErrInvalid)
	}
	prev := 0
	for _, p := range st {
		if p <= prev || p > 100 {
			return fmt.Errorf("%w: stages must be strictly increasing percentages in (0,100]", ErrInvalid)
		}
		prev = p
	}
	if prev != 100 {
		return fmt.Errorf("%w: last stage must be 100", ErrInvalid)
	}
	return nil
}

// Create validates req, selects candidate devices and releases the first stage.
func (e *Engine) Create(ctx context.Context, req Request) (*model.Rollout, error) {
	if req.Group == "" || req.TargetVersion == "" {
		return nil, fmt.Errorf("%w: group and target_version are required", ErrInvalid)
	}
	if req.Stages == nil {
		req.Stages = []int{5, 25, 100}
	}
	if err := validateStages(req.Stages); err != nil {
		return nil, err
	}
	if req.MaxFailureRate == 0 {
		req.MaxFailureRate = 0.10
	}
	if req.MaxFailureRate < 0 || req.MaxFailureRate >= 1 {
		return nil, fmt.Errorf("%w: max_failure_rate must be in [0,1)", ErrInvalid)
	}
	if req.SoakSeconds == 0 {
		req.SoakSeconds = 30
	}
	devices, err := e.Store.ListDevices(ctx, req.Group)
	if err != nil {
		return nil, err
	}
	now := e.Now()
	type cand struct {
		id      string
		healthy bool
	}
	var cs []cand
	for _, d := range devices {
		if d.FirmwareVersion == req.TargetVersion {
			continue
		}
		cs = append(cs, cand{d.ID, health.Evaluate(d, e.Thresholds, now).State == health.Healthy})
	}
	if len(cs) == 0 {
		return nil, ErrNoDevices
	}
	// Healthy devices go first so the canary stage is not polluted by
	// devices that were already failing.
	sort.SliceStable(cs, func(i, j int) bool {
		if cs[i].healthy != cs[j].healthy {
			return cs[i].healthy
		}
		return cs[i].id < cs[j].id
	})
	r := &model.Rollout{
		ID: model.NewID("ro"), Group: req.Group, TargetVersion: req.TargetVersion, FirmwareURL: req.FirmwareURL,
		Stages: req.Stages, MaxFailureRate: req.MaxFailureRate, SoakSeconds: req.SoakSeconds,
		Status: model.RolloutInProgress, CreatedAt: now,
	}
	for _, c := range cs {
		r.Candidates = append(r.Candidates, c.id)
	}
	if err := e.releaseStage(ctx, r); err != nil {
		return nil, err
	}
	e.Log.Info("rollout created", "rollout_id", r.ID, "group", r.Group, "target", r.TargetVersion, "candidates", len(r.Candidates), "stages", r.Stages)
	return r, nil
}

// stageSize is the cumulative number of devices released by stage i.
func stageSize(r *model.Rollout, i int) int {
	return int(math.Ceil(float64(r.Stages[i]) * float64(len(r.Candidates)) / 100))
}

func (e *Engine) releaseStage(ctx context.Context, r *model.Rollout) error {
	n := stageSize(r, r.CurrentStage)
	var cmds []model.Command
	now := e.Now()
	for _, id := range r.Candidates[len(r.Updated):n] {
		_, err := e.Store.UpdateDevice(ctx, id, func(d *model.Device) error {
			d.PreviousFirmware = d.FirmwareVersion
			d.TargetFirmware = r.TargetVersion
			return nil
		})
		if errors.Is(err, store.ErrNotFound) {
			r.Failed = append(r.Failed, id)
			r.Updated = append(r.Updated, id)
			continue
		}
		if err != nil {
			return err
		}
		cmds = append(cmds, model.Command{
			ID: model.NewID("cmd"), DeviceID: id, Type: model.CmdUpdateFirmware, RolloutID: r.ID, CreatedAt: now,
			Payload: map[string]string{"version": r.TargetVersion, "url": r.FirmwareURL},
		})
		r.Updated = append(r.Updated, id)
	}
	if err := e.Queue.Send(ctx, cmds...); err != nil {
		return err
	}
	r.StageStartedAt = now
	r.UpdatedAt = now
	e.Log.Info("rollout stage released", "rollout_id", r.ID, "stage", r.CurrentStage, "percent", r.Stages[r.CurrentStage], "devices", len(cmds), "cumulative", len(r.Updated))
	return e.Store.PutRollout(ctx, r)
}

// Reconcile advances a single in-progress rollout one step.
func (e *Engine) Reconcile(ctx context.Context, id string) (*model.Rollout, error) {
	r, err := e.Store.GetRollout(ctx, id)
	if err != nil {
		return nil, err
	}
	if r.Status != model.RolloutInProgress {
		return r, nil
	}
	now := e.Now()
	timedOut := now.Sub(r.StageStartedAt) > e.StageTimeout
	failed := map[string]bool{}
	for _, f := range r.Failed {
		failed[f] = true
	}
	pending := 0
	for _, id := range r.Updated {
		if failed[id] {
			continue
		}
		d, err := e.Store.GetDevice(ctx, id)
		if errors.Is(err, store.ErrNotFound) {
			failed[id] = true
			continue
		}
		if err != nil {
			return nil, err
		}
		h := health.Evaluate(d, e.Thresholds, now)
		switch {
		case d.FirmwareState == model.FirmwareFailed:
			failed[id] = true
		case d.FirmwareVersion == r.TargetVersion && d.FirmwareState == model.FirmwareOK && h.State != health.Offline:
			// succeeded
		case timedOut:
			failed[id] = true
		default:
			pending++
		}
	}
	r.Failed = r.Failed[:0]
	for _, id := range r.Updated {
		if failed[id] {
			r.Failed = append(r.Failed, id)
		}
	}
	rate := float64(len(r.Failed)) / float64(len(r.Updated))
	if rate > r.MaxFailureRate {
		return r, e.rollback(ctx, r, fmt.Sprintf("failure rate %.1f%% exceeded %.1f%% at stage %d (%d%%)", rate*100, r.MaxFailureRate*100, r.CurrentStage, r.Stages[r.CurrentStage]))
	}
	if pending > 0 || now.Sub(r.StageStartedAt) < time.Duration(r.SoakSeconds)*time.Second {
		r.UpdatedAt = now
		return r, e.Store.PutRollout(ctx, r)
	}
	if r.CurrentStage == len(r.Stages)-1 {
		r.Status = model.RolloutCompleted
		r.UpdatedAt = now
		e.Log.Info("rollout completed", "rollout_id", r.ID, "devices", len(r.Updated), "failed", len(r.Failed))
		return r, e.Store.PutRollout(ctx, r)
	}
	r.CurrentStage++
	return r, e.releaseStage(ctx, r)
}

// Pause stops promotion; devices already updated keep the new version.
func (e *Engine) Pause(ctx context.Context, id string) (*model.Rollout, error) {
	return e.setStatus(ctx, id, model.RolloutInProgress, model.RolloutPaused)
}

// Resume continues a paused rollout.
func (e *Engine) Resume(ctx context.Context, id string) (*model.Rollout, error) {
	return e.setStatus(ctx, id, model.RolloutPaused, model.RolloutInProgress)
}

func (e *Engine) setStatus(ctx context.Context, id, from, to string) (*model.Rollout, error) {
	r, err := e.Store.GetRollout(ctx, id)
	if err != nil {
		return nil, err
	}
	if r.Status != from {
		return nil, fmt.Errorf("%w: status is %s", ErrState, r.Status)
	}
	r.Status = to
	r.UpdatedAt = e.Now()
	e.Log.Info("rollout status changed", "rollout_id", id, "from", from, "to", to)
	return r, e.Store.PutRollout(ctx, r)
}

// Rollback reverts every device the rollout has touched.
func (e *Engine) Rollback(ctx context.Context, id, reason string) (*model.Rollout, error) {
	r, err := e.Store.GetRollout(ctx, id)
	if err != nil {
		return nil, err
	}
	if r.Status == model.RolloutRolledBack {
		return nil, fmt.Errorf("%w: already rolled back", ErrState)
	}
	if reason == "" {
		reason = "operator requested rollback"
	}
	return r, e.rollback(ctx, r, reason)
}

func (e *Engine) rollback(ctx context.Context, r *model.Rollout, reason string) error {
	var cmds []model.Command
	now := e.Now()
	for _, id := range r.Updated {
		c, err := e.rollbackCommand(ctx, id, r.ID)
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if c != nil {
			cmds = append(cmds, *c)
		}
	}
	if err := e.Queue.Send(ctx, cmds...); err != nil {
		return err
	}
	r.Status = model.RolloutRolledBack
	r.Reason = reason
	r.UpdatedAt = now
	e.Log.Warn("rollout rolled back", "rollout_id", r.ID, "reason", reason, "devices", len(cmds))
	return e.Store.PutRollout(ctx, r)
}

func (e *Engine) rollbackCommand(ctx context.Context, deviceID, rolloutID string) (*model.Command, error) {
	var prev string
	_, err := e.Store.UpdateDevice(ctx, deviceID, func(d *model.Device) error {
		prev = d.PreviousFirmware
		if prev != "" {
			d.TargetFirmware = prev
		}
		return nil
	})
	if err != nil || prev == "" {
		return nil, err
	}
	return &model.Command{
		ID: model.NewID("cmd"), DeviceID: deviceID, Type: model.CmdRollbackFirmware, RolloutID: rolloutID,
		CreatedAt: e.Now(), Payload: map[string]string{"version": prev},
	}, nil
}

// RollbackDevice reverts a single device to its previous firmware.
func (e *Engine) RollbackDevice(ctx context.Context, deviceID string) (*model.Command, error) {
	c, err := e.rollbackCommand(ctx, deviceID, "")
	if err != nil {
		return nil, err
	}
	if c == nil {
		return nil, fmt.Errorf("%w: device has no previous firmware", ErrState)
	}
	return c, e.Queue.Send(ctx, *c)
}

// Run reconciles all in-progress rollouts every interval until ctx is done.
func (e *Engine) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		rs, err := e.Store.ListRollouts(ctx)
		if err != nil {
			e.Log.Error("list rollouts failed", "err", err)
			continue
		}
		for _, r := range rs {
			if r.Status != model.RolloutInProgress {
				continue
			}
			if _, err := e.Reconcile(ctx, r.ID); err != nil {
				e.Log.Error("reconcile failed", "rollout_id", r.ID, "err", err)
			}
		}
	}
}
