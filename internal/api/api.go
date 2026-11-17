// Package api exposes the operator and device-agent REST APIs.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/sakshat-patil/edgefleet/internal/health"
	"github.com/sakshat-patil/edgefleet/internal/logging"
	"github.com/sakshat-patil/edgefleet/internal/model"
	"github.com/sakshat-patil/edgefleet/internal/queue"
	"github.com/sakshat-patil/edgefleet/internal/rollout"
	"github.com/sakshat-patil/edgefleet/internal/store"
)

// Server holds the API dependencies.
type Server struct {
	Store    store.Store
	Queue    queue.Queue
	Rollouts *rollout.Engine
	Log      *slog.Logger
	Metrics  *Metrics
	// OperatorToken, when set, is required as "Authorization: Bearer <token>"
	// on all /v1 operator routes. Device routes use per-fleet DeviceToken.
	OperatorToken string
	DeviceToken   string
	Now           func() time.Time
}

// Handler builds the HTTP router.
func (s *Server) Handler() http.Handler {
	if s.Now == nil {
		s.Now = time.Now
	}
	if s.Metrics == nil {
		s.Metrics = NewMetrics()
	}
	mux := http.NewServeMux()
	op := func(pattern string, h http.HandlerFunc) { mux.Handle(pattern, s.auth(s.OperatorToken, h)) }
	dev := func(pattern string, h http.HandlerFunc) { mux.Handle(pattern, s.auth(s.DeviceToken, h)) }

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /readyz", s.ready)
	mux.HandleFunc("GET /metrics", s.metrics)

	// Operator API
	op("POST /v1/devices", s.registerDevice)
	op("GET /v1/devices", s.listDevices)
	op("GET /v1/devices/{id}", s.getDevice)
	op("DELETE /v1/devices/{id}", s.deleteDevice)
	op("GET /v1/devices/{id}/health", s.deviceHealth)
	op("PUT /v1/devices/{id}/group", s.setGroup)
	op("PATCH /v1/devices/{id}/config", s.deviceConfig)
	op("POST /v1/devices/{id}/rollback", s.deviceRollback)
	op("POST /v1/devices/{id}/reboot", s.deviceReboot)
	op("GET /v1/groups", s.listGroups)
	op("GET /v1/groups/{group}/health", s.groupHealth)
	op("PATCH /v1/groups/{group}/config", s.groupConfig)
	op("POST /v1/rollouts", s.createRollout)
	op("GET /v1/rollouts", s.listRollouts)
	op("GET /v1/rollouts/{id}", s.getRollout)
	op("POST /v1/rollouts/{id}/pause", s.rolloutAction)
	op("POST /v1/rollouts/{id}/resume", s.rolloutAction)
	op("POST /v1/rollouts/{id}/rollback", s.rolloutAction)

	// Device agent API (ESP32 firmware, cv-agent, simulator)
	dev("POST /v1/agent/{id}/heartbeat", s.heartbeat)

	return logging.Middleware(s.Log, s.Metrics.Observe, mux)
}

func (s *Server) auth(token string, h http.HandlerFunc) http.Handler {
	if token == "" {
		return h
	}
	want := "Bearer " + token
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != want {
			writeErr(w, http.StatusUnauthorized, "missing or invalid bearer token")
			return
		}
		h(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, err.Error())
	case errors.Is(err, rollout.ErrInvalid), errors.Is(err, rollout.ErrNoDevices):
		writeErr(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, rollout.ErrState):
		writeErr(w, http.StatusConflict, err.Error())
	default:
		logging.From(r.Context(), s.Log).Error("request failed", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
	}
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return false
	}
	return true
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.Store.Ping(ctx); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "store unavailable: "+err.Error())
		return
	}
	w.Write([]byte("ready"))
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	gauges := map[string]float64{}
	if ds, err := s.Store.ListDevices(r.Context(), ""); err == nil {
		now := s.Now()
		counts := map[string]float64{health.Healthy: 0, health.Degraded: 0, health.Offline: 0}
		for _, d := range ds {
			counts[health.Evaluate(d, health.DefaultThresholds, now).State]++
		}
		for k, v := range counts {
			gauges["edgefleet_devices_"+k] = v
		}
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	s.Metrics.Write(w, gauges)
}

type registerReq struct {
	ID              string            `json:"id"`
	Kind            string            `json:"kind"`
	Group           string            `json:"group"`
	FirmwareVersion string            `json:"firmware_version"`
	Config          map[string]string `json:"config"`
}

func (s *Server) registerDevice(w http.ResponseWriter, r *http.Request) {
	var req registerReq
	if !decode(w, r, &req) {
		return
	}
	if req.ID == "" || req.FirmwareVersion == "" {
		writeErr(w, http.StatusBadRequest, "id and firmware_version are required")
		return
	}
	if req.Group == "" {
		req.Group = "default"
	}
	if req.Kind == "" {
		req.Kind = model.KindSim
	}
	if req.Config == nil {
		req.Config = map[string]string{}
	}
	if _, err := s.Store.GetDevice(r.Context(), req.ID); err == nil {
		writeErr(w, http.StatusConflict, "device already registered")
		return
	}
	d := &model.Device{
		ID: req.ID, Kind: req.Kind, Group: req.Group, FirmwareVersion: req.FirmwareVersion,
		FirmwareState: model.FirmwareOK, Config: req.Config, RegisteredAt: s.Now(),
	}
	if err := s.Store.PutDevice(r.Context(), d); err != nil {
		s.fail(w, r, err)
		return
	}
	logging.From(r.Context(), s.Log).Info("device registered", "device_id", d.ID, "group", d.Group, "kind", d.Kind)
	writeJSON(w, http.StatusCreated, d)
}

func (s *Server) listDevices(w http.ResponseWriter, r *http.Request) {
	ds, err := s.Store.ListDevices(r.Context(), r.URL.Query().Get("group"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if st := r.URL.Query().Get("health"); st != "" {
		now := s.Now()
		out := ds[:0]
		for _, d := range ds {
			if health.Evaluate(d, health.DefaultThresholds, now).State == st {
				out = append(out, d)
			}
		}
		ds = out
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit > 0 && limit < len(ds) {
		ds = ds[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{"count": len(ds), "devices": ds})
}

func (s *Server) getDevice(w http.ResponseWriter, r *http.Request) {
	d, err := s.Store.GetDevice(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (s *Server) deleteDevice(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.DeleteDevice(r.Context(), r.PathValue("id")); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) deviceHealth(w http.ResponseWriter, r *http.Request) {
	d, err := s.Store.GetDevice(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, health.Evaluate(d, health.DefaultThresholds, s.Now()))
}

func (s *Server) setGroup(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Group string `json:"group"`
	}
	if !decode(w, r, &req) {
		return
	}
	if req.Group == "" {
		writeErr(w, http.StatusBadRequest, "group is required")
		return
	}
	d, err := s.Store.UpdateDevice(r.Context(), r.PathValue("id"), func(d *model.Device) error { d.Group = req.Group; return nil })
	if err != nil {
		s.fail(w, r, err)
		return
	}
	logging.From(r.Context(), s.Log).Info("device regrouped", "device_id", d.ID, "group", d.Group)
	writeJSON(w, http.StatusOK, d)
}

func (s *Server) listGroups(w http.ResponseWriter, r *http.Request) {
	g, err := s.Store.Groups(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, g)
}

func (s *Server) groupHealth(w http.ResponseWriter, r *http.Request) {
	ds, err := s.Store.ListDevices(r.Context(), r.PathValue("group"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	now := s.Now()
	summary := map[string]int{health.Healthy: 0, health.Degraded: 0, health.Offline: 0}
	var unhealthy []health.Report
	for _, d := range ds {
		rep := health.Evaluate(d, health.DefaultThresholds, now)
		summary[rep.State]++
		if rep.State != health.Healthy {
			unhealthy = append(unhealthy, rep)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"group": r.PathValue("group"), "total": len(ds), "summary": summary, "unhealthy": unhealthy})
}

// configChange merges cfg into each device's desired config, bumps the config
// version and queues a set_config command carrying the full desired config.
func (s *Server) configChange(ctx context.Context, ids []string, cfg map[string]string) (int, error) {
	var cmds []model.Command
	now := s.Now()
	for _, id := range ids {
		d, err := s.Store.UpdateDevice(ctx, id, func(d *model.Device) error {
			if d.Config == nil {
				d.Config = map[string]string{}
			}
			for k, v := range cfg {
				if v == "" {
					delete(d.Config, k)
				} else {
					d.Config[k] = v
				}
			}
			d.ConfigVersion++
			return nil
		})
		if err != nil {
			return 0, err
		}
		payload := map[string]string{"config_version": strconv.FormatInt(d.ConfigVersion, 10)}
		for k, v := range d.Config {
			payload[k] = v
		}
		cmds = append(cmds, model.Command{ID: model.NewID("cmd"), DeviceID: id, Type: model.CmdSetConfig, Payload: payload, CreatedAt: now})
	}
	if err := s.Queue.Send(ctx, cmds...); err != nil {
		return 0, err
	}
	s.Metrics.CommandsQueued.Add(int64(len(cmds)))
	return len(cmds), nil
}

func (s *Server) deviceConfig(w http.ResponseWriter, r *http.Request) {
	var cfg map[string]string
	if !decode(w, r, &cfg) {
		return
	}
	if _, err := s.configChange(r.Context(), []string{r.PathValue("id")}, cfg); err != nil {
		s.fail(w, r, err)
		return
	}
	d, _ := s.Store.GetDevice(r.Context(), r.PathValue("id"))
	writeJSON(w, http.StatusAccepted, d)
}

func (s *Server) groupConfig(w http.ResponseWriter, r *http.Request) {
	var cfg map[string]string
	if !decode(w, r, &cfg) {
		return
	}
	ds, err := s.Store.ListDevices(r.Context(), r.PathValue("group"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if len(ds) == 0 {
		writeErr(w, http.StatusNotFound, "group has no devices")
		return
	}
	ids := make([]string, len(ds))
	for i, d := range ds {
		ids[i] = d.ID
	}
	n, err := s.configChange(r.Context(), ids, cfg)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	logging.From(r.Context(), s.Log).Info("group config pushed", "group", r.PathValue("group"), "devices", n, "keys", len(cfg))
	writeJSON(w, http.StatusAccepted, map[string]any{"group": r.PathValue("group"), "commands_queued": n})
}

func (s *Server) deviceRollback(w http.ResponseWriter, r *http.Request) {
	c, err := s.Rollouts.RollbackDevice(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.Metrics.CommandsQueued.Add(1)
	writeJSON(w, http.StatusAccepted, c)
}

func (s *Server) deviceReboot(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.Store.GetDevice(r.Context(), id); err != nil {
		s.fail(w, r, err)
		return
	}
	c := model.Command{ID: model.NewID("cmd"), DeviceID: id, Type: model.CmdReboot, CreatedAt: s.Now()}
	if err := s.Queue.Send(r.Context(), c); err != nil {
		s.fail(w, r, err)
		return
	}
	s.Metrics.CommandsQueued.Add(1)
	writeJSON(w, http.StatusAccepted, c)
}

func (s *Server) createRollout(w http.ResponseWriter, r *http.Request) {
	var req rollout.Request
	if !decode(w, r, &req) {
		return
	}
	ro, err := s.Rollouts.Create(r.Context(), req)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, ro)
}

func (s *Server) listRollouts(w http.ResponseWriter, r *http.Request) {
	rs, err := s.Store.ListRollouts(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, rs)
}

func (s *Server) getRollout(w http.ResponseWriter, r *http.Request) {
	ro, err := s.Store.GetRollout(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, ro)
}

func (s *Server) rolloutAction(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var (
		ro  *model.Rollout
		err error
	)
	switch path := r.URL.Path; {
	case hasSuffix(path, "/pause"):
		ro, err = s.Rollouts.Pause(r.Context(), id)
	case hasSuffix(path, "/resume"):
		ro, err = s.Rollouts.Resume(r.Context(), id)
	default:
		ro, err = s.Rollouts.Rollback(r.Context(), id, r.URL.Query().Get("reason"))
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, ro)
}

func hasSuffix(s, suf string) bool { return len(s) >= len(suf) && s[len(s)-len(suf):] == suf }

// heartbeat records device telemetry and returns any pending commands.
func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	var hb model.Heartbeat
	if !decode(w, r, &hb) {
		return
	}
	id := r.PathValue("id")
	now := s.Now()
	d, err := s.Store.UpdateDevice(r.Context(), id, func(d *model.Device) error {
		if hb.FirmwareVersion != "" && hb.FirmwareVersion != d.FirmwareVersion {
			logging.From(r.Context(), s.Log).Info("device firmware changed", "device_id", id, "from", d.FirmwareVersion, "to", hb.FirmwareVersion)
		}
		if hb.FirmwareVersion != "" {
			d.FirmwareVersion = hb.FirmwareVersion
		}
		if hb.FirmwareState != "" {
			d.FirmwareState = hb.FirmwareState
		}
		d.AppliedConfig = hb.ConfigVersion
		d.Health = hb.Telemetry
		d.LastSeen = now
		return nil
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.Metrics.Heartbeats.Add(1)
	cmds, err := s.Store.PopCommands(r.Context(), d.ID, 16)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if cmds == nil {
		cmds = []model.Command{}
	}
	s.Metrics.CommandsSent.Add(int64(len(cmds)))
	writeJSON(w, http.StatusOK, map[string]any{"commands": cmds, "server_time": now.Unix()})
}
