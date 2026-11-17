package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sakshat-patil/edgefleet/internal/model"
	"github.com/sakshat-patil/edgefleet/internal/queue"
	"github.com/sakshat-patil/edgefleet/internal/rollout"
	"github.com/sakshat-patil/edgefleet/internal/store"
)

func setup(t *testing.T) (*httptest.Server, *store.Memory, *queue.Memory) {
	t.Helper()
	st, q := store.NewMemory(), queue.NewMemory(1000)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := &Server{Store: st, Queue: q, Rollouts: rollout.New(st, q, log), Log: log, OperatorToken: "op", DeviceToken: "dev"}
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go queue.Dispatch(ctx, q, st, log)
	return ts, st, q
}

func call(t *testing.T, ts *httptest.Server, method, path, token string, body any, out any) int {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(method, ts.URL+path, bytes.NewReader(b))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil {
		_ = json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode
}

func TestAuthRequired(t *testing.T) {
	ts, _, _ := setup(t)
	if code := call(t, ts, "GET", "/v1/devices", "", nil, nil); code != 401 {
		t.Fatalf("got %d want 401", code)
	}
	if code := call(t, ts, "GET", "/v1/devices", "dev", nil, nil); code != 401 {
		t.Fatalf("device token accepted on operator route: %d", code)
	}
}

func TestConfigPushReachesDevice(t *testing.T) {
	ts, _, _ := setup(t)
	for _, id := range []string{"cam-1", "cam-2"} {
		if code := call(t, ts, "POST", "/v1/devices", "op", map[string]any{"id": id, "group": "lobby", "firmware_version": "1.0.0"}, nil); code != 201 {
			t.Fatalf("register %s: %d", id, code)
		}
	}
	var res map[string]any
	if code := call(t, ts, "PATCH", "/v1/groups/lobby/config", "op", map[string]string{"fps": "10", "motion_threshold": "25"}, &res); code != 202 {
		t.Fatalf("group config: %d", code)
	}
	if res["commands_queued"].(float64) != 2 {
		t.Fatalf("queued %v", res["commands_queued"])
	}
	var hb struct{ Commands []model.Command }
	deadline := time.Now().Add(2 * time.Second)
	for len(hb.Commands) == 0 && time.Now().Before(deadline) {
		call(t, ts, "POST", "/v1/agent/cam-1/heartbeat", "dev", model.Heartbeat{FirmwareVersion: "1.0.0", FirmwareState: "ok"}, &hb)
		time.Sleep(10 * time.Millisecond)
	}
	if len(hb.Commands) != 1 || hb.Commands[0].Type != model.CmdSetConfig || hb.Commands[0].Payload["fps"] != "10" {
		t.Fatalf("unexpected commands: %+v", hb.Commands)
	}
	var h map[string]any
	call(t, ts, "GET", "/v1/devices/cam-1/health", "op", nil, &h)
	if h["state"] != "healthy" {
		t.Fatalf("health %v", h)
	}
}

func TestRolloutAPI(t *testing.T) {
	ts, _, _ := setup(t)
	for _, id := range []string{"a", "b", "c", "d"} {
		call(t, ts, "POST", "/v1/devices", "op", map[string]any{"id": id, "group": "g", "firmware_version": "1.0.0"}, nil)
	}
	var ro model.Rollout
	if code := call(t, ts, "POST", "/v1/rollouts", "op", map[string]any{"group": "g", "target_version": "1.1.0", "stages": []int{25, 100}}, &ro); code != 201 {
		t.Fatalf("create: %d", code)
	}
	if len(ro.Updated) != 1 {
		t.Fatalf("canary size %d want 1", len(ro.Updated))
	}
	if code := call(t, ts, "POST", "/v1/rollouts/"+ro.ID+"/pause", "op", nil, &ro); code != 200 || ro.Status != model.RolloutPaused {
		t.Fatalf("pause: %d %s", code, ro.Status)
	}
	if code := call(t, ts, "POST", "/v1/rollouts/"+ro.ID+"/rollback", "op", nil, &ro); code != 200 || ro.Status != model.RolloutRolledBack {
		t.Fatalf("rollback: %d %s", code, ro.Status)
	}
	if code := call(t, ts, "POST", "/v1/rollouts", "op", map[string]any{"group": "g", "target_version": "1.1.0", "stages": []int{50}}, nil); code != 400 {
		t.Fatalf("bad stages accepted: %d", code)
	}
}
