// Command simulator runs N simulated edge cameras against the control plane.
// Each camera registers, heartbeats with realistic telemetry, applies config,
// performs (and sometimes fails) firmware updates, rolls back on command, and
// optionally publishes detection events to Kafka.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/sakshat-patil/edgefleet/internal/logging"
	"github.com/sakshat-patil/edgefleet/internal/model"
)

type cfg struct {
	n            int
	url          string
	token        string
	groups       []string
	interval     time.Duration
	fwFailRate   float64
	badVersion   string
	eventsPerSec float64
	brokers      []string
	topic        string
}

type stats struct {
	heartbeats, errors, commands, updates, updateFails, rollbacks, events atomic.Int64
}

func main() {
	var c cfg
	var groups, brokers string
	flag.IntVar(&c.n, "n", 500, "number of simulated cameras")
	flag.StringVar(&c.url, "url", "http://localhost:8080", "control plane base URL")
	flag.StringVar(&c.token, "token", os.Getenv("DEVICE_TOKEN"), "device bearer token")
	flag.StringVar(&groups, "groups", "lobby,parking,warehouse,loading-dock", "comma-separated device groups")
	flag.DurationVar(&c.interval, "interval", 5*time.Second, "heartbeat interval")
	flag.Float64Var(&c.fwFailRate, "fw-fail-rate", 0.02, "probability a firmware update fails")
	flag.StringVar(&c.badVersion, "bad-version", "", "firmware version that always fails (to demo auto-rollback)")
	flag.Float64Var(&c.eventsPerSec, "events-per-sec", 0, "detection events per second per camera (0 disables Kafka)")
	flag.StringVar(&brokers, "kafka", os.Getenv("KAFKA_BROKERS"), "Kafka brokers")
	flag.StringVar(&c.topic, "topic", "detections", "Kafka topic")
	flag.Parse()
	c.groups = strings.Split(groups, ",")
	if brokers != "" {
		c.brokers = strings.Split(brokers, ",")
	}

	log := logging.New("simulator")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var w *kafka.Writer
	if c.eventsPerSec > 0 && len(c.brokers) > 0 {
		w = &kafka.Writer{Addr: kafka.TCP(c.brokers...), Topic: c.topic, Balancer: &kafka.Hash{}, BatchTimeout: 20 * time.Millisecond, BatchSize: 500, Async: true, AllowAutoTopicCreation: true}
		defer w.Close()
	}

	st := &stats{}
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 256}}
	var wg sync.WaitGroup
	for i := 0; i < c.n; i++ {
		wg.Add(1)
		cam := &camera{id: fmt.Sprintf("cam-%04d", i), group: c.groups[i%len(c.groups)], cfg: &c, st: st, http: client, kw: w, log: log}
		go func() {
			defer wg.Done()
			// Stagger startup so 1000 cameras do not register in the same millisecond.
			time.Sleep(time.Duration(rand.Int64N(int64(c.interval))))
			cam.run(ctx)
		}()
	}
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		var lastEv int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				ev := st.events.Load()
				log.Info("simulator stats", "cameras", c.n, "heartbeats", st.heartbeats.Load(), "errors", st.errors.Load(),
					"commands", st.commands.Load(), "fw_updates", st.updates.Load(), "fw_failures", st.updateFails.Load(),
					"rollbacks", st.rollbacks.Load(), "events", ev, "events_per_sec", float64(ev-lastEv)/10)
				lastEv = ev
			}
		}
	}()
	wg.Wait()
}

type camera struct {
	id, group string
	cfg       *cfg
	st        *stats
	http      *http.Client
	kw        *kafka.Writer
	log       *slog.Logger

	mu      sync.Mutex
	fw      string
	fwState string
	cfgVer  int64
	fps     float64
	errors  int
	boot    time.Time
	seen    map[string]bool
	seq     uint64
}

func (c *camera) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, method, c.cfg.url+path, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.cfg.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.token)
	}
	return c.http.Do(req)
}

func (c *camera) run(ctx context.Context) {
	c.fw, c.fwState, c.fps, c.boot, c.seen = "1.0.0", model.FirmwareOK, 15, time.Now(), map[string]bool{}
	resp, err := c.do(ctx, "POST", "/v1/devices", map[string]any{"id": c.id, "kind": model.KindSim, "group": c.group, "firmware_version": c.fw})
	if err == nil {
		resp.Body.Close()
	}
	if c.kw != nil {
		go c.emitEvents(ctx)
	}
	t := time.NewTicker(c.cfg.interval)
	defer t.Stop()
	for {
		c.heartbeat(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (c *camera) heartbeat(ctx context.Context) {
	c.mu.Lock()
	hb := model.Heartbeat{
		FirmwareVersion: c.fw, FirmwareState: c.fwState, ConfigVersion: c.cfgVer,
		Telemetry: model.Telemetry{
			CPUPercent: 20 + rand.Float64()*40, MemFreeKB: 80 + rand.Int64N(60), TempC: 45 + rand.Float64()*15,
			FPS: c.fps * (0.9 + rand.Float64()*0.1), RSSI: -50 - rand.IntN(30), Errors: c.errors,
			UptimeSec: int64(time.Since(c.boot).Seconds()),
		},
	}
	c.mu.Unlock()
	resp, err := c.do(ctx, "POST", "/v1/agent/"+c.id+"/heartbeat", hb)
	if err != nil {
		if ctx.Err() == nil {
			c.st.errors.Add(1)
		}
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		c.st.errors.Add(1)
		return
	}
	c.st.heartbeats.Add(1)
	var out struct {
		Commands []model.Command `json:"commands"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return
	}
	for _, cmd := range out.Commands {
		c.apply(cmd)
	}
}

func (c *camera) apply(cmd model.Command) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.seen[cmd.ID] { // at-least-once delivery: dedupe
		return
	}
	c.seen[cmd.ID] = true
	c.st.commands.Add(1)
	switch cmd.Type {
	case model.CmdSetConfig:
		if v, err := strconv.ParseInt(cmd.Payload["config_version"], 10, 64); err == nil {
			c.cfgVer = v
		}
		if f, err := strconv.ParseFloat(cmd.Payload["fps"], 64); err == nil {
			c.fps = f
		}
	case model.CmdUpdateFirmware:
		target := cmd.Payload["version"]
		c.fwState = model.FirmwareUpdating
		c.st.updates.Add(1)
		// Download + flash + reboot takes a few seconds on real hardware.
		delay := time.Duration(2+rand.IntN(6)) * time.Second
		fail := target == c.cfg.badVersion || rand.Float64() < c.cfg.fwFailRate
		time.AfterFunc(delay, func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			if fail {
				c.fwState = model.FirmwareFailed // bootloader rolled back to the old slot
				c.errors++
				c.st.updateFails.Add(1)
				return
			}
			c.fw, c.fwState, c.boot = target, model.FirmwareOK, time.Now()
		})
	case model.CmdRollbackFirmware:
		c.fw, c.fwState, c.boot = cmd.Payload["version"], model.FirmwareOK, time.Now()
		c.st.rollbacks.Add(1)
	case model.CmdReboot:
		c.boot = time.Now()
	}
}

var labels = []string{"person", "person", "person", "car", "truck", "bicycle", "dog"}

func (c *camera) emitEvents(ctx context.Context) {
	period := time.Duration(float64(time.Second) / c.cfg.eventsPerSec)
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		c.seq++
		ev := model.DetectionEvent{DeviceID: c.id, Seq: c.seq, Label: labels[rand.IntN(len(labels))], Confidence: 0.5 + rand.Float64()*0.5,
			BBox: [4]int{rand.IntN(600), rand.IntN(400), 20 + rand.IntN(120), 40 + rand.IntN(200)}, FrameTS: time.Now()}
		b, _ := json.Marshal(ev)
		if err := c.kw.WriteMessages(ctx, kafka.Message{Key: []byte(c.id), Value: b}); err == nil {
			c.st.events.Add(1)
		}
	}
}
