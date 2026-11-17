// Package model holds the domain types shared by the control plane,
// the device simulator and the load generator.
package model

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// Device kinds in the fleet.
const (
	KindESP32Cam = "esp32-cam" // physical ESP32-CAM running firmware/esp32-cam
	KindRPiCV    = "rpi-cv"    // Raspberry Pi running cv-agent (OpenCV)
	KindSim      = "sim"       // simulated camera from cmd/simulator
)

// Firmware states reported by a device in its heartbeat.
const (
	FirmwareOK       = "ok"
	FirmwareUpdating = "updating"
	FirmwareFailed   = "failed"
)

// Device is the control plane's view of one edge camera. It is stored in Redis.
type Device struct {
	ID               string            `json:"id"`
	Kind             string            `json:"kind"`
	Group            string            `json:"group"`
	FirmwareVersion  string            `json:"firmware_version"`
	FirmwareState    string            `json:"firmware_state"`
	TargetFirmware   string            `json:"target_firmware,omitempty"`
	PreviousFirmware string            `json:"previous_firmware,omitempty"`
	Config           map[string]string `json:"config"`
	ConfigVersion    int64             `json:"config_version"`
	AppliedConfig    int64             `json:"applied_config_version"`
	Health           Telemetry         `json:"telemetry"`
	Detections       int64             `json:"detections_total"`
	RegisteredAt     time.Time         `json:"registered_at"`
	LastSeen         time.Time         `json:"last_seen"`
}

// Telemetry is the health snapshot a device sends with every heartbeat.
type Telemetry struct {
	CPUPercent float64 `json:"cpu_percent"`
	MemFreeKB  int64   `json:"mem_free_kb"`
	TempC      float64 `json:"temp_c"`
	FPS        float64 `json:"fps"`
	RSSI       int     `json:"rssi_dbm"`
	Errors     int     `json:"errors"`
	UptimeSec  int64   `json:"uptime_sec"`
}

// Command types sent from the control plane to devices.
const (
	CmdSetConfig        = "set_config"
	CmdUpdateFirmware   = "update_firmware"
	CmdRollbackFirmware = "rollback_firmware"
	CmdReboot           = "reboot"
)

// Command is queued in SQS, fanned out into a per-device pending list in Redis,
// and delivered to the device in the response to its next heartbeat.
type Command struct {
	ID        string            `json:"id"`
	DeviceID  string            `json:"device_id"`
	Type      string            `json:"type"`
	Payload   map[string]string `json:"payload,omitempty"`
	RolloutID string            `json:"rollout_id,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
}

// Heartbeat is what a device POSTs to /v1/agent/{id}/heartbeat.
type Heartbeat struct {
	FirmwareVersion string    `json:"firmware_version"`
	FirmwareState   string    `json:"firmware_state"`
	ConfigVersion   int64     `json:"config_version"`
	Telemetry       Telemetry `json:"telemetry"`
}

// Rollout statuses.
const (
	RolloutInProgress = "in_progress"
	RolloutPaused     = "paused"
	RolloutCompleted  = "completed"
	RolloutRolledBack = "rolled_back"
)

// Rollout is a staged firmware deployment to a device group.
type Rollout struct {
	ID             string    `json:"id"`
	Group          string    `json:"group"`
	TargetVersion  string    `json:"target_version"`
	FirmwareURL    string    `json:"firmware_url"`
	Stages         []int     `json:"stages"`           // cumulative percentages, e.g. [5, 25, 100]
	MaxFailureRate float64   `json:"max_failure_rate"` // e.g. 0.1 -> roll back if >10% of a stage fails
	SoakSeconds    int       `json:"soak_seconds"`     // minimum time a stage must be healthy before advancing
	CurrentStage   int       `json:"current_stage"`
	Candidates     []string  `json:"candidates"` // ordered device IDs, fixed at creation
	Updated        []string  `json:"updated"`    // devices that have been sent the update so far
	Failed         []string  `json:"failed,omitempty"`
	Status         string    `json:"status"`
	Reason         string    `json:"reason,omitempty"`
	StageStartedAt time.Time `json:"stage_started_at"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// DetectionEvent is published by cv-agent / firmware / simulator to Kafka.
type DetectionEvent struct {
	DeviceID   string    `json:"device_id"`
	Seq        uint64    `json:"seq"`
	Label      string    `json:"label"`
	Confidence float64   `json:"confidence"`
	BBox       [4]int    `json:"bbox"` // x, y, w, h
	FrameTS    time.Time `json:"frame_ts"`
}

// NewID returns a short random identifier with the given prefix.
func NewID(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
}
