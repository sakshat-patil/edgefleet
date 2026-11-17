// Package health derives a per-device health verdict from its last heartbeat.
package health

import (
	"fmt"
	"time"

	"github.com/sakshat-patil/edgefleet/internal/model"
)

// Health states.
const (
	Healthy  = "healthy"
	Degraded = "degraded"
	Offline  = "offline"
)

// Thresholds controls when a device is considered degraded or offline.
type Thresholds struct {
	OfflineAfter time.Duration
	MaxTempC     float64
	MaxCPU       float64
	MinFreeMemKB int64
	MinRSSI      int
	MaxErrors    int
}

// DefaultThresholds are tuned for ESP32-CAM (~4MB PSRAM) and Raspberry Pi 4.
var DefaultThresholds = Thresholds{
	OfflineAfter: 30 * time.Second,
	MaxTempC:     80,
	MaxCPU:       95,
	MinFreeMemKB: 20,
	MinRSSI:      -85,
	MaxErrors:    10,
}

// Report is the result of a health check.
type Report struct {
	DeviceID string    `json:"device_id"`
	State    string    `json:"state"`
	Reasons  []string  `json:"reasons,omitempty"`
	LastSeen time.Time `json:"last_seen"`
	Firmware string    `json:"firmware_version"`
}

// Evaluate checks d against t at time now.
func Evaluate(d *model.Device, t Thresholds, now time.Time) Report {
	r := Report{DeviceID: d.ID, State: Healthy, LastSeen: d.LastSeen, Firmware: d.FirmwareVersion}
	if d.LastSeen.IsZero() || now.Sub(d.LastSeen) > t.OfflineAfter {
		r.State = Offline
		r.Reasons = append(r.Reasons, fmt.Sprintf("no heartbeat for %s", now.Sub(d.LastSeen).Round(time.Second)))
		return r
	}
	h := d.Health
	add := func(cond bool, format string, args ...any) {
		if cond {
			r.State = Degraded
			r.Reasons = append(r.Reasons, fmt.Sprintf(format, args...))
		}
	}
	add(h.TempC > t.MaxTempC, "temperature %.1fC > %.0fC", h.TempC, t.MaxTempC)
	add(h.CPUPercent > t.MaxCPU, "cpu %.0f%% > %.0f%%", h.CPUPercent, t.MaxCPU)
	add(h.MemFreeKB > 0 && h.MemFreeKB < t.MinFreeMemKB, "free memory %dKB < %dKB", h.MemFreeKB, t.MinFreeMemKB)
	add(h.RSSI != 0 && h.RSSI < t.MinRSSI, "wifi rssi %ddBm < %ddBm", h.RSSI, t.MinRSSI)
	add(h.Errors > t.MaxErrors, "%d errors since boot > %d", h.Errors, t.MaxErrors)
	add(d.FirmwareState == model.FirmwareFailed, "firmware update failed")
	return r
}
