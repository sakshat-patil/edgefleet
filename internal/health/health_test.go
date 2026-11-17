package health

import (
	"testing"
	"time"

	"github.com/sakshat-patil/edgefleet/internal/model"
)

func TestEvaluate(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name string
		d    model.Device
		want string
	}{
		{"healthy", model.Device{LastSeen: now, Health: model.Telemetry{TempC: 50, CPUPercent: 40}}, Healthy},
		{"offline", model.Device{LastSeen: now.Add(-time.Minute)}, Offline},
		{"never seen", model.Device{}, Offline},
		{"hot", model.Device{LastSeen: now, Health: model.Telemetry{TempC: 85}}, Degraded},
		{"weak wifi", model.Device{LastSeen: now, Health: model.Telemetry{RSSI: -90}}, Degraded},
		{"fw failed", model.Device{LastSeen: now, FirmwareState: model.FirmwareFailed}, Degraded},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Evaluate(&c.d, DefaultThresholds, now).State; got != c.want {
				t.Fatalf("got %s want %s", got, c.want)
			}
		})
	}
}
