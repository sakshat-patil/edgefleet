// Package logging sets up structured JSON logging and HTTP request logging.
package logging

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/sakshat-patil/edgefleet/internal/model"
)

// New returns a JSON slog logger at the level named by LOG_LEVEL (default info).
func New(service string) *slog.Logger {
	var lvl slog.Level
	_ = lvl.UnmarshalText([]byte(strings.ToUpper(envOr("LOG_LEVEL", "info"))))
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(h).With("service", service)
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

type ctxKey struct{}

// From returns the request-scoped logger stored by Middleware.
func From(ctx context.Context, fallback *slog.Logger) *slog.Logger {
	if l, ok := ctx.Value(ctxKey{}).(*slog.Logger); ok {
		return l
	}
	return fallback
}

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusWriter) WriteHeader(code int) { w.status = code; w.ResponseWriter.WriteHeader(code) }
func (w *statusWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}

// Middleware attaches a request ID, logs one line per request, and reports
// to the observe callback (used for metrics). Heartbeats log at debug level to
// keep log volume proportional to operator activity, not fleet size.
func Middleware(log *slog.Logger, observe func(route string, status int, d time.Duration), next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rid := r.Header.Get("X-Request-ID")
		if rid == "" {
			rid = model.NewID("req")
		}
		w.Header().Set("X-Request-ID", rid)
		l := log.With("request_id", rid)
		sw := &statusWriter{ResponseWriter: w, status: 200}
		r = r.WithContext(context.WithValue(r.Context(), ctxKey{}, l))
		next.ServeHTTP(sw, r) // the mux sets r.Pattern on this request
		d := time.Since(start)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		if observe != nil {
			observe(route, sw.status, d)
		}
		lvl := slog.LevelInfo
		if strings.Contains(route, "/heartbeat") || strings.HasSuffix(route, "z") || strings.HasSuffix(route, "/metrics") {
			lvl = slog.LevelDebug
		}
		if sw.status >= 500 {
			lvl = slog.LevelError
		}
		l.Log(r.Context(), lvl, "http request",
			"method", r.Method, "route", route, "path", r.URL.Path, "status", sw.status,
			"bytes", sw.bytes, "duration_ms", float64(d.Microseconds())/1000, "device_id", r.PathValue("id"))
	})
}
