// Command control-plane runs the EdgeFleet REST API, the SQS->Redis command
// dispatcher, the rollout reconciler and the Kafka detection consumer.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sakshat-patil/edgefleet/internal/api"
	"github.com/sakshat-patil/edgefleet/internal/events"
	"github.com/sakshat-patil/edgefleet/internal/logging"
	"github.com/sakshat-patil/edgefleet/internal/queue"
	"github.com/sakshat-patil/edgefleet/internal/rollout"
	"github.com/sakshat-patil/edgefleet/internal/store"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	log := logging.New("control-plane")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var st store.Store = store.NewMemory()
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		st = store.NewRedis(addr, os.Getenv("REDIS_PASSWORD"))
		log.Info("using redis store", "addr", addr)
	} else {
		log.Warn("REDIS_ADDR not set; using in-memory store")
	}

	var q queue.Queue = queue.NewMemory(100_000)
	if url := os.Getenv("SQS_QUEUE_URL"); url != "" {
		sq, err := queue.NewSQS(ctx, url)
		if err != nil {
			log.Error("sqs init failed", "err", err)
			os.Exit(1)
		}
		q = sq
		log.Info("using sqs command queue", "url", url)
	} else {
		log.Warn("SQS_QUEUE_URL not set; using in-memory queue")
	}

	eng := rollout.New(st, q, log)
	srv := &api.Server{
		Store: st, Queue: q, Rollouts: eng, Log: log, Metrics: api.NewMetrics(),
		OperatorToken: os.Getenv("OPERATOR_TOKEN"), DeviceToken: os.Getenv("DEVICE_TOKEN"),
	}
	handler := srv.Handler()

	workers := 4
	for i := 0; i < workers; i++ {
		go queue.Dispatch(ctx, q, st, log.With("component", "dispatcher"))
	}
	go eng.Run(ctx, 2*time.Second)

	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		go func() {
			err := events.Consume(ctx, strings.Split(brokers, ","), env("KAFKA_TOPIC", "detections"), env("KAFKA_GROUP", "edgefleet-control-plane"),
				st, func(n int) { srv.Metrics.EventsIngested.Add(int64(n)) }, log.With("component", "kafka"))
			if err != nil {
				log.Error("kafka consumer stopped", "err", err)
			}
		}()
	}

	hs := &http.Server{Addr: env("HTTP_ADDR", ":8080"), Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("http listening", "addr", hs.Addr)
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "err", err)
			stop()
		}
	}()
	<-ctx.Done()
	log.Info("shutting down")
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = hs.Shutdown(shutdown)
}
