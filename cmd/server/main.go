// Command server runs the darkstore inventory and order API.
//
// Configuration (environment variables):
//
//	ADDR             listen address, default :8080
//	RESERVATION_TTL  how long checkout holds stock, default 2m
//	WORKERS          fulfilment goroutines, default 4
//	QUEUE_SIZE       confirmed orders that can wait for a worker, default 256
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"darkstore/internal/api"
	"darkstore/internal/fulfilment"
	"darkstore/internal/inventory"
	"darkstore/internal/worker"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	addr := envOr("ADDR", ":8080")
	ttl, err := time.ParseDuration(envOr("RESERVATION_TTL", "2m"))
	if err != nil {
		return fmt.Errorf("RESERVATION_TTL: %w", err)
	}
	workers, err := strconv.Atoi(envOr("WORKERS", "4"))
	if err != nil {
		return fmt.Errorf("WORKERS: %w", err)
	}
	queueSize, err := strconv.Atoi(envOr("QUEUE_SIZE", "256"))
	if err != nil {
		return fmt.Errorf("QUEUE_SIZE: %w", err)
	}

	inv := inventory.NewStore(ttl)
	seed(inv)

	tracker := fulfilment.NewTracker()
	f := &fulfilment.Fulfiller{
		Riders: fulfilment.NewRiders(
			fulfilment.Rider{ID: "rider-1", X: 0.5, Y: 0.2},
			fulfilment.Rider{ID: "rider-2", X: 1.2, Y: -0.4},
			fulfilment.Rider{ID: "rider-3", X: -2.0, Y: 1.1},
		),
		Tracker:      tracker,
		PickTime:     2 * time.Second,
		PackTime:     1 * time.Second,
		DeliveryTime: 10 * time.Second,
		RiderRetry:   500 * time.Millisecond,
	}
	pool := worker.New(workers, queueSize, func(ctx context.Context, orderID string) {
		f.Process(ctx, orderID)
		st, _ := tracker.Get(orderID)
		log.Info("order finished", "order_id", orderID, "stage", st.Stage, "rider", st.RiderID)
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Workers get their own context so in-flight orders keep running while
	// HTTP shuts down, and are only cut off if draining takes too long.
	workCtx, cancelWork := context.WithCancel(context.Background())
	defer cancelWork()
	pool.Start(workCtx)

	go inv.RunJanitor(ctx, time.Second)

	srv := &http.Server{
		Addr:              addr,
		Handler:           api.New(inv, pool, tracker),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", addr, "reservation_ttl", ttl, "workers", workers)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
		close(serveErr)
	}()

	select {
	case err := <-serveErr:
		if err != nil {
			return err
		}
	case <-ctx.Done():
	}

	log.Info("shutting down: closing HTTP, then draining workers")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Order matters: stop taking requests first so nothing new is confirmed,
	// then let workers finish what was already accepted.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "err", err)
	}
	if err := pool.Shutdown(shutdownCtx); err != nil {
		log.Warn("workers did not drain in time; cancelling in-flight orders", "err", err)
		cancelWork()
		pool.Shutdown(context.Background())
	}
	log.Info("stopped")
	return nil
}

// seed loads a small catalogue so the API is usable straight away.
func seed(inv *inventory.Store) {
	for sku, qty := range map[string]int{
		"milk-1l":     50,
		"bread-whole": 20,
		"eggs-12":     30,
		"banana-6":    40,
	} {
		inv.AddStock(sku, qty)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
