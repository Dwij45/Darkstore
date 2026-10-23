// Command server runs the darkstore inventory API.
//
// Configuration (environment variables):
//
//	ADDR             listen address, default :8080
//	RESERVATION_TTL  how long checkout holds stock, default 2m
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"darkstore/internal/api"
	"darkstore/internal/inventory"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	addr := envOr("ADDR", ":8080")
	ttl, err := time.ParseDuration(envOr("RESERVATION_TTL", "2m"))
	if err != nil {
		log.Error("invalid RESERVATION_TTL", "err", err)
		os.Exit(1)
	}

	inv := inventory.NewStore(ttl)
	seed(inv)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go inv.RunJanitor(ctx, time.Second)

	srv := &http.Server{
		Addr:              addr,
		Handler:           api.New(inv, nil),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("listening", "addr", addr, "reservation_ttl", ttl)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "err", err)
	}
	log.Info("stopped")
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
