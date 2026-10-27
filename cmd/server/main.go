package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"realtime-chat/internal/api"
	"realtime-chat/internal/app"
	"realtime-chat/internal/auth"
)

func main() {
	addr := flag.String("addr", ":8096", "HTTP listen address")
	db := flag.String("db", "data/chat.db", "SQLite database path")
	dev := flag.Bool("dev", true, "enable POST /dev/token")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(*addr, *db, *dev, logger); err != nil {
		logger.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func run(addr, db string, dev bool, logger *slog.Logger) error {
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		if !dev {
			return errors.New("JWT_SECRET is required when -dev=false")
		}
		secret = "dev-only-secret"
		logger.Warn("JWT_SECRET not set, using an insecure development secret")
	}

	opts := app.DefaultOptions(db)
	opts.Logger = logger
	a, err := app.New(opts)
	if err != nil {
		return err
	}
	defer a.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := a.Start(ctx); err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           api.NewRouter(api.Deps{Signer: auth.NewSigner([]byte(secret)), Connector: a, Dev: dev}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	logger.Info("chat server listening", "addr", addr, "db", db)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
