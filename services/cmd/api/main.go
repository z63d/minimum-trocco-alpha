package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/z63d/minimum-trocco-alpha/services/internal/api"
	"github.com/z63d/minimum-trocco-alpha/services/pkg/db"
	"github.com/z63d/minimum-trocco-alpha/services/pkg/logger"
	mtotel "github.com/z63d/minimum-trocco-alpha/services/pkg/otel"
)

const serviceName = "api"

func main() {
	log := logger.NewJSON(os.Stdout, serviceName)
	slog.SetDefault(log)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	otelShutdown, err := mtotel.Setup(ctx, serviceName)
	if err != nil {
		log.Error("otel setup failed", slog.Any("err", err))
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = otelShutdown(shutdownCtx)
	}()

	conn, err := db.Open(ctx)
	if err != nil {
		log.Error("db open failed", slog.Any("err", err))
		os.Exit(1)
	}
	defer conn.Close()

	srv, err := api.New(ctx, conn, log)
	if err != nil {
		log.Error("server init failed", slog.Any("err", err))
		os.Exit(1)
	}

	if err := srv.Run(ctx); err != nil {
		log.Error("server exited", slog.Any("err", err))
		os.Exit(1)
	}
}
