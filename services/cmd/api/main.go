package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/z63d/minimum-trocco-alpha/services/internal/api"
	"github.com/z63d/minimum-trocco-alpha/services/pkg/db"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	conn, err := db.Open(ctx)
	if err != nil {
		logger.Error("db open failed", slog.Any("err", err))
		os.Exit(1)
	}
	defer conn.Close()

	srv, err := api.New(ctx, conn, logger)
	if err != nil {
		logger.Error("server init failed", slog.Any("err", err))
		os.Exit(1)
	}

	if err := srv.Run(ctx); err != nil {
		logger.Error("server exited", slog.Any("err", err))
		os.Exit(1)
	}
}
