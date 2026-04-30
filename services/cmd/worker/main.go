package main

import (
	"log/slog"
	"os"

	"github.com/z63d/minimum-trocco-alpha/services/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	if err := worker.Run(logger); err != nil {
		logger.Error("worker failed", slog.Any("err", err))
		os.Exit(1)
	}
}
