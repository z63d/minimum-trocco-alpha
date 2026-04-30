package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/z63d/minimum-trocco-alpha/services/internal/manager"
	"github.com/z63d/minimum-trocco-alpha/services/pkg/db"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := manager.LoadConfig()
	if err != nil {
		logger.Error("config load failed", slog.Any("err", err))
		os.Exit(1)
	}

	conn, err := db.Open(ctx)
	if err != nil {
		logger.Error("db open failed", slog.Any("err", err))
		os.Exit(1)
	}
	defer conn.Close()

	sqsClient, err := manager.NewSQSClient(ctx, cfg.SQSEndpoint)
	if err != nil {
		logger.Error("sqs client failed", slog.Any("err", err))
		os.Exit(1)
	}

	k8s, err := manager.NewK8sClient()
	if err != nil {
		logger.Error("k8s client failed", slog.Any("err", err))
		os.Exit(1)
	}

	dispatcher := &manager.Dispatcher{
		DB: conn, SQS: sqsClient, K8s: k8s, Cfg: cfg, Logger: logger.With(slog.String("component", "dispatcher")),
	}
	watcher := &manager.Watcher{
		DB: conn, K8s: k8s, Cfg: cfg, Logger: logger.With(slog.String("component", "watcher")),
	}

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); _ = dispatcher.RunMainLoop(ctx) }()
	go func() { defer wg.Done(); _ = dispatcher.RunDLQLoop(ctx) }()
	go func() { defer wg.Done(); _ = watcher.Run(ctx) }()
	wg.Wait()

	logger.Info("manager exited")
}
