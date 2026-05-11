package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/z63d/minimum-trocco-alpha/services/internal/manager"
	"github.com/z63d/minimum-trocco-alpha/services/pkg/db"
	"github.com/z63d/minimum-trocco-alpha/services/pkg/logger"
	mtotel "github.com/z63d/minimum-trocco-alpha/services/pkg/otel"
)

const serviceName = "manager"

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

	cfg, err := manager.LoadConfig()
	if err != nil {
		log.Error("config load failed", slog.Any("err", err))
		os.Exit(1)
	}

	conn, err := db.Open(ctx)
	if err != nil {
		log.Error("db open failed", slog.Any("err", err))
		os.Exit(1)
	}
	defer conn.Close()

	sqsClient, err := manager.NewSQSClient(ctx, cfg.SQSEndpoint)
	if err != nil {
		log.Error("sqs client failed", slog.Any("err", err))
		os.Exit(1)
	}

	k8s, err := manager.NewK8sClient()
	if err != nil {
		log.Error("k8s client failed", slog.Any("err", err))
		os.Exit(1)
	}

	dispatcher, err := manager.NewDispatcher(conn, sqsClient, k8s, cfg, log.With(slog.String("component", "dispatcher")))
	if err != nil {
		log.Error("dispatcher init failed", slog.Any("err", err))
		os.Exit(1)
	}
	watcher := &manager.Watcher{
		DB: conn, K8s: k8s, Cfg: cfg, Logger: log.With(slog.String("component", "watcher")),
	}

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); _ = dispatcher.RunMainLoop(ctx) }()
	go func() { defer wg.Done(); _ = dispatcher.RunDLQLoop(ctx) }()
	go func() { defer wg.Done(); _ = watcher.Run(ctx) }()
	wg.Wait()

	log.Info("manager exited")
}
