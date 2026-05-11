package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/z63d/minimum-trocco-alpha/services/internal/worker"
	"github.com/z63d/minimum-trocco-alpha/services/pkg/logger"
	mtotel "github.com/z63d/minimum-trocco-alpha/services/pkg/otel"
)

const serviceName = "worker"

func main() {
	log := logger.NewJSON(os.Stdout, serviceName)
	slog.SetDefault(log)

	ctx := context.Background()

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

	// W3C Trace Context を env から復元する。
	// manager (services/internal/manager/k8s.go) が k8s Job 作成時に現在の
	// span context を traceparent (例: 00-<trace_id>-<span_id>-<flags>) に
	// シリアライズして TRACEPARENT env に詰めてくれている。
	// ここで MapCarrier に戻して propagator.Extract で SpanContext を復元し、
	// runCtx の親 span として埋め込む。これで worker.run span は
	// manager 側 PRODUCER span の子として trace に連結される。
	//
	// TRACEPARENT が空 (manager 経由ではなく単独実行) の場合は、worker.run が
	// trace のルートとして新規 trace_id を生成する。
	carrier := propagation.MapCarrier{}
	if v := os.Getenv("TRACEPARENT"); v != "" {
		carrier.Set("traceparent", v)
	}
	if v := os.Getenv("TRACESTATE"); v != "" {
		carrier.Set("tracestate", v)
	}
	runCtx := otel.GetTextMapPropagator().Extract(ctx, carrier)

	if err := worker.Run(runCtx, log); err != nil {
		log.ErrorContext(runCtx, "worker failed", slog.Any("err", err))
		os.Exit(1)
	}
}
