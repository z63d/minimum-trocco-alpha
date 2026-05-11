package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func Run(ctx context.Context, logger *slog.Logger) error {
	jobID := os.Getenv("JOB_ID")
	if jobID == "" {
		return errors.New("JOB_ID is required")
	}

	durStr := os.Getenv("DUMMY_DURATION_SEC")
	dur, err := strconv.Atoi(durStr)
	if err != nil || dur < 0 {
		return fmt.Errorf("invalid DUMMY_DURATION_SEC=%q", durStr)
	}

	failureRate := 0.0
	if v := os.Getenv("FAILURE_RATE"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return fmt.Errorf("invalid FAILURE_RATE=%q", v)
		}
		failureRate = f
	}

	tracer := otel.Tracer("worker")
	// CONSUMER span: manager の PRODUCER span (worker dispatch) とペアになる。
	ctx, span := tracer.Start(ctx, "worker run",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "k8s_job"),
			attribute.String("messaging.destination", "worker"),
			attribute.String("messaging.operation", "process"),
			attribute.String("job_id", jobID),
			attribute.Int("duration_sec", dur),
			attribute.Float64("failure_rate", failureRate),
		),
	)
	defer span.End()

	logger.InfoContext(ctx, "worker started",
		slog.String("job_id", jobID),
		slog.Int("duration_sec", dur),
		slog.Float64("failure_rate", failureRate),
	)

	if err := sleepCtx(ctx, time.Duration(dur)*time.Second); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	if rand.Float64() < failureRate {
		err := errors.New("dummy failure")
		span.RecordError(err)
		span.SetStatus(codes.Error, "intentional dummy failure")
		logger.ErrorContext(ctx, "worker failing intentionally", slog.String("job_id", jobID))
		return err
	}

	logger.InfoContext(ctx, "worker completed", slog.String("job_id", jobID))
	return nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
