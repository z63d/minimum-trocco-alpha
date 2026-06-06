package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
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

	// CONSUMER span: worker は独自の trace を開始し、manager の PRODUCER span
	// (worker dispatch) を SpanLink で参照する。これにより Tempo では
	// service.name="worker" を root とした独立したトレースとして表示され、
	// リンク経由で api→manager トレースに辿れる。
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "k8s_job"),
			attribute.String("messaging.destination", "worker"),
			attribute.String("messaging.operation", "process"),
			attribute.String("job_id", jobID),
			attribute.Int("duration_sec", dur),
			attribute.Float64("failure_rate", failureRate),
		),
	}
	if remoteSpanCtx := trace.SpanContextFromContext(ctx); remoteSpanCtx.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: remoteSpanCtx}))
	}
	// context.Background() を渡すことで新規 trace_id を生成し、親子関係を断ち切る。
	workerCtx, span := tracer.Start(context.Background(), "worker run", opts...)
	ctx = workerCtx
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

	if err := fetchRandomPost(ctx, logger); err != nil {
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

func fetchRandomPost(ctx context.Context, logger *slog.Logger) error {
	client := &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://jsonplaceholder.typicode.com/posts", nil)
	if err != nil {
		return fmt.Errorf("build posts request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("fetch posts: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read posts body: %w", err)
	}

	var posts []struct {
		ID    int    `json:"id"`
		Title string `json:"title"`
	}
	if err := json.Unmarshal(body, &posts); err != nil {
		return fmt.Errorf("unmarshal posts: %w", err)
	}
	if len(posts) == 0 {
		return errors.New("no posts returned")
	}

	picked := posts[rand.IntN(len(posts))]
	logger.InfoContext(ctx, "picked post", slog.Int("post_id", picked.ID), slog.String("title", picked.Title))

	req2, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("https://jsonplaceholder.typicode.com/posts/%d", picked.ID), nil)
	if err != nil {
		return fmt.Errorf("build post request: %w", err)
	}
	resp2, err := client.Do(req2)
	if err != nil {
		return fmt.Errorf("fetch post %d: %w", picked.ID, err)
	}
	defer resp2.Body.Close()

	body2, err := io.ReadAll(resp2.Body)
	if err != nil {
		return fmt.Errorf("read post body: %w", err)
	}

	var post struct {
		ID    int    `json:"id"`
		Title string `json:"title"`
		Body  string `json:"body"`
	}
	if err := json.Unmarshal(body2, &post); err != nil {
		return fmt.Errorf("unmarshal post: %w", err)
	}

	logger.InfoContext(ctx, "fetched post detail", slog.Int("post_id", post.ID), slog.String("title", post.Title))
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
