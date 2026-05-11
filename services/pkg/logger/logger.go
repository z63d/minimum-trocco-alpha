package logger

import (
	"context"
	"io"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// NewJSON returns a slog.Logger that emits JSON logs and automatically
// injects `service_name`, plus `trace_id` / `span_id` whenever the calling
// context contains a recording span.
func NewJSON(w io.Writer, serviceName string) *slog.Logger {
	base := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo})
	return slog.New(&ctxHandler{base: base.WithAttrs([]slog.Attr{slog.String("service_name", serviceName)})})
}

type ctxHandler struct {
	base slog.Handler
}

func (h *ctxHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.base.Enabled(ctx, level)
}

func (h *ctxHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanFromContext(ctx).SpanContext(); sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.base.Handle(ctx, r)
}

func (h *ctxHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ctxHandler{base: h.base.WithAttrs(attrs)}
}

func (h *ctxHandler) WithGroup(name string) slog.Handler {
	return &ctxHandler{base: h.base.WithGroup(name)}
}
