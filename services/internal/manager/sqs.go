package manager

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/client-go/kubernetes"

	mtotel "github.com/z63d/minimum-trocco-alpha/services/pkg/otel"
)

type Dispatcher struct {
	DB     *sql.DB
	SQS    *sqs.Client
	K8s    kubernetes.Interface
	Cfg    *Config
	Logger *slog.Logger

	tracer       trace.Tracer
	dispatchCnt  metric.Int64Counter
	dispatchHist metric.Float64Histogram
	dlqCnt       metric.Int64Counter
}

type sqsPayload struct {
	JobID int64 `json:"job_id"`
}

func NewDispatcher(db *sql.DB, sqsClient *sqs.Client, k8s kubernetes.Interface, cfg *Config, logger *slog.Logger) (*Dispatcher, error) {
	meter := otel.Meter("manager")
	dispatchCnt, err := meter.Int64Counter("manager_dispatch_total",
		metric.WithDescription("number of dispatched jobs"))
	if err != nil {
		return nil, err
	}
	dispatchHist, err := meter.Float64Histogram("manager_dispatch_duration_ms",
		metric.WithDescription("dispatch duration in ms"),
		metric.WithUnit("ms"))
	if err != nil {
		return nil, err
	}
	dlqCnt, err := meter.Int64Counter("manager_dlq_total",
		metric.WithDescription("number of DLQ messages processed"))
	if err != nil {
		return nil, err
	}
	return &Dispatcher{
		DB: db, SQS: sqsClient, K8s: k8s, Cfg: cfg, Logger: logger,
		tracer:       otel.Tracer("manager"),
		dispatchCnt:  dispatchCnt,
		dispatchHist: dispatchHist,
		dlqCnt:       dlqCnt,
	}, nil
}

func (d *Dispatcher) RunMainLoop(ctx context.Context) error {
	d.Logger.Info("main queue loop started", slog.String("queue", d.Cfg.QueueURL))
	return d.poll(ctx, d.Cfg.QueueURL, d.handleMain)
}

func (d *Dispatcher) RunDLQLoop(ctx context.Context) error {
	d.Logger.Info("dlq loop started", slog.String("queue", d.Cfg.DLQURL))
	return d.poll(ctx, d.Cfg.DLQURL, d.handleDLQ)
}

func (d *Dispatcher) poll(ctx context.Context, queueURL string, handle func(context.Context, types.Message) error) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// MessageAttributeNames: ["All"] が必須。
		// SQS は ReceiveMessage 時にデフォルトでは MessageAttributes を
		// 返さない。この指定を忘れると api 側で inject した
		// traceparent / tracestate が取れず、Extract が空の SpanContext を
		// 返して trace が分断される。
		out, err := d.SQS.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:              &queueURL,
			MaxNumberOfMessages:   10,
			WaitTimeSeconds:       5,
			VisibilityTimeout:     60,
			MessageAttributeNames: []string{"All"},
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			d.Logger.Error("receive failed", slog.String("queue", queueURL), slog.Any("err", err))
			time.Sleep(2 * time.Second)
			continue
		}

		for _, msg := range out.Messages {
			// SQS MessageAttributes から W3C Trace Context を復元する。
			// api 側 (api/handler/handler.go の "jobs send" PRODUCER span) が
			// SQSCarrier 経由で書き込んだ traceparent / tracestate を拾い、
			// Extract で SpanContext を再構築。msgCtx を以降のハンドラに
			// 引き回すことで、handleMain 内で start する CONSUMER span が
			// api 側 PRODUCER の子として trace に連結される。
			//
			// MessageAttributes が無い (api 以外からの publish 等) の場合は
			// 空の SpanContext が返るので、handleMain は新規 trace の
			// ルート span として開始する。
			msgCtx := otel.GetTextMapPropagator().Extract(ctx, mtotel.SQSCarrier(msg.MessageAttributes))
			if err := handle(msgCtx, msg); err != nil {
				d.Logger.ErrorContext(msgCtx, "handle failed", slog.Any("err", err), slog.String("body", deref(msg.Body)))
				continue
			}
			if _, err := d.SQS.DeleteMessage(msgCtx, &sqs.DeleteMessageInput{
				QueueUrl:      &queueURL,
				ReceiptHandle: msg.ReceiptHandle,
			}); err != nil {
				d.Logger.ErrorContext(msgCtx, "delete failed", slog.Any("err", err))
			}
		}
	}
}

func (d *Dispatcher) handleMain(ctx context.Context, msg types.Message) (err error) {
	// CONSUMER span: api 側の PRODUCER span と trace_id がつながり、
	// Tempo service graph で api→manager の edge を作る。
	ctx, span := d.tracer.Start(ctx, "jobs receive",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystem("aws_sqs"),
			semconv.MessagingDestinationName("jobs"),
			attribute.String("messaging.operation", "process"),
		),
	)
	start := time.Now()
	status := "ok"
	defer func() {
		if err != nil {
			status = "error"
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
		d.dispatchCnt.Add(ctx, 1, metric.WithAttributes(attribute.String("status", status)))
		d.dispatchHist.Record(ctx, float64(time.Since(start).Milliseconds()),
			metric.WithAttributes(attribute.String("status", status)))
	}()

	jobID, err := parseJobID(msg.Body)
	if err != nil {
		return err
	}
	span.SetAttributes(attribute.Int64("job_id", jobID))

	def, err := d.fetchDefinition(ctx, jobID)
	if err != nil {
		return fmt.Errorf("fetch definition for job %d: %w", jobID, err)
	}

	k8sJobName := fmt.Sprintf("job-%d", jobID)

	if _, err := d.DB.ExecContext(ctx, `
		UPDATE jobs SET status = 'pending', k8s_job_name = $1, updated_at = NOW()
		WHERE id = $2
	`, k8sJobName, jobID); err != nil {
		return fmt.Errorf("update pending: %w", err)
	}

	if err := d.createWorkerJob(ctx, k8sJobName, jobID, def); err != nil {
		_, _ = d.DB.ExecContext(ctx,
			`UPDATE jobs SET status = 'failed', finished_at = NOW(), updated_at = NOW() WHERE id = $1`, jobID)
		return fmt.Errorf("create k8s job: %w", err)
	}

	d.Logger.InfoContext(ctx, "dispatched", slog.Int64("job_id", jobID), slog.String("k8s_job", k8sJobName))
	return nil
}

func (d *Dispatcher) handleDLQ(ctx context.Context, msg types.Message) error {
	ctx, span := d.tracer.Start(ctx, "jobs-dlq receive",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystem("aws_sqs"),
			semconv.MessagingDestinationName("jobs-dlq"),
		),
	)
	defer span.End()

	jobID, err := parseJobID(msg.Body)
	if err != nil {
		return err
	}
	span.SetAttributes(attribute.Int64("job_id", jobID))

	if _, err := d.DB.ExecContext(ctx, `
		UPDATE jobs
		SET status = 'failed', finished_at = COALESCE(finished_at, NOW()), updated_at = NOW()
		WHERE id = $1 AND status NOT IN ('succeeded', 'failed')
	`, jobID); err != nil {
		return fmt.Errorf("dlq update: %w", err)
	}
	d.dlqCnt.Add(ctx, 1)
	d.Logger.WarnContext(ctx, "dlq processed", slog.Int64("job_id", jobID))
	return nil
}

type definition struct {
	DummyDurationSec int
	FailureRate      float64
}

func (d *Dispatcher) fetchDefinition(ctx context.Context, jobID int64) (*definition, error) {
	var def definition
	err := d.DB.QueryRowContext(ctx, `
		SELECT jd.dummy_duration_sec, jd.failure_rate
		FROM jobs j
		JOIN job_definitions jd ON j.job_definition_id = jd.id
		WHERE j.id = $1
	`, jobID).Scan(&def.DummyDurationSec, &def.FailureRate)
	if err != nil {
		return nil, err
	}
	return &def, nil
}

func parseJobID(body *string) (int64, error) {
	if body == nil {
		return 0, errors.New("nil body")
	}
	var p sqsPayload
	if err := json.Unmarshal([]byte(*body), &p); err != nil {
		return 0, fmt.Errorf("unmarshal: %w", err)
	}
	if p.JobID == 0 {
		return 0, errors.New("job_id missing")
	}
	return p.JobID, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
