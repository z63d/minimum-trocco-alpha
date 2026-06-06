package handler

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"

	mtotel "github.com/z63d/minimum-trocco-alpha/services/pkg/otel"
)

type Handler struct {
	DB          *sql.DB
	SQS         *sqs.Client
	QueueURL    string
	Logger      *slog.Logger
	reqDuration metric.Float64Histogram
}

func New(db *sql.DB, sqsClient *sqs.Client, queueURL string, logger *slog.Logger) (*Handler, error) {
	hist, err := otel.Meter("api").Float64Histogram(
		"http_server_request_duration_ms",
		metric.WithDescription("HTTP server request duration in milliseconds"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("create histogram: %w", err)
	}
	return &Handler{
		DB:          db,
		SQS:         sqsClient,
		QueueURL:    queueURL,
		Logger:      logger,
		reqDuration: hist,
	}, nil
}

type JobDefinition struct {
	ID               int64     `json:"id"`
	Name             string    `json:"name"`
	DummyDurationSec int       `json:"dummy_duration_sec"`
	FailureRate      float64   `json:"failure_rate"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type Job struct {
	ID              int64      `json:"id"`
	JobDefinitionID int64      `json:"job_definition_id"`
	JobName         string     `json:"job_name"`
	Status          string     `json:"status"`
	K8sJobName      *string    `json:"k8s_job_name"`
	StartedAt       *time.Time `json:"started_at"`
	FinishedAt      *time.Time `json:"finished_at"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.HandleFunc("GET /job-definitions", h.metric("/job-definitions", h.listJobDefinitions))
	mux.HandleFunc("POST /job-definitions", h.metric("/job-definitions", h.createJobDefinition))
	mux.HandleFunc("POST /job-definitions/{id}/run", h.metric("/job-definitions/{id}/run", h.runJobDefinition))
	mux.HandleFunc("GET /jobs", h.metric("/jobs", h.listJobs))
	return cors(mux)
}

func (h *Handler) metric(route string, fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		fn(sw, r)
		h.reqDuration.Record(r.Context(), float64(time.Since(start).Milliseconds()),
			metric.WithAttributes(
				attribute.String("route", route),
				attribute.Int("status", sw.code),
				attribute.String("method", r.Method),
			),
		)
	}
}

func (h *Handler) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) listJobDefinitions(w http.ResponseWriter, r *http.Request) {
	rows, err := h.DB.QueryContext(r.Context(), `
		SELECT id, name, dummy_duration_sec, failure_rate, created_at, updated_at
		FROM job_definitions
		ORDER BY id ASC
	`)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer rows.Close()

	defs := []JobDefinition{}
	for rows.Next() {
		var d JobDefinition
		if err := rows.Scan(&d.ID, &d.Name, &d.DummyDurationSec, &d.FailureRate, &d.CreatedAt, &d.UpdatedAt); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		defs = append(defs, d)
	}
	writeJSON(w, http.StatusOK, defs)
}

func (h *Handler) createJobDefinition(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name             string  `json:"name"`
		DummyDurationSec int     `json:"dummy_duration_sec"`
		FailureRate      float64 `json:"failure_rate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, errors.New("name is required"))
		return
	}
	if req.DummyDurationSec <= 0 {
		req.DummyDurationSec = 10
	}

	var d JobDefinition
	err := h.DB.QueryRowContext(r.Context(), `
		INSERT INTO job_definitions (name, dummy_duration_sec, failure_rate)
		VALUES ($1, $2, $3)
		RETURNING id, name, dummy_duration_sec, failure_rate, created_at, updated_at
	`, req.Name, req.DummyDurationSec, req.FailureRate).Scan(
		&d.ID, &d.Name, &d.DummyDurationSec, &d.FailureRate, &d.CreatedAt, &d.UpdatedAt,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusCreated, d)
}

func (h *Handler) runJobDefinition(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	idStr := r.PathValue("id")
	defID, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid id: %w", err))
		return
	}

	var exists bool
	if err := h.DB.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM job_definitions WHERE id = $1)`, defID,
	).Scan(&exists); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if !exists {
		writeError(w, http.StatusNotFound, errors.New("job_definition not found"))
		return
	}

	var jobID int64
	if err := h.DB.QueryRowContext(ctx, `
		INSERT INTO jobs (job_definition_id, status)
		VALUES ($1, 'queued')
		RETURNING id
	`, defID).Scan(&jobID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	body, _ := json.Marshal(map[string]int64{"job_id": jobID})
	bodyStr := string(body)

	// PRODUCER span: SQSへの publish。これと manager 側の CONSUMER span が
	// 同一 trace_id でペアリングされ、Tempo の service graph に api→manager の
	// edge が生成される。
	sendCtx, sendSpan := otel.Tracer("api").Start(ctx, "jobs send",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystem("aws_sqs"),
			semconv.MessagingDestinationName("jobs"),
			attribute.String("messaging.operation", "publish"),
			attribute.Int64("job_id", jobID),
		),
	)

	// W3C Trace Context を SQS MessageAttributes に inject する。
	// MessageBody (job_id だけの JSON) には触らず、SQS の sidecar 領域である
	// MessageAttributes に traceparent / tracestate キーを書き込むことで、
	// アプリケーションペイロードと制御メタデータを分離する。
	//
	// 仕組み:
	//   1. SQSCarrier は map[string]types.MessageAttributeValue を
	//      OTel propagation.TextMapCarrier に適合させる薄いアダプタ
	//      (services/pkg/otel/sqs.go)。
	//   2. propagator.Inject は sendCtx の SpanContext を W3C 形式の
	//      traceparent 文字列にシリアライズし、carrier.Set 経由で
	//      MessageAttributes へ書き込む。
	//   3. 受信側 (manager/sqs.go) は ReceiveMessage 時に
	//      MessageAttributeNames=["All"] でこれらを取得し、
	//      SQSCarrier(msg.MessageAttributes) → propagator.Extract で復元する。
	//
	// 結果として、本 PRODUCER span と manager の CONSUMER span が同一
	// trace_id・親子関係でつながり、Tempo 上で 1 trace として可視化される。
	attrs := map[string]types.MessageAttributeValue{}
	otel.GetTextMapPropagator().Inject(sendCtx, mtotel.SQSCarrier(attrs))

	_, sendErr := h.SQS.SendMessage(sendCtx, &sqs.SendMessageInput{
		QueueUrl:          &h.QueueURL,
		MessageBody:       &bodyStr,
		MessageAttributes: attrs,
	})
	sendSpan.End()
	if sendErr != nil {
		_, _ = h.DB.ExecContext(ctx,
			`UPDATE jobs SET status = 'failed', updated_at = NOW() WHERE id = $1`, jobID)
		writeError(w, http.StatusInternalServerError, fmt.Errorf("sqs send: %w", sendErr))
		return
	}

	h.Logger.InfoContext(ctx, "job enqueued", slog.Int64("job_id", jobID), slog.Int64("job_definition_id", defID))
	writeJSON(w, http.StatusAccepted, map[string]any{
		"job_id": jobID,
		"status": "queued",
	})
}

func (h *Handler) listJobs(w http.ResponseWriter, r *http.Request) {
	rows, err := h.DB.QueryContext(r.Context(), `
		SELECT j.id, j.job_definition_id, jd.name, j.status, j.k8s_job_name, j.started_at, j.finished_at, j.created_at, j.updated_at
		FROM jobs j
		JOIN job_definitions jd ON j.job_definition_id = jd.id
		ORDER BY j.id DESC
		LIMIT 100
	`)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer rows.Close()

	jobs := []Job{}
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.ID, &j.JobDefinitionID, &j.JobName, &j.Status, &j.K8sJobName, &j.StartedAt, &j.FinishedAt, &j.CreatedAt, &j.UpdatedAt); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		jobs = append(jobs, j)
	}
	writeJSON(w, http.StatusOK, jobs)
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (sw *statusWriter) WriteHeader(code int) {
	sw.code = code
	sw.ResponseWriter.WriteHeader(code)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
