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
)

type Handler struct {
	DB        *sql.DB
	SQS       *sqs.Client
	QueueURL  string
	Logger    *slog.Logger
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
	mux.HandleFunc("GET /job-definitions", h.listJobDefinitions)
	mux.HandleFunc("POST /job-definitions", h.createJobDefinition)
	mux.HandleFunc("POST /job-definitions/{id}/run", h.runJobDefinition)
	mux.HandleFunc("GET /jobs", h.listJobs)
	return cors(mux)
}

func (h *Handler) healthz(w http.ResponseWriter, r *http.Request) {
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
	idStr := r.PathValue("id")
	defID, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid id: %w", err))
		return
	}

	var exists bool
	if err := h.DB.QueryRowContext(r.Context(),
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
	if err := h.DB.QueryRowContext(r.Context(), `
		INSERT INTO jobs (job_definition_id, status)
		VALUES ($1, 'queued')
		RETURNING id
	`, defID).Scan(&jobID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	body, _ := json.Marshal(map[string]int64{"job_id": jobID})
	bodyStr := string(body)
	if _, err := h.SQS.SendMessage(r.Context(), &sqs.SendMessageInput{
		QueueUrl:    &h.QueueURL,
		MessageBody: &bodyStr,
	}); err != nil {
		_, _ = h.DB.ExecContext(r.Context(),
			`UPDATE jobs SET status = 'failed', updated_at = NOW() WHERE id = $1`, jobID)
		writeError(w, http.StatusInternalServerError, fmt.Errorf("sqs send: %w", err))
		return
	}

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
