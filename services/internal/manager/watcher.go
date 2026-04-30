package manager

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Watcher struct {
	DB     *sql.DB
	K8s    kubernetes.Interface
	Cfg    *Config
	Logger *slog.Logger
}

func (w *Watcher) Run(ctx context.Context) error {
	interval := time.Duration(w.Cfg.PollIntervalSec) * time.Second
	t := time.NewTicker(interval)
	defer t.Stop()

	w.Logger.Info("watcher started", slog.Duration("interval", interval))
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := w.tick(ctx); err != nil {
				w.Logger.Error("tick failed", slog.Any("err", err))
			}
		}
	}
}

func (w *Watcher) tick(ctx context.Context) error {
	list, err := w.K8s.BatchV1().Jobs(w.Cfg.WorkerNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=worker",
	})
	if err != nil {
		return err
	}

	for _, j := range list.Items {
		jobID := j.Labels["job-id"]
		if jobID == "" {
			continue
		}

		switch {
		case j.Status.Failed >= 1:
			w.update(ctx, jobID, "failed", true)
		case j.Status.Succeeded >= 1:
			w.update(ctx, jobID, "succeeded", true)
		case j.Status.Active >= 1:
			w.update(ctx, jobID, "running", false)
		}
	}
	return nil
}

func (w *Watcher) update(ctx context.Context, jobID string, status string, finished bool) {
	var query string
	if finished {
		query = `
			UPDATE jobs
			SET status = $1,
			    started_at = COALESCE(started_at, NOW()),
			    finished_at = COALESCE(finished_at, NOW()),
			    updated_at = NOW()
			WHERE id = $2 AND status NOT IN ('succeeded', 'failed')
		`
	} else {
		query = `
			UPDATE jobs
			SET status = $1,
			    started_at = COALESCE(started_at, NOW()),
			    updated_at = NOW()
			WHERE id = $2 AND status IN ('queued', 'pending')
		`
	}

	res, err := w.DB.ExecContext(ctx, query, status, jobID)
	if err != nil {
		w.Logger.Error("update failed", slog.String("job_id", jobID), slog.String("status", status), slog.Any("err", err))
		return
	}
	if rows, _ := res.RowsAffected(); rows > 0 {
		w.Logger.Info("status updated", slog.String("job_id", jobID), slog.String("status", status))
	}
}
