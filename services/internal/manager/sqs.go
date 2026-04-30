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
	"k8s.io/client-go/kubernetes"
)

type Dispatcher struct {
	DB     *sql.DB
	SQS    *sqs.Client
	K8s    kubernetes.Interface
	Cfg    *Config
	Logger *slog.Logger
}

type sqsPayload struct {
	JobID int64 `json:"job_id"`
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

		out, err := d.SQS.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            &queueURL,
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     5,
			VisibilityTimeout:   60,
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
			if err := handle(ctx, msg); err != nil {
				d.Logger.Error("handle failed", slog.Any("err", err), slog.String("body", aws(msg.Body)))
				continue
			}
			if _, err := d.SQS.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      &queueURL,
				ReceiptHandle: msg.ReceiptHandle,
			}); err != nil {
				d.Logger.Error("delete failed", slog.Any("err", err))
			}
		}
	}
}

func (d *Dispatcher) handleMain(ctx context.Context, msg types.Message) error {
	jobID, err := parseJobID(msg.Body)
	if err != nil {
		return err
	}

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

	d.Logger.Info("dispatched", slog.Int64("job_id", jobID), slog.String("k8s_job", k8sJobName))
	return nil
}

func (d *Dispatcher) handleDLQ(ctx context.Context, msg types.Message) error {
	jobID, err := parseJobID(msg.Body)
	if err != nil {
		return err
	}
	if _, err := d.DB.ExecContext(ctx, `
		UPDATE jobs
		SET status = 'failed', finished_at = COALESCE(finished_at, NOW()), updated_at = NOW()
		WHERE id = $1 AND status NOT IN ('succeeded', 'failed')
	`, jobID); err != nil {
		return fmt.Errorf("dlq update: %w", err)
	}
	d.Logger.Warn("dlq processed", slog.Int64("job_id", jobID))
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

func aws(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
