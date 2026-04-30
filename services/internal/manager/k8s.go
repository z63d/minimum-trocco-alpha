package manager

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (d *Dispatcher) createWorkerJob(ctx context.Context, name string, jobID int64, def *definition) error {
	backoffLimit := int32(0)
	ttl := int32(300)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: d.Cfg.WorkerNamespace,
			Labels: map[string]string{
				"app":    "worker",
				"job-id": strconv.FormatInt(jobID, 10),
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":    "worker",
						"job-id": strconv.FormatInt(jobID, 10),
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:            "worker",
							Image:           d.Cfg.WorkerImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Env: []corev1.EnvVar{
								{Name: "JOB_ID", Value: strconv.FormatInt(jobID, 10)},
								{Name: "DUMMY_DURATION_SEC", Value: strconv.Itoa(def.DummyDurationSec)},
								{Name: "FAILURE_RATE", Value: strconv.FormatFloat(def.FailureRate, 'f', -1, 64)},
							},
						},
					},
				},
			},
		},
	}

	_, err := d.K8s.BatchV1().Jobs(d.Cfg.WorkerNamespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			d.Logger.Warn("k8s job already exists, treating as idempotent", slog.Int64("job_id", jobID), slog.String("name", name))
			return nil
		}
		return fmt.Errorf("create job: %w", err)
	}
	return nil
}
