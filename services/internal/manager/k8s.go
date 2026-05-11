package manager

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (d *Dispatcher) createWorkerJob(ctx context.Context, name string, jobID int64, def *definition) error {
	// PRODUCER span: worker は k8s Job として起動するが、論理的には
	// manager がジョブを「発行」している。これと worker.run (CONSUMER) が
	// ペアになり、Tempo service graph で manager→worker edge ができる。
	ctx, span := d.tracer.Start(ctx, "worker dispatch",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "k8s_job"),
			attribute.String("messaging.destination", "worker"),
			attribute.String("k8s.job.name", name),
			attribute.Int64("job_id", jobID),
		),
	)
	defer span.End()

	// 現在の span context を W3C Trace Context 形式 (traceparent / tracestate)
	// に変換する。SQS の MessageAttributes 経由 (api→manager) と違い、k8s Job
	// は通信路を持たないので、container env 変数として worker に渡す。
	//
	// traceparent の形式:
	//   00-<trace_id>-<span_id>-<flags>
	//   例) 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
	//
	// worker 側はこの env を MapCarrier に詰めて propagator.Extract で
	// SpanContext を復元し、worker.run span の親に据える。これで
	// api → manager → worker が同一 trace_id でつながる。
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

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
								{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: d.Cfg.OTelEndpoint},
								{Name: "TRACEPARENT", Value: carrier.Get("traceparent")},
								{Name: "TRACESTATE", Value: carrier.Get("tracestate")},
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
			d.Logger.WarnContext(ctx, "k8s job already exists, treating as idempotent",
				slog.Int64("job_id", jobID), slog.String("name", name))
			return nil
		}
		return fmt.Errorf("create job: %w", err)
	}
	return nil
}
