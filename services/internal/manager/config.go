package manager

import (
	"errors"
	"os"
	"strconv"
)

type Config struct {
	SQSEndpoint     string
	QueueURL        string
	DLQURL          string
	WorkerImage     string
	WorkerNamespace string
	PollIntervalSec int
	OTelEndpoint    string
}

func LoadConfig() (*Config, error) {
	queue := os.Getenv("SQS_QUEUE_URL")
	if queue == "" {
		return nil, errors.New("SQS_QUEUE_URL is required")
	}
	dlq := os.Getenv("SQS_DLQ_URL")
	if dlq == "" {
		return nil, errors.New("SQS_DLQ_URL is required")
	}
	image := os.Getenv("WORKER_IMAGE")
	if image == "" {
		return nil, errors.New("WORKER_IMAGE is required")
	}

	ns := os.Getenv("WORKER_NAMESPACE")
	if ns == "" {
		ns = "default"
	}

	poll := 3
	if v := os.Getenv("POLL_INTERVAL_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			poll = n
		}
	}

	return &Config{
		SQSEndpoint:     os.Getenv("SQS_ENDPOINT"),
		QueueURL:        queue,
		DLQURL:          dlq,
		WorkerImage:     image,
		WorkerNamespace: ns,
		PollIntervalSec: poll,
		OTelEndpoint:    os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
	}, nil
}
