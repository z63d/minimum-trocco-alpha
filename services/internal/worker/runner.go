package worker

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"strconv"
	"time"
)

func Run(logger *slog.Logger) error {
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

	logger.Info("worker started",
		slog.String("job_id", jobID),
		slog.Int("duration_sec", dur),
		slog.Float64("failure_rate", failureRate),
	)

	time.Sleep(time.Duration(dur) * time.Second)

	if rand.Float64() < failureRate {
		logger.Error("worker failing intentionally", slog.String("job_id", jobID))
		return errors.New("dummy failure")
	}

	logger.Info("worker completed", slog.String("job_id", jobID))
	return nil
}
