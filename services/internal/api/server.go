package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/z63d/minimum-trocco-alpha/services/internal/api/handler"
)

type Server struct {
	addr   string
	srv    *http.Server
	logger *slog.Logger
}

func New(ctx context.Context, db *sql.DB, logger *slog.Logger) (*Server, error) {
	endpoint := os.Getenv("SQS_ENDPOINT")
	queueURL := os.Getenv("SQS_QUEUE_URL")
	if queueURL == "" {
		return nil, errors.New("SQS_QUEUE_URL is required")
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("ap-northeast-1"),
	)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	sqsClient := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if endpoint != "" {
			o.BaseEndpoint = &endpoint
		}
	})

	h := &handler.Handler{
		DB:       db,
		SQS:      sqsClient,
		QueueURL: queueURL,
		Logger:   logger,
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	addr := ":" + port

	return &Server{
		addr: addr,
		srv: &http.Server{
			Addr:              addr,
			Handler:           h.Routes(),
			ReadHeaderTimeout: 10 * time.Second,
		},
		logger: logger,
	}, nil
}

func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("api listening", slog.String("addr", s.addr))
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}
