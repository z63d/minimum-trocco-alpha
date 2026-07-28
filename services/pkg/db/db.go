package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/XSAM/otelsql"
	_ "github.com/jackc/pgx/v5/stdlib"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func Open(ctx context.Context) (*sql.DB, error) {
	return open(ctx, true)
}

func OpenPlain(ctx context.Context) (*sql.DB, error) {
	return open(ctx, false)
}

func open(ctx context.Context, withOtel bool) (*sql.DB, error) {
	host := env("DB_HOST", "postgresql")
	port := env("DB_PORT", "5432")
	user := env("DB_USER", "postgres")
	pass := env("DB_PASSWORD", "postgres")
	name := env("DB_NAME", "mintrcc")

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, pass, name)

	var (
		conn *sql.DB
		err  error
	)
	if withOtel {
		conn, err = otelsql.Open("pgx", dsn, otelsql.WithAttributes(semconv.DBSystemPostgreSQL))
	} else {
		conn, err = sql.Open("pgx", dsn)
	}
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	}
	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxLifetime(time.Hour)

	pingCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		if err := conn.PingContext(pingCtx); err == nil {
			return conn, nil
		} else if pingCtx.Err() != nil {
			return nil, fmt.Errorf("db ping timeout: %w", err)
		}
		time.Sleep(1 * time.Second)
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
