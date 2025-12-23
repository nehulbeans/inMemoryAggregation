package main

import (
	"context"
	"fmt"
	"inMemoryAggregation/internal/endpoint"
	"inMemoryAggregation/internal/memory"
	"inMemoryAggregation/internal/middleware"
	"inMemoryAggregation/internal/repository"
	"inMemoryAggregation/internal/service"
	"inMemoryAggregation/internal/transport"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		level.Warn(log.NewNopLogger()).Log("msg", "no .env file found, using OS env vars")
	}

	dsn := buildDSN()

	port := getEnv("PORT", "6789")

	// Logger
	var logger log.Logger
	logger = log.NewLogfmtLogger(os.Stderr)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	logger = log.With(logger, "caller", log.DefaultCaller)

	// Database
	repo, err := repository.NewRepository(dsn, logger)
	if err != nil {
		level.Error(logger).Log("msg", "failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer repo.Close()

	// Aggregator (with shards)
	aggregator := memory.NewAggregator(repo, logger)

	// Service (with validation)
	var svc service.SignalService
	svc = service.NewSignalService(aggregator)
	svc = middleware.NewLoggingMiddleware(logger)(svc)

	// Endpoint
	ingestEndpoint := endpoint.MakeIngestEndpoint(svc)

	handler := transport.NewHTTPHandler(ingestEndpoint)

	// HTTP Server
	httpServer := &http.Server{
		Addr:    ":" + port,
		Handler: handler,
	}

	// Start HTTP server
	go func() {
		level.Info(logger).Log("msg", "HTTP server starting", "addr", port)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			level.Error(logger).Log("msg", "HTTP server failed", "error", err)
			os.Exit(1)
		}
	}()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	level.Info(logger).Log("msg", "shutting down gracefully")
	level.Info(logger).Log("msg", "performing final flush")

	if err := aggregator.Flush(); err != nil {
		level.Error(logger).Log("msg", "final flush failed", "error", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		level.Error(logger).Log("msg", "server shutdown failed", "error", err)
	}

	level.Info(logger).Log("msg", "shutdown complete")
}

// construct pgsql connection dsn
func buildDSN() string {
	host := getEnv("DB_HOST", "localhost")
	portStr := getEnv("DB_PORT", "5432")
	user := getEnv("DB_USER", "postgres")
	password := getEnv("DB_PASSWORD", "")
	dbname := getEnv("DB_NAME", "postgres")
	sslmode := getEnv("DB_SSLMODE", "disable")

	dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=%s",
		host, portStr, user, dbname, sslmode)

	if password != "" {
		dsn += fmt.Sprintf(" password=%s", password)
	}

	return dsn
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
