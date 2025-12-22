package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/go-kit/kit/endpoint"
	httptransport "github.com/go-kit/kit/transport/http"
	"github.com/go-kit/log"
)

// Service interface
type HealthService interface {
	Health(ctx context.Context) string
}

// Service implementation
type healthService struct{}

func (healthService) Health(ctx context.Context) string {
	return "OK"
}

// Request and Response structs
type healthRequest struct{}

type healthResponse struct {
	Status string `json:"status"`
}

// Endpoint maker
func makeHealthEndpoint(svc HealthService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		status := svc.Health(ctx)
		return healthResponse{Status: status}, nil
	}
}

// HTTP decode/encode functions
func decodeHealthRequest(_ context.Context, r *http.Request) (interface{}, error) {
	return healthRequest{}, nil
}

func encodeResponse(_ context.Context, w http.ResponseWriter, response interface{}) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	return json.NewEncoder(w).Encode(response)
}

func main() {
	// Create logger
	logger := log.NewLogfmtLogger(os.Stderr)

	// Create service
	var svc HealthService
	svc = healthService{}

	// Create endpoint
	healthEndpoint := makeHealthEndpoint(svc)

	// Create HTTP handler
	healthHandler := httptransport.NewServer(
		healthEndpoint,
		decodeHealthRequest,
		encodeResponse,
	)

	// Register routes
	http.Handle("/health", healthHandler)

	// Start server
	port := ":8080"
	logger.Log("msg", "HTTP server starting", "port", port)
	fmt.Printf("Server listening on %s\n", port)

	if err := http.ListenAndServe(port, nil); err != nil {
		logger.Log("error", err)
		os.Exit(1)
	}
}
