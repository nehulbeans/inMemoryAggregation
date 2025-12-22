package transport

import (
	"context"
	"encoding/json"
	endpoints "inMemoryAggregation/internal/endpoint"
	"inMemoryAggregation/internal/models"
	"inMemoryAggregation/internal/service"
	"net/http"

	"github.com/go-kit/kit/endpoint"
	httptransport "github.com/go-kit/kit/transport/http"
)

func NewHTTPHandler(ingestEndpoint endpoint.Endpoint) http.Handler {
	mux := http.NewServeMux()

	ingestHandler := httptransport.NewServer(
		ingestEndpoint,
		decodeIngestRequest,
		encodeResponse,
		httptransport.ServerErrorEncoder(encodeError),
	)

	mux.Handle("/v1/signal/ingest", ingestHandler)
	return mux
}

func decodeIngestRequest(_ context.Context, r *http.Request) (interface{}, error) {
	if r.Method != http.MethodPost {
		return nil, service.ErrInvalidRequest
	}

	var signal models.SignalRequest
	if err := json.NewDecoder(r.Body).Decode(&signal); err != nil {
		return nil, err
	}

	return endpoints.IngestRequest{Signal: signal}, nil
}

func encodeResponse(_ context.Context, w http.ResponseWriter, response interface{}) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	resp := response.(endpoints.IngestResponse)
	if resp.Err != "" {
		w.WriteHeader(http.StatusBadRequest)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	return json.NewEncoder(w).Encode(response)
}

func encodeError(_ context.Context, err error, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	code := http.StatusInternalServerError
	if err == service.ErrInvalidRequest || err == service.ErrInvalidSignal {
		code = http.StatusBadRequest
	}

	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{
		"error": err.Error(),
	})
}
