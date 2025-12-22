package endpoint

import (
	"context"
	"inMemoryAggregation/internal/models"
	"inMemoryAggregation/internal/service"

	"github.com/go-kit/kit/endpoint"
)

type IngestRequest struct {
	Signal models.SignalRequest
}

type IngestResponse struct {
	Err string `json:"error,omitempty"`
}

func MakeIngestEndpoint(svc service.SignalService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(IngestRequest)
		err := svc.IngestSignal(ctx, req.Signal)

		if err != nil {
			return IngestResponse{Err: err.Error()}, err
		}
		return IngestResponse{}, nil
	}
}
