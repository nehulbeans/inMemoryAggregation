package service

import (
	"context"
	"errors"
	"inMemoryAggregation/internal/memory"
	"inMemoryAggregation/internal/models"
)

var (
	ErrInvalidRequest = errors.New("app, country, and signal are required")
	ErrInvalidSignal  = errors.New("invalid signal type")
)

type SignalService interface {
	IngestSignal(ctx context.Context, req models.SignalRequest) error
}

type signalService struct {
	aggregator *memory.Aggregator
}

func NewSignalService(aggr *memory.Aggregator) SignalService {
	return &signalService{
		aggregator: aggr,
	}
}

// validates and processes incoming signals
func (s *signalService) IngestSignal(ctx context.Context, req models.SignalRequest) error {
	// required fields check
	if req.App == "" || req.Country == "" || req.Signal == "" {
		return ErrInvalidRequest
	}

	// valid signal types
	validSignals := map[string]bool{
		"AdRequest":    true,
		"AdResponse":   true,
		"AdImpression": true,
		"AdClicks":     true,
	}
	if !validSignals[req.Signal] {
		return ErrInvalidSignal
	}

	// tell to aggregator to add (update or create)
	return s.aggregator.Add(req)
}
