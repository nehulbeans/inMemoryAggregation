package middleware

import (
	"context"
	"inMemoryAggregation/internal/models"
	"time"

	"github.com/go-kit/log"
)

type Logger interface {
	Log(keyvals ...interface{}) error
}

type SignalService interface {
	IngestSignal(ctx context.Context, req models.SignalRequest) error
}

type loggingMiddleware struct {
	logger log.Logger
	next   SignalService
}

func NewLoggingMiddleware(logger log.Logger) func(SignalService) SignalService {
	return func(next SignalService) SignalService {
		return &loggingMiddleware{
			logger: logger,
			next:   next,
		}
	}
}

func (mw loggingMiddleware) IngestSignal(ctx context.Context, req models.SignalRequest) (err error) {
	defer func(begin time.Time) {
		mw.logger.Log(
			"method", "IngestSignal",
			"app", req.App,
			"country", req.Country,
			"signal", req.Signal,
			"took", time.Since(begin),
			"err", err,
		)
	}(time.Now())

	return mw.next.IngestSignal(ctx, req)
}
