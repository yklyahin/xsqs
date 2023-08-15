package xsqs

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/yklyahin/xsqs/logging"
)

type (
	// Worker continuously polls an SQS queue for messages and consumes them using a Consumer implementation.
	Worker struct {
		name     string
		client   Client
		consumer Consumer
		limit    int
		idle     time.Duration
		drain    bool
		logger   logging.Log
	}

	// Consumer defines an interface for message consumers.
	// Implementations should define the behavior of processing messages.
	Consumer interface {
		Consume(ctx context.Context, messages []*sqs.Message) error
	}
)

// NewWorker creates a new Worker with the given name.
func NewWorker(name string, client Client, consumer Consumer, opts ...WorkerOption) *Worker {
	worker := &Worker{
		name:     name,
		client:   client,
		consumer: consumer,
		limit:    10,
		logger:   logging.DefaultZapAdapter().With("worker.name", name),
	}
	for _, opt := range opts {
		opt(worker)
	}
	return worker
}

// Start begins the worker's operation, which includes polling the SQS queue for messages.
// It takes a context as input to control the worker's lifecycle.
func (w *Worker) Start(ctx context.Context) {
	ctx = ContextWithWorkerCtx(ctx, WorkerCtx{
		Name:   w.name,
		Logger: w.logger,
	})

	w.logger.Infow("starting the worker")
	if w.idle > 0 {
		w.startWithIdle(ctx)
	} else {
		w.startWithoutIdle(ctx)
	}
	w.logger.Infow("shutting down the worker")
}

// startWithIdle starts the worker with an idle duration between polling requests.
// It uses an idle duration to regulate the polling frequency.
func (w *Worker) startWithIdle(ctx context.Context) {
	ticker := time.NewTicker(w.idle)
	defer ticker.Stop()

	w.poll(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.poll(ctx)
		}
	}
}

// startWithoutIdle starts the worker without an idle duration between polling requests.
// It polls for messages continuously without pausing between requests to SQS.
func (w *Worker) startWithoutIdle(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			w.poll(ctx)
		}
	}
}

// poll retrieves messages from the client and consumes them using the provided consumer.
func (w *Worker) poll(ctx context.Context) {
	defer func() {
		if rvr := recover(); rvr != nil {
			info, _ := GetWorkerCtxFromContext(ctx)
			info.Logger.Errorw("panic recovered in xsqs worker", "panic", rvr)
		}
	}()

	for {
		select {
		case <-ctx.Done(): // Cancelation in drain mode
			return
		default:
			messages, err := w.client.Receive(ctx, w.limit)
			if err != nil {
				w.logger.Errorw("couldn't receive messages", "error", err)
			}
			if len(messages) > 0 {
				w.consumer.Consume(ctx, messages)
			}
			if !w.drain || len(messages) < w.limit {
				return
			}
		}
	}
}
