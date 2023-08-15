package xsqs

import (
	"time"

	"github.com/yklyahin/xsqs/logging"
)

type (
	// WorkerOption represents an option for configuring a worker.
	WorkerOption func(*Worker)

	// ClientOption represents an option for configuring a client.
	ClientOption func(*client)

	// ConsumerOption represents an option for configuring a consumer.
	ConsumerOption func(*consumerOptions)
)

// WithIdle sets the idle delay duration between ReceiveMessage requests for a Worker.
// It also specifies whether the Worker should drain remaining messages before entering idle mode.
func WithIdle(duration time.Duration, drain bool) WorkerOption {
	return func(w *Worker) {
		w.idle = duration
		w.drain = drain
	}
}

// WithMessagesLimit sets the maximum number of messages to return per polling request for a Worker.
func WithMessagesLimit(limit int) WorkerOption {
	return func(w *Worker) {
		w.limit = limit
	}
}

// WithLogger sets a logger for an SQS worker.
func WithLogger(logger logging.Log) WorkerOption {
	return func(w *Worker) {
		w.logger = logger
	}
}

// WithVisibilityTimeout sets the visibility timeout duration for messages in an SQS client.
func WithVisibilityTimeout(timeout time.Duration) ClientOption {
	return func(c *client) {
		c.visibility = timeout
	}
}

// WithWaitTimeout sets the duration for which the client waits for a message to arrive in the queue before returning.
func WithWaitTimeout(timeout time.Duration) ClientOption {
	return func(c *client) {
		c.wait = timeout
	}
}

// WithBackoff sets the backoff strategy for a consumer
func WithBackoff(backoff Backoff) ConsumerOption {
	return func(o *consumerOptions) {
		o.backoff = backoff
	}
}
