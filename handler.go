package xsqs

import (
	"context"
)

type (
	// Handler is an interface that defines the contract for handling messages of type T.
	// The Handle method should process the message and return an error if any.
	// By default, any error is considered retryable. To return an unrecoverable error, see UnrecoverableError.
	Handler[T any] interface {
		Handle(ctx context.Context, message T) error
	}

	// HandlerFunc is a function type that implements the Handler interface for messages of type T.
	// It allows using functions as handlers.
	HandlerFunc[T any] func(ctx context.Context, message T) error
)

// Handle calls the underlying HandlerFunc function.
func (fn HandlerFunc[T]) Handle(ctx context.Context, message T) error {
	return fn(ctx, message)
}
