// Package deduplication provides middleware for FIFO queue handler.
package deduplication

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/yklyahin/xsqs"
)

// ErrMissingDeduplicationID is returned when the deduplication ID is missing from the message attributes.
var ErrMissingDeduplicationID = errors.New("deduplication id is missing")

type (
	// Storage is an interface for deduplication storage.
	Storage interface {
		Save(ctx context.Context, deduplicationID string) error
		Exists(ctx context.Context, deduplicationID string) (bool, error)
	}

	// FifoDeduplicationMiddleware is a middleware that provides FIFO deduplication for messages.
	FifoDeduplicationMiddleware struct {
		next    xsqs.Handler[*sqs.Message]
		storage Storage
	}
)

// FifoMiddleware creates and returns a new instance of the FifoDeduplicationMiddleware.
func FifoMiddleware(next xsqs.Handler[*sqs.Message], storage Storage) *FifoDeduplicationMiddleware {
	return &FifoDeduplicationMiddleware{
		next:    next,
		storage: storage,
	}
}

// Handle handles the SQS message and provides deduplication logic.
func (middleware *FifoDeduplicationMiddleware) Handle(ctx context.Context, message *sqs.Message) error {
	deduplicationID := aws.StringValue(message.Attributes["MessageDeduplicationId"])
	if len(deduplicationID) == 0 {
		return ErrMissingDeduplicationID
	}
	exists, err := middleware.storage.Exists(ctx, deduplicationID)
	if err != nil {
		return fmt.Errorf("failed to check for duplicates, driver returned an error: %+w", err)
	}
	if exists {
		return nil
	}
	if err := middleware.next.Handle(ctx, message); err != nil {
		return err
	}
	if err := middleware.storage.Save(ctx, deduplicationID); err != nil {
		return xsqs.UnrecoverableError(fmt.Errorf("failed to create a deduplication record in database: %w", err))
	}
	return nil
}
