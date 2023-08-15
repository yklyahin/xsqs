package xsqs

import (
	"context"
	"errors"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/yklyahin/xsqs/logging"
)

// NewSequentialConsumer creates a consumer for the XSQS worker that processes messages one by one.
func NewSequentialConsumer(client Client, handler Handler[*sqs.Message], opts ...ConsumerOption) *SequentialConsumer {
	var options consumerOptions

	for _, opt := range opts {
		opt(&options)
	}
	return &SequentialConsumer{
		client:  client,
		handler: handler,
		backoff: options.backoff,
	}
}

// SequentialConsumer processes messages one by one
type SequentialConsumer struct {
	client  Client
	handler Handler[*sqs.Message]
	backoff Backoff
}

// Consume processes the messages one by one using the provided handler.
// It delegates the processing of each message to the handler and takes appropriate action based on the error result.
func (c *SequentialConsumer) Consume(ctx context.Context, messages []*sqs.Message) error {
	for _, message := range messages {
		c.consumeSingleMessage(ctx, message)
	}
	return nil
}

// It deletes the message if it is successfully processed or an unrecoverable error occurs.
// Otherwise, it changes the visibility timeout of the message.
func (c *SequentialConsumer) consumeSingleMessage(ctx context.Context, message *sqs.Message) {
	err := c.handler.Handle(ctx, message)
	if err == nil || IsUnrecoverableError(err) {
		c.deleteMessage(ctx, message)
		return
	}
	retry, retryable := convertErrorToRetryInput(c.backoff, message, err)
	if !retryable {
		c.deleteMessage(ctx, message)
		return
	}
	if retry.Delay > 0 {
		if err := c.client.Retry(ctx, retry); err != nil {
			info, _ := GetWorkerCtxFromContext(ctx)
			info.Logger.Errorw("failed to change visibility timeout",
				"message.id", message.MessageId,
			)
		}
	}
}

func (c *SequentialConsumer) deleteMessage(ctx context.Context, message *sqs.Message) {
	if err := c.client.Delete(ctx, message); err != nil {
		info, _ := GetWorkerCtxFromContext(ctx)
		info.Logger.Errorw("failed to delete a message",
			"message.id", message.MessageId,
		)
	}
}

// NewParallelConsumer creates a consumer for the XSQS worker that processes messages in parallel, each message in its own goroutine.
func NewParallelConsumer(client Client, handler Handler[*sqs.Message], opts ...ConsumerOption) *ParallelConsumer {
	return &ParallelConsumer{
		inner: NewSequentialConsumer(client, handler, opts...),
	}
}

// ParallelConsumer processes messages in parallel using a SequentialConsumer for each message.
type ParallelConsumer struct {
	inner *SequentialConsumer
}

// Consume processes the messages in parallel by creating separate goroutines for each message.
func (c *ParallelConsumer) Consume(ctx context.Context, messages []*sqs.Message) error {
	var wg sync.WaitGroup
	wg.Add(len(messages))

	for _, message := range messages {
		go func(message *sqs.Message) {
			defer wg.Done()
			c.inner.consumeSingleMessage(ctx, message)
		}(message)
	}
	wg.Wait()

	return nil
}

// NewBulkConsumer creates a consumer for the XSQS worker that handles messages in bulk.
func NewBulkConsumer(client Client, handler Handler[[]*sqs.Message], opts ...ConsumerOption) *BulkConsumer {
	var options consumerOptions

	for _, opt := range opts {
		opt(&options)
	}
	return &BulkConsumer{
		client:  client,
		handler: handler,
		backoff: options.backoff,
	}
}

// BulkConsumer is a bulk implementation of the Consumer interface that handles messages in bulk.
type BulkConsumer struct {
	client  Client
	handler Handler[[]*sqs.Message]
	backoff Backoff
}

// Consume processes the messages in bulk using the provided handler.
// It handles errors differently based on the error type:
// - For UnrecoverableError, all received messages will be deleted.
// - For BulkError, it categorizes messages to be deleted or retried based on error types.
func (c *BulkConsumer) Consume(ctx context.Context, messages []*sqs.Message) error {
	err := c.handler.Handle(ctx, messages)

	if err == nil || IsUnrecoverableError(err) {
		info, _ := GetWorkerCtxFromContext(ctx)

		err := c.client.DeleteBatch(ctx, messages)
		if err != nil {
			info.Logger.Errorw("failed to delete a batch of message")
			return nil
		}

		if err == nil {
			c.writeStatisticsLog(info.Logger, len(messages), 0, 0)
		} else {
			c.writeStatisticsLog(info.Logger, 0, len(messages), 0)
		}
	}
	var bulkError *BulkError
	if !errors.As(err, &bulkError) {
		return err
	}

	c.handleErrors(ctx, bulkError, messages)

	return nil
}

// handleErrors processes the errors returned by the handler for bulk processing.
// It categorizes the errors into messages to delete and messages to retry based on their error types.
func (c *BulkConsumer) handleErrors(ctx context.Context, errors *BulkError, messages []*sqs.Message) {
	hashmap := make(map[string]*sqs.Message)

	for _, message := range messages {
		hashmap[aws.StringValue(message.MessageId)] = message
	}

	var (
		deleteMessages                    []*sqs.Message
		retryMessages                     []RetryInput
		success, retryable, unrecoverable int
	)

	info, _ := GetWorkerCtxFromContext(ctx)

	for _, err := range errors.Errors {
		message, ok := hashmap[err.MessageID]
		if !ok {
			info.Logger.Warnw("message.id not exists: you may have set the wrong message id in the bulk error", "message.id", err.MessageID)
			continue
		}
		if IsUnrecoverableError(err) {
			deleteMessages = append(deleteMessages, message)
			unrecoverable++
		} else {
			retryInput, isRetryable := convertErrorToRetryInput(c.backoff, message, err)
			if !isRetryable {
				deleteMessages = append(deleteMessages, message)
				unrecoverable++
			} else {
				if retryInput.Delay > 0 {
					retryMessages = append(retryMessages, retryInput)
				}
				retryable++
			}
		}
		delete(hashmap, err.MessageID)
	}
	for _, message := range hashmap {
		success++
		deleteMessages = append(deleteMessages, message)
	}

	if len(deleteMessages) > 0 {
		if err := c.client.DeleteBatch(ctx, deleteMessages); err != nil {
			info.Logger.Errorw("failed to delete messages", "err", err)
		}
	}

	if len(retryMessages) > 0 {
		if err := c.client.RetryBatch(ctx, retryMessages); err != nil {
			info.Logger.Errorw("failed to change visibility timeout", "err", err)
		}
	}
	c.writeStatisticsLog(info.Logger, success, unrecoverable, retryable)
}

func (c *BulkConsumer) writeStatisticsLog(log logging.Log, success, unrecoverable, recoverable int) {
	log.Infow("bulk messages processing statistics",
		"stats.success", success,
		"stats.recoverable", recoverable,
		"stats.unrecoverable", unrecoverable,
	)
}

type consumerOptions struct {
	backoff Backoff
}
