package xsqs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/yklyahin/xsqs"
	"github.com/yklyahin/xsqs/logging"
	"github.com/yklyahin/xsqs/mocks"
)

func TestSequentialConsumer(t *testing.T) {
	ctx := xsqs.ContextWithWorkerCtx(context.Background(), xsqs.WorkerCtx{
		Name:   "worker-1",
		Logger: logging.DefaultZapAdapter(),
	})
	t.Run("Success", func(t *testing.T) {
		client := mocks.NewClient(t)
		client.On("Delete", mock.Anything, mock.Anything).Times(3).Return(nil, nil)
		consumer := xsqs.NewSequentialConsumer(client, createMockHandler[*sqs.Message](nil))
		consumer.Consume(ctx, make([]*sqs.Message, 3))
		client.AssertExpectations(t)
	})

	t.Run("Unrecoverable", func(t *testing.T) {
		client := mocks.NewClient(t)
		client.On("Delete", mock.Anything, mock.Anything).Times(1).Return(nil, nil)
		err := xsqs.UnrecoverableError(errors.New("something went wrong"))
		consumer := xsqs.NewSequentialConsumer(client, createMockHandler[*sqs.Message](err))
		consumer.Consume(ctx, make([]*sqs.Message, 1))
		client.AssertExpectations(t)
	})

	t.Run("Retry without backoff", func(t *testing.T) {
		client := mocks.NewClient(t)
		consumer := xsqs.NewSequentialConsumer(
			client,
			createMockHandler[*sqs.Message](errors.New("timeout error")),
		)
		consumer.Consume(ctx, []*sqs.Message{{
			MessageId: aws.String("message-id-1"),
		}})
	})

	t.Run("Retry with backoff", func(t *testing.T) {
		client := mocks.NewClient(t)
		client.On("Retry", mock.Anything, mock.Anything).Return(nil)
		consumer := xsqs.NewSequentialConsumer(
			client,
			createMockHandler[*sqs.Message](errors.New("timeout error")),
			xsqs.WithBackoff(xsqs.ConstantBackoff(time.Hour)),
		)
		consumer.Consume(ctx, []*sqs.Message{{
			MessageId: aws.String("message-id-1"),
			Attributes: map[string]*string{
				"ApproximateReceiveCount": aws.String("5"),
			},
		}})
		client.AssertNotCalled(t, "Delete", mock.Anything, mock.Anything)
	})

	t.Run("Backoff return non-retryable", func(t *testing.T) {
		client := mocks.NewClient(t)
		client.On("Delete", mock.Anything, mock.Anything).Return(nil)
		backoff := xsqs.Backoff(func(_ int, _ error) (time.Duration, bool) {
			return 0, false
		})
		consumer := xsqs.NewSequentialConsumer(
			client,
			createMockHandler[*sqs.Message](errors.New("timeout error")),
			xsqs.WithBackoff(backoff),
		)
		consumer.Consume(ctx, []*sqs.Message{{
			MessageId: aws.String("message-id-1"),
			Attributes: map[string]*string{
				"ApproximateReceiveCount": aws.String("5"),
			},
		}})
	})
}

func TestParallelConsumer(t *testing.T) {
	ctx := xsqs.ContextWithWorkerCtx(context.Background(), xsqs.WorkerCtx{
		Name:   "worker-1",
		Logger: logging.DefaultZapAdapter(),
	})
	client := mocks.NewClient(t)
	client.On("Delete", mock.Anything, mock.Anything).Return(nil, nil)
	consumer := xsqs.NewParallelConsumer(client, createMockHandler[*sqs.Message](nil))
	consumer.Consume(ctx, make([]*sqs.Message, 3))
	client.AssertExpectations(t)
}

func TestBulkConsumer(t *testing.T) {
	ctx := xsqs.ContextWithWorkerCtx(context.Background(), xsqs.WorkerCtx{
		Name:   "worker-1",
		Logger: logging.DefaultZapAdapter(),
	})
	client := mocks.NewClient(t)
	t.Run("All unrecoverable", func(t *testing.T) {
		client.On("DeleteBatch", mock.Anything, mock.Anything).Once().Return(nil, nil)
		err := xsqs.UnrecoverableError(errors.New("something went wrong"))
		consumer := xsqs.NewBulkConsumer(client, createMockHandler[[]*sqs.Message](err))
		consumer.Consume(ctx, make([]*sqs.Message, 10))
	})
	t.Run("Mixed result", func(t *testing.T) {
		unrecoverableErr := xsqs.UnrecoverableError(errors.New("something went wrong"))
		scenarios := []struct {
			MessageId string
			Err       error
		}{
			{MessageId: "unrecoverable-1", Err: unrecoverableErr},
			{MessageId: "unrecoverable-2", Err: unrecoverableErr},
			{MessageId: "retryable-1", Err: errors.New("timeout")},
			{MessageId: "success-1"},
		}

		var (
			messages []*sqs.Message
			err      xsqs.BulkError
		)

		for _, scenario := range scenarios {
			messages = append(messages, &sqs.Message{
				MessageId: aws.String(scenario.MessageId),
			})
			if scenario.Err == nil {
				continue
			}
			err.Errors = append(err.Errors, xsqs.BulkMessageError{
				MessageID: scenario.MessageId,
				Err:       scenario.Err,
			})
		}

		deleteBatchAssert := func(ctx context.Context, messages []*sqs.Message) error {
			expected := map[string]any{"unrecoverable-1": nil, "unrecoverable-2": nil, "success-1": nil}
			assert.Len(t, messages, 3)
			for _, message := range messages {
				delete(expected, aws.StringValue(message.MessageId))
			}
			assert.Empty(t, expected, "not all messages are in the deletion list")
			return nil
		}

		client.On("DeleteBatch", mock.Anything, mock.Anything).Once().Return(deleteBatchAssert)
		client.On("RetryBatch", mock.Anything, mock.Anything).Return(nil)
		consumer := xsqs.NewBulkConsumer(
			client,
			createMockHandler[[]*sqs.Message](&err),
			xsqs.WithBackoff(xsqs.ConstantBackoff(time.Hour)),
		)
		consumer.Consume(ctx, messages)
	})
}

func createMockHandler[T any](err error) xsqs.Handler[T] {
	return xsqs.HandlerFunc[T](func(ctx context.Context, message T) error {
		return err
	})
}
