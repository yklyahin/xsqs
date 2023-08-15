package deduplication

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/yklyahin/xsqs"
	"github.com/yklyahin/xsqs/deduplication/mocks"
)

func TestFifoMiddleware_Handle(t *testing.T) {
	storage := mocks.NewStorage(t)
	storage.On("Exists", mock.Anything, "deduplication-id").Return(false, nil)
	message := &sqs.Message{
		Attributes: map[string]*string{
			"MessageDeduplicationId": aws.String("deduplication-id"),
		},
	}

	t.Run("Success", func(t *testing.T) {
		storage.On("Save", mock.Anything, "deduplication-id").Once().Return(nil)
		handler := FifoMiddleware(xsqs.HandlerFunc[*sqs.Message](func(_ context.Context, _ *sqs.Message) error {
			return nil
		}), storage)
		err := handler.Handle(context.Background(), message)
		assert.NoError(t, err)
	})

	t.Run("Handler error", func(t *testing.T) {
		handler := FifoMiddleware(xsqs.HandlerFunc[*sqs.Message](func(_ context.Context, _ *sqs.Message) error {
			return errors.New("something went wrong")
		}), storage)
		err := handler.Handle(context.Background(), message)
		assert.Errorf(t, err, "something went wrong")
	})

	t.Run("Storage save error", func(t *testing.T) {
		storage.On("Save", mock.Anything, "deduplication-id").Once().Return(errors.New("table not found"))
		handler := FifoMiddleware(xsqs.HandlerFunc[*sqs.Message](func(_ context.Context, _ *sqs.Message) error {
			return nil
		}), storage)
		err := handler.Handle(context.Background(), message)
		assert.Errorf(t, err, "table not found")
	})
}

func TestFifoMiddleware_Duplicate(t *testing.T) {
	storage := mocks.NewStorage(t)
	storage.On("Exists", mock.Anything, "deduplication-id").Return(true, nil)
	handler := FifoMiddleware(nil, storage)
	err := handler.Handle(context.Background(), &sqs.Message{
		Attributes: map[string]*string{
			"MessageDeduplicationId": aws.String("deduplication-id"),
		},
	})
	assert.NoError(t, err)
}

func TestFifoMiddleware_StorageError(t *testing.T) {
	storage := mocks.NewStorage(t)
	storage.On("Exists", mock.Anything, "deduplication-id").Return(false, errors.New("something went wrong"))
	handler := FifoMiddleware(nil, storage)
	err := handler.Handle(context.Background(), &sqs.Message{
		Attributes: map[string]*string{
			"MessageDeduplicationId": aws.String("deduplication-id"),
		},
	})
	assert.Errorf(t, err, "something went wrong")
}

func TestFifoMiddleware_DeduplicationID_NotExists(t *testing.T) {
	handler := FifoMiddleware(nil, nil)
	err := handler.Handle(context.Background(), &sqs.Message{})
	assert.Equal(t, err, ErrMissingDeduplicationID)
}
