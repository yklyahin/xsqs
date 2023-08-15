package xsqs_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/yklyahin/xsqs"
	"github.com/yklyahin/xsqs/mocks"
)

func TestWorker_StartWithoutIdle(t *testing.T) {
	client := mocks.NewClient(t)
	client.On("Receive", mock.Anything, mock.Anything).Return(make([]*sqs.Message, 10), nil)

	ctx, cancel := context.WithCancel(context.Background())

	calls := 0
	consumer := mocks.NewConsumer(t)
	consumer.On("Consume", mock.Anything, mock.Anything).Return(func(ctx context.Context, messages []*sqs.Message) error {
		workerCtx, ok := xsqs.GetWorkerCtxFromContext(ctx)
		assert.True(t, ok)
		assert.NotNil(t, workerCtx.Logger)
		assert.Equal(t, "no-idle", workerCtx.Name)
		calls++
		if calls == 2 {
			cancel()
		}
		return nil
	})

	worker := xsqs.NewWorker("no-idle", client, consumer)
	go worker.Start(ctx)
	<-ctx.Done()

	client.AssertNumberOfCalls(t, "Receive", 2)
	consumer.AssertNumberOfCalls(t, "Consume", 2)
}

func TestWorker_StartWithIdle(t *testing.T) {
	client := mocks.NewClient(t)
	client.On("Receive", mock.Anything, mock.Anything).Return(make([]*sqs.Message, 10), nil)

	var calls []time.Time

	ctx, cancel := context.WithCancel(context.Background())
	consumer := mocks.NewConsumer(t)
	consumer.On("Consume", mock.Anything, mock.Anything).Return(func(ctx context.Context, messages []*sqs.Message) error {
		calls = append(calls, time.Now())
		if len(calls) == 2 {
			cancel()
		}
		return nil
	})

	worker := xsqs.NewWorker("with-idle", client, consumer, xsqs.WithIdle(time.Millisecond*500, false))
	go worker.Start(ctx)
	<-ctx.Done()

	sub := calls[1].Sub(calls[0]).Milliseconds()
	assert.GreaterOrEqual(t, sub, int64(400))
}

func TestWorker_StartWithIdleAndDrainMode(t *testing.T) {
	client := mocks.NewClient(t)
	client.On("Receive", mock.Anything, mock.Anything).Return(make([]*sqs.Message, 10), nil).Once()
	client.On("Receive", mock.Anything, mock.Anything).Return(make([]*sqs.Message, 10), nil).Once()
	client.On("Receive", mock.Anything, mock.Anything).Return(make([]*sqs.Message, 5), nil).Once()

	var calls []time.Time

	ctx, cancel := context.WithCancel(context.Background())
	consumer := mocks.NewConsumer(t)
	consumer.On("Consume", mock.Anything, mock.Anything).Return(func(ctx context.Context, messages []*sqs.Message) error {
		calls = append(calls, time.Now())
		if len(calls) == 3 {
			cancel()
		}
		return nil
	})

	worker := xsqs.NewWorker("with-idle", client, consumer, xsqs.WithIdle(time.Minute, true))
	go worker.Start(ctx)
	<-ctx.Done()

	assert.Len(t, calls, 3)

	for i := 1; i < len(calls); i++ {
		sub := calls[i].Sub(calls[i-1])
		assert.LessOrEqual(t, sub, time.Second)
	}
}

func TestWorker_PanicRecovery(t *testing.T) {
	client := mocks.NewClient(t)
	client.On("Receive", mock.Anything, mock.Anything).Panic("something wrong").Once()
	client.On("Receive", mock.Anything, mock.Anything).Return(make([]*sqs.Message, 5), nil).Once()

	ctx, cancel := context.WithCancel(context.Background())
	consumer := mocks.NewConsumer(t)
	consumer.On("Consume", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		cancel()
	}).Return(nil)
	worker := xsqs.NewWorker("with-idle", client, consumer)
	go worker.Start(ctx)
	<-ctx.Done()

	consumer.AssertNumberOfCalls(t, "Consume", 1)
}
