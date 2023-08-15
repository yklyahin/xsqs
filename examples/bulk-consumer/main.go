package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/yklyahin/xsqs"
	"go.uber.org/zap"
)

func main() {
	sqsClient := setupExample()

	xsqsClient, err := xsqs.NewClient(sqsClient, "bulk")
	if err != nil {
		log.Fatalf("failed to create xsqs client: %s", err)
	}

	handler := xsqs.HandlerFunc[[]*sqs.Message](handler)
	consumer := xsqs.NewBulkConsumer(xsqsClient, handler)
	worker := xsqs.NewWorker("bulk", xsqsClient, consumer,
		xsqs.WithMessagesLimit(30),
		xsqs.WithIdle(time.Minute*10, true),
	)
	worker.Start(context.Background())
}

func handler(ctx context.Context, messages []*sqs.Message) error {
	log.Printf("number of messages: %d", len(messages))

	err := new(xsqs.BulkError)

	// These messages won't be retried
	for _, message := range messages[:5] {
		err.Errors = append(err.Errors, xsqs.BulkMessageError{
			MessageID: aws.StringValue(message.MessageId),
			Err:       xsqs.UnrecoverableError(errors.New("unrecoverable error: something went wrong")),
		})
	}

	lastMessageID := messages[len(messages)-1].MessageId

	err.Errors = append(err.Errors, xsqs.BulkMessageError{
		MessageID: aws.StringValue(lastMessageID),
		Err:       errors.New("connection error"), // This message will be retried
	})
	return err
}

func setupExample() *sqs.SQS {
	zap.ReplaceGlobals(zap.NewExample())

	cfg := aws.NewConfig().
		WithEndpoint("http://localhost:4566").
		WithRegion("eu-west-1")
	client := sqs.New(session.Must(session.NewSession(cfg)))

	createQueueOut, err := client.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String("bulk"),
	})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 50; i++ {
		client.SendMessage(&sqs.SendMessageInput{
			QueueUrl:    createQueueOut.QueueUrl,
			MessageBody: aws.String(fmt.Sprintf("message-%d", i)),
		})
	}
	return client
}
