package main

import (
	"context"
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

	xsqsClient, err := xsqs.NewClient(sqsClient, "simple")
	if err != nil {
		panic(err)
	}
	handler := xsqs.HandlerFunc[*sqs.Message](handler)
	consumer := xsqs.NewParallelConsumer(xsqsClient, handler,
		xsqs.WithBackoff(xsqs.ExponentialBackoff(time.Hour*5)),
	)
	worker := xsqs.NewWorker("simple-worker", xsqsClient, consumer)
	worker.Start(context.Background())
}

func handler(ctx context.Context, message *sqs.Message) error {
	log.Printf("new message: %+v", message)
	return nil
}

func setupExample() *sqs.SQS {
	zap.ReplaceGlobals(zap.NewExample())

	cfg := aws.NewConfig().
		WithEndpoint("http://localhost:4566").
		WithRegion("eu-west-1")
	client := sqs.New(session.Must(session.NewSession(cfg)))

	createQueueOut, err := client.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String("simple"),
	})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 10; i++ {
		client.SendMessage(&sqs.SendMessageInput{
			QueueUrl:    createQueueOut.QueueUrl,
			MessageBody: aws.String(fmt.Sprintf("message-%d", i)),
		})
	}
	return client
}
