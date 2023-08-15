package xsqs

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"golang.org/x/sync/errgroup"
)

const awsLimitPerRequest = 10

type (
	// Client interface for SQS
	Client interface {
		// Receive retrieves messages from the queue.
		// It returns a slice of messages up to the specified limit.
		Receive(ctx context.Context, limit int) ([]*sqs.Message, error)
		// Delete removes a message from the queue.
		Delete(ctx context.Context, message *sqs.Message) error
		// DeleteBatch removes a batch of messages from the queue.
		DeleteBatch(ctx context.Context, messages []*sqs.Message) error
		// Retry changes the visibility timeout of a message, effectively making it
		// available for processing again after a certain delay.
		Retry(ctx context.Context, input RetryInput) error
		// RetryBatch changes the visibility timeout of a batch of messages, allowing
		// them to be retried after specific delays.
		RetryBatch(ctx context.Context, input []RetryInput) error
	}

	// RetryInput provides the necessary information for retrying a message.
	RetryInput struct {
		Message *sqs.Message
		// The delay before the message becomes visible again.
		Delay time.Duration
	}
)

// NewClient create the client for SQS
func NewClient(api sqsiface.SQSAPI, queue string, opts ...ClientOption) (Client, error) {
	url, err := GetQueueURL(api, queue)
	if err != nil {
		return nil, fmt.Errorf("failed to get an queue url: %w", err)
	}
	c := &client{
		sqs:  api,
		url:  url,
		wait: time.Second * 10,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// GetQueueURL retrieves the queue URL based on the provided queue name.
// If the queue name is already a URL, it is returned as is.
// If the queue name is an ARN, the associated queue URL is fetched.
func GetQueueURL(client sqsiface.SQSAPI, queue string) (string, error) {
	if !arn.IsARN(queue) {
		return queue, nil
	}
	queueARN, err := arn.Parse(queue)
	if err != nil {
		return "", err
	}
	input := sqs.GetQueueUrlInput{
		QueueName: aws.String(queueARN.Resource),
	}
	if len(queueARN.AccountID) > 0 {
		input.QueueOwnerAWSAccountId = aws.String(queueARN.AccountID)
	}
	response, err := client.GetQueueUrl(&input)
	if err != nil {
		return "", err
	}
	return aws.StringValue(response.QueueUrl), nil
}

type client struct {
	sqs        sqsiface.SQSAPI
	url        string
	wait       time.Duration
	visibility time.Duration
}

// Receive messages from the queue
func (c *client) Receive(ctx context.Context, limit int) ([]*sqs.Message, error) {
	if limit <= awsLimitPerRequest {
		return c.recieve(ctx, limit)
	}

	messages := make([]*sqs.Message, 0, limit)

	for len(messages) < limit {
		batch, err := c.recieve(ctx, awsLimitPerRequest)
		if err != nil {
			return nil, err
		}
		messages = append(
			messages,
			batch...,
		)
		if len(batch) < awsLimitPerRequest {
			break
		}
	}

	return messages, nil
}

// Delete the message from the queue
func (c *client) Delete(ctx context.Context, message *sqs.Message) error {
	_, err := c.sqs.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		ReceiptHandle: message.ReceiptHandle,
		QueueUrl:      aws.String(c.url),
	})
	return err
}

// Delete the batch of messages from the queue
func (c *client) DeleteBatch(ctx context.Context, messages []*sqs.Message) error {
	if len(messages) <= awsLimitPerRequest {
		return c.deleteBatch(ctx, messages)
	}
	chunks := splitIntoChunks(messages, 10)
	var wg errgroup.Group
	for _, chunk := range chunks {
		cp := chunk
		wg.Go(func() error {
			return c.deleteBatch(ctx, cp)
		})
	}
	return wg.Wait()
}

// Retry the message
// If the Backoff option has not been set, then a visibility timeout won't be changed.
func (c *client) Retry(ctx context.Context, message RetryInput) error {
	timeout := int64(message.Delay.Seconds())

	input := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(c.url),
		ReceiptHandle:     message.Message.ReceiptHandle,
		VisibilityTimeout: aws.Int64(timeout),
	}
	_, err := c.sqs.ChangeMessageVisibilityWithContext(ctx, input)
	return err
}

// Retry the batch of messages
// If the Backoff option has not been set, then a visibility timeout won't be changed.
func (c *client) RetryBatch(ctx context.Context, messages []RetryInput) error {
	if len(messages) <= awsLimitPerRequest {
		return c.retryBatch(ctx, messages)
	}

	chunks := splitIntoChunks(messages, awsLimitPerRequest)
	var wg errgroup.Group
	for _, chunk := range chunks {
		cp := chunk
		wg.Go(func() error {
			return c.retryBatch(ctx, cp)
		})
	}
	return wg.Wait()
}

func (c *client) recieve(ctx context.Context, limit int) ([]*sqs.Message, error) {
	input := &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(c.url),
		AttributeNames: []*string{
			aws.String("All"),
		},
		MessageAttributeNames: []*string{
			aws.String("All"), // Required
		},
		MaxNumberOfMessages: aws.Int64(int64(limit)),
	}
	if c.wait > 0 {
		input.WaitTimeSeconds = aws.Int64(int64(c.wait.Seconds()))
	}
	if c.visibility > 0 {
		input.VisibilityTimeout = aws.Int64(int64(c.visibility.Seconds()))
	}
	response, err := c.sqs.ReceiveMessageWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	return response.Messages, nil
}

func (c *client) deleteBatch(ctx context.Context, messages []*sqs.Message) error {
	entries := make([]*sqs.DeleteMessageBatchRequestEntry, 0, len(messages))
	for _, message := range messages {
		entries = append(entries, &sqs.DeleteMessageBatchRequestEntry{
			Id:            message.MessageId,
			ReceiptHandle: message.ReceiptHandle,
		})
	}
	_, err := c.sqs.DeleteMessageBatchWithContext(ctx, &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(c.url),
		Entries:  entries,
	})
	return err
}

func (c *client) retryBatch(ctx context.Context, messages []RetryInput) error {
	entries := make([]*sqs.ChangeMessageVisibilityBatchRequestEntry, 0, len(messages))

	for _, message := range messages {
		timeout := int64(message.Delay.Seconds())
		entries = append(entries, &sqs.ChangeMessageVisibilityBatchRequestEntry{
			Id:                message.Message.MessageId,
			ReceiptHandle:     message.Message.ReceiptHandle,
			VisibilityTimeout: aws.Int64(timeout),
		})
	}
	_, err := c.sqs.ChangeMessageVisibilityBatchWithContext(ctx, &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: aws.String(c.url),
		Entries:  entries,
	})
	return err
}

func splitIntoChunks[T any](messages []T, size int) [][]T {
	var chunks [][]T

	for i := 0; i < len(messages); i += size {
		end := i + size
		if end > len(messages) {
			end = len(messages)
		}
		chunks = append(chunks, messages[i:end])
	}
	return chunks
}
