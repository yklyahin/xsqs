package xsqs_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/yklyahin/xsqs"
	"github.com/yklyahin/xsqs/mocks"
)

func TestClientSuiteRunner(t *testing.T) {
	suite.Run(t, new(ClientTestSuite))
}

type ClientTestSuite struct {
	suite.Suite

	api    *mocks.SQSAPI
	client xsqs.Client
}

func (suite *ClientTestSuite) SetupTest() {
	suite.api = mocks.NewSQSAPI(suite.T())
	client, err := xsqs.NewClient(suite.api, "test-queue")
	suite.NoError(err)
	suite.client = client
}

func (suite *ClientTestSuite) TestRecieve() {
	suite.api.On("ReceiveMessageWithContext", mock.Anything, mock.Anything).Return(&sqs.ReceiveMessageOutput{
		Messages: make([]*sqs.Message, 10),
	}, nil).Once()
	suite.api.On("ReceiveMessageWithContext", mock.Anything, mock.Anything).Return(&sqs.ReceiveMessageOutput{
		Messages: make([]*sqs.Message, 7),
	}, nil).Once()

	messages, err := suite.client.Receive(context.Background(), 20)
	suite.NoError(err)
	suite.Len(messages, 17)
}

func (suite *ClientTestSuite) TestDeleteBatch() {
	suite.api.On("DeleteMessageBatchWithContext", mock.Anything, mock.Anything).Return(nil, nil).Once()
	suite.api.On("DeleteMessageBatchWithContext", mock.Anything, mock.Anything).Return(nil, nil).Once()
	messages := suite.generateSQSMessages(17)
	err := suite.client.DeleteBatch(context.Background(), messages)
	suite.NoError(err)
}

func (suite *ClientTestSuite) TestRetry() {
	suite.api.On("ChangeMessageVisibilityWithContext", mock.Anything, mock.MatchedBy(func(input *sqs.ChangeMessageVisibilityInput) bool {
		suite.Assert().EqualValues(30, aws.Int64Value(input.VisibilityTimeout))
		return true
	})).Return(nil, nil).Once()
	err := suite.client.Retry(context.Background(), xsqs.RetryInput{
		Message: &sqs.Message{
			MessageId: aws.String("message"),
		},
		Delay: time.Second * 30,
	},
	)
	suite.NoError(err)
}

func (suite *ClientTestSuite) TestRetryBatch() {
	suite.api.On("ChangeMessageVisibilityBatchWithContext", mock.Anything, mock.Anything).Return(nil, nil).Once()
	suite.api.On("ChangeMessageVisibilityBatchWithContext", mock.Anything, mock.MatchedBy(func(input *sqs.ChangeMessageVisibilityBatchInput) bool {
		suite.Assert().EqualValues(30, aws.Int64Value(input.Entries[0].VisibilityTimeout))
		return true
	})).Return(nil, nil).Once()

	messages := suite.generateSQSMessages(17)

	var input []xsqs.RetryInput

	for _, message := range messages {
		input = append(input, xsqs.RetryInput{
			Message: message,
			Delay:   time.Second * 30,
		})
	}
	err := suite.client.RetryBatch(context.Background(), input)
	suite.NoError(err)
}

func (suite *ClientTestSuite) TestDelete() {
	suite.api.On("DeleteMessageWithContext", mock.Anything, mock.Anything).Return(nil, nil).Once()
	err := suite.client.Delete(context.Background(), &sqs.Message{
		MessageId: aws.String("message"),
	})
	suite.NoError(err)
}

func (suite *ClientTestSuite) generateSQSMessages(n int) []*sqs.Message {
	var messages []*sqs.Message
	for i := 0; i < n; i++ {
		messages = append(messages, &sqs.Message{
			MessageId:     aws.String("message-id"),
			ReceiptHandle: aws.String("receipt"),
		})
	}
	return messages
}

func TestGetQueueURL(t *testing.T) {
	api := mocks.NewSQSAPI(t)
	api.On("GetQueueUrl", &sqs.GetQueueUrlInput{
		QueueName:              aws.String("test"),
		QueueOwnerAWSAccountId: aws.String("000000000000"),
	}).Return(&sqs.GetQueueUrlOutput{
		QueueUrl: aws.String("https://queue"),
	}, nil)

	url, err := xsqs.GetQueueURL(api, "arn:aws:sqs:eu-west-1:000000000000:test")
	assert.NoError(t, err)
	assert.Equal(t, "https://queue", url)
}
