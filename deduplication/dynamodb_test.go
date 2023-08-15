package deduplication

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/yklyahin/xsqs/deduplication/mocks"
)

func TestDynamoDB_Exists(t *testing.T) {
	db := mocks.NewDynamoDBAPI(t)
	storage := NewDynamoDB(db, "table", 0)

	input := &dynamodb.GetItemInput{
		TableName: aws.String("table"),
		Key: map[string]*dynamodb.AttributeValue{
			"deduplication_id": {
				S: aws.String("deduplication-id"),
			},
		},
	}

	t.Run("Exists", func(t *testing.T) {
		db.On("GetItemWithContext", mock.Anything, input).Once().Return(&dynamodb.GetItemOutput{
			Item: map[string]*dynamodb.AttributeValue{
				"deduplicate_id": {
					S: aws.String("deduplication-id"),
				},
			},
		}, nil)
		exists, err := storage.Exists(context.Background(), "deduplication-id")
		assert.NoError(t, err)
		assert.True(t, exists)
	})
	t.Run("Not exists", func(t *testing.T) {
		db.On("GetItemWithContext", mock.Anything, input).Once().Return(&dynamodb.GetItemOutput{}, nil)
		exists, err := storage.Exists(context.Background(), "deduplication-id")
		assert.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestDynamoDB_Save(t *testing.T) {
	db := mocks.NewDynamoDBAPI(t)
	db.On("PutItemWithContext", mock.Anything, mock.Anything).Return(nil, nil)

	t.Run("With TTL", func(t *testing.T) {
		storage := NewDynamoDB(db, "table", 100)
		err := storage.Save(context.Background(), "deduplication-id")
		assert.NoError(t, err)

		dynamodbInput := func(input *dynamodb.PutItemInput) bool {
			_, ok := input.Item["ttl"]
			return ok
		}
		db.AssertCalled(t, "PutItemWithContext", mock.Anything, mock.MatchedBy(dynamodbInput))
	})

	t.Run("Without TTL", func(t *testing.T) {
		storage := NewDynamoDB(db, "table", 0)
		err := storage.Save(context.Background(), "deduplication-id")
		assert.NoError(t, err)
	})
}
