package deduplication

import (
	"context"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// NewDynamoDB creates a new DynamoDB instance for deduplication storage.
// It takes a DynamoDBAPI instance, table name, and TTL duration as input and returns the created Storage instance.
func NewDynamoDB(db dynamodbiface.DynamoDBAPI, table string, ttl time.Duration) Storage {
	return dynamoDB{
		table: table,
		db:    db,
		ttl:   ttl,
	}
}

// dynamoDB is an implementation of the Storage interface using DynamoDB.
type dynamoDB struct {
	table string
	ttl   time.Duration
	db    dynamodbiface.DynamoDBAPI
}

// Exists checks if the deduplication ID exists in the DynamoDB table.
func (storage dynamoDB) Exists(ctx context.Context, deduplicationID string) (bool, error) {
	out, err := storage.db.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(storage.table),
		Key: map[string]*dynamodb.AttributeValue{
			"deduplication_id": {
				S: aws.String(deduplicationID),
			},
		},
	})
	if err != nil {
		return false, err
	}
	return len(out.Item) > 0, nil
}

// Save saves the deduplication ID and timestamp in the DynamoDB table.
func (driver dynamoDB) Save(ctx context.Context, deduplicationID string) error {
	current := time.Now()
	item := map[string]*dynamodb.AttributeValue{
		"deduplication_id": {
			S: aws.String(deduplicationID),
		},
		"created_at": {
			N: aws.String(strconv.FormatInt(current.Unix(), 10)),
		},
	}
	if driver.ttl > 0 {
		item["ttl"] = &dynamodb.AttributeValue{
			N: aws.String(strconv.FormatInt(current.Add(driver.ttl).Unix(), 10)),
		}
	}
	_, err := driver.db.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(driver.table),
		Item:      item,
	})
	return err
}
