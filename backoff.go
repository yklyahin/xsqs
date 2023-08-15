package xsqs

import (
	"math"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

const (
	// MaxVisibilityTimeout represents the maximum visibility timeout duration for an SQS message.
	MaxVisibilityTimeout = time.Second*43200 - time.Second*1

	// MinVisibilityTimeout represents the minimum visibility timeout duration for an SQS message.
	MinVisibilityTimeout = time.Second * 10
)

// Backoff represents a function that calculates the backoff duration based on the number of attempts and an error.
// It returns the calculated backoff duration and a boolean indicating whether the message is retryable.
// If the message is retryable, but the duration is 0, the message won't be deleted from the queue and
// the VisibilityTimeout will not be updated.
type Backoff func(attemtps int, err error) (time.Duration, bool)

// ConstantBackoff returns a Backoff function that always returns the same duration value for each attempt.
// The provided duration is used as the fixed backoff duration for all retries.
func ConstantBackoff(duration time.Duration) Backoff {
	return func(_ int, err error) (time.Duration, bool) {
		return duration, true
	}
}

// ExponentialBackoff returns a Backoff function that calculates the backoff duration in an exponentially growing manner.
// The provided max duration specifies the upper limit for the backoff duration.
func ExponentialBackoff(max time.Duration) Backoff {
	return func(attemtps int, err error) (time.Duration, bool) {
		seconds := math.Pow(2, float64(attemtps))
		seconds = math.Min(seconds, max.Seconds())
		seconds = math.Max(MinVisibilityTimeout.Seconds(), seconds)
		return time.Second * time.Duration(seconds), true
	}
}

func convertErrorToRetryInput(backoff Backoff, message *sqs.Message, err error) (RetryInput, bool) {
	var input RetryInput
	if backoff == nil {
		return input, true
	}
	attempts := 1
	if val := aws.StringValue(message.Attributes["ApproximateReceiveCount"]); val != "" {
		attempts, _ = strconv.Atoi(val)
	}
	delay, retryable := backoff(attempts, err)
	input.Message = message
	input.Delay = delay
	return input, retryable
}
