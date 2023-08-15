package xsqs_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/yklyahin/xsqs"
)

func TestConstantBackoff(t *testing.T) {
	err := errors.New("internal error")

	backoff := xsqs.ConstantBackoff(time.Second)
	duration, ok := backoff(1, err)
	assert.True(t, ok)
	assert.EqualValues(t, time.Second, duration)

	duration2, ok2 := backoff(10000, err)
	assert.True(t, ok2)
	assert.EqualValues(t, time.Second, duration2)
}

func TestExponentialBackoff(t *testing.T) {
	err := errors.New("internal error")

	backoff := xsqs.ExponentialBackoff(xsqs.MaxVisibilityTimeout)
	duration, ok := backoff(1, err)
	assert.True(t, ok)
	assert.Equal(t, xsqs.MinVisibilityTimeout, duration)

	duration2, ok2 := backoff(10000, err)
	assert.True(t, ok2)
	assert.Equal(t, xsqs.MaxVisibilityTimeout, duration2)
}
