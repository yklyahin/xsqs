package xsqs

import (
	"errors"
	"fmt"
)

var _, _ error = new(BulkError), new(BulkMessageError)

type (
	// BulkError represents an error that occurs during bulk message processing.
	// It contains a list of BulkMessageError instances that provide detailed information about individual message errors.
	// This error is intended for use with BulkConsumer.
	BulkError struct {
		Errors []BulkMessageError // List of individual message errors.
	}

	// BulkMessageError represents an error that occurs while processing an individual message in a bulk operation.
	// It contains the MessageID of the message and the corresponding error.
	BulkMessageError struct {
		MessageID string
		Err       error
	}
)

// unrecoverableError is a wrapper error type indicating that an error is unrecoverable.
// This type is used for errors that signal that the processing of a message is not recoverable and should not be retried.
type unrecoverableError struct{ error }

// UnrecoverableError creates a new unrecoverable error.
// Unrecoverable errors indicate that the processing of a message is not recoverable and should not be retried.
func UnrecoverableError(err error) error {
	return &unrecoverableError{err}
}

// IsUnrecoverableError checks if an error is an unrecoverable error.
// It returns true if the error is an unrecoverable error created using the UnrecoverableError function.
func IsUnrecoverableError(err error) bool {
	var unrecovable *unrecoverableError
	return errors.As(err, &unrecovable)
}

// Error returns the string representation of a BulkError.
func (e BulkError) Error() string {
	return fmt.Sprintf("bulk messages processing error, %d messages were unprocessed", len(e.Errors))
}

// Error returns the string representation of a BulkMessageError.
func (e BulkMessageError) Error() string {
	return fmt.Sprintf("message %s was not been processed: %s", e.MessageID, e.Err.Error())
}

// Unwrap returns the underlying error of a BulkMessageError.
// It allows accessing the original error that caused the BulkMessageError.
func (e BulkMessageError) Unwrap() error {
	return e.Err
}
