// Package logging provides the logger interface for XSQS
package logging

// Log is the interface for a logger.
type Log interface {
	// Infow logs an informational message with optional key-value pairs.
	Infow(msg string, kv ...any)
	// Warnw logs a warning message with optional key-value pairs.
	Warnw(msg string, kv ...any)
	// Errorw logs an error message with optional key-value pairs.
	Errorw(msg string, kv ...any)
	// With returns a new logger instance with additional key-value pairs.
	With(kv ...any) Log
}
