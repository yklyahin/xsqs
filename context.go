package xsqs

import (
	"context"

	"github.com/yklyahin/xsqs/logging"
)

type contextKey int

const (
	workerCtxKey contextKey = iota
)

// WorkerCtx stores the context information for an SQS worker.
type WorkerCtx struct {
	Name   string // Name of the worker.
	Logger logging.Log
}

// GetWorkerCtxFromContext retrieves the WorkerCtx from the provided context.
// It returns the retrieved WorkerCtx and a boolean indicating whether the WorkerCtx was found in the context.
func GetWorkerCtxFromContext(ctx context.Context) (WorkerCtx, bool) {
	value := ctx.Value(workerCtxKey)
	if value == nil {
		return WorkerCtx{}, false
	}
	return value.(WorkerCtx), true
}

// ContextWithWorkerCtx returns a new context with WorkerCtx derived from the provided parent context.
func ContextWithWorkerCtx(ctx context.Context, worker WorkerCtx) context.Context {
	return context.WithValue(ctx, workerCtxKey, worker)
}
