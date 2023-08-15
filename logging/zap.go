package logging

import (
	"go.uber.org/zap"
)

var _ Log = new(ZapAdapter)

// ZapAdapter is an implementation of the Log interface using the Zap logger.
type ZapAdapter struct {
	*zap.SugaredLogger
}

// DefaultZapAdapter returns a new instance of the ZapAdapter as the default logger.
func DefaultZapAdapter() Log {
	return &ZapAdapter{
		SugaredLogger: zap.S(),
	}
}

// With returns a new Log instance with additional key-value pairs.
func (log ZapAdapter) With(kv ...any) Log {
	return &ZapAdapter{
		SugaredLogger: log.SugaredLogger.With(kv...),
	}
}
