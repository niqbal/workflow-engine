package activity

import (
	"context"
	"time"
)

// Info describes the currently running activity attempt.
type Info struct {
	WorkflowID     string
	RunID          string
	ActivityID     string
	ActivityType   string
	Attempt        int
	IdempotencyKey string
	Deadline       time.Time
}

type infoKey struct{}

// WithInfo attaches activity metadata to ctx. The worker uses this before calling
// activity code.
func WithInfo(ctx context.Context, info Info) context.Context {
	return context.WithValue(ctx, infoKey{}, info)
}

// GetInfo returns metadata for the current activity attempt.
func GetInfo(ctx context.Context) Info {
	if v, ok := ctx.Value(infoKey{}).(Info); ok {
		return v
	}
	return Info{}
}

// RecordHeartbeat is reserved for heartbeat support. In M1 it is a no-op.
func RecordHeartbeat(context.Context, any) {}
