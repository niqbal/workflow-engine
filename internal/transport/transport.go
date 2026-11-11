// Package transport is node-to-node RPC (SPEC §6.1). Production uses HTTP; the simulator
// uses an in-process network that can drop, delay and partition.
package transport

import (
	"context"
	"errors"
)

// NodeID names a cluster node, as configured in the static peer list.
type NodeID string

// ErrUnreachable means the call did not reach the node or its reply was lost.
// The callee may or may not have acted: callers must treat it as ambiguous.
var ErrUnreachable = errors.New("transport: node unreachable")

// Transport sends one request to a node and decodes its reply into resp.
type Transport interface {
	Call(ctx context.Context, node NodeID, method string, req, resp any) error
}
