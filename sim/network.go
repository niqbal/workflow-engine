package sim

// The simulated network (WI-051, WI-052, SPEC §6.1). Nodes in one process, reached the way a
// cluster reaches them: a Transport whose calls are JSON, cross a scheduler yield point, and
// can be lost.
//
// What makes this worth having rather than calling the peer's method directly is the failure
// it can produce. A call that is dropped *after* the peer acted returns ErrUnreachable, which
// is exactly the ambiguity the real HTTP transport promises and the one G1 rests on: the
// caller cannot tell whether the work happened. Routing has to be safe under it, and here the
// oracle checks that it is.

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/transport"
)

// method is a registered peer method on one node.
type method func(ctx context.Context, decode func(any) error) (any, error)

// network delivers calls between the cluster's nodes.
type network struct {
	c *Cluster
	// methods[node][method] is what that node serves. A node registers its methods when an
	// incarnation starts; a crashed node has none, so a call to it is unreachable.
	methods map[string]map[string]method
}

func newNetwork(c *Cluster) *network {
	return &network{c: c, methods: map[string]map[string]method{}}
}

// register installs a node's methods, replacing whatever the previous incarnation served.
// The returned function has the shape Server.Register expects, which is the same shape the
// HTTP transport's handler takes.
func (n *network) register(node string) func(string, func(context.Context, func(any) error) (any, error)) {
	n.methods[node] = map[string]method{}
	return func(name string, fn func(context.Context, func(any) error) (any, error)) {
		n.methods[node][name] = method(fn)
	}
}

// unregister takes a node off the network, as a crash does.
func (n *network) unregister(node string) {
	delete(n.methods, node)
}

// transportFor returns the Transport a node uses to reach its peers.
func (n *network) transportFor(from string) transport.Transport {
	return &nodeTransport{net: n, from: from}
}

type nodeTransport struct {
	net  *network
	from string
}

// Call delivers one request. It serializes through JSON like the real transport, so a type
// that does not survive the wire fails here too, and it yields, so a call is a scheduling
// point the simulator can interleave.
func (t *nodeTransport) Call(ctx context.Context, node transport.NodeID, name string, req, resp any) error {
	c := t.net.c
	c.rt.Yield()

	// A node that is down, or paused past the point of answering, is unreachable.
	target := c.slotByNode(string(node))
	if target == nil || target.server() == nil {
		return fmt.Errorf("%w: %s is down", transport.ErrUnreachable, node)
	}
	if c.faults.dropsMessage(c.rt.Rand()) {
		c.rt.Tracef("fault drop-call %s->%s %s", t.from, node, name)
		return fmt.Errorf("%w: call to %s was dropped", transport.ErrUnreachable, node)
	}
	if d := c.faults.messageDelay(c.rt.Rand()); d > 0 {
		c.rt.Sleep(d)
	}

	fn, ok := t.net.methods[string(node)][name]
	if !ok {
		return fmt.Errorf("%w: %s does not serve %s", transport.ErrUnreachable, node, name)
	}

	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("sim: encode %s: %w", name, err)
	}
	out, err := fn(ctx, func(into any) error { return json.Unmarshal(body, into) })
	if err != nil {
		return err // a typed error crosses the wire as itself, like the HTTP transport's
	}

	// The reply can be lost after the work was done, which is the ambiguity that matters.
	if c.faults.dropsMessage(c.rt.Rand()) {
		c.rt.Tracef("fault drop-reply %s->%s %s", t.from, node, name)
		return fmt.Errorf("%w: reply from %s was lost", transport.ErrUnreachable, node)
	}
	if resp == nil || out == nil {
		return nil
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		return fmt.Errorf("sim: encode reply of %s: %w", name, err)
	}
	if err := json.Unmarshal(encoded, resp); err != nil {
		return fmt.Errorf("sim: decode reply of %s: %w", name, err)
	}
	return nil
}

// slotByNode finds a node slot by the ID the cluster gave it.
func (c *Cluster) slotByNode(node string) *node {
	for _, n := range c.slots {
		if n.name() == node {
			return n
		}
	}
	return nil
}

// Frontend returns one node's cluster-wide frontend: the router that answers for any shard,
// forwarding to the owner when this node is not it. Workers and clients use it, which is what
// makes "talk to any node" true.
func (c *Cluster) Frontend(i int) api.Service {
	if i < 0 || i >= len(c.slots) {
		return nil
	}
	return c.slots[i].frontend()
}
