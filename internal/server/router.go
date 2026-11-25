package server

// WI-051 and WI-052: a frontend that answers for the whole cluster.
//
// A client may talk to any node. The node that can *serve* a request is the one that owns the
// shard the request belongs to, so the frontend either handles the call itself or forwards it
// to the owner over the transport. Three things decide where a call belongs:
//
//	a workflow ID   its shard is hash(workflowID) mod NumShards (SPEC §9)
//	a task queue    queue q lives on the owner of shard hash(q), because matching is per
//	                node and a queue has to live on exactly one of them (SPEC §9, D10)
//	a task token    the token carries the shard of the run it belongs to
//
// Ownership moves, and a cached owner is only ever a guess. So a forwarded call that comes
// back not_owner costs a cache entry and a retry, and a call that cannot be delivered is
// retried too: the transport reports a lost reply as unreachable, which is ambiguous, and
// every request routed here is safe to repeat — starts and signals carry request IDs, and task
// completions are conditional on state the store checks.
//
// What the router does not do is decide ownership. It asks the store, through the shard
// manager, and the fence inside each write is what makes a wrong answer harmless.

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/niqbal/workflow-engine/internal/clock"
	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/server/matching"
	"github.com/niqbal/workflow-engine/internal/shard"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/transport"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

// Peer method names. A node registers these to serve its peers; the router calls them.
const (
	MethodStartWorkflow        = "StartWorkflow"
	MethodSignalWorkflow       = "SignalWorkflow"
	MethodTerminateWorkflow    = "TerminateWorkflow"
	MethodDescribeWorkflow     = "DescribeWorkflow"
	MethodGetHistory           = "GetHistory"
	MethodPollWorkflowTask     = "PollWorkflowTask"
	MethodCompleteWorkflowTask = "CompleteWorkflowTask"
	MethodFailWorkflowTask     = "FailWorkflowTask"
	MethodPollActivityTask     = "PollActivityTask"
	MethodCompleteActivityTask = "CompleteActivityTask"
	MethodFailActivityTask     = "FailActivityTask"
)

// RouterOptions configures a router.
type RouterOptions struct {
	// Attempts is how many times a call is tried before giving up. Zero means 5.
	Attempts int
	// RetryDelay is how long to wait between attempts, on the node's clock. Zero means
	// 100ms. Ownership changes take a lease, so a caller that cannot wait that long gets an
	// error and decides for itself.
	RetryDelay time.Duration
}

func (o RouterOptions) withDefaults() RouterOptions {
	if o.Attempts <= 0 {
		o.Attempts = 5
	}
	if o.RetryDelay <= 0 {
		o.RetryDelay = 100 * time.Millisecond
	}
	return o
}

// Router is an api.Service for a whole cluster: local when this node owns the shard,
// forwarded to the owner when it does not.
type Router struct {
	node      string
	rt        xsync.Runtime
	local     api.Service
	shards    *shard.Manager
	tr        transport.Transport
	clk       clock.Clock
	numShards int
	tokenKey  []byte
	opts      RouterOptions
}

var _ api.Service = (*Router)(nil)

// NewRouter returns a router in front of a node's own service. A nil transport means a single
// node: every shard it does not own is an error rather than someone else's work.
func NewRouter(s *Server, tr transport.Transport, opts RouterOptions) *Router {
	return &Router{
		node: s.cfg.Node, rt: s.rt, local: s.Service(), shards: s.shards, tr: tr, clk: s.clk,
		numShards: s.cfg.NumShards, tokenKey: s.cfg.TokenKey, opts: opts.withDefaults(),
	}
}

// Register makes this node serve its peers. register is the transport handler's Register.
func (s *Server) Register(register func(method string, fn func(ctx context.Context, decode func(any) error) (any, error))) {
	svc := s.Service()
	bind(register, MethodStartWorkflow, func(ctx context.Context, r api.StartRequest) (api.StartResponse, error) {
		return svc.StartWorkflow(ctx, r)
	})
	bind(register, MethodSignalWorkflow, func(ctx context.Context, r api.SignalRequest) (empty, error) {
		return empty{}, svc.SignalWorkflow(ctx, r)
	})
	bind(register, MethodTerminateWorkflow, func(ctx context.Context, r api.TerminateRequest) (empty, error) {
		return empty{}, svc.TerminateWorkflow(ctx, r)
	})
	bind(register, MethodDescribeWorkflow, func(ctx context.Context, r idRequest) (api.Description, error) {
		return svc.DescribeWorkflow(ctx, r.WorkflowID)
	})
	bind(register, MethodGetHistory, func(ctx context.Context, r idRequest) (historyReply, error) {
		evs, err := svc.GetHistory(ctx, r.WorkflowID)
		return historyReply{Events: evs}, err
	})
	bind(register, MethodPollWorkflowTask, func(ctx context.Context, r api.PollRequest) (workflowTaskReply, error) {
		t, err := svc.PollWorkflowTask(ctx, r)
		return workflowTaskReply{Task: t}, err
	})
	bind(register, MethodCompleteWorkflowTask, func(ctx context.Context, r completeWorkflowWire) (empty, error) {
		req, err := r.request()
		if err != nil {
			return empty{}, err
		}
		return empty{}, svc.CompleteWorkflowTask(ctx, req)
	})
	bind(register, MethodFailWorkflowTask, func(ctx context.Context, r api.FailWorkflowTaskRequest) (empty, error) {
		return empty{}, svc.FailWorkflowTask(ctx, r)
	})
	bind(register, MethodPollActivityTask, func(ctx context.Context, r api.PollRequest) (activityTaskReply, error) {
		t, err := svc.PollActivityTask(ctx, r)
		return activityTaskReply{Task: t}, err
	})
	bind(register, MethodCompleteActivityTask, func(ctx context.Context, r api.CompleteActivityTaskRequest) (empty, error) {
		return empty{}, svc.CompleteActivityTask(ctx, r)
	})
	bind(register, MethodFailActivityTask, func(ctx context.Context, r api.FailActivityTaskRequest) (empty, error) {
		return empty{}, svc.FailActivityTask(ctx, r)
	})
	s.registerAddTask(register)
	s.registerStartTask(register)
}

// bind adapts a typed handler to the transport's decode-into-any shape.
func bind[Req, Resp any](register func(string, func(context.Context, func(any) error) (any, error)),
	method string, fn func(context.Context, Req) (Resp, error)) {
	register(method, func(ctx context.Context, decode func(any) error) (any, error) {
		var req Req
		if err := decode(&req); err != nil {
			return nil, invalid("%s: %v", method, err)
		}
		return fn(ctx, req)
	})
}

// Wire types for the methods whose signatures are not already a struct.
type (
	empty     struct{}
	idRequest struct {
		WorkflowID string `json:"workflowID"`
	}
	historyReply struct {
		Events []history.Event `json:"events"`
	}
	workflowTaskReply struct {
		Task *api.WorkflowTask `json:"task,omitempty"`
	}
	activityTaskReply struct {
		Task *api.ActivityTask `json:"task,omitempty"`
	}
)

// completeWorkflowWire carries commands over the wire, which api.CompleteWorkflowTaskRequest
// deliberately does not: its Commands field is an interface slice that only
// history.EncodeCommands can serialize.
type completeWorkflowWire struct {
	TaskToken string `json:"taskToken"`
	Commands  []byte `json:"commands,omitempty"`
	Identity  string `json:"identity"`
}

func wireForComplete(r api.CompleteWorkflowTaskRequest) (completeWorkflowWire, error) {
	encoded, err := history.EncodeCommands(r.Commands)
	if err != nil {
		return completeWorkflowWire{}, err
	}
	return completeWorkflowWire{TaskToken: r.TaskToken, Commands: encoded, Identity: r.Identity}, nil
}

func (w completeWorkflowWire) request() (api.CompleteWorkflowTaskRequest, error) {
	req := api.CompleteWorkflowTaskRequest{TaskToken: w.TaskToken, Identity: w.Identity}
	if len(w.Commands) == 0 {
		return req, nil
	}
	cmds, err := history.DecodeCommands(w.Commands)
	if err != nil {
		return req, invalid("malformed commands")
	}
	req.Commands = cmds
	return req, nil
}

// ---- the api.Service implementation --------------------------------------------------------

func (r *Router) StartWorkflow(ctx context.Context, req api.StartRequest) (api.StartResponse, error) {
	return call(ctx, r, r.ShardForWorkflow(req.WorkflowID), MethodStartWorkflow, req,
		func(ctx context.Context) (api.StartResponse, error) { return r.local.StartWorkflow(ctx, req) })
}

func (r *Router) SignalWorkflow(ctx context.Context, req api.SignalRequest) error {
	_, err := call(ctx, r, r.ShardForWorkflow(req.WorkflowID), MethodSignalWorkflow, req,
		func(ctx context.Context) (empty, error) { return empty{}, r.local.SignalWorkflow(ctx, req) })
	return err
}

func (r *Router) TerminateWorkflow(ctx context.Context, req api.TerminateRequest) error {
	_, err := call(ctx, r, r.ShardForWorkflow(req.WorkflowID), MethodTerminateWorkflow, req,
		func(ctx context.Context) (empty, error) { return empty{}, r.local.TerminateWorkflow(ctx, req) })
	return err
}

func (r *Router) DescribeWorkflow(ctx context.Context, workflowID string) (api.Description, error) {
	return call(ctx, r, r.ShardForWorkflow(workflowID), MethodDescribeWorkflow,
		idRequest{WorkflowID: workflowID},
		func(ctx context.Context) (api.Description, error) { return r.local.DescribeWorkflow(ctx, workflowID) })
}

func (r *Router) GetHistory(ctx context.Context, workflowID string) ([]history.Event, error) {
	reply, err := call(ctx, r, r.ShardForWorkflow(workflowID), MethodGetHistory,
		idRequest{WorkflowID: workflowID},
		func(ctx context.Context) (historyReply, error) {
			evs, err := r.local.GetHistory(ctx, workflowID)
			return historyReply{Events: evs}, err
		})
	return reply.Events, err
}

// PollWorkflowTask forwards to the node that owns the task queue, so a worker may poll any
// node and still reach the one matching holds the queue's tasks (SPEC §9, WI-052).
func (r *Router) PollWorkflowTask(ctx context.Context, req api.PollRequest) (*api.WorkflowTask, error) {
	reply, err := call(ctx, r, r.ShardForQueue(req.TaskQueue), MethodPollWorkflowTask, req,
		func(ctx context.Context) (workflowTaskReply, error) {
			t, err := r.local.PollWorkflowTask(ctx, req)
			return workflowTaskReply{Task: t}, err
		})
	return reply.Task, err
}

func (r *Router) PollActivityTask(ctx context.Context, req api.PollRequest) (*api.ActivityTask, error) {
	reply, err := call(ctx, r, r.ShardForQueue(req.TaskQueue), MethodPollActivityTask, req,
		func(ctx context.Context) (activityTaskReply, error) {
			t, err := r.local.PollActivityTask(ctx, req)
			return activityTaskReply{Task: t}, err
		})
	return reply.Task, err
}

func (r *Router) CompleteWorkflowTask(ctx context.Context, req api.CompleteWorkflowTaskRequest) error {
	shardID, err := r.shardForWorkflowToken(req.TaskToken)
	if err != nil {
		return err
	}
	wire, err := wireForComplete(req)
	if err != nil {
		return err
	}
	_, err = call(ctx, r, shardID, MethodCompleteWorkflowTask, wire,
		func(ctx context.Context) (empty, error) { return empty{}, r.local.CompleteWorkflowTask(ctx, req) })
	return err
}

func (r *Router) FailWorkflowTask(ctx context.Context, req api.FailWorkflowTaskRequest) error {
	shardID, err := r.shardForWorkflowToken(req.TaskToken)
	if err != nil {
		return err
	}
	_, err = call(ctx, r, shardID, MethodFailWorkflowTask, req,
		func(ctx context.Context) (empty, error) { return empty{}, r.local.FailWorkflowTask(ctx, req) })
	return err
}

func (r *Router) CompleteActivityTask(ctx context.Context, req api.CompleteActivityTaskRequest) error {
	shardID, err := r.shardForActivityToken(req.TaskToken)
	if err != nil {
		return err
	}
	_, err = call(ctx, r, shardID, MethodCompleteActivityTask, req,
		func(ctx context.Context) (empty, error) { return empty{}, r.local.CompleteActivityTask(ctx, req) })
	return err
}

func (r *Router) FailActivityTask(ctx context.Context, req api.FailActivityTaskRequest) error {
	shardID, err := r.shardForActivityToken(req.TaskToken)
	if err != nil {
		return err
	}
	_, err = call(ctx, r, shardID, MethodFailActivityTask, req,
		func(ctx context.Context) (empty, error) { return empty{}, r.local.FailActivityTask(ctx, req) })
	return err
}

// ---- routing ------------------------------------------------------------------------------

// ShardForWorkflow maps a workflow ID to its shard, as the server does.
func (r *Router) ShardForWorkflow(workflowID string) int { return shardOf(workflowID, r.numShards) }

// ShardForQueue maps a task queue to the shard whose owner holds it.
func (r *Router) ShardForQueue(queue string) int { return shardOf(queue, r.numShards) }

func shardOf(key string, numShards int) int {
	if numShards <= 0 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(numShards))
}

func (r *Router) shardForWorkflowToken(tok string) (int, error) {
	t, err := decodeTaskToken[workflowTaskToken](r.tokenKey, tok)
	if err != nil {
		return 0, err
	}
	return t.Shard, nil
}

func (r *Router) shardForActivityToken(tok string) (int, error) {
	t, err := decodeTaskToken[activityTaskToken](r.tokenKey, tok)
	if err != nil {
		return 0, err
	}
	return t.Shard, nil
}

// call runs one request against the shard's owner: locally when this node owns it, over the
// transport when another node does, retrying while the answer is "not me" or "could not
// reach you".
func call[Req, Resp any](ctx context.Context, r *Router, shardID int, method string, req Req,
	localFn func(context.Context) (Resp, error)) (Resp, error) {
	var zero Resp
	var last error
	for attempt := 1; attempt <= r.opts.Attempts; attempt++ {
		if attempt > 1 {
			if err := r.pause(ctx); err != nil {
				return zero, err
			}
		}
		if _, owned := r.shards.Fence(shardID); owned {
			resp, err := localFn(ctx)
			if !retryable(err) {
				return resp, err
			}
			last = err
			r.shards.Forget(shardID)
			continue
		}
		owner, err := r.ownerOf(ctx, shardID)
		if err != nil {
			last = err
			continue
		}
		if owner == r.node {
			// The store says this node owns the shard but it is not serving it: the lease
			// is inside its stop-serving margin, or placement has not caught up. Waiting is
			// the only correct answer; the caller's attempt budget bounds it.
			last = notOwner(shardID)
			continue
		}
		if r.tr == nil {
			return zero, fmt.Errorf("%w: shard %d belongs to %s and this node has no transport",
				api.ErrNotOwner, shardID, owner)
		}
		var resp Resp
		err = r.tr.Call(ctx, transport.NodeID(owner), method, req, &resp)
		if err == nil {
			return resp, nil
		}
		if !retryable(err) {
			return zero, err
		}
		last = err
		r.shards.Forget(shardID)
	}
	if last == nil {
		last = notOwner(shardID)
	}
	return zero, fmt.Errorf("routing %s for shard %d gave up after %d attempts: %w",
		method, shardID, r.opts.Attempts, last)
}

// retryable reports whether an answer is worth asking again somewhere else. not_owner means
// the ownership cache was stale; unreachable means the call may not have arrived, and every
// request the router carries is safe to repeat.
func retryable(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, api.ErrNotOwner) || errors.Is(err, transport.ErrUnreachable)
}

// ownerOf returns the node that owns a shard, from the cache when it has one.
func (r *Router) ownerOf(ctx context.Context, shardID int) (string, error) {
	if owner, ok := r.shards.CachedOwner(shardID); ok {
		return owner, nil
	}
	owner, err := r.shards.Owner(ctx, shardID)
	if err != nil {
		return "", err
	}
	if owner == "" {
		return "", fmt.Errorf("%w: shard %d has no owner yet", api.ErrNotOwner, shardID)
	}
	return owner, nil
}

// pause waits between attempts on the node's clock: a condition nothing ever signals, with a
// deadline, which is how server code waits without touching the wall clock or a channel
// (SPEC §6.1). The simulator therefore sees a routing retry as time passing, not as a spin.
func (r *Router) pause(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline := r.clk.Now().Add(r.opts.RetryDelay)
	l := r.rt.NewLocker()
	c := r.rt.NewCond(l)
	l.Lock()
	for r.clk.Now().Before(deadline) && ctx.Err() == nil {
		if c.WaitUntil(deadline) {
			break // signalled, which nothing does; a spurious wake is harmless
		}
	}
	l.Unlock()
	return ctx.Err()
}

// ---- task queues (WI-052) ------------------------------------------------------------------

// MethodAddTask is how a shard owner hands a task to the node that holds its queue.
const MethodAddTask = "AddTask"

// addTaskRequest is one task pushed to the queue's owner.
type addTaskRequest struct {
	Kind  string     `json:"kind"` // "workflow" or "activity"
	Queue string     `json:"queue"`
	Task  store.Task `json:"task"`
}

func kindName(k matching.Kind) string {
	if k == matching.ActivityTasks {
		return "activity"
	}
	return "workflow"
}

func kindFromName(s string) (matching.Kind, error) {
	switch s {
	case "workflow":
		return matching.WorkflowTasks, nil
	case "activity":
		return matching.ActivityTasks, nil
	}
	return 0, invalid("unknown task kind %q", s)
}

// UseTransport gives the node a way to reach its peers, which it needs to put a task on a
// queue another node holds. A node without one serves only the queues it owns.
func (s *Server) UseTransport(tr transport.Transport) {
	s.mu.Lock()
	s.peerTransport = tr
	s.mu.Unlock()
}

func (s *Server) transportOrNil() transport.Transport {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peerTransport
}

// QueueShard is the shard whose owner holds a task queue.
func (s *Server) QueueShard(queue string) int { return shardOf(queue, s.cfg.NumShards) }

// addTask puts a task on its queue, wherever that queue lives.
func (s *Server) addTask(ctx context.Context, kind matching.Kind, queue string, t store.Task) error {
	shardID := s.QueueShard(queue)
	if _, mine := s.shards.Fence(shardID); mine {
		s.match.Add(kind, queue, t)
		return nil
	}
	tr := s.transportOrNil()
	owner, err := s.shards.Owner(ctx, shardID)
	if err != nil || owner == "" || owner == s.cfg.Node || tr == nil {
		// Nobody else can be handed this task: keep it here, where a worker polling the
		// queue through any frontend will still be forwarded to us if we own the queue by
		// the time it asks, and redispatch will try again if not.
		s.match.Add(kind, queue, t)
		return nil
	}
	var reply empty
	err = tr.Call(ctx, transport.NodeID(owner), MethodAddTask, addTaskRequest{
		Kind: kindName(kind), Queue: queue, Task: t,
	}, &reply)
	if err != nil {
		s.metrics().IncQueuePushFailure()
		return nil // redispatch is the retry (SPEC §8.2-8.3)
	}
	return nil
}

// registerAddTask serves peers pushing tasks onto a queue this node holds.
func (s *Server) registerAddTask(register func(method string, fn func(ctx context.Context, decode func(any) error) (any, error))) {
	bind(register, MethodAddTask, func(ctx context.Context, r addTaskRequest) (empty, error) {
		kind, err := kindFromName(r.Kind)
		if err != nil {
			return empty{}, err
		}
		if _, mine := s.shards.Fence(s.QueueShard(r.Queue)); !mine {
			// The queue moved. Saying so lets the sender look the owner up again, and the
			// task is still in its outbox either way.
			return empty{}, notOwner(s.QueueShard(r.Queue))
		}
		s.match.Add(kind, r.Queue, r.Task)
		return empty{}, nil
	})
}

// ---- starting a task whose run lives elsewhere (WI-052) ------------------------------------

// A task queue and the runs whose tasks pass through it are two different things, owned by
// two different shards and so possibly by two different nodes. The queue's owner holds the
// task in matching and hands it to a worker; starting it is a write to the run's history, so
// only the run's shard owner can do it. These two methods are that hand-off.
const (
	MethodStartWorkflowTask = "StartWorkflowTask"
	MethodStartActivityTask = "StartActivityTask"
)

type startTaskRequest struct {
	Task     store.Task `json:"task"`
	Identity string     `json:"identity"`
}

// startWorkflowTaskAnywhere starts a task taken from this node's matching, wherever its run
// lives: here when this node owns the run's shard, on the owner otherwise.
func (s *Server) startWorkflowTaskAnywhere(ctx context.Context, t store.Task, identity string) (*api.WorkflowTask, error) {
	if _, mine := s.shards.Fence(t.Run.Shard); mine {
		return s.startWorkflowTask(ctx, t, identity)
	}
	tr := s.transportOrNil()
	if tr == nil {
		return nil, notOwner(t.Run.Shard)
	}
	owner, err := s.shards.Owner(ctx, t.Run.Shard)
	if err != nil {
		return nil, err
	}
	if owner == "" || owner == s.cfg.Node {
		return nil, notOwner(t.Run.Shard)
	}
	var reply workflowTaskReply
	if err := tr.Call(ctx, transport.NodeID(owner), MethodStartWorkflowTask,
		startTaskRequest{Task: t, Identity: identity}, &reply); err != nil {
		return nil, err
	}
	return reply.Task, nil
}

func (s *Server) startActivityTaskAnywhere(ctx context.Context, t store.Task, identity string) (*api.ActivityTask, error) {
	if _, mine := s.shards.Fence(t.Run.Shard); mine {
		return s.startActivityTask(ctx, t, identity)
	}
	tr := s.transportOrNil()
	if tr == nil {
		return nil, notOwner(t.Run.Shard)
	}
	owner, err := s.shards.Owner(ctx, t.Run.Shard)
	if err != nil {
		return nil, err
	}
	if owner == "" || owner == s.cfg.Node {
		return nil, notOwner(t.Run.Shard)
	}
	var reply activityTaskReply
	if err := tr.Call(ctx, transport.NodeID(owner), MethodStartActivityTask,
		startTaskRequest{Task: t, Identity: identity}, &reply); err != nil {
		return nil, err
	}
	return reply.Task, nil
}

// registerStartTask serves peers starting a task on a run this node owns.
func (s *Server) registerStartTask(register func(method string, fn func(ctx context.Context, decode func(any) error) (any, error))) {
	bind(register, MethodStartWorkflowTask, func(ctx context.Context, r startTaskRequest) (workflowTaskReply, error) {
		if _, mine := s.shards.Fence(r.Task.Run.Shard); !mine {
			return workflowTaskReply{}, notOwner(r.Task.Run.Shard)
		}
		t, err := s.startWorkflowTask(ctx, r.Task, r.Identity)
		return workflowTaskReply{Task: t}, err
	})
	bind(register, MethodStartActivityTask, func(ctx context.Context, r startTaskRequest) (activityTaskReply, error) {
		if _, mine := s.shards.Fence(r.Task.Run.Shard); !mine {
			return activityTaskReply{}, notOwner(r.Task.Run.Shard)
		}
		t, err := s.startActivityTask(ctx, r.Task, r.Identity)
		return activityTaskReply{Task: t}, err
	})
}

// droppable reports whether a task taken from matching should be abandoned rather than
// reported as a failure: the run's shard has moved, or its owner could not be reached. The
// task is still in the shard's outbox, so its owner's redispatch brings it back (SPEC
// §8.2-8.3). Dropping it here keeps one unlucky task from failing a poll that could still
// serve the next one.
func droppable(err error) bool {
	return errors.Is(err, api.ErrNotOwner) || errors.Is(err, transport.ErrUnreachable) ||
		errors.Is(err, api.ErrStaleTask) || errors.Is(err, api.ErrNotFound)
}
