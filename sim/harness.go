package sim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/niqbal/workflow-engine/internal/engine"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/rnd"
	"github.com/niqbal/workflow-engine/internal/server"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
	"github.com/niqbal/workflow-engine/internal/transport"
)

// Registry binds workflow and activity code for the simulated workers.
type Registry interface {
	RegisterWorkflow(workflowType string, fn any) error
	RegisterActivity(activityType string, fn any) error
}

// Workload is one simulated scenario. Register runs before any node starts. Run starts
// workflows through Cluster and then returns; the quiet phase finishes them.
type Workload struct {
	Nodes      int           // servers sharing the store; < 1 means 1
	Shards     int           // cluster shard count; < 1 means 4
	Workers    int           // simulated workers per node; < 1 means 1
	TaskQueues []string      // queues workers poll; empty means "q"
	QuietLimit time.Duration // virtual deadline of the quiet phase; <= 0 means 366 days
	LongPoll   time.Duration // server long-poll; <= 0 means QuietLimit
	Register   func(Registry) error
	Run        func(ctx context.Context, c *Cluster) error

	// Leases runs the nodes as a real cluster: each takes its share of the shards by lease
	// and renews them, a node that cannot renew gives its shards up, and a dead node's
	// shards are taken over (WI-050). Without it each node is pinned to a fixed set, which
	// is what the earlier milestones' tests want.
	Leases bool
	// LeaseDuration is the shard lease when Leases is set. Zero means the server default.
	LeaseDuration time.Duration
	// Routing puts the nodes on a simulated network and sends every client and worker call
	// through a node's router instead of straight to the shard's owner (WI-051, WI-052).
	// Calls are then forwarded, retried and sometimes lost, like a real cluster's.
	Routing bool

	// Oracle checks the invariants after the quiet phase. Nil means Invariants{}: every
	// check of SPEC §11.3. Use NoOracle for a workload that is deliberately not correct,
	// such as one that leaves a run open on purpose.
	Oracle Oracle
	// Ledger is the side-effect ledger activities apply through, for I5. Nil means the
	// workload has no external effects to check.
	Ledger *Ledger
	// Reference is the I8 model: the result each workflow must produce, derived from its
	// own final history.
	Reference func(id string, evs []history.Event) (json.RawMessage, error)
}

// Result is one execution of Run.
type Result struct {
	Seed   uint64
	Trace  []string // schedule plus the final history of each started workflow
	Err    error
	Closed bool      // every workflow started through Cluster is closed
	Now    time.Time // virtual time when the run stopped
	Oracle error     // the invariant violations found, also joined into Err
	Record *Record   // what the run observed, for the oracle and for failure reports
}

// Cluster is the N nodes, the shared store and the virtual clock of one run.
// Start workflows through StartWorkflow so the quiet phase knows what to wait for.
type Cluster struct {
	rt      *simrt.Runtime
	st      *memstore.Store
	slots   []*node
	shards  int
	leases  bool
	routing bool
	net     *network
	faults  *Faults
	oracle  Oracle
	rec     *Record

	ids     []string
	done    bool
	halt    bool
	failErr error
	quiet   bool
}

// Run executes workload under seed. A nil faults (or one with nothing set) injects nothing
// and leaves the run identical to one with no injector.
func Run(seed uint64, wl Workload, faults *Faults) (res Result) {
	wl = wl.normalized()
	rt := simrt.New(seed, simrt.Start)
	defer func() {
		if p := recover(); p != nil && res.Err == nil {
			res.Err = fmt.Errorf("sim: %v", p)
			// A panic (a livelock above all) is the case where the trace matters most, so
			// keep what the run produced before it died.
			if res.Trace == nil {
				res.Trace = rt.Trace()
				res.Now = rt.Now()
				res.Seed = seed
			}
		}
	}()
	defer rt.Close()

	oracle := wl.Oracle
	if oracle == nil {
		oracle = Invariants{}
	}
	rec := &Record{Seed: seed, Finals: map[string]Final{}, Ledger: wl.Ledger,
		Workflows: map[string]engine.Workflow{}, Reference: wl.Reference, Epsilon: faults.epsilon()}
	c := &Cluster{rt: rt, faults: faults, oracle: oracle, rec: rec}
	reg := newRegistry()
	if wl.Register != nil {
		if err := wl.Register(reg); err != nil {
			res.Err = err
			return
		}
	}
	rec.Workflows = reg.workflows
	if err := c.boot(seed, wl); err != nil {
		res.Err = err
		return
	}
	c.startWorkers(reg, wl)
	if faults.any() {
		rt.Go("faults", func(ctx context.Context) { faults.run(ctx, c) })
	}
	rt.Go("workload", func(ctx context.Context) {
		var err error
		if wl.Run != nil {
			err = wl.Run(ctx, c)
		}
		// Quiet phase: faults would stop here. Workers and loops keep running.
		c.quiet = true
		if err != nil {
			c.fail(err)
			rt.Tracef("workload error: %v", err)
		} else {
			rt.Tracef("quiet")
		}
		c.done = true
	})

	until := simrt.Start.Add(wl.QuietLimit)
	reason := rt.Run(until, func() bool {
		if !c.done {
			return false
		}
		if c.halt {
			return true
		}
		return c.allClosed()
	})

	res.Seed = seed
	res.Now = rt.Now()
	res.Trace = append(rt.Trace(), c.outcome()...)
	res.Closed = c.failErr == nil && c.allClosed()
	switch {
	case c.failErr != nil:
		res.Err = c.failErr
	case !res.Closed:
		res.Err = fmt.Errorf("sim: quiet phase ended (%s) at %s with workflows still open; tasks: %s",
			runResult(reason), rt.Now().UTC().Format(time.RFC3339), rt.Tasks())
	}

	// The oracle runs on the record, after the cluster has stopped. Reads from here on are
	// outside any simulator task, so they add nothing to the trace: turning the oracle on
	// must not change the simulation it is judging.
	rec.QuietDeadline = reason == simrt.Deadline
	c.collectFinals()
	res.Record = rec
	res.Oracle = oracle.Check(rec)
	res.Err = errors.Join(res.Err, res.Oracle)
	return res
}

// collectFinals reads each started run's end state into the record.
func (c *Cluster) collectFinals() {
	ctx := context.Background()
	c.rec.Incarnations = make([]int, len(c.slots))
	for i, n := range c.slots {
		c.rec.Incarnations[i] = n.starts
	}
	for _, id := range c.ids {
		if _, done := c.rec.Finals[id]; done {
			continue
		}
		d, err := c.DescribeWorkflow(ctx, id)
		if err != nil {
			continue
		}
		evs, err := c.readHistory(ctx, id)
		if err != nil {
			continue
		}
		c.rec.Finals[id] = Final{RunID: d.RunID, Status: d.Status, Events: evs}
	}
}

func (w Workload) normalized() Workload {
	if w.Nodes < 1 {
		w.Nodes = 1
	}
	if w.Shards < 1 {
		w.Shards = 4
	}
	if w.Workers < 1 {
		w.Workers = 1
	}
	if len(w.TaskQueues) == 0 {
		w.TaskQueues = []string{"q"}
	}
	if w.QuietLimit <= 0 {
		w.QuietLimit = 366 * 24 * time.Hour
	}
	if w.LongPoll <= 0 {
		w.LongPoll = w.QuietLimit
	}
	return w
}

func (c *Cluster) boot(seed uint64, wl Workload) error {
	// The ghost log (I6): every committed write, in commit order, recorded outside the
	// store so the fence check does not read back the state it is judging.
	opts := []memstore.Option{
		memstore.WithRuntime(c.rt),
		memstore.WithCommitLog(func(e memstore.CommitEntry) {
			c.rec.Commits = append(c.rec.Commits, e)
		}),
	}
	if c.faults.storeFaults() {
		opts = append(opts, memstore.WithFaults(c.faults.hooks(c.rt.Rand(), c.rt.Tracef, c.rt.InTask, c.Quiet)))
	}
	c.st = memstore.New(opts...)
	c.shards = wl.Shards
	key := tokenKey(seed)
	ctx := context.Background()
	cfg := server.Config{NumShards: wl.Shards, LongPoll: wl.LongPoll, TokenKey: key,
		LeaseDuration: wl.LeaseDuration}
	if wl.Leases {
		cfg.Nodes = wl.Nodes
		cfg.FailoverAllowance = wl.Shards // a survivor may take everything
	}
	c.leases = wl.Leases
	c.routing = wl.Routing
	if wl.Routing {
		c.net = newNetwork(c)
	}
	c.slots = make([]*node, wl.Nodes)
	for i := 0; i < wl.Nodes; i++ {
		c.slots[i] = &node{c: c, index: i, seed: seed, cfgBase: cfg,
			shards: ownedShards(i, wl.Nodes, wl.Shards), skew: c.skewFor(i)}
	}
	// Defaults are needed before the first incarnation starts: a pause is measured against
	// the lease, and server.Config fills that in itself.
	for _, n := range c.slots {
		if err := n.start(ctx); err != nil {
			return err
		}
		n.cfgBase.LeaseDuration = n.srv.Config().LeaseDuration
	}
	return nil
}

// skewFor draws one node's clock offset, within plus or minus ClockSkewMax.
func (c *Cluster) skewFor(i int) time.Duration {
	max := c.faults.epsilon()
	if max <= 0 {
		return 0
	}
	r := c.rt.Rand()
	off := time.Duration(uint64(r.Int63()) % uint64(2*max+1))
	return off - max
}

func (c *Cluster) startWorkers(reg *registry, wl Workload) {
	for ni := 0; ni < len(c.slots); ni++ {
		index := ni
		for w := 0; w < wl.Workers; w++ {
			for _, queue := range wl.TaskQueues {
				id := fmt.Sprintf("n%d-w%d", ni, w)
				q := queue
				c.rt.Go(id+"-"+q+"-workflow", func(ctx context.Context) {
					c.workflowLoop(ctx, index, reg, q, id)
				})
				c.rt.Go(id+"-"+q+"-activity", func(ctx context.Context) {
					c.activityLoop(ctx, index, reg, q, id)
				})
			}
		}
	}
}

// NumNodes is how many node slots share the store.
func (c *Cluster) NumNodes() int { return len(c.slots) }

// Node returns a node's current incarnation, or nil while it is down. Reads may go to any
// node; writes for a workflow must go to the node that owns its shard, which is what
// StartWorkflow does.
func (c *Cluster) Node(i int) *server.Server { return c.slots[i].server() }

// Incarnations is how many times a node has been started, which a crash increments.
func (c *Cluster) Incarnations(i int) int { return c.slots[i].starts }

// Rand is the deterministic RNG for the code under test.
func (c *Cluster) Rand() rnd.Rand { return c.rt.Rand() }

// Now is the virtual clock.
func (c *Cluster) Now() time.Time { return c.rt.Now() }

// Sleep blocks the calling workload task for d of virtual time.
func (c *Cluster) Sleep(d time.Duration) { c.rt.Sleep(d) }

// Quiet reports whether the workload has returned and faults must stop.
func (c *Cluster) Quiet() bool { return c.quiet }

// StartWorkflow routes the start to the node that owns the workflow's shard and records
// the id for the quiet phase.
func (c *Cluster) StartWorkflow(ctx context.Context, r api.StartRequest) (api.StartResponse, error) {
	svc, err := c.clientService(r.WorkflowID)
	if err != nil {
		return api.StartResponse{}, err
	}
	resp, err := svc.StartWorkflow(ctx, r)
	if err != nil {
		return resp, err
	}
	c.ids = append(c.ids, r.WorkflowID)
	c.rec.Starts = append(c.rec.Starts, StartRecord{WorkflowID: r.WorkflowID, RunID: resp.RunID,
		Type: r.WorkflowType, TaskQueue: r.TaskQueue, RequestID: r.RequestID})
	return resp, nil
}

// StartRetrying starts a workflow, retrying a transient failure with the same request ID.
// That is what a client must do when a call's outcome is unknown: the start either committed
// or it did not, and the request ID makes the retry safe either way (G1, SPEC §8.4).
func (c *Cluster) StartRetrying(ctx context.Context, r api.StartRequest) (api.StartResponse, error) {
	var last error
	deadline := c.rt.Now().Add(c.clientRetryWindow())
	for attempt := 1; ; attempt++ {
		resp, err := c.StartWorkflow(ctx, r)
		if err == nil || !transientForClient(err) {
			return resp, err
		}
		last = err
		if c.giveUp(attempt, deadline) {
			return api.StartResponse{}, fmt.Errorf("sim: start %s gave up after %d attempts in %s: %w",
				r.WorkflowID, attempt, c.clientRetryWindow(), last)
		}
		c.rt.Sleep(clientRetryDelay)
	}
}

// SignalRetrying signals a workflow, retrying a transient failure with the same request ID.
// G7 makes the retry exactly-once in history however many times it is sent.
func (c *Cluster) SignalRetrying(ctx context.Context, r api.SignalRequest) error {
	var last error
	deadline := c.rt.Now().Add(c.clientRetryWindow())
	for attempt := 1; ; attempt++ {
		err := c.SignalWorkflow(ctx, r)
		if err == nil || !transientForClient(err) {
			return err
		}
		last = err
		if c.giveUp(attempt, deadline) {
			return fmt.Errorf("sim: signal %s gave up after %d attempts in %s: %w",
				r.WorkflowID, attempt, c.clientRetryWindow(), last)
		}
		c.rt.Sleep(clientRetryDelay)
	}
}

// HistoryRetrying reads a run's history, retrying an unknown outcome the way a client must.
// A read needs an owner too: the node that holds the shard is the one that can serve it.
func (c *Cluster) HistoryRetrying(ctx context.Context, workflowID string) ([]history.Event, error) {
	var last error
	deadline := c.rt.Now().Add(c.clientRetryWindow())
	for attempt := 1; ; attempt++ {
		evs, err := c.GetHistory(ctx, workflowID)
		if err == nil || !transientForClient(err) {
			return evs, err
		}
		last = err
		if c.giveUp(attempt, deadline) {
			return nil, fmt.Errorf("sim: history of %s gave up after %d attempts in %s: %w",
				workflowID, attempt, c.clientRetryWindow(), last)
		}
		c.rt.Sleep(clientRetryDelay)
	}
}

const (
	clientRetryDelay = 100 * time.Millisecond
	// clientMinAttempts is how many times a client tries whatever the clock says. A single
	// attempt can itself eat the whole window — a forwarded call that waits out an
	// ownership change, a reply held up by the network — and giving up after one try
	// because time passed inside it is not a retry policy.
	clientMinAttempts = 10
)

// giveUp reports whether a client has run out of both attempts and time.
func (c *Cluster) giveUp(attempt int, deadline time.Time) bool {
	return attempt >= clientMinAttempts && !c.rt.Now().Before(deadline)
}

// clientRetryWindow is how long a client keeps retrying an unknown outcome. It has to
// outlast an ownership change, or a client gives up while the cluster is still deciding who
// owns a shard: several lease durations, which is what a real client's deadline must allow
// for too.
func (c *Cluster) clientRetryWindow() time.Duration {
	lease := 30 * time.Second
	if len(c.slots) > 0 && c.slots[0].cfgBase.LeaseDuration > 0 {
		lease = c.slots[0].cfgBase.LeaseDuration
	}
	window := 4 * lease
	if window < 10*time.Second {
		window = 10 * time.Second
	}
	return window
}

// transientForClient reports whether a client should retry. An injected store fault, an
// ambiguous commit, a node that is down or a lost shard are all "unknown outcome, try
// again"; anything else is a real answer the workload must honour.
func transientForClient(err error) bool {
	switch {
	case errors.Is(err, errInjected), errors.Is(err, errAmbiguous), errors.Is(err, errDown):
		return true
	case errors.Is(err, store.ErrFenced), errors.Is(err, store.ErrConflict):
		return true
	case errors.Is(err, api.ErrNotOwner):
		return true
	case errors.Is(err, transport.ErrUnreachable):
		// The node could not be reached, or its reply was lost: unknown outcome, so retry
		// with the same request ID.
		return true
	}
	return false
}

// SignalWorkflow delivers a signal on the owning node. Every outcome is recorded for I7:
// an acknowledged signal must appear exactly once in history, a rejected one not at all.
func (c *Cluster) SignalWorkflow(ctx context.Context, r api.SignalRequest) error {
	svc, err := c.clientService(r.WorkflowID)
	if err != nil {
		return err
	}
	err = svc.SignalWorkflow(ctx, r)
	c.rec.Signals = append(c.rec.Signals, SignalRecord{WorkflowID: r.WorkflowID,
		Name: r.SignalName, RequestID: r.RequestID, Err: err})
	return err
}

// TerminateWorkflow terminates the latest run.
func (c *Cluster) TerminateWorkflow(ctx context.Context, r api.TerminateRequest) error {
	svc, err := c.clientService(r.WorkflowID)
	if err != nil {
		return err
	}
	return svc.TerminateWorkflow(ctx, r)
}

// DescribeWorkflow reads the latest run.
func (c *Cluster) DescribeWorkflow(ctx context.Context, workflowID string) (api.Description, error) {
	svc, err := c.clientService(workflowID)
	if err != nil {
		return api.Description{}, err
	}
	return svc.DescribeWorkflow(ctx, workflowID)
}

// GetHistory reads the latest run's history from the owning node, and records what it saw
// for I2: a read is an observation, and every observation must be a prefix of the end state.
func (c *Cluster) GetHistory(ctx context.Context, workflowID string) ([]history.Event, error) {
	evs, err := c.readHistory(ctx, workflowID)
	if err == nil {
		c.observe(workflowID, "", "read", evs)
	}
	return evs, err
}

func (c *Cluster) readHistory(ctx context.Context, workflowID string) ([]history.Event, error) {
	svc, err := c.clientService(workflowID)
	if err != nil {
		return nil, err
	}
	return svc.GetHistory(ctx, workflowID)
}

// clientService is the frontend a client call goes to. With routing it is any live node's
// router, picked from the run's own RNG, which is what makes "a client may talk to any node"
// a claim the simulator tests rather than one the harness arranges around. Without routing it
// is the owner's own service, which is what the earlier milestones' tests expect.
//
// Outside a simulator task — the harness's own bookkeeping between runs — it is always the
// owner, because a router may wait between attempts and only a task may wait.
func (c *Cluster) clientService(workflowID string) (api.Service, error) {
	if c.routing && c.rt.InTask() {
		if svc := c.anyFrontend(); svc != nil {
			return svc, nil
		}
	}
	n, err := c.owner(workflowID)
	if err != nil {
		return nil, err
	}
	return n.Service(), nil
}

// anyFrontend returns the router of a live node, chosen deterministically from the run's RNG.
func (c *Cluster) anyFrontend() api.Service {
	live := make([]int, 0, len(c.slots))
	for i, n := range c.slots {
		if n.server() != nil {
			live = append(live, i)
		}
	}
	if len(live) == 0 {
		return nil
	}
	pick := live[int(uint64(c.rt.Rand().Int63())%uint64(len(live)))]
	return c.slots[pick].frontend()
}

// observe records one history snapshot. It copies nothing: the events are immutable once
// written, which is the property I2 exists to check.
func (c *Cluster) observe(workflowID, runID, source string, evs []history.Event) {
	if c.rec == nil || len(evs) == 0 {
		return
	}
	c.rec.Snapshots = append(c.rec.Snapshots, Snapshot{WorkflowID: workflowID, RunID: runID,
		Source: source, At: c.rt.Now(), Events: evs})
}

// owner returns the node that owns the workflow's shard. While that node is down, a caller
// inside a simulator task waits for its next incarnation, which is what a client retrying
// through a crash does; the harness's own reads, outside any task, get errDown instead.
func (c *Cluster) owner(workflowID string) (*server.Server, error) {
	if len(c.slots) == 0 {
		return nil, fmt.Errorf("sim: cluster has no nodes")
	}
	if c.leases {
		return c.leaseOwner(c.shardFor(workflowID))
	}
	n := c.slots[c.shardFor(workflowID)%len(c.slots)]
	for {
		if srv := n.server(); srv != nil {
			return srv, nil
		}
		if !c.rt.InTask() {
			return nil, fmt.Errorf("%w: node-%d", errDown, n.index)
		}
		c.rt.Sleep(10 * time.Millisecond)
	}
}

// ShardFor maps a workflow ID to its shard, as the owning node would.
func (c *Cluster) ShardFor(workflowID string) int { return c.shardFor(workflowID) }

// routerRetryDelay is how long a node's router waits between attempts. It is short relative
// to a lease so a forwarded call rides out an ownership change rather than failing.
func (c *Cluster) routerRetryDelay() time.Duration {
	lease := 30 * time.Second
	if len(c.slots) > 0 && c.slots[0].cfgBase.LeaseDuration > 0 {
		lease = c.slots[0].cfgBase.LeaseDuration
	}
	return lease / 10
}

// leaseOwner finds the node that holds a shard by lease, waiting while ownership moves. It
// stands in for the frontend's router (WI-051): in one process the owner can be asked
// directly, where a real cluster forwards over the transport.
func (c *Cluster) leaseOwner(shard int) (*server.Server, error) {
	for attempt := 0; ; attempt++ {
		for _, n := range c.slots {
			srv := n.server()
			if srv == nil {
				continue
			}
			if _, ok := srv.ShardManager().Fence(shard); ok {
				return srv, nil
			}
		}
		if !c.rt.InTask() || attempt >= ownerAttempts {
			return nil, fmt.Errorf("%w: shard %d has no owner that can serve it", errDown, shard)
		}
		c.rt.Sleep(ownerRetryDelay)
	}
}

const (
	ownerAttempts   = 2000
	ownerRetryDelay = 50 * time.Millisecond
)

// shardFor maps a workflow ID to its shard exactly as server.ShardFor does. The cluster
// needs the mapping to pick a node before it has a live server to ask, so the hash is
// repeated here; TestWI042ShardForMatchesServer keeps the two in step.
func (c *Cluster) shardFor(workflowID string) int {
	h := fnv.New32a()
	h.Write([]byte(workflowID))
	return int(h.Sum32() % uint32(c.shards))
}

func (c *Cluster) fail(err error) {
	if err == nil || c.failErr != nil {
		return
	}
	c.failErr = err
	c.halt = true
}

func (c *Cluster) allClosed() bool {
	ctx := context.Background()
	seen := map[string]bool{}
	for _, id := range c.ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		d, err := c.DescribeWorkflow(ctx, id)
		if err != nil || d.Status.Open() {
			return false
		}
	}
	return true
}

func (c *Cluster) outcome() []string {
	ctx := context.Background()
	lines := []string{"virtual " + c.rt.Now().UTC().Format(time.RFC3339Nano)}
	for _, id := range c.ids {
		d, err := c.DescribeWorkflow(ctx, id)
		if err != nil {
			lines = append(lines, fmt.Sprintf("describe %s %v", id, err))
			continue
		}
		lines = append(lines, fmt.Sprintf("status %s %s", id, d.Status))
		evs, err := c.GetHistory(ctx, id)
		if err != nil {
			lines = append(lines, fmt.Sprintf("history %s %v", id, err))
			continue
		}
		for _, e := range evs {
			lines = append(lines, fmt.Sprintf("history %s %d %s", id, e.ID, e.Type()))
		}
	}
	return lines
}

func ownedShards(node, nodes, shards int) []int {
	out := make([]int, 0, shards/nodes+1)
	for s := 0; s < shards; s++ {
		if s%nodes == node {
			out = append(out, s)
		}
	}
	return out
}

func split(seed uint64, i int) uint64 {
	x := seed + uint64(i+1)*0x9e3779b97f4a7c15
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func tokenKey(seed uint64) []byte {
	r := rnd.New(split(seed, -1))
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(r.Int63())
	}
	return key
}

func runResult(r simrt.Result) string {
	switch r {
	case simrt.Idle:
		return "idle"
	case simrt.Deadline:
		return "deadline"
	case simrt.Stopped:
		return "stopped"
	default:
		return fmt.Sprintf("result %d", int(r))
	}
}
