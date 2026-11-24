package sim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/niqbal/workflow-engine/internal/engine"

	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/rnd"
	"github.com/niqbal/workflow-engine/internal/server"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
)

// Faults is the fault-injection hook (WI-042). It has no methods yet: Run accepts any
// value, including nil, and does not call it. Faults stop at the quiet phase once the
// hook exists.
type Faults interface{}

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
	rt     *simrt.Runtime
	st     *memstore.Store
	nodes  []*server.Server
	faults Faults
	oracle Oracle
	rec    *Record

	ids     []string
	done    bool
	halt    bool
	failErr error
	quiet   bool
}

// Run executes workload under seed. faults is ignored until WI-042; pass nil.
func Run(seed uint64, wl Workload, faults Faults) (res Result) {
	wl = wl.normalized()
	rt := simrt.New(seed, simrt.Start)
	defer func() {
		if p := recover(); p != nil && res.Err == nil {
			res.Err = fmt.Errorf("sim: %v", p)
		}
	}()
	defer rt.Close()

	oracle := wl.Oracle
	if oracle == nil {
		oracle = Invariants{}
	}
	rec := &Record{Seed: seed, Finals: map[string]Final{}, Ledger: wl.Ledger,
		Workflows: map[string]engine.Workflow{}, Reference: wl.Reference}
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
	c.st = memstore.New(memstore.WithRuntime(c.rt),
		memstore.WithCommitLog(func(e memstore.CommitEntry) {
			c.rec.Commits = append(c.rec.Commits, e)
		}))
	key := tokenKey(seed)
	ctx := context.Background()
	c.nodes = make([]*server.Server, wl.Nodes)
	for i := 0; i < wl.Nodes; i++ {
		cfg := server.Config{
			Node:      fmt.Sprintf("node-%d", i),
			NumShards: wl.Shards,
			LongPoll:  wl.LongPoll,
			TokenKey:  key,
		}
		n := server.New(c.rt, c.st, rnd.New(split(seed, i)), cfg)
		if err := n.StartOwned(ctx, ownedShards(i, wl.Nodes, wl.Shards)); err != nil {
			return err
		}
		if err := n.Run(ctx); err != nil {
			return err
		}
		c.nodes[i] = n
	}
	return nil
}

func (c *Cluster) startWorkers(reg *registry, wl Workload) {
	for ni := 0; ni < len(c.nodes); ni++ {
		node := c.nodes[ni]
		for w := 0; w < wl.Workers; w++ {
			for _, queue := range wl.TaskQueues {
				id := fmt.Sprintf("n%d-w%d", ni, w)
				q := queue
				svc := node.Service()
				c.rt.Go(id+"-"+q+"-workflow", func(ctx context.Context) {
					c.workflowLoop(ctx, svc, reg, q, id)
				})
				c.rt.Go(id+"-"+q+"-activity", func(ctx context.Context) {
					c.activityLoop(ctx, svc, reg, q, id)
				})
			}
		}
	}
}

// NumNodes is how many servers share the store.
func (c *Cluster) NumNodes() int { return len(c.nodes) }

// Node returns one server. Reads may go to any node; writes for a workflow must go to
// the node that owns its shard, which is what StartWorkflow does.
func (c *Cluster) Node(i int) *server.Server { return c.nodes[i] }

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
	n, err := c.owner(r.WorkflowID)
	if err != nil {
		return api.StartResponse{}, err
	}
	resp, err := n.StartWorkflow(ctx, r)
	if err != nil {
		return resp, err
	}
	c.ids = append(c.ids, r.WorkflowID)
	c.rec.Starts = append(c.rec.Starts, StartRecord{WorkflowID: r.WorkflowID, RunID: resp.RunID,
		Type: r.WorkflowType, TaskQueue: r.TaskQueue, RequestID: r.RequestID})
	return resp, nil
}

// SignalWorkflow delivers a signal on the owning node. Every outcome is recorded for I7:
// an acknowledged signal must appear exactly once in history, a rejected one not at all.
func (c *Cluster) SignalWorkflow(ctx context.Context, r api.SignalRequest) error {
	n, err := c.owner(r.WorkflowID)
	if err != nil {
		return err
	}
	err = n.SignalWorkflow(ctx, r)
	c.rec.Signals = append(c.rec.Signals, SignalRecord{WorkflowID: r.WorkflowID,
		Name: r.SignalName, RequestID: r.RequestID, Err: err})
	return err
}

// TerminateWorkflow terminates the latest run on the owning node.
func (c *Cluster) TerminateWorkflow(ctx context.Context, r api.TerminateRequest) error {
	n, err := c.owner(r.WorkflowID)
	if err != nil {
		return err
	}
	return n.TerminateWorkflow(ctx, r)
}

// DescribeWorkflow reads the latest run from the owning node.
func (c *Cluster) DescribeWorkflow(ctx context.Context, workflowID string) (api.Description, error) {
	n, err := c.owner(workflowID)
	if err != nil {
		return api.Description{}, err
	}
	return n.DescribeWorkflow(ctx, workflowID)
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
	n, err := c.owner(workflowID)
	if err != nil {
		return nil, err
	}
	return n.GetHistory(ctx, workflowID)
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

func (c *Cluster) owner(workflowID string) (*server.Server, error) {
	if len(c.nodes) == 0 {
		return nil, fmt.Errorf("sim: cluster has no nodes")
	}
	shard := c.nodes[0].ShardFor(workflowID)
	return c.nodes[shard%len(c.nodes)], nil
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
