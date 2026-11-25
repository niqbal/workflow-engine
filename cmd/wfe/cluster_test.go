package main

// WI-054's acceptance test: two nodes, started in one process from two config files, sharing
// a store and a cluster key. A task token one node issues has to be good at the other, which
// is the whole reason the key comes from the config rather than being generated per node.
//
// It needs a real Postgres, because two nodes that do not share a store are not a cluster.
// PG_URL is what the pgstore tests use, and CI sets it for the same job.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/niqbal/workflow-engine/internal/cluster"
	"github.com/niqbal/workflow-engine/internal/history"
	"github.com/niqbal/workflow-engine/internal/server/api"
)

func pgURLForCluster(t *testing.T) string {
	t.Helper()
	u := os.Getenv("PG_URL")
	if u == "" {
		t.Skip("PG_URL is not set")
	}
	return u
}

// freshSchema gives the test its own schema, so a cluster's tables are its own.
func freshSchema(t *testing.T, url string) string {
	t.Helper()
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	name := fmt.Sprintf("c_%016x", rand.Uint64())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+name); err != nil {
		admin.Close(ctx)
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+name+" CASCADE"); err != nil {
			t.Errorf("drop schema %s: %v", name, err)
		}
		admin.Close(ctx)
	})
	return name
}

// schemaURL points a connection at one schema.
func schemaURL(base, schema string) string {
	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	return base + sep + "search_path=" + schema
}

// freePort reserves a port and releases it, so a config can name it before anything listens.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// clusterOfTwo starts two nodes from two configs over one store, serving both listeners of
// each, and returns them.
func clusterOfTwo(t *testing.T, numShards int) (a, b *Node) {
	t.Helper()
	base := pgURLForCluster(t)
	url := schemaURL(base, freshSchema(t, base))
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	t.Setenv("WFE_CLUSTER_TEST_URL", url)
	t.Setenv("WFE_CLUSTER_TEST_KEY", key)

	internalA, internalB := freePort(t), freePort(t)
	peers := fmt.Sprintf("  node-a: %s\n  node-b: %s\n", internalA, internalB)

	configFor := func(node, internal string) cluster.Config {
		yaml := fmt.Sprintf(`
node: %s
listen: {api: "%s", internal: "%s"}
peers:
%sstore: {kind: postgres, url_env: WFE_CLUSTER_TEST_URL}
cluster_key_env: WFE_CLUSTER_TEST_KEY
num_shards: %d
lease_duration: 5s
`, node, freePort(t), internal, peers, numShards)
		cfg, err := cluster.Parse([]byte(yaml), os.Getenv)
		if err != nil {
			t.Fatalf("config for %s: %v", node, err)
		}
		return cfg
	}

	ctx := context.Background()
	start := func(cfg cluster.Config) *Node {
		n, err := StartNode(ctx, cfg)
		if err != nil {
			t.Fatalf("start %s: %v", cfg.Node, err)
		}
		t.Cleanup(func() { n.Stop(context.Background()) })
		// Both listeners, as the command would.
		for _, l := range []struct {
			addr    string
			handler http.Handler
		}{{cfg.Listen.API, n.API}, {cfg.Listen.Internal, n.Internal}} {
			if l.handler == nil {
				continue
			}
			ln, err := net.Listen("tcp", l.addr)
			if err != nil {
				t.Fatalf("listen %s: %v", l.addr, err)
			}
			srv := &http.Server{Handler: l.handler, ReadHeaderTimeout: 5 * time.Second}
			go func() { _ = srv.Serve(ln) }()
			t.Cleanup(func() {
				shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_ = srv.Shutdown(shutdown)
			})
		}
		return n
	}
	a = start(configFor("node-a", internalA))
	b = start(configFor("node-b", internalB))
	return a, b
}

// TestTwoNodesShareTaskTokens is the acceptance criterion. A workflow task is polled through
// one node and reported through the other: the second node has to decode a token the first
// one signed before it can even tell which shard the completion belongs to, so a key that
// differed between them would fail here.
func TestTwoNodesShareTaskTokens(t *testing.T) {
	a, b := clusterOfTwo(t, 8)
	ctx := context.Background()

	if string(a.Config.ClusterKey) != string(b.Config.ClusterKey) {
		t.Fatal("the two nodes read different cluster keys")
	}

	// Both nodes take shards, between them all of them.
	waitFor(t, "both nodes to hold shards", func() bool {
		return len(a.Server.Shards()) > 0 && len(b.Server.Shards()) > 0
	})
	owned := map[int]bool{}
	for _, s := range append(a.Server.Shards(), b.Server.Shards()...) {
		if owned[s] {
			t.Fatalf("shard %d is held by both nodes", s)
		}
		owned[s] = true
	}
	if len(owned) != 8 {
		t.Fatalf("%d of 8 shards are owned: a=%v b=%v", len(owned), a.Server.Shards(), b.Server.Shards())
	}

	// Start through node-a's frontend, wherever the run's shard lives.
	input, _ := json.Marshal(map[string]string{"ticket": "T-1"})
	if _, err := a.Frontend.StartWorkflow(ctx, api.StartRequest{
		WorkflowID: "wf-tokens", WorkflowType: "W", TaskQueue: "q",
		Input: input, RequestID: "req-1",
	}); err != nil {
		t.Fatalf("start through node-a: %v", err)
	}

	// Poll through node-a: the token is signed by whichever node owns the run's shard.
	var task *api.WorkflowTask
	waitFor(t, "a workflow task", func() bool {
		got, err := a.Frontend.PollWorkflowTask(ctx, api.PollRequest{TaskQueue: "q", Identity: "w1"})
		if err == nil && got != nil {
			task = got
		}
		return task != nil
	})
	if task.WorkflowID != "wf-tokens" {
		t.Fatalf("task for %s", task.WorkflowID)
	}

	// Report it through node-b. It decodes the token to find the shard, then forwards.
	if err := b.Frontend.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{
		TaskToken: task.TaskToken,
		Commands:  []history.Command{history.CompleteWorkflow{Result: json.RawMessage(`"done"`)}},
		Identity:  "w1",
	}); err != nil {
		t.Fatalf("complete through node-b: %v", err)
	}

	// And the run is closed, read through node-b as well.
	desc, err := b.Frontend.DescribeWorkflow(ctx, "wf-tokens")
	if err != nil {
		t.Fatalf("describe through node-b: %v", err)
	}
	if desc.Status != history.StatusCompleted {
		t.Fatalf("status %s, want completed", desc.Status)
	}
}

// TestASecondNodeWithADifferentShardCountIsRefused is the check the config file cannot make
// on its own: the shard count belongs to the cluster, and the store is what remembers it.
func TestASecondNodeWithADifferentShardCountIsRefused(t *testing.T) {
	base := pgURLForCluster(t)
	url := schemaURL(base, freshSchema(t, base))
	t.Setenv("WFE_CLUSTER_TEST_URL", url)
	t.Setenv("WFE_CLUSTER_TEST_KEY",
		base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef")))

	cfgFor := func(shards int) cluster.Config {
		yaml := fmt.Sprintf(`
node: only
listen: {api: "%s", internal: "%s"}
peers: {only: 127.0.0.1:1, other: 127.0.0.1:2}
store: {kind: postgres, url_env: WFE_CLUSTER_TEST_URL}
cluster_key_env: WFE_CLUSTER_TEST_KEY
num_shards: %d
`, freePort(t), freePort(t), shards)
		cfg, err := cluster.Parse([]byte(yaml), os.Getenv)
		if err != nil {
			t.Fatal(err)
		}
		return cfg
	}

	ctx := context.Background()
	first, err := StartNode(ctx, cfgFor(8))
	if err != nil {
		t.Fatalf("first node: %v", err)
	}
	first.Stop(ctx)

	_, err = StartNode(ctx, cfgFor(16))
	if err == nil {
		t.Fatal("a node configured for 16 shards joined a cluster of 8")
	}
	for _, want := range []string{"created with 8", "configured for 16"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("message %q does not contain %q", err, want)
		}
	}
}

// waitFor polls a condition for a few seconds. These tests talk to real listeners over
// loopback, so they wait on the wall clock rather than a manual one.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
