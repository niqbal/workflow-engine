package main

// wfe server --config: one node of a cluster (WI-054).
//
// Without --config, wfe server is what it has always been: one node, an in-memory store, and
// nothing to agree with. With one, the node takes its name, its peers, its shard count and
// its shared key from the file, checks every one of them before it serves anything, and then
// does what any other node does — takes its share of the shards by lease, renews them, routes
// what it cannot serve to the node that can.
//
// The checks are the point. docs/MULTI-MACHINE.md lists the mistakes that would otherwise
// look like data loss rather than a bad config, and internal/cluster refuses each of them by
// name. The one it cannot check from the file is the shard count the cluster was created
// with: that lives in the store, and EnsureCluster is what compares them.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/niqbal/workflow-engine/internal/clock"
	"github.com/niqbal/workflow-engine/internal/cluster"
	"github.com/niqbal/workflow-engine/internal/httpapi"
	"github.com/niqbal/workflow-engine/internal/rnd"
	"github.com/niqbal/workflow-engine/internal/server"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
	"github.com/niqbal/workflow-engine/internal/store/pgstore"
	"github.com/niqbal/workflow-engine/internal/transport/httptransport"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

// Node is a running cluster node: its server, the frontend that answers for the cluster, and
// the listeners.
type Node struct {
	Config   cluster.Config
	Server   *server.Server
	Frontend *server.Router
	API      http.Handler
	Internal http.Handler

	closeStore func()
}

// StartNode brings a node up: store, cluster record, server, leases, transport and router.
// It does not listen on anything — the caller serves Node.API and Node.Internal, which is
// what lets a test run two nodes in one process.
func StartNode(ctx context.Context, cfg cluster.Config) (*Node, error) {
	rt := &xsync.Real{Ctx: ctx, C: clock.Real{}}

	st, closeStore, err := openStore(ctx, cfg)
	if err != nil {
		return nil, err
	}
	// The shard count is the cluster's, not this node's. A disagreement here is the one
	// misconfiguration that would route writes by a mapping nobody else uses.
	if _, err := st.EnsureCluster(ctx, cfg.NumShards, time.Now()); err != nil {
		closeStore()
		return nil, fmt.Errorf("cluster: %w", err)
	}

	srv := server.New(rt, st, rnd.New(uint64(time.Now().UnixNano())), server.Config{
		Node:          cfg.Node,
		NumShards:     cfg.NumShards,
		Nodes:         len(cfg.Peers),
		LeaseDuration: cfg.LeaseDuration,
		TokenKey:      cfg.ClusterKey,
	})

	n := &Node{Config: cfg, Server: srv, closeStore: closeStore}

	var peerHandler *httptransport.Handler
	if cfg.Clustered() {
		client, err := httptransport.New(cfg.ClusterKey, cfg.TransportPeers())
		if err != nil {
			closeStore()
			return nil, err
		}
		peerHandler = httptransport.NewHandler(cfg.ClusterKey)
		srv.Register(peerRegistrar(peerHandler))
		srv.UseTransport(client)
		// A forwarded call has to ride out an ownership change rather than hand the client
		// a not_owner it cannot act on: a shard moves only after a lease lapses, so the
		// budget is measured against the lease. A client with its own deadline still gets
		// an answer; it is just not asked to understand shard placement.
		n.Frontend = server.NewRouter(srv, client, server.RouterOptions{
			RetryDelay: cfg.LeaseDuration / 10,
			Attempts:   25,
		})
		n.Internal = peerHandler
	} else {
		n.Frontend = server.NewRouter(srv, nil, server.RouterOptions{})
	}

	// Ownership: a cluster places shards by lease; a single node simply takes them all.
	if cfg.Clustered() {
		if err := srv.StartCluster(ctx); err != nil {
			closeStore()
			return nil, err
		}
	} else if err := srv.Start(ctx); err != nil {
		closeStore()
		return nil, err
	}
	if err := srv.Run(ctx); err != nil {
		closeStore()
		return nil, err
	}
	if cfg.Clustered() {
		if err := srv.RunShards(ctx); err != nil {
			closeStore()
			return nil, err
		}
	}

	// The server keeps its own collector, so /metrics has to serve that one rather than the
	// process default: the smoke test reads wfe_shards_owned from here to see a failover.
	n.API = httpapi.New(n.Frontend, httpapi.Options{Metrics: srv.Metrics().Handler()})
	return n, nil
}

// Stop ends the node's loops and closes its store.
func (n *Node) Stop(ctx context.Context) {
	n.Server.StopShards()
	_ = n.Server.Stop(ctx)
	if n.closeStore != nil {
		n.closeStore()
	}
}

// openStore opens the configured store and returns a function that closes it.
func openStore(ctx context.Context, cfg cluster.Config) (store.Store, func(), error) {
	switch cfg.Store.Kind {
	case cluster.StoreMemory:
		rt := &xsync.Real{Ctx: ctx, C: clock.Real{}}
		return memstore.New(memstore.WithRuntime(rt)), func() {}, nil
	case cluster.StorePostgres:
		pg, err := pgstore.Open(ctx, string(cfg.StoreURL))
		if err != nil {
			return nil, nil, fmt.Errorf("open store: %w", err)
		}
		if err := pg.Migrate(ctx); err != nil {
			pg.Close()
			return nil, nil, fmt.Errorf("migrate store: %w", err)
		}
		return pg, pg.Close, nil
	default:
		return nil, nil, fmt.Errorf("cluster: store kind %q", cfg.Store.Kind)
	}
}

// peerRegistrar adapts the server's registration shape to the transport handler's: the
// handler hands over a raw JSON body, the server wants to decode into its own type.
func peerRegistrar(h *httptransport.Handler) func(string, func(context.Context, func(any) error) (any, error)) {
	return func(method string, fn func(context.Context, func(any) error) (any, error)) {
		h.Register(method, func(ctx context.Context, req json.RawMessage) (any, error) {
			return fn(ctx, func(into any) error {
				if len(req) == 0 {
					return nil
				}
				return json.Unmarshal(req, into)
			})
		})
	}
}

// runClusterServer is `wfe server --config wfe.yaml`.
func runClusterServer(ctx context.Context, path string, stdout, stderr io.Writer) error {
	cfg, err := cluster.Load(path, os.Getenv)
	if err != nil {
		return err
	}
	node, err := StartNode(ctx, cfg)
	if err != nil {
		return err
	}
	defer node.Stop(context.Background())

	fmt.Fprintf(stdout, "wfe %s\n", cfg)

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	errs := make(chan error, 2)
	api := &http.Server{Addr: cfg.Listen.API, Handler: node.API,
		ReadHeaderTimeout: 10 * time.Second}
	go func() { errs <- serve(api, "api", cfg.Listen.API, stdout) }()

	var internal *http.Server
	if node.Internal != nil {
		internal = &http.Server{Addr: cfg.Listen.Internal, Handler: node.Internal,
			ReadHeaderTimeout: 10 * time.Second}
		go func() { errs <- serve(internal, "internal", cfg.Listen.Internal, stdout) }()
	}

	select {
	case <-ctx.Done():
		fmt.Fprintln(stdout, "shutting down")
	case err = <-errs:
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = api.Shutdown(shutdownCtx)
	if internal != nil {
		_ = internal.Shutdown(shutdownCtx)
	}
	return err
}

func serve(s *http.Server, name, addr string, stdout io.Writer) error {
	fmt.Fprintf(stdout, "%s listening on %s\n", name, addr)
	if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("%s listener: %w", name, err)
	}
	return nil
}

// printConfigHelp documents wfe.yaml under `wfe server --help`, because a config file nobody
// can find the fields of is a config file nobody can write.
func printConfigHelp(w io.Writer) {
	fmt.Fprintln(w, "Config file (--config wfe.yaml):")
	fmt.Fprintln(w, "  node             this node's name; must appear in peers")
	fmt.Fprintln(w, "  listen.api       public API address (default :7233)")
	fmt.Fprintln(w, "  listen.internal  node-to-node address, on its own port (default :7234)")
	fmt.Fprintln(w, "  advertise        what peers dial for this node; defaults to its peers entry")
	fmt.Fprintln(w, "  peers            every node's name to host:port, including this one")
	fmt.Fprintln(w, "  store.kind       postgres, or memory for a single node")
	fmt.Fprintln(w, "  store.url_env    environment variable holding the Postgres URL")
	fmt.Fprintln(w, "  cluster_key_env  environment variable holding the base64 cluster key,")
	fmt.Fprintln(w, "                   at least 32 bytes; it signs task tokens and peer calls,")
	fmt.Fprintln(w, "                   and every node must have the same one")
	fmt.Fprintln(w, "  num_shards       fixed when the cluster is created (default 64); a node")
	fmt.Fprintln(w, "                   configured for another count is refused by the store")
	fmt.Fprintln(w, "  lease_duration   how long a shard lease lasts (default 30s)")
	fmt.Fprintln(w, "  max_clock_skew   the clock disagreement timers allow for (default 1s)")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Secrets are never in the file: it names the environment variables that hold them.")
	fmt.Fprintln(w, "docs/RUNNING-A-CLUSTER.md has a worked example.")
}
