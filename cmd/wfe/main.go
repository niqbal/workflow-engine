package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/niqbal/workflow-engine/client"
	"github.com/niqbal/workflow-engine/internal/httpapi"
	"github.com/niqbal/workflow-engine/internal/rnd"
	"github.com/niqbal/workflow-engine/internal/server"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
	"github.com/niqbal/workflow-engine/internal/store/pgstore"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

const defaultAPIAddr = "http://127.0.0.1:7233"

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "wfe:", err)
		os.Exit(2)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		printRootHelp(stderr)
		return fmt.Errorf("missing command")
	}
	switch args[0] {
	case "-h", "--help", "help":
		printRootHelp(stdout)
		return nil
	case "server":
		return runServer(args[1:], stdout, stderr)
	case "workflow":
		return runWorkflow(args[1:], stdout, stderr)
	case "sim":
		return runSim(args[1:], stdout, stderr)
	default:
		printRootHelp(stderr)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func printRootHelp(w io.Writer) {
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  wfe server [flags]")
	fmt.Fprintln(w, "  wfe workflow <start|signal|history> ...")
	fmt.Fprintln(w, "  wfe sim [flags]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Commands:")
	fmt.Fprintln(w, "  server                 Run a single-node server with in-memory store")
	fmt.Fprintln(w, "  workflow start         Start a workflow")
	fmt.Fprintln(w, "  workflow signal        Send a signal to a workflow")
	fmt.Fprintln(w, "  workflow history       Print workflow history events")
	fmt.Fprintln(w, "  sim                    Run the deterministic simulator over a range of seeds")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Use 'wfe <command> --help' for command-specific flags.")
}

func runServer(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	fs.SetOutput(stderr)
	listen := fs.String("listen", ":7233", "HTTP listen address")
	node := fs.String("node", "node-1", "node ID")
	numShards := fs.Int("num-shards", 4, "number of shards")
	longPoll := fs.Duration("long-poll", 30*time.Second, "workflow/activity poll timeout")
	redispatch := fs.Duration("redispatch-interval", 30*time.Second, "task redispatch interval")
	lease := fs.Duration("lease-duration", 30*time.Second, "shard lease duration")
	seed := fs.Uint64("seed", 1, "deterministic run ID seed")
	storeURL := fs.String("store", "", "Postgres URL; empty uses the in-memory store, which a restart loses")
	tokenKey := fs.String("token-key", "", "HMAC key for task tokens; must match across restarts and nodes")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "Usage:")
		fmt.Fprintln(stderr, "  wfe server [flags]")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Run one server node. Without --store it keeps everything in memory,")
		fmt.Fprintln(stderr, "so a restart starts empty; with --store it survives one.")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Flags:")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("server takes no positional arguments")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rt := xsync.NewReal(ctx)
	var st store.Store
	if *storeURL == "" {
		st = memstore.New(memstore.WithRuntime(rt))
	} else {
		pg, err := pgstore.Open(ctx, *storeURL)
		if err != nil {
			return fmt.Errorf("open store: %w", err)
		}
		defer pg.Close()
		if err := pg.Migrate(ctx); err != nil {
			return fmt.Errorf("migrate store: %w", err)
		}
		st = pg
	}
	s := server.New(rt, st, rnd.New(*seed), server.Config{
		Node:               *node,
		NumShards:          *numShards,
		LongPoll:           *longPoll,
		RedispatchInterval: *redispatch,
		LeaseDuration:      *lease,
		TokenKey:           []byte(*tokenKey),
	})
	if err := s.Start(ctx); err != nil {
		return fmt.Errorf("start server: %w", err)
	}
	if err := s.Run(ctx); err != nil {
		return fmt.Errorf("run loops: %w", err)
	}

	httpSrv := &http.Server{
		Addr: *listen,
		Handler: httpapi.New(s.Service(), httpapi.Options{
			Metrics: s.Metrics().Handler(),
		}),
	}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutCtx)
	}()

	fmt.Fprintf(stdout, "wfe server listening on %s\n", *listen)
	err := httpSrv.ListenAndServe()
	stop()

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.Stop(stopCtx)
	rt.Wait()

	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

func runWorkflow(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		printWorkflowHelp(stderr)
		return fmt.Errorf("missing workflow subcommand")
	}
	switch args[0] {
	case "-h", "--help", "help":
		printWorkflowHelp(stdout)
		return nil
	case "start":
		return runWorkflowStart(args[1:], stdout, stderr)
	case "signal":
		return runWorkflowSignal(args[1:], stderr)
	case "history":
		return runWorkflowHistory(args[1:], stdout, stderr)
	default:
		printWorkflowHelp(stderr)
		return fmt.Errorf("unknown workflow subcommand %q", args[0])
	}
}

func printWorkflowHelp(w io.Writer) {
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  wfe workflow start <id> [flags]")
	fmt.Fprintln(w, "  wfe workflow signal <id> <name> [flags]")
	fmt.Fprintln(w, "  wfe workflow history <id> [flags]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Use 'wfe workflow <subcommand> --help' for flags.")
}

func parseSubcommandFlags(fs *flag.FlagSet, args []string, leadingPositional int) error {
	parsedArgs := args
	if leadingPositional > 0 && len(args) >= leadingPositional {
		leadingArePositionals := true
		for _, arg := range args[:leadingPositional] {
			if strings.HasPrefix(arg, "-") {
				leadingArePositionals = false
				break
			}
		}
		if leadingArePositionals {
			parsedArgs = append([]string{}, args[leadingPositional:]...)
			parsedArgs = append(parsedArgs, args[:leadingPositional]...)
		}
	}
	return fs.Parse(parsedArgs)
}

func runWorkflowStart(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("workflow start", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", defaultAPIAddr, "server base URL")
	workflowType := fs.String("workflow-type", "", "workflow type (required)")
	taskQueue := fs.String("task-queue", "", "task queue (required)")
	inputText := fs.String("input", "", "workflow input as JSON")
	requestID := fs.String("request-id", "", "dedup request ID")
	executionTimeout := fs.Duration("execution-timeout", 0, "execution timeout")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "Usage:")
		fmt.Fprintln(stderr, "  wfe workflow start <id> [flags]")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Start a workflow execution.")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Flags:")
		fs.PrintDefaults()
	}
	if err := parseSubcommandFlags(fs, args, 1); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("workflow start requires exactly 1 argument: <id>")
	}
	if *workflowType == "" {
		return fmt.Errorf("--workflow-type is required")
	}
	if *taskQueue == "" {
		return fmt.Errorf("--task-queue is required")
	}

	input, err := jsonFlag("input", *inputText)
	if err != nil {
		return err
	}
	c := client.New(*addr, client.Options{})
	resp, err := c.StartWorkflow(context.Background(), api.StartRequest{
		WorkflowID:       fs.Arg(0),
		WorkflowType:     *workflowType,
		TaskQueue:        *taskQueue,
		Input:            input,
		RequestID:        *requestID,
		ExecutionTimeout: *executionTimeout,
	})
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, resp.RunID)
	return nil
}

func runWorkflowSignal(args []string, stderr io.Writer) error {
	fs := flag.NewFlagSet("workflow signal", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", defaultAPIAddr, "server base URL")
	payloadText := fs.String("payload", "", "signal payload as JSON")
	requestID := fs.String("request-id", "", "dedup request ID")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "Usage:")
		fmt.Fprintln(stderr, "  wfe workflow signal <id> <name> [flags]")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Send a signal to the latest run of a workflow.")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Flags:")
		fs.PrintDefaults()
	}
	if err := parseSubcommandFlags(fs, args, 2); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() != 2 {
		return fmt.Errorf("workflow signal requires 2 arguments: <id> <name>")
	}

	payload, err := jsonFlag("payload", *payloadText)
	if err != nil {
		return err
	}
	c := client.New(*addr, client.Options{})
	return c.SignalWorkflow(context.Background(), api.SignalRequest{
		WorkflowID: fs.Arg(0),
		SignalName: fs.Arg(1),
		Payload:    payload,
		RequestID:  *requestID,
	})
}

func runWorkflowHistory(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("workflow history", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", defaultAPIAddr, "server base URL")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "Usage:")
		fmt.Fprintln(stderr, "  wfe workflow history <id> [flags]")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Print the latest run's workflow history.")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Flags:")
		fs.PrintDefaults()
	}
	if err := parseSubcommandFlags(fs, args, 1); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("workflow history requires exactly 1 argument: <id>")
	}

	c := client.New(*addr, client.Options{})
	evs, err := c.GetHistory(context.Background(), fs.Arg(0))
	if err != nil {
		return err
	}

	tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tTime\tType\tAttrs")
	for _, e := range evs {
		attrs, aerr := json.Marshal(e.Attrs)
		if aerr != nil {
			attrs = []byte(fmt.Sprintf(`{"error":%q}`, aerr.Error()))
		}
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\n", e.ID, e.Time.UTC().Format(time.RFC3339Nano), e.Type(), attrs)
	}
	return tw.Flush()
}

func jsonFlag(name, val string) (json.RawMessage, error) {
	if strings.TrimSpace(val) == "" {
		return nil, nil
	}
	b := []byte(val)
	if !json.Valid(b) {
		return nil, fmt.Errorf("--%s must be valid JSON", name)
	}
	return json.RawMessage(b), nil
}
