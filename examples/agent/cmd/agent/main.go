// Command agent runs the showcase's worker, and reads its sinks.
//
//	agent worker --addr http://127.0.0.1:7233 --ledger /tmp/ledger.json
//	agent ledger --ledger /tmp/ledger.json
//
// The worker is an ordinary process: kill it whenever you like. The engine will hand whatever
// it was doing to the next one, and the idempotency key on each attempt is what keeps the
// refund at exactly one. --hang-after-refund is there to make that easy to do by hand: the
// first refund attempt applies the money and then stops, so you can kill the worker in the
// one window where a double charge would be possible.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/niqbal/workflow-engine/client"
	"github.com/niqbal/workflow-engine/examples/agent"
	"github.com/niqbal/workflow-engine/worker"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "agent:", err)
		os.Exit(2)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		usage(stderr)
		return errors.New("missing command")
	}
	switch args[0] {
	case "-h", "--help", "help":
		usage(stdout)
		return nil
	case "worker":
		return runWorker(args[1:], stdout, stderr)
	case "ledger":
		return runLedger(args[1:], stdout, stderr)
	default:
		usage(stderr)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  agent worker [flags]   Poll a task queue and run the agent's activities")
	fmt.Fprintln(w, "  agent ledger [flags]   Print what a sink holds")
}

func runWorker(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", "http://127.0.0.1:7233", "server base URL")
	queue := fs.String("task-queue", "q", "task queue to poll")
	identity := fs.String("identity", hostIdentity(), "worker identity, which appears in history")
	ledger := fs.String("ledger", "ledger.json", "payment sink file")
	mail := fs.String("mail", "mail.json", "reply sink file")
	hang := fs.Bool("hang-after-refund", false,
		"apply the first refund and then stop reporting, so the worker can be killed in that window")
	if err := fs.Parse(args); err != nil {
		return err
	}

	acts := agent.Activities{
		Model:    agent.Scripted{},
		Orders:   agent.DemoOrders(),
		Payments: agent.NewFileSink(*ledger),
		Mail:     agent.NewFileSink(*mail),
	}
	if *hang {
		first := true
		acts.AfterRefund = func(ctx context.Context, key string) error {
			if !first {
				return nil
			}
			first = false
			fmt.Fprintf(stdout, "refund %s is in the ledger and this attempt will not report it.\n", key)
			fmt.Fprintln(stdout, "kill this worker now: the next one has to finish the job without paying twice.")
			<-ctx.Done()
			return ctx.Err()
		}
	}

	w := worker.New(client.New(*addr, client.Options{}), worker.Options{
		TaskQueue: *queue, Identity: *identity,
		// A worker keeps working through a cluster's hiccups, and says what it saw.
		OnError: func(err error) { fmt.Fprintf(stderr, "retrying: %v\n", err) },
	})
	if err := acts.Register(w); err != nil {
		return err
	}
	// The smoke workflow needs no human, which is what an unattended script needs
	// (scripts/cluster-smoke.sh).
	if err := agent.RegisterSmoke(w); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Fprintf(stdout, "agent worker %s polling %s on %s\n", *identity, *queue, *addr)
	err := w.Run(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func runLedger(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("ledger", flag.ContinueOnError)
	fs.SetOutput(stderr)
	path := fs.String("ledger", "ledger.json", "sink file to read")
	if err := fs.Parse(args); err != nil {
		return err
	}
	sink := agent.NewFileSink(*path)
	entries, err := sink.Entries()
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		fmt.Fprintf(stdout, "%s is empty\n", *path)
		return nil
	}
	var total int64
	for _, e := range entries {
		var p agent.Payment
		if err := json.Unmarshal(e.Value, &p); err == nil && p.Cents > 0 {
			total += p.Cents
			fmt.Fprintf(stdout, "%s  %-28s  %8s  %s\n", e.At.Format(time.RFC3339), p.OrderID,
				fmt.Sprintf("$%d.%02d", p.Cents/100, p.Cents%100), e.Key)
			continue
		}
		fmt.Fprintf(stdout, "%s  %-28s  %s\n", e.At.Format(time.RFC3339), "(reply)", e.Key)
	}
	fmt.Fprintf(stdout, "\n%d entr%s, $%d.%02d in total\n", len(entries),
		plural(len(entries)), total/100, total%100)
	return nil
}

func plural(n int) string {
	if n == 1 {
		return "y"
	}
	return "ies"
}

func hostIdentity() string {
	host, err := os.Hostname()
	if err != nil {
		host = "worker"
	}
	return fmt.Sprintf("%s/%d", host, os.Getpid())
}
