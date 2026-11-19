package server_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/niqbal/workflow-engine/internal/rnd"
	"github.com/niqbal/workflow-engine/internal/server"
	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/simrt"
	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
)

func TestWI040StartOwned(t *testing.T) {
	ctx := context.Background()
	rt := simrt.New(1, simrt.Start)
	t.Cleanup(rt.Close)
	st := memstore.New(memstore.WithRuntime(rt))
	key := bytes.Repeat([]byte{7}, 32)
	newSrv := func(name string, shards int) *server.Server {
		return server.New(rt, st, rnd.New(uint64(len(name)+3)), server.Config{
			Node: name, NumShards: shards, TokenKey: append([]byte(nil), key...),
		})
	}

	a := newSrv("a", 4)
	if err := a.StartOwned(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got := a.Shards(); len(got) != 0 {
		t.Fatalf("nil list owns %v", got)
	}
	if err := a.StartOwned(ctx, []int{}); err != nil {
		t.Fatal(err)
	}
	if got := a.Shards(); len(got) != 0 {
		t.Fatalf("empty list owns %v", got)
	}

	b := newSrv("b", 4)
	err := b.StartOwned(ctx, []int{4})
	if err == nil || !strings.Contains(err.Error(), "shard 4") || !strings.Contains(err.Error(), "0..3") {
		t.Fatalf("invalid shard: %v", err)
	}
	if got := b.Shards(); len(got) != 0 {
		t.Fatalf("rejected shard list owns %v", got)
	}
	err = b.StartOwned(ctx, []int{-1})
	if err == nil || !strings.Contains(err.Error(), "-1") {
		t.Fatalf("negative shard: %v", err)
	}
	if got := b.Shards(); len(got) != 0 {
		t.Fatalf("negative shard owns %v", got)
	}

	// The bad id is first, so a rejected call must not acquire the later valid shard.
	err = b.StartOwned(ctx, []int{9, 1})
	if err == nil || !strings.Contains(err.Error(), "shard 9") || !strings.Contains(err.Error(), "0..3") {
		t.Fatalf("leading invalid shard: %v", err)
	}
	if got := b.Shards(); len(got) != 0 {
		t.Fatalf("leading invalid shard acquired %v", got)
	}

	if err := b.StartOwned(ctx, []int{2, 0}); err != nil {
		t.Fatal(err)
	}
	if got := b.Shards(); !reflect.DeepEqual(got, []int{0, 2}) {
		t.Fatalf("shards %v", got)
	}
	if err := b.StartOwned(ctx, []int{0}); err != nil {
		t.Fatal(err)
	}
	if got := b.Shards(); !reflect.DeepEqual(got, []int{0, 2}) {
		t.Fatalf("re-acquire shards %v", got)
	}

	c := newSrv("c", 4)
	err = c.StartOwned(ctx, []int{0})
	if !errors.Is(err, store.ErrFenced) || !strings.Contains(err.Error(), "shard 0") {
		t.Fatalf("steal shard: %v", err)
	}
	if got := c.Shards(); len(got) != 0 {
		t.Fatalf("failed acquire owns %v", got)
	}
	if got := b.Shards(); !reflect.DeepEqual(got, []int{0, 2}) {
		t.Fatalf("owner after conflict %v", got)
	}

	// A later conflict must not drop the shard acquired earlier in the same call,
	// and must not take the shard the other node holds.
	err = c.StartOwned(ctx, []int{1, 0})
	if !errors.Is(err, store.ErrFenced) || !strings.Contains(err.Error(), "shard 0") {
		t.Fatalf("partial acquire: %v", err)
	}
	if got := c.Shards(); !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("partial shards %v", got)
	}
	if got := b.Shards(); !reflect.DeepEqual(got, []int{0, 2}) {
		t.Fatalf("owner after partial %v", got)
	}

	oneStore := memstore.New(memstore.WithRuntime(rt))
	one := server.New(rt, oneStore, rnd.New(99), server.Config{
		Node: "one", NumShards: 1, TokenKey: append([]byte(nil), key...),
	})
	err = one.StartOwned(ctx, []int{1})
	if err == nil || !strings.Contains(err.Error(), "shard 1") || !strings.Contains(err.Error(), "0..0") {
		t.Fatalf("single-shard range: %v", err)
	}
	if err := one.StartOwned(ctx, []int{0}); err != nil {
		t.Fatal(err)
	}
	if got := one.Shards(); !reflect.DeepEqual(got, []int{0}) {
		t.Fatalf("single shard %v", got)
	}

	id0 := idOnShard(b, 0)
	id1 := idOnShard(b, 1)
	if _, err := b.StartWorkflow(ctx, api.StartRequest{
		WorkflowID: id1, WorkflowType: "W", TaskQueue: "q", RequestID: "r1",
	}); !errors.Is(err, api.ErrNotOwner) {
		t.Fatalf("unowned start: %v", err)
	}
	if _, err := c.StartWorkflow(ctx, api.StartRequest{
		WorkflowID: id1, WorkflowType: "W", TaskQueue: "q", RequestID: "r1",
	}); err != nil {
		t.Fatalf("owner start after rejected start: %v", err)
	}
	if _, err := b.StartWorkflow(ctx, api.StartRequest{
		WorkflowID: id0, WorkflowType: "W", TaskQueue: "q", RequestID: "r0",
	}); err != nil {
		t.Fatal(err)
	}
}

func idOnShard(s *server.Server, shard int) string {
	for i := 0; i < 10000; i++ {
		id := fmt.Sprintf("id-%d", i)
		if s.ShardFor(id) == shard {
			return id
		}
	}
	panic("no workflow id for shard")
}
