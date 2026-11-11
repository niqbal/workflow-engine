package memstore_test

import (
	"context"
	"testing"

	"github.com/niqbal/workflow-engine/internal/store"
	"github.com/niqbal/workflow-engine/internal/store/memstore"
	"github.com/niqbal/workflow-engine/internal/store/storetest"
	"github.com/niqbal/workflow-engine/internal/xsync"
)

func TestMemstoreContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store { return memstore.New() })
}

// The same contract with a production runtime, so the yielding path is covered too.
func TestMemstoreContractWithRealRuntime(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store {
		return memstore.New(memstore.WithRuntime(xsync.NewReal(context.Background())))
	})
}
