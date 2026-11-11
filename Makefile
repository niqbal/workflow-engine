# Uses the Go toolchain in ~/sdk when present (system Go is too old), else `go` on PATH.
GO ?= $(shell ls -d $(HOME)/sdk/go1.2*/bin/go 2>/dev/null | sort -V | tail -1 || echo go)
ifeq ($(GO),)
GO := go
endif

.PHONY: build vet test race lint ci contract
build:
	$(GO) build ./...
vet:
	$(GO) vet ./...
# The contract scenarios turn green item by item (WORK-ITEMS M1); `make contract` shows progress.
test:
	$(GO) test -skip 'TestContract_' ./...
race:
	$(GO) test -race -skip 'TestContract_' ./...
contract:
	@$(GO) test -count=1 -run 'TestContract_' ./internal/server/ 2>&1 | grep -E '^--- (PASS|FAIL)' | sort | uniq -c -w9 || true
	@$(GO) test -count=1 -v -run 'TestContract_' ./internal/server/ 2>&1 | grep -cE '^--- PASS' | xargs echo "passing scenarios:"
lint:
	$(GO) test ./internal/lint/...
ci: build vet race
