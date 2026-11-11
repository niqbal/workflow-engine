# Uses the Go toolchain in ~/sdk when present (system Go is too old), else `go` on PATH.
GO ?= $(shell ls -d $(HOME)/sdk/go1.2*/bin/go 2>/dev/null | sort -V | tail -1 || echo go)
ifeq ($(GO),)
GO := go
endif

.PHONY: build vet test race lint ci
build:
	$(GO) build ./...
vet:
	$(GO) vet ./...
test:
	$(GO) test ./...
race:
	$(GO) test -race ./...
lint:
	$(GO) test ./internal/lint/...
ci: build vet race
