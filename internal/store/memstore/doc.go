// Package memstore is the in-memory Store (WI-011, SPEC §12) used by unit tests and the
// simulator. With WithRuntime every method is a yield point and commit delays run on the
// runtime's clock; WithFaults injects errors before commit, ambiguous commits (commit, then
// error) and variable commit latency (SPEC §11.2).
package memstore
