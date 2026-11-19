// Package sim is the deterministic cluster harness (WI-040, SPEC §11.1).
//
// Run builds N nodes on one in-memory store, drives them with simulated workers under
// internal/simrt, then keeps the cluster in a quiet phase until every workflow the
// workload started has closed or the virtual deadline passes. Same seed, same
// workload: the same trace, event for event.
//
// Faults and Oracle are empty interfaces. WI-042 fills in fault injection and WI-041
// fills in the oracle; this harness does not call them.
package sim
