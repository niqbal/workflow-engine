// Package rnd is the only source of randomness for server code (SPEC §6.1).
package rnd

import "math/rand/v2"

// Rand is a source of pseudo-random numbers. Implementations need not be safe for
// concurrent use; callers hold their own lock or use one Rand per goroutine.
type Rand interface {
	Int63() int64
}

// New returns a deterministic Rand for the seed.
func New(seed uint64) Rand { return pcg{rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))} }

type pcg struct{ r *rand.Rand }

func (p pcg) Int63() int64 { return p.r.Int64() }
