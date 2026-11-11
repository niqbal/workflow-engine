package pgstore

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/niqbal/workflow-engine/internal/xsync"
)

var errClosed = errors.New("pgstore: store is closed")

// pool is a small connection pool over xsync primitives, so the package needs neither
// package sync nor channels (SPEC §6.1).
type pool struct {
	cfg    *pgx.ConnConfig
	mu     xsync.Locker
	cond   xsync.Cond
	idle   []*pgx.Conn
	open   int // connections handed out or idle, plus those being dialed
	max    int
	closed bool
}

func newPool(cfg *pgx.ConnConfig, rt xsync.Runtime, max int) *pool {
	mu := rt.NewLocker()
	return &pool{cfg: cfg, mu: mu, cond: rt.NewCond(mu), max: max}
}

// acquire returns an idle connection, dials a new one below the limit, or waits.
func (p *pool) acquire(ctx context.Context) (*pgx.Conn, error) {
	stop := context.AfterFunc(ctx, func() {
		p.mu.Lock()
		p.cond.Broadcast()
		p.mu.Unlock()
	})
	defer stop()
	p.mu.Lock()
	for {
		if p.closed {
			p.mu.Unlock()
			return nil, errClosed
		}
		if err := ctx.Err(); err != nil {
			p.mu.Unlock()
			return nil, err
		}
		if n := len(p.idle); n > 0 {
			c := p.idle[n-1]
			p.idle = p.idle[:n-1]
			p.mu.Unlock()
			return c, nil
		}
		if p.open < p.max {
			break
		}
		p.cond.Wait()
	}
	p.open++
	p.mu.Unlock()
	c, err := pgx.ConnectConfig(ctx, p.cfg)
	if err != nil {
		p.mu.Lock()
		p.open--
		p.cond.Signal()
		p.mu.Unlock()
		return nil, err
	}
	return c, nil
}

// release returns c to the pool, or closes it if it is broken, mid-transaction or the pool
// is closed.
func (p *pool) release(c *pgx.Conn) {
	p.mu.Lock()
	reusable := !p.closed && !c.IsClosed() && c.PgConn().TxStatus() == 'I'
	if reusable {
		p.idle = append(p.idle, c)
	} else {
		p.open--
	}
	p.cond.Signal()
	p.mu.Unlock()
	if !reusable {
		_ = c.Close(context.Background())
	}
}

// with runs fn on a pooled connection.
func (p *pool) with(ctx context.Context, fn func(*pgx.Conn) error) error {
	c, err := p.acquire(ctx)
	if err != nil {
		return err
	}
	defer p.release(c)
	return fn(c)
}

// close closes idle connections and makes later acquires fail. Connections in use are
// closed when released.
func (p *pool) close() {
	p.mu.Lock()
	p.closed = true
	idle := p.idle
	p.idle = nil
	p.open -= len(idle)
	p.cond.Broadcast()
	p.mu.Unlock()
	for _, c := range idle {
		_ = c.Close(context.Background())
	}
}
