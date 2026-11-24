package agent

// The sinks behind the agent's tool calls, and the scripted model.
//
// FileSink is deliberately the dumbest durable store that is still honest: a JSON file,
// rewritten atomically, that refuses to apply a key it already holds. That is all an
// idempotent sink has to be, and keeping it this simple is what makes the demo's claim easy
// to check — you can read the file.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// FileSink applies effects once per key and keeps them in a JSON file, so the record survives
// the process that wrote it being killed.
type FileSink struct {
	path string
	mu   sync.Mutex
}

// NewFileSink returns a sink backed by path. The file is created on the first Apply.
func NewFileSink(path string) *FileSink { return &FileSink{path: path} }

// Entry is one applied effect.
type Entry struct {
	Key     string          `json:"key"`
	At      time.Time       `json:"at"`
	Value   json.RawMessage `json:"value"`
	Applied int             `json:"applied"` // always 1: a key is applied once
}

// Apply records value under key unless the key is already there, and reports whether it did.
func (s *FileSink) Apply(key string, value any) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := s.load()
	if err != nil {
		return false, err
	}
	if _, exists := entries[key]; exists {
		return false, nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return false, fmt.Errorf("sink %s: %w", s.path, err)
	}
	entries[key] = Entry{Key: key, At: time.Now().UTC(), Value: raw, Applied: 1}
	if err := s.save(entries); err != nil {
		return false, err
	}
	return true, nil
}

// Entries returns what the sink holds, by key.
func (s *FileSink) Entries() ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := s.load()
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]Entry, 0, len(keys))
	for _, k := range keys {
		out = append(out, entries[k])
	}
	return out, nil
}

// Total adds up the Cents of every payment in the sink.
func (s *FileSink) Total() (int64, error) {
	entries, err := s.Entries()
	if err != nil {
		return 0, err
	}
	var total int64
	for _, e := range entries {
		var p Payment
		if err := json.Unmarshal(e.Value, &p); err != nil {
			continue
		}
		total += p.Cents
	}
	return total, nil
}

func (s *FileSink) load() (map[string]Entry, error) {
	b, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return map[string]Entry{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sink %s: %w", s.path, err)
	}
	entries := map[string]Entry{}
	if len(strings.TrimSpace(string(b))) == 0 {
		return entries, nil
	}
	if err := json.Unmarshal(b, &entries); err != nil {
		return nil, fmt.Errorf("sink %s: %w", s.path, err)
	}
	return entries, nil
}

// save writes the file atomically: a kill in the middle leaves the old file, never half a
// new one.
func (s *FileSink) save(entries map[string]Entry) error {
	b, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}
	if dir := filepath.Dir(s.path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return fmt.Errorf("sink %s: %w", s.path, err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("sink %s: %w", s.path, err)
	}
	return nil
}

// MemSink is FileSink's in-memory sibling, for tests and the simulator.
type MemSink struct {
	mu      sync.Mutex
	entries map[string]Entry
	order   []string
}

// NewMemSink returns an empty sink.
func NewMemSink() *MemSink { return &MemSink{entries: map[string]Entry{}} }

// Apply records value under key unless it is already there.
func (s *MemSink) Apply(key string, value any) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.entries[key]; exists {
		return false, nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return false, err
	}
	s.entries[key] = Entry{Key: key, Value: raw, Applied: 1}
	s.order = append(s.order, key)
	return true, nil
}

// Len is how many distinct keys were applied.
func (s *MemSink) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

// Keys lists the applied keys in the order they were applied.
func (s *MemSink) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.order...)
}

// Payments returns the entries decoded as payments, in order.
func (s *MemSink) Payments() []Payment {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Payment, 0, len(s.order))
	for _, k := range s.order {
		var p Payment
		if err := json.Unmarshal(s.entries[k].Value, &p); err == nil {
			out = append(out, p)
		}
	}
	return out
}

// ---- orders -------------------------------------------------------------------------------

// OrderBook is a fixed set of orders, which is all the demo needs behind LookupOrder.
type OrderBook map[string]Order

// Order returns the order or an error, like any lookup against a real service.
func (b OrderBook) Order(_ context.Context, id string) (Order, error) {
	o, ok := b[id]
	if !ok {
		return Order{}, fmt.Errorf("order %s not found", id)
	}
	return o, nil
}

// DemoOrders is the order book the demo and the tests use: one small order and one large
// enough to need a human.
func DemoOrders() OrderBook {
	return OrderBook{
		"A-1001": {ID: "A-1001", Cents: 2400, Status: "delivered", Delivered: true},
		"A-2002": {ID: "A-2002", Cents: 18900, Status: "delivered", Delivered: true},
	}
}

// ---- the scripted model -------------------------------------------------------------------

// Scripted is a Model that needs no network: it reads the order and asks for a refund of its
// full value. It is what the tests and the simulator run, and what the demo runs unless a
// real model is configured.
type Scripted struct{}

// Plan looks up the order and asks for a full refund, which is enough to exercise both paths:
// the small order goes straight through, the large one stops for a human.
func (Scripted) Plan(_ context.Context, t Ticket, order *Order) (Plan, error) {
	if order == nil {
		return Plan{Reply: "We could not find that order. Could you confirm the number?"}, nil
	}
	lookup, err := json.Marshal(LookupArgs{OrderID: order.ID})
	if err != nil {
		return Plan{}, err
	}
	refund, err := json.Marshal(RefundArgs{
		OrderID: order.ID,
		Cents:   order.Cents,
		Reason:  "customer reported the item was damaged",
	})
	if err != nil {
		return Plan{}, err
	}
	return Plan{
		Tools: []ToolCall{
			{Name: ActivityLookupOrder, Args: lookup},
			{Name: ActivityIssueRefund, Args: refund},
		},
		Reply: fmt.Sprintf("Thanks for letting us know, %s. We have refunded order %s in full (%s). "+
			"It should reach your account within a few days.", t.Customer, order.ID, money(order.Cents)),
	}, nil
}

func money(cents int64) string {
	return fmt.Sprintf("$%d.%02d", cents/100, cents%100)
}
