package httptransport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/transport"
)

const testNode = transport.NodeID("node-b")

func TestCallResultMapping(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 32)
	cases := []struct {
		name string
		fn   func(*testing.T, []byte)
	}{
		{"refused", testRefused},
		{"reset", testResetMidReply},
		{"timeout", testTimeout},
		{"5xx", testBodyless5xx},
		{"typed error", testTypedError},
		{"success", testSuccess},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, key)
		})
	}
}

func testRefused(t *testing.T, key []byte) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	err = call(t, key, addr, "Op", map[string]int{"x": 1}, &map[string]int{}, time.Second)
	assertUnreachable(t, err)
}

func testResetMidReply(t *testing.T, key []byte) {
	ln := listen(t)
	var accepts atomic.Int32
	errc := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			errc <- err
			return
		}
		accepts.Add(1)
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(2 * time.Second))
		if err := readHTTPRequest(c); err != nil {
			errc <- err
			return
		}
		_, err = c.Write([]byte("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 32\r\n\r\n{\"ok\":"))
		if err != nil {
			errc <- err
			return
		}
		if tcp, ok := c.(*net.TCPConn); ok {
			_ = tcp.SetLinger(0)
		}
		errc <- nil
	}()
	err := call(t, key, ln.Addr().String(), "Op", map[string]int{"x": 1}, &map[string]int{}, 5*time.Second)
	assertUnreachable(t, err)
	select {
	case err := <-errc:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not reset the reply")
	}
	if n := accepts.Load(); n != 1 {
		t.Fatalf("accepts = %d, transport retried", n)
	}
}

func testTimeout(t *testing.T, key []byte) {
	ln := listen(t)
	accepted := make(chan struct{})
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		close(accepted)
		_, _ = io.Copy(io.Discard, c)
	}()
	err := call(t, key, ln.Addr().String(), "Op", map[string]int{"x": 1}, &map[string]int{}, 300*time.Millisecond)
	assertUnreachable(t, err)
	select {
	case <-accepted:
	default:
		t.Fatal("timeout was not a stalled reply")
	}
}

func testBodyless5xx(t *testing.T, key []byte) {
	var hits atomic.Int32
	ln := listen(t)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})}
	go func() { _ = srv.Serve(ln) }()
	err := call(t, key, ln.Addr().String(), "Op", map[string]int{"x": 1}, &map[string]int{}, time.Second)
	assertUnreachable(t, err)
	if n := hits.Load(); n != 1 {
		t.Fatalf("server hits = %d, transport retried", n)
	}
}

func testTypedError(t *testing.T, key []byte) {
	h := NewHandler(key)
	h.Register("Op", func(context.Context, json.RawMessage) (any, error) {
		return nil, &Error{Code: "not_owner", Message: "owned by node-b"}
	})
	addr := serve(t, h)
	err := call(t, key, addr, "Op", map[string]int{"x": 1}, &map[string]int{}, time.Second)
	assertTyped(t, err, "not_owner", "node-b")
}

func testSuccess(t *testing.T, key []byte) {
	h := NewHandler(key)
	h.Register("Op", func(_ context.Context, raw json.RawMessage) (any, error) {
		var req struct {
			X int `json:"x"`
		}
		if err := json.Unmarshal(raw, &req); err != nil {
			return nil, &Error{Code: "invalid_argument", Message: err.Error()}
		}
		return struct {
			Y int `json:"y"`
		}{Y: req.X * 2}, nil
	})
	addr := serve(t, h)
	var got struct {
		Y int `json:"y"`
	}
	err := call(t, key, addr, "Op", map[string]int{"x": 21}, &got, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got.Y != 42 {
		t.Fatalf("response = %+v, want y=42", got)
	}
}

func TestHMACRejectedBeforeDispatch(t *testing.T) {
	key := []byte("cluster-key")
	var calls atomic.Int32
	h := NewHandler(key)
	h.Register("Op", func(context.Context, json.RawMessage) (any, error) {
		calls.Add(1)
		return map[string]string{"ok": "yes"}, nil
	})
	addr := serve(t, h)
	url := "http://" + addr + "/internal/v1/Op"
	body := []byte(`{"x":1}`)

	// Signature over a different body.
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderHMAC, sign(key, []byte(`{"x":2}`)))
	res, err := testClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	rejectBody, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong HMAC status = %d body %s", res.StatusCode, rejectBody)
	}
	assertTypedBody(t, rejectBody, "unauthenticated")

	// No signature header.
	req, err = http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err = testClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	rejectBody, _ = io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing HMAC status = %d body %s", res.StatusCode, rejectBody)
	}
	assertTypedBody(t, rejectBody, "unauthenticated")

	// Wrong cluster key, through Call.
	err = call(t, []byte("other-key"), addr, "Op", map[string]int{"x": 1}, &map[string]string{}, time.Second)
	assertTyped(t, err, "unauthenticated", "HMAC")
	if n := calls.Load(); n != 0 {
		t.Fatalf("method dispatched %d times before a valid HMAC", n)
	}

	var got map[string]string
	err = call(t, key, addr, "Op", map[string]int{"x": 1}, &got, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got["ok"] != "yes" || calls.Load() != 1 {
		t.Fatalf("valid call got %+v calls=%d", got, calls.Load())
	}
}

func call(t *testing.T, key []byte, addr, method string, req, resp any, budget time.Duration) error {
	t.Helper()
	c, err := New(key, map[transport.NodeID]string{testNode: addr})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	return c.Call(ctx, testNode, method, req, resp)
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

func serve(t *testing.T, h http.Handler) string {
	t.Helper()
	ln := listen(t)
	srv := &http.Server{Handler: h}
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().String()
}

func testClient() *http.Client {
	return &http.Client{Transport: &http.Transport{Proxy: nil}}
}

func assertUnreachable(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, transport.ErrUnreachable) {
		t.Fatalf("error = %v, want ErrUnreachable", err)
	}
	var te *Error
	if errors.As(err, &te) {
		t.Fatalf("unreachable carries typed code %s", te.Code)
	}
}

func assertTyped(t *testing.T, err error, code, messageSub string) {
	t.Helper()
	if err == nil {
		t.Fatal("error = nil")
	}
	if errors.Is(err, transport.ErrUnreachable) {
		t.Fatalf("typed error reported unreachable: %v", err)
	}
	var te *Error
	if !errors.As(err, &te) {
		t.Fatalf("error = %T %v, want code %s", err, err, code)
	}
	if te.Code != code || !strings.Contains(te.Message, messageSub) {
		t.Fatalf("error = %+v, want code %s message containing %q", te, code, messageSub)
	}
}

func assertTypedBody(t *testing.T, body []byte, code string) {
	t.Helper()
	var te Error
	if err := json.Unmarshal(body, &te); err != nil {
		t.Fatal(err)
	}
	if te.Code != code {
		t.Fatalf("body code = %q, want %s (%s)", te.Code, code, body)
	}
}

func readHTTPRequest(c net.Conn) error {
	var buf []byte
	tmp := make([]byte, 1024)
	for {
		n, err := c.Read(tmp)
		buf = append(buf, tmp[:n]...)
		hdr := bytes.Index(buf, []byte("\r\n\r\n"))
		if hdr >= 0 {
			need, perr := contentLength(buf[:hdr])
			if perr != nil {
				return perr
			}
			if len(buf)-(hdr+4) >= need {
				return nil
			}
		}
		if err != nil {
			return err
		}
		if len(buf) > 1<<20 {
			return errors.New("request too large")
		}
	}
}

func contentLength(header []byte) (int, error) {
	for _, line := range strings.Split(string(header), "\r\n") {
		if strings.HasPrefix(strings.ToLower(line), "content-length:") {
			return strconv.Atoi(strings.TrimSpace(line[len("content-length:"):]))
		}
	}
	return 0, nil
}
