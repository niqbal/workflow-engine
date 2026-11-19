package httptransport

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/server/api"
	"github.com/niqbal/workflow-engine/internal/transport"
)

// TestWI053LoopbackResultMapping is the acceptance table on a real loopback
// socket: refused, reset mid-reply, timeout, bodyless 5xx, typed error and
// success. Replies that cannot be interpreted are ambiguous (ErrUnreachable),
// and none of those paths is retried.
func TestWI053LoopbackResultMapping(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 32)
	cases := []struct {
		name string
		fn   func(*testing.T, []byte)
	}{
		{"refused", wi053Refused},
		{"reset mid-reply", wi053ResetMidReply},
		{"timeout", wi053Timeout},
		{"5xx", wi053Bodyless5xx},
		{"typed error", wi053TypedError},
		{"success", wi053Success},
		{"empty success", wi053EmptySuccess},
		{"garbled success", wi053GarbledSuccess},
		{"drop after request", wi053DropAfterRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.fn(t, key)
		})
	}
}

func wi053Refused(t *testing.T, key []byte) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := callY(t, key, addr, "Op", time.Second)
	mustUnreachable(t, err)
	if got.Y != -1 {
		t.Fatalf("refused connection wrote resp %+v", got)
	}
}

func wi053ResetMidReply(t *testing.T, key []byte) {
	var wire atomic.Value
	addr, hits := serveRaw(t, func(c net.Conn, req []byte) {
		wire.Store(append([]byte(nil), req...))
		_, _ = c.Write([]byte("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 64\r\n\r\n{\"y\":"))
		rst(c)
	})
	got, err := callY(t, key, addr, "Op", 5*time.Second)
	mustUnreachable(t, err)
	if got.Y != -1 {
		t.Fatalf("reset mid-reply decoded resp %+v", got)
	}
	assertHits(t, hits, 1)
	if raw, _ := wire.Load().([]byte); raw != nil {
		assertWire(t, raw, key, "Op")
	}
}

func wi053Timeout(t *testing.T, key []byte) {
	addr, hits := serveRaw(t, func(c net.Conn, _ []byte) {
		_ = c.SetDeadline(time.Time{})
		_, _ = io.Copy(io.Discard, c)
	})
	got, err := callY(t, key, addr, "Op", time.Second)
	mustUnreachable(t, err)
	if got.Y != -1 {
		t.Fatalf("timeout decoded resp %+v", got)
	}
	if n := hits.Load(); n == 0 {
		t.Fatal("deadline fired before the peer accepted; not a stalled reply")
	} else if n != 1 {
		t.Fatalf("accepts = %d, transport retried a timed-out call", n)
	}
}

func wi053Bodyless5xx(t *testing.T, key []byte) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var wire atomic.Value
			addr, hits := serveRaw(t, func(c net.Conn, req []byte) {
				wire.Store(append([]byte(nil), req...))
				_, _ = fmt.Fprintf(c, "HTTP/1.1 %d status\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", status)
			})
			got, err := callY(t, key, addr, "Op", time.Second)
			mustUnreachable(t, err)
			if got.Y != -1 {
				t.Fatalf("HTTP %d wrote resp %+v", status, got)
			}
			assertHits(t, hits, 1)
			raw, _ := wire.Load().([]byte)
			assertWire(t, raw, key, "Op")
		})
	}
	// A 5xx that closes with no length and no body is the same ambiguous reply.
	t.Run("eof", func(t *testing.T) {
		addr, hits := serveRaw(t, func(c net.Conn, _ []byte) {
			_, _ = io.WriteString(c, "HTTP/1.1 500 Internal Server Error\r\nConnection: close\r\n\r\n")
		})
		_, err := callY(t, key, addr, "Op", time.Second)
		mustUnreachable(t, err)
		assertHits(t, hits, 1)
	})
}

func wi053TypedError(t *testing.T, key []byte) {
	body := `{"code":"not_owner","message":"owned by node-b"}`
	// A typed body stays typed on 5xx. Collapsing every 5xx into
	// ErrUnreachable would hide not_owner and make routing retry a final answer.
	for _, status := range []int{http.StatusInternalServerError, http.StatusServiceUnavailable, http.StatusBadRequest} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			addr, hits := serveRaw(t, func(c net.Conn, _ []byte) {
				fmt.Fprintf(c, "HTTP/1.1 %d status\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", status, len(body), body)
			})
			got, err := callY(t, key, addr, "Op", time.Second)
			mustTyped(t, err, "not_owner", "owned by node-b")
			if got.Y != -1 {
				t.Fatalf("typed error filled resp %+v", got)
			}
			assertHits(t, hits, 1)
		})
	}

	var calls atomic.Int32
	h := NewHandler(key)
	h.Register("Op", func(context.Context, json.RawMessage) (any, error) {
		calls.Add(1)
		return map[string]int{"y": 99}, &Error{Code: "not_owner", Message: "owned by node-b"}
	})
	addr := serveHandler(t, h)
	got, err := callY(t, key, addr, "Op", time.Second)
	mustTyped(t, err, "not_owner", "owned by node-b")
	if got.Y != -1 {
		t.Fatalf("handler typed error filled resp %+v", got)
	}
	assertHits(t, &calls, 1)
}

func wi053Success(t *testing.T, key []byte) {
	var calls atomic.Int32
	cap := &wireCapture{}
	h := NewHandler(key)
	h.Register("Double", func(_ context.Context, raw json.RawMessage) (any, error) {
		calls.Add(1)
		var req struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(raw, &req); err != nil {
			return nil, &Error{Code: "invalid_argument", Message: err.Error()}
		}
		return map[string]int{"y": req.N * 2}, nil
	})
	cap.inner = h
	addr := serveHandler(t, cap)
	got, err := callY(t, key, addr, "Double", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got.Y != 42 {
		t.Fatalf("response = %+v, want y=42", got)
	}
	assertHits(t, &calls, 1)
	if cap.method.Load() != http.MethodPost {
		t.Fatalf("method = %v, want POST", cap.method.Load())
	}
	if cap.path.Load() != "/internal/v1/Double" {
		t.Fatalf("path = %v, want /internal/v1/Double", cap.path.Load())
	}
	body, _ := cap.body.Load().(string)
	if body != `{"n":21}` {
		t.Fatalf("body = %q, want {\"n\":21}", body)
	}
	mac, _ := cap.mac.Load().(string)
	if mac != wiMAC(key, []byte(body)) {
		t.Fatalf("HMAC = %q, want HMAC-SHA256 over the raw body", mac)
	}
}

func wi053EmptySuccess(t *testing.T, key []byte) {
	addr, hits := serveRaw(t, func(c net.Conn, _ []byte) {
		_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
	})
	got, err := callY(t, key, addr, "Op", time.Second)
	mustUnreachable(t, err)
	if got.Y != -1 {
		t.Fatalf("empty success wrote resp %+v", got)
	}
	assertHits(t, hits, 1)
}

func wi053GarbledSuccess(t *testing.T, key []byte) {
	const body = "{not-json"
	addr, hits := serveRaw(t, func(c net.Conn, _ []byte) {
		fmt.Fprintf(c, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(body), body)
	})
	got, err := callY(t, key, addr, "Op", time.Second)
	mustUnreachable(t, err)
	if got.Y != -1 {
		t.Fatalf("garbled success wrote resp %+v", got)
	}
	assertHits(t, hits, 1)
}

func wi053DropAfterRequest(t *testing.T, key []byte) {
	addr, hits := serveRaw(t, func(c net.Conn, _ []byte) {
		rst(c)
	})
	got, err := callY(t, key, addr, "Op", 5*time.Second)
	mustUnreachable(t, err)
	if got.Y != -1 {
		t.Fatalf("dropped reply wrote resp %+v", got)
	}
	assertHits(t, hits, 1)
}

// TestWI053HMACRejectedBeforeDispatch checks that a wrong, missing or empty
// signature is refused before the registered method runs, and that a good
// signature runs it once.
func TestWI053HMACRejectedBeforeDispatch(t *testing.T) {
	key := []byte("cluster-key")
	var calls atomic.Int32
	h := NewHandler(key)
	h.Register("Op", func(context.Context, json.RawMessage) (any, error) {
		calls.Add(1)
		return map[string]string{"ok": "yes"}, nil
	})
	addr := serveHandler(t, h)
	url := "http://" + addr + "/internal/v1/Op"
	body := []byte(`{"x":1,"z":2}`)

	wrong := wiMAC(key, []byte(`{"z":2,"x":1}`))
	status, resp := postHMAC(t, url, body, &wrong)
	mustUnauth(t, status, resp)
	if n := calls.Load(); n != 0 {
		t.Fatalf("wrong HMAC dispatched the method %d times", n)
	}

	// SHA-256 of the body is not an HMAC.
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	status, resp = postHMAC(t, url, body, &digest)
	mustUnauth(t, status, resp)
	if n := calls.Load(); n != 0 {
		t.Fatalf("non-HMAC digest dispatched the method %d times", n)
	}

	status, resp = postHMAC(t, url, body, nil)
	mustUnauth(t, status, resp)
	if n := calls.Load(); n != 0 {
		t.Fatalf("missing HMAC dispatched the method %d times", n)
	}

	empty := ""
	status, resp = postHMAC(t, url, body, &empty)
	mustUnauth(t, status, resp)
	if n := calls.Load(); n != 0 {
		t.Fatalf("empty HMAC dispatched the method %d times", n)
	}

	var out map[string]string
	err := wiCall(t, []byte("other-key"), addr, "Op", map[string]int{"x": 1}, &out, time.Second)
	if err == nil || errors.Is(err, transport.ErrUnreachable) {
		t.Fatalf("wrong key via Call = %v, want a typed rejection", err)
	}
	te := asTransportError(t, err)
	if te.Code != "unauthenticated" {
		t.Fatalf("wrong key error = %+v, want unauthenticated", te)
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("wrong key dispatched the method %d times", n)
	}
	if out != nil {
		t.Fatalf("rejected call filled resp %#v", out)
	}

	// A valid signature still dispatches exactly once.
	good := wiMAC(key, body)
	status, resp = postHMAC(t, url, body, &good)
	if status != http.StatusOK {
		t.Fatalf("valid HMAC status = %d body %s", status, resp)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("valid HMAC calls = %d, want 1", n)
	}
	err = wiCall(t, key, addr, "Op", map[string]int{"x": 1}, &out, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if out["ok"] != "yes" || calls.Load() != 2 {
		t.Fatalf("valid Call got %#v calls=%d", out, calls.Load())
	}
}

type yResp struct {
	Y int `json:"y"`
}

func callY(t *testing.T, key []byte, addr, method string, budget time.Duration) (yResp, error) {
	t.Helper()
	got := yResp{Y: -1}
	err := wiCall(t, key, addr, method, struct {
		N int `json:"n"`
	}{N: 21}, &got, budget)
	return got, err
}

func wiCall(t *testing.T, key []byte, addr, method string, req, resp any, budget time.Duration) error {
	t.Helper()
	c, err := New(key, map[transport.NodeID]string{"node-b": addr})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	return c.Call(ctx, "node-b", method, req, resp)
}

func serveRaw(t *testing.T, respond func(net.Conn, []byte)) (string, *atomic.Int32) {
	t.Helper()
	ln := listenLoop(t)
	var hits atomic.Int32
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			hits.Add(1)
			go func(c net.Conn) {
				defer c.Close()
				req, err := readFullRequest(c)
				if err != nil {
					return
				}
				respond(c, req)
			}(c)
		}
	}()
	return ln.Addr().String(), &hits
}

func serveHandler(t *testing.T, h http.Handler) string {
	t.Helper()
	ln := listenLoop(t)
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String()
}

func listenLoop(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

func readFullRequest(c net.Conn) ([]byte, error) {
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var buf []byte
	tmp := make([]byte, 2048)
	for {
		n, err := c.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if hdr := bytes.Index(buf, []byte("\r\n\r\n")); hdr >= 0 {
			need := contentLen(buf[:hdr])
			if len(buf)-(hdr+4) >= need {
				return buf[:hdr+4+need], nil
			}
		}
		if err != nil {
			return buf, err
		}
		if len(buf) > 1<<20 {
			return buf, errors.New("request too large")
		}
	}
}

func contentLen(header []byte) int {
	for _, line := range bytes.Split(header, []byte("\r\n")) {
		if bytes.HasPrefix(bytes.ToLower(line), []byte("content-length:")) {
			n, err := strconv.Atoi(string(bytes.TrimSpace(line[len("content-length:"):])))
			if err != nil {
				return 0
			}
			return n
		}
	}
	return 0
}

func rst(c net.Conn) {
	if tcp, ok := c.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}
	_ = c.Close()
}

func postHMAC(t *testing.T, rawURL string, body []byte, mac *string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, rawURL, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if mac != nil {
		req.Header.Set(HeaderHMAC, *mac)
	}
	cli := &http.Client{Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}
	res, err := cli.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	got, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, got
}

func wiMAC(key, body []byte) string {
	m := hmac.New(sha256.New, key)
	_, _ = m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}

func mustUnreachable(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, transport.ErrUnreachable) {
		t.Fatalf("error = %v, want ErrUnreachable", err)
	}
	var te *Error
	if errors.As(err, &te) {
		t.Fatalf("unreachable carries typed code %q", te.Code)
	}
}

func mustTyped(t *testing.T, err error, code, message string) {
	t.Helper()
	if err == nil {
		t.Fatal("error = nil, want typed " + code)
	}
	if errors.Is(err, transport.ErrUnreachable) {
		t.Fatalf("typed %s reported unreachable: %v", code, err)
	}
	te := asTransportError(t, err)
	if te.Code != code || te.Message != message {
		t.Fatalf("error = %+v, want code %s message %q", te, code, message)
	}
	if code == "not_owner" && !errors.Is(err, api.ErrNotOwner) {
		t.Fatalf("errors.Is(err, api.ErrNotOwner) = false (%v)", err)
	}
	if code == "not_owner" && errors.Is(err, api.ErrUnavailable) {
		t.Fatal("not_owner also matches api.ErrUnavailable")
	}
}

func asTransportError(t *testing.T, err error) Error {
	t.Helper()
	var pe *Error
	if errors.As(err, &pe) && pe != nil {
		return *pe
	}
	var val Error
	if errors.As(err, &val) && val.Code != "" {
		return val
	}
	t.Fatalf("error = %T %v, want httptransport.Error", err, err)
	return Error{}
}

func mustUnauth(t *testing.T, status int, body []byte) {
	t.Helper()
	if status != http.StatusUnauthorized {
		t.Fatalf("status = %d, body %s, want 401", status, body)
	}
	var te Error
	if err := json.Unmarshal(body, &te); err != nil {
		t.Fatalf("reject body %s: %v", body, err)
	}
	if te.Code != "unauthenticated" {
		t.Fatalf("code = %q, body %s", te.Code, body)
	}
}

func assertHits(t *testing.T, hits *atomic.Int32, want int32) {
	t.Helper()
	if n := hits.Load(); n != want {
		t.Fatalf("attempts = %d, want %d (transport retried)", n, want)
	}
}

func assertWire(t *testing.T, req []byte, key []byte, method string) {
	t.Helper()
	if len(req) == 0 {
		t.Fatal("peer saw no request")
	}
	head, body, ok := bytes.Cut(req, []byte("\r\n\r\n"))
	if !ok {
		t.Fatalf("request has no header terminator: %q", req)
	}
	line := head
	if i := bytes.Index(head, []byte("\r\n")); i >= 0 {
		line = head[:i]
	}
	wantLine := "POST /internal/v1/" + method + " HTTP/1.1"
	if string(line) != wantLine {
		t.Fatalf("request line = %q, want %s", line, wantLine)
	}
	got := headerValue(head, HeaderHMAC)
	want := wiMAC(key, body)
	if got != want {
		t.Fatalf("HMAC = %q, want %q over body %q", got, want, body)
	}
}

func headerValue(head []byte, name string) string {
	prefix := strings.ToLower(name) + ":"
	for _, line := range strings.Split(string(head), "\r\n") {
		if strings.HasPrefix(strings.ToLower(line), prefix) {
			return strings.TrimSpace(line[len(prefix):])
		}
	}
	return ""
}

type wireCapture struct {
	inner  http.Handler
	method atomic.Value
	path   atomic.Value
	mac    atomic.Value
	body   atomic.Value
}

func (c *wireCapture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	c.method.Store(r.Method)
	c.path.Store(r.URL.Path)
	c.mac.Store(r.Header.Get(HeaderHMAC))
	c.body.Store(string(body))
	c.inner.ServeHTTP(w, r)
}
