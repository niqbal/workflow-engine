// Package httptransport is the production node-to-node transport (WI-053).
//
// Call posts JSON to http://<peer>/internal/v1/<method> and signs the raw body
// with HMAC-SHA256 under the cluster key. The handler checks that header before
// it dispatches. Deadlines come only from the call context. There are no retries:
// a second attempt is the caller's decision, because the first one may already
// have been applied.
//
// A reply the caller cannot interpret is transport.ErrUnreachable. Reporting that
// case as a clean failure would let a retry write twice, and the simulator would
// not see the duplicate. The only clean remote failures are typed {code, message}
// bodies.
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
	"net/http"
	"net/url"
	"strings"

	"github.com/niqbal/workflow-engine/internal/transport"
)

// HeaderHMAC carries hex(HMAC-SHA256(clusterKey, rawBody)).
const HeaderHMAC = "X-Wfe-Hmac"

const (
	pathPrefix = "/internal/v1/"
	maxBody    = 16 << 20
)

// Error is a typed peer reply. Code is the stable identifier ("not_owner");
// Message is the human-readable detail. errors.Is matches a sentinel whose
// Error() text equals Code, so api.ErrNotOwner matches a "not_owner" reply.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e Error) Error() string {
	if e.Message == "" {
		return e.Code
	}
	return e.Message
}

func (e Error) Is(target error) bool {
	if target == nil || e.Code == "" {
		return false
	}
	switch t := target.(type) {
	case *Error:
		return t != nil && t.Code == e.Code
	case Error:
		return t.Code == e.Code
	default:
		return target.Error() == e.Code
	}
}

// Method handles one registered RPC. req is the raw JSON body, which may be
// empty. Return *Error for a typed failure; any other error is sent as internal.
type Method func(ctx context.Context, req json.RawMessage) (any, error)

// Handler is the peer's /internal/v1 endpoint.
type Handler struct {
	key     []byte
	methods map[string]Method
}

// NewHandler returns a handler that authenticates bodies with key.
func NewHandler(key []byte) *Handler {
	return &Handler{key: cloneKey(key), methods: map[string]Method{}}
}

// Register binds method to fn. Register before ServeHTTP; the map is not locked.
func (h *Handler) Register(method string, fn Method) {
	if h.methods == nil {
		h.methods = map[string]Method{}
	}
	h.methods[method] = fn
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	method, ok := methodFromPath(r.URL.Path)
	if !ok {
		writeError(w, http.StatusNotFound, Error{Code: "not_found", Message: "no such method"})
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, Error{Code: "invalid_argument", Message: "request body too large"})
		return
	}
	// Authenticate before dispatch. A bad signature must not run the method,
	// including when the method name is unknown.
	if !verify(h.key, body, r.Header.Get(HeaderHMAC)) {
		writeError(w, http.StatusUnauthorized, Error{Code: "unauthenticated", Message: "missing or invalid HMAC"})
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, Error{Code: "invalid_argument", Message: "POST required"})
		return
	}
	fn, ok := h.methods[method]
	if !ok {
		writeError(w, http.StatusNotFound, Error{Code: "not_found", Message: "unknown method " + method})
		return
	}
	resp, err := fn(r.Context(), json.RawMessage(body))
	if err != nil {
		e := asError(err)
		writeError(w, statusFor(e.Code), e)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func methodFromPath(p string) (string, bool) {
	if !strings.HasPrefix(p, pathPrefix) {
		return "", false
	}
	m, err := url.PathUnescape(strings.TrimPrefix(p, pathPrefix))
	if err != nil || m == "" || strings.Contains(m, "/") {
		return "", false
	}
	return m, true
}

func asError(err error) Error {
	var pe *Error
	if errors.As(err, &pe) && pe != nil && pe.Code != "" {
		return *pe
	}
	var e Error
	if errors.As(err, &e) && e.Code != "" {
		return e
	}
	return Error{Code: "internal", Message: err.Error()}
}

func statusFor(code string) int {
	switch code {
	case "unauthenticated":
		return http.StatusUnauthorized
	case "not_found":
		return http.StatusNotFound
	case "not_owner", "unavailable":
		return http.StatusServiceUnavailable
	case "internal":
		return http.StatusInternalServerError
	default:
		return http.StatusBadRequest
	}
}

func writeError(w http.ResponseWriter, status int, e Error) {
	writeJSON(w, status, e)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		status = http.StatusInternalServerError
		b = []byte(`{"code":"internal","message":"encode response"}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(b, '\n'))
}

// Client calls peers over HTTP. It implements transport.Transport.
type Client struct {
	key   []byte
	peers map[transport.NodeID]string
	hc    *http.Client
}

// New builds a client. peers maps a node to host:port or an http URL.
// Addresses are dialed directly; the process HTTP proxy is ignored.
func New(key []byte, peers map[transport.NodeID]string) (*Client, error) {
	addrs := make(map[transport.NodeID]string, len(peers))
	for id, addr := range peers {
		n, err := normalize(addr)
		if err != nil {
			return nil, fmt.Errorf("transport: peer %s: %w", id, err)
		}
		addrs[id] = n
	}
	return &Client{key: cloneKey(key), peers: addrs, hc: newHTTPClient()}, nil
}

func newHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{Proxy: nil},
		// A redirect would hide which peer answered. Fail the call instead.
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("redirects are not followed")
		},
	}
}

// Call implements transport.Transport. It performs one attempt.
func (c *Client) Call(ctx context.Context, node transport.NodeID, method string, req, resp any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return unreachable(err)
	}
	end, err := c.endpoint(node, method)
	if err != nil {
		return err
	}
	body, err := json.Marshal(req)
	if err != nil {
		// Nothing was sent.
		return fmt.Errorf("transport: encode request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, end, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("transport: build request: %w", err)
	}
	// NewRequest sets GetBody for a bytes.Reader, which lets Transport replay
	// the POST. Drop it so a lost connection cannot be tried twice.
	httpReq.GetBody = nil
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set(HeaderHMAC, sign(c.key, body))

	res, err := c.hc.Do(httpReq)
	if err != nil {
		return unreachable(err)
	}
	defer res.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(res.Body, maxBody+1))
	if err != nil {
		return unreachable(err)
	}
	if int64(len(respBody)) > maxBody {
		return fmt.Errorf("%w: response exceeds %d bytes", transport.ErrUnreachable, maxBody)
	}
	return classify(res.StatusCode, respBody, resp)
}

func (c *Client) endpoint(node transport.NodeID, method string) (string, error) {
	if strings.TrimSpace(method) == "" || strings.Contains(method, "/") {
		return "", fmt.Errorf("transport: invalid method %q", method)
	}
	base, ok := c.peers[node]
	if !ok || base == "" {
		return "", fmt.Errorf("%w: unknown node %q", transport.ErrUnreachable, node)
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("%w: %v", transport.ErrUnreachable, err)
	}
	u.Path = pathPrefix + url.PathEscape(method)
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

// classify maps a completed HTTP response. A 2xx with a decodable body is
// success. A JSON object with a non-empty code is that typed error, whatever
// the status. Every other outcome is ambiguous: the peer may already have
// applied the call, so it is ErrUnreachable.
func classify(status int, body []byte, resp any) error {
	if status >= 200 && status < 300 {
		if resp == nil {
			return nil
		}
		if len(bytes.TrimSpace(body)) == 0 {
			return fmt.Errorf("%w: empty success body", transport.ErrUnreachable)
		}
		if err := json.Unmarshal(body, resp); err != nil {
			return fmt.Errorf("%w: decode response: %v", transport.ErrUnreachable, err)
		}
		return nil
	}
	if te, ok := typedError(body); ok {
		return &te
	}
	// Bodyless 5xx, a reset that still produced a status, or any other
	// non-typed reply: the peer may already have applied the call.
	return fmt.Errorf("%w: HTTP %d", transport.ErrUnreachable, status)
}

func typedError(body []byte) (Error, bool) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 || body[0] != '{' {
		return Error{}, false
	}
	var te Error
	if err := json.Unmarshal(body, &te); err != nil || te.Code == "" {
		return Error{}, false
	}
	return te, true
}

func unreachable(err error) error {
	if err == nil {
		return transport.ErrUnreachable
	}
	return fmt.Errorf("%w: %v", transport.ErrUnreachable, err)
}

func sign(key, body []byte) string {
	m := hmac.New(sha256.New, key)
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}

func verify(key, body []byte, header string) bool {
	got, err := hex.DecodeString(strings.TrimSpace(header))
	if err != nil || len(got) != sha256.Size {
		return false
	}
	m := hmac.New(sha256.New, key)
	m.Write(body)
	return hmac.Equal(m.Sum(nil), got)
}

func normalize(addr string) (string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", errors.New("empty address")
	}
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	u, err := url.Parse(addr)
	if err != nil {
		return "", err
	}
	if u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return "", fmt.Errorf("address %q needs an http host", addr)
	}
	u.Path = ""
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

func cloneKey(key []byte) []byte {
	return append([]byte(nil), key...)
}

var (
	_ transport.Transport = (*Client)(nil)
	_ http.Handler        = (*Handler)(nil)
)
