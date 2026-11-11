// Package httpapi serves the /v1 HTTP/JSON API of SPEC §13.1 over an api.Service, and defines
// the wire forms the Go client (package client) shares. Heartbeats are a stretch item and not
// served. This package is outside the determinism lint scope: it is the HTTP edge of a node.
package httpapi
