package lint

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestHTTPTransportExcludedFromDeterminismLint is WI-053: node-to-node HTTP is
// excluded next to internal/httpapi, and a violation planted outside that
// exclusion is still reported.
func TestHTTPTransportExcludedFromDeterminismLint(t *testing.T) {
	httpapi, httptransport := false, false
	for _, pkg := range Excluded {
		switch pkg {
		case "internal/httpapi":
			httpapi = true
		case "internal/transport/httptransport":
			httptransport = true
		}
	}
	if !httpapi || !httptransport {
		t.Fatalf("Excluded = %v, want internal/httpapi and internal/transport/httptransport next to each other", Excluded)
	}

	root := t.TempDir()
	if !isExcluded(root, filepath.Join(root, "internal", "httpapi", "httpapi.go")) {
		t.Fatal("internal/httpapi is not excluded")
	}
	if !isExcluded(root, filepath.Join(root, "internal", "transport", "httptransport", "httptransport.go")) {
		t.Fatal("internal/transport/httptransport is not excluded")
	}
	if isExcluded(root, filepath.Join(root, "internal", "server", "server.go")) {
		t.Fatal("internal/server must stay in the determinism scope")
	}

	vs, err := CheckDir(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range vs {
		name := filepath.ToSlash(v.Pos.Filename)
		if strings.Contains(name, "internal/httpapi") || strings.Contains(name, "internal/transport/httptransport") {
			t.Errorf("excluded package linted: %s", v)
		}
	}
	if len(vs) != 0 {
		t.Fatalf("lint violations with httptransport excluded:\n%v", vs)
	}

	planted := []byte("package server\nimport \"net\"\nfunc f() { _, _ = net.Dial(\"tcp\", \"x\") }\n")
	vs, err = CheckSource("internal/server/planted.go", planted)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, v := range vs {
		if strings.Contains(v.What, "imports net") {
			found = true
		}
	}
	if !found {
		t.Fatalf("planted net import outside the exclusion was not reported: %v", vs)
	}
}
