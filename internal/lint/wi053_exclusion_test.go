package lint

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestWI053LintExclusionPassesAndPlantedViolationFails checks WI-053: the
// determinism lint stays clean with internal/transport/httptransport excluded
// beside internal/httpapi, and a violation planted outside that exclusion is
// still reported. The exclusion is what keeps the real package's net imports
// out of the report; widening the walk without it would flag them.
func TestWI053LintExclusionPassesAndPlantedViolationFails(t *testing.T) {
	joined := strings.Join(Excluded, ",")
	if !strings.Contains(joined, "internal/httpapi,internal/transport/httptransport") &&
		!strings.Contains(joined, "internal/transport/httptransport,internal/httpapi") {
		t.Fatalf("Excluded = %v, want httptransport next to internal/httpapi", Excluded)
	}

	root := repoRoot(t)
	src, err := os.ReadFile(filepath.Join(root, "internal/transport/httptransport/httptransport.go"))
	if err != nil {
		t.Fatal(err)
	}
	vs, err := CheckSource("internal/transport/httptransport/httptransport.go", src)
	if err != nil {
		t.Fatal(err)
	}
	if !hasWhat(vs, "imports net") {
		t.Fatalf("httptransport source is clean without the exclusion; got %v", vs)
	}

	vs, err = CheckDir(root)
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

	tree := t.TempDir()
	mustWrite(t, filepath.Join(tree, "internal/server/planted.go"), "package server\n\nimport \"net\"\n\nfunc Dial() { _, _ = net.Dial(\"tcp\", \"127.0.0.1:9\") }\n")
	mustWrite(t, filepath.Join(tree, "internal/transport/httptransport/net.go"), "package httptransport\n\nimport \"net\"\n\nfunc Dial() { _, _ = net.Dial(\"tcp\", \"127.0.0.1:9\") }\n")
	mustWrite(t, filepath.Join(tree, "internal/httpapi/net.go"), "package httpapi\n\nimport \"net\"\n\nfunc Dial() { _, _ = net.Dial(\"tcp\", \"127.0.0.1:9\") }\n")
	mustWrite(t, filepath.Join(tree, "internal/history/ok.go"), "package history\n\nimport \"time\"\n\nfunc f(d time.Duration) time.Duration { return d }\n")

	old := Scope
	t.Cleanup(func() { Scope = old })
	Scope = []string{"internal"}

	vs, err = CheckDir(tree)
	if err != nil {
		t.Fatal(err)
	}
	var planted int
	for _, v := range vs {
		name := filepath.ToSlash(v.Pos.Filename)
		switch {
		case strings.Contains(name, "internal/transport/httptransport"), strings.Contains(name, "internal/httpapi"):
			t.Errorf("excluded package reported under a wider scope: %s", v)
		case strings.Contains(name, "internal/history"):
			t.Errorf("safe history file reported: %s", v)
		case strings.Contains(name, "internal/server/planted.go") && strings.Contains(v.What, "imports net"):
			planted++
		default:
			t.Errorf("unexpected violation: %s", v)
		}
	}
	if planted == 0 {
		t.Fatalf("planted net import outside the exclusion was not reported: %v", vs)
	}
}

func hasWhat(vs []Violation, sub string) bool {
	for _, v := range vs {
		if strings.Contains(v.What, sub) {
			return true
		}
	}
	return false
}

func mustWrite(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}
