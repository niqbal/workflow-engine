package lint

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func repoRoot(t *testing.T) string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..")
}

// TestServerCodeIsDeterministic is the gate: it fails the build on any violation in scope.
func TestServerCodeIsDeterministic(t *testing.T) {
	vs, err := CheckDir(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range vs {
		t.Errorf("%s", v)
	}
}

// TestLintCatchesEachBannedForm plants one violation of each kind.
func TestLintCatchesEachBannedForm(t *testing.T) {
	planted := map[string]string{
		"time.Now":            `import "time"; func f() { _ = time.Now() }`,
		"time.Since":          `import "time"; func f(t time.Time) { _ = time.Since(t) }`,
		"time.Sleep":          `import "time"; func f() { time.Sleep(1) }`,
		"time.After":          `import "time"; func f() { _ = time.After(1) }`,
		"context.WithTimeout": `import "context"; func f(c context.Context) { _, _ = context.WithTimeout(c, 1) }`,
		"imports math/rand":   `import "math/rand"; func f() { _ = rand.Int() }`,
		"imports net":         `import "net"; func f() { _, _ = net.Dial("tcp", "x") }`,
		"imports sync":        `import "sync"; var m sync.Mutex`,
		"go statement":        `func f() { go f() }`,
		"select statement":    `func f() { select {} }`,
		"channel type":        `var c chan int`,
		"range over a map":    `func f(m map[string]int) { for k := range m { _ = k } }`,
	}
	for want, body := range planted {
		src := "package p\n" + body + "\n"
		vs, err := CheckSource("planted.go", []byte(src))
		if err != nil {
			t.Fatalf("%s: %v", want, err)
		}
		found := false
		for _, v := range vs {
			if strings.Contains(v.What, want) {
				found = true
			}
		}
		if !found {
			t.Errorf("planted %q not caught; got %v", want, vs)
		}
	}
}

func TestLintAllowsSafeForms(t *testing.T) {
	src := `package p
import (
	"sync/atomic"
	"time"
)
var n atomic.Int64
func f(d time.Duration, s []int) time.Duration {
	for range s {}
	return d * time.Second
}`
	vs, err := CheckSource("ok.go", []byte(src))
	if err != nil || len(vs) != 0 {
		t.Fatalf("false positives: %v %v", vs, err)
	}
}
