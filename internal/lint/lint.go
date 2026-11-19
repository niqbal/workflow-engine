// Package lint holds the determinism lint (WI-003, SPEC §6.1). See lint_test.go.
package lint

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Scope lists the server packages that must be deterministic under simulation.
// Paths under Excluded are skipped even when they sit inside a scope root.
var Scope = []string{
	"internal/server", "internal/shard", "internal/history",
	"internal/store/memstore", "internal/store/pgstore",
}

// Excluded lists packages that may use the real network. They sit outside the
// simulation scope, next to each other (SPEC §6.1, WI-053): internal/httpapi is
// the public HTTP API, and internal/transport/httptransport is node-to-node HTTP.
var Excluded = []string{
	"internal/httpapi",
	"internal/transport/httptransport",
}

// Exempt lists packages inside Scope that are allowed to break the rules.
// internal/server/api is plain data plus interfaces.
var Exempt = []string{}

var bannedTime = map[string]bool{"Now": true, "Since": true, "Until": true, "Sleep": true,
	"After": true, "AfterFunc": true, "NewTimer": true, "NewTicker": true, "Tick": true}

// Violation is one banned construct.
type Violation struct {
	Pos  token.Position
	What string
}

func (v Violation) String() string { return fmt.Sprintf("%s: %s", v.Pos, v.What) }

// CheckDir lints every non-test .go file under root/dir for each dir in Scope.
func CheckDir(root string) ([]Violation, error) {
	var out []Violation
	for _, d := range Scope {
		base := filepath.Join(root, d)
		if _, err := os.Stat(base); err != nil {
			continue
		}
		if isExcluded(root, base) {
			continue
		}
		err := filepath.WalkDir(base, func(p string, e fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if isExcluded(root, p) {
				if e.IsDir() {
					return fs.SkipDir
				}
				return nil
			}
			if e.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			src, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			vs, err := CheckSource(p, src)
			out = append(out, vs...)
			return err
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// CheckSource lints one file. Map ranges are detected with type information when the file
// type-checks on its own; otherwise ranges over expressions declared as maps in the file.
func CheckSource(filename string, src []byte) ([]Violation, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, src, 0)
	if err != nil {
		return nil, err
	}
	imports := map[string]string{} // local name -> path
	for _, im := range f.Imports {
		path := strings.Trim(im.Path.Value, `"`)
		name := filepath.Base(path)
		if path == "math/rand/v2" {
			name = "rand"
		}
		if im.Name != nil {
			name = im.Name.Name
		}
		imports[name] = path
	}
	var out []Violation
	add := func(n ast.Node, what string) { out = append(out, Violation{fset.Position(n.Pos()), what}) }
	for _, im := range f.Imports {
		path := strings.Trim(im.Path.Value, `"`)
		switch {
		case path == "math/rand" || path == "math/rand/v2" || path == "crypto/rand":
			add(im, "imports "+path+" (use internal/rnd)")
		case path == "net" || strings.HasPrefix(path, "net/"):
			add(im, "imports "+path+" (use internal/transport; HTTP lives in internal/httpapi and internal/transport/httptransport)")
		case path == "sync":
			add(im, "imports sync (use internal/xsync; sync/atomic is allowed)")
		}
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}}
	conf := types.Config{Error: func(error) {}, Importer: nil}
	_, _ = conf.Check(f.Name.Name, fset, []*ast.File{f}, info)

	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.GoStmt:
			add(x, "go statement (use xsync.Runtime.Go)")
		case *ast.SelectStmt:
			add(x, "select statement (use xsync)")
		case *ast.ChanType:
			add(x, "channel type (use xsync.Chan)")
		case *ast.SendStmt:
			add(x, "channel send (use xsync.Chan)")
		case *ast.UnaryExpr:
			if x.Op == token.ARROW {
				add(x, "channel receive (use xsync.Chan)")
			}
		case *ast.SelectorExpr:
			id, ok := x.X.(*ast.Ident)
			if !ok {
				return true
			}
			switch imports[id.Name] {
			case "time":
				if bannedTime[x.Sel.Name] {
					add(x, "time."+x.Sel.Name+" (use clock.Clock)")
				}
			case "context":
				if x.Sel.Name == "WithTimeout" || x.Sel.Name == "WithDeadline" {
					add(x, "context."+x.Sel.Name+" (uses the wall clock; use clock.Clock deadlines)")
				}
			}
		case *ast.RangeStmt:
			if tv, ok := info.Types[x.X]; ok && tv.Type != nil {
				if _, isMap := tv.Type.Underlying().(*types.Map); isMap {
					add(x, "range over a map (use xsync.SortedKeys)")
				}
			} else if isMapLiteralish(x.X) {
				add(x, "range over a map (use xsync.SortedKeys)")
			}
		}
		return true
	})
	return out, nil
}

func isMapLiteralish(e ast.Expr) bool {
	cl, ok := e.(*ast.CompositeLit)
	if !ok {
		return false
	}
	_, isMap := cl.Type.(*ast.MapType)
	return isMap
}

// isExcluded reports whether path is a package in Excluded, or a file inside one.
func isExcluded(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	rel = filepath.ToSlash(rel)
	for _, ex := range Excluded {
		ex = filepath.ToSlash(ex)
		if rel == ex || strings.HasPrefix(rel, ex+"/") {
			return true
		}
	}
	return false
}
