package protocol

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// stateTypes are the protocol types stored in KV as MessagePack (ADR-0017).
// They MUST be decoded with protocol.UnmarshalState, never json.Unmarshal.
//
// This list is curated rather than derived from "has a generated UnmarshalMsg",
// because msgp is generated for the *config* types too (SourceConfig,
// PipelineConfig, GlobalConfig, ...) and those are deliberately JSON in KV.
// Having msgp methods is therefore not the signal -- being written to a state
// key is. When a type joins that set, add it here.
var stateTypes = map[string]bool{
	"Checkpoint": true,
	"TableStats": true,
}

// stateKeyFuncs are the protocol constructors for KV keys whose values are
// MessagePack state. Anything Put under one of these must be encoded by
// protocol.MarshalState.
var stateKeyFuncs = map[string]bool{
	"TableStatsKey":        true,
	"EgressCheckpointKey":  true,
	"IngressCheckpointKey": true,
	"SourceWatermarkKey":   true,
}

// TestNoJSONDecodeOfKVStateValues is the executable form of the ADR-0017
// boundary. The original defect was `json.Unmarshal(entry.Value(), &st)` on
// msgp-written state under an `err == nil` guard: it failed on every real
// entry and silently skipped it, and nothing in the build objected.
//
// It matches on `<expr>.Value()` as the first argument -- the NATS KV entry
// accessor -- so it constrains only KV decoding. JSON-encoding a TableStats
// into an HTTP response stays legal, which is why a blanket ban on JSON for
// these types would be wrong.
func TestNoJSONDecodeOfKVStateValues(t *testing.T) {
	violations := scanProductionFuncs(t, findJSONStateDecodes)
	for _, v := range violations {
		t.Errorf("%s\n\tuse protocol.UnmarshalState instead -- see docs/decisions/0017-msgpack-for-state-json-for-config.md", v)
	}
}

// TestStateKeysAreWrittenWithMarshalState guards the *write* side of ADR-0017.
// TestNoJSONDecodeOfKVStateValues covers reads; this covers the half that
// actually caused the original split, where one writer used json.Marshal and
// two used MarshalMsg on the same key.
//
// It is an allowlist, not a ban on json.Marshal: a value Put under a state key
// must come from protocol.MarshalState. That catches a raw MarshalMsg or any
// future third encoder too, not just the JSON case that happened to bite.
func TestStateKeysAreWrittenWithMarshalState(t *testing.T) {
	violations := scanProductionFuncs(t, findNonMarshalStateWrites)
	for _, v := range violations {
		t.Errorf("%s\n\tstate keys must be encoded with protocol.MarshalState -- see docs/decisions/0017-msgpack-for-state-json-for-config.md", v)
	}
}

// funcChecker reports violations within a single function body. rel is the
// repo-relative path, used for reporting.
type funcChecker func(fn *ast.FuncDecl, fset *token.FileSet, rel string) []string

// scanProductionFuncs applies check to every function in the non-test,
// non-generated, non-vendored Go under internal/.
func scanProductionFuncs(t *testing.T, check funcChecker) []string {
	t.Helper()

	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}

	var violations []string
	fset := token.NewFileSet()

	err = filepath.WalkDir(filepath.Join(root, "internal"), func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			// Vendored code is not ours to hold to this rule.
			if d.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if !isProductionGoFile(d.Name()) {
			return nil
		}

		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		rel, _ := filepath.Rel(root, path)

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			violations = append(violations, check(fn, fset, rel)...)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	return violations
}

func isProductionGoFile(name string) bool {
	return strings.HasSuffix(name, ".go") &&
		!strings.HasSuffix(name, "_test.go") &&
		!strings.HasSuffix(name, "_gen.go")
}

// findJSONStateDecodes flags `json.Unmarshal(<entry>.Value(), &x)` where x is
// declared as a state type.
func findJSONStateDecodes(fn *ast.FuncDecl, fset *token.FileSet, rel string) []string {
	declared := localVarTypes(fn.Body)

	var out []string
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) != 2 {
			return true
		}
		if !isSelector(call.Fun, "json", "Unmarshal") || !isKVEntryValue(call.Args[0]) {
			return true
		}
		name := addressedIdent(call.Args[1])
		if name == "" {
			return true
		}
		if tn := declared[name]; stateTypes[tn] {
			out = append(out, fmt.Sprintf("%s:%d decodes a KV entry into protocol.%s with json.Unmarshal",
				rel, fset.Position(call.Pos()).Line, tn))
		}
		return true
	})
	return out
}

// findNonMarshalStateWrites flags a Put/Update/Create under a state key whose
// value did not come from protocol.MarshalState.
func findNonMarshalStateWrites(fn *ast.FuncDecl, fset *token.FileSet, rel string) []string {
	keyVars, valueSrc := localCallSources(fn.Body)

	var out []string
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) < 2 || !isKVWrite(call.Fun) {
			return true
		}
		keyFn := resolveKeyFunc(call.Args[0], keyVars)
		if keyFn == "" {
			return true
		}
		src := resolveValueSource(call.Args[1], valueSrc)
		if src == "MarshalState" {
			return true
		}
		out = append(out, fmt.Sprintf("%s:%d writes a %s value encoded by %q",
			rel, fset.Position(call.Pos()).Line, keyFn, orUnknown(src)))
		return true
	})
	return out
}

// localVarTypes maps ident -> type name for `var x protocol.T` declarations,
// which is how every decode target in this codebase is introduced.
func localVarTypes(body *ast.BlockStmt) map[string]string {
	declared := map[string]string{}
	ast.Inspect(body, func(n ast.Node) bool {
		gd, ok := n.(*ast.GenDecl)
		if !ok || gd.Tok != token.VAR {
			return true
		}
		for _, spec := range gd.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok || vs.Type == nil {
				continue
			}
			if tn := typeName(vs.Type); tn != "" {
				for _, id := range vs.Names {
					declared[id.Name] = tn
				}
			}
		}
		return true
	})
	return declared
}

// localCallSources splits `x := f(...)` assignments into idents holding a
// state-key constructor and idents holding some other call's result.
func localCallSources(body *ast.BlockStmt) (keyVars, valueSrc map[string]string) {
	keyVars, valueSrc = map[string]string{}, map[string]string{}
	ast.Inspect(body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Rhs) != 1 {
			return true
		}
		call, ok := as.Rhs[0].(*ast.CallExpr)
		if !ok {
			return true
		}
		callee := calleeName(call.Fun)
		for _, lhs := range as.Lhs {
			id, ok := lhs.(*ast.Ident)
			if !ok || id.Name == "_" {
				continue
			}
			if stateKeyFuncs[callee] {
				keyVars[id.Name] = callee
			} else {
				valueSrc[id.Name] = callee
			}
		}
		return true
	})
	return keyVars, valueSrc
}

func isKVWrite(fun ast.Expr) bool {
	sel, ok := fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	switch sel.Sel.Name {
	case "Put", "Update", "Create":
		return true
	}
	return false
}

func resolveKeyFunc(arg ast.Expr, keyVars map[string]string) string {
	switch k := arg.(type) {
	case *ast.Ident:
		return keyVars[k.Name]
	case *ast.CallExpr:
		if c := calleeName(k.Fun); stateKeyFuncs[c] {
			return c
		}
	}
	return ""
}

func resolveValueSource(arg ast.Expr, valueSrc map[string]string) string {
	switch v := arg.(type) {
	case *ast.Ident:
		return valueSrc[v.Name]
	case *ast.CallExpr:
		return calleeName(v.Fun)
	}
	return ""
}

// addressedIdent returns the name in `&x`, or "".
func addressedIdent(e ast.Expr) string {
	unary, ok := e.(*ast.UnaryExpr)
	if !ok || unary.Op != token.AND {
		return ""
	}
	id, ok := unary.X.(*ast.Ident)
	if !ok {
		return ""
	}
	return id.Name
}

// isKVEntryValue reports whether e is a call of the form `<expr>.Value()`,
// the nats.KeyValueEntry accessor.
func isKVEntryValue(e ast.Expr) bool {
	call, ok := e.(*ast.CallExpr)
	if !ok || len(call.Args) != 0 {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "Value"
}

func isSelector(e ast.Expr, pkg, name string) bool {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != name {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == pkg
}

// calleeName renders `f`, `pkg.F` and `x.y.F` as the final identifier.
func calleeName(e ast.Expr) string {
	switch f := e.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		return f.Sel.Name
	}
	return ""
}

// typeName renders `T` and `protocol.T` as "T"; anything else as "".
func typeName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.SelectorExpr:
		return t.Sel.Name
	}
	return ""
}

func orUnknown(s string) string {
	if s == "" {
		return "an unrecognised expression"
	}
	return s
}
