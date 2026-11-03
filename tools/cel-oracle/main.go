// Command celoracle is a test oracle for the Rust CEL checker: it compiles
// expressions with cel-go in the Kubernetes CEL environment
// (k8s.io/apiserver/pkg/cel/environment, release-1.35) and reports what the
// real checker, cost estimator and interpreter say.
//
// It is built only from the vendored Kubernetes sources (see build.sh), never
// shipped, and not part of any Rusternetes binary or the Cargo workspace.
//
// Protocol: one JSON object per line on stdin, one per line on stdout.
//
//	request:  {"expr": "...",
//	           "vars": {"name": "list(int)"},       // declared variable types
//	           "values": {"name": [1, 2]},          // optional, enables evaluation
//	           "kubeVersion": "1.35",               // default 1.35
//	           "envType": "new" | "stored",         // default "new"
//	           "libs": ["jsonpatch"]}               // extra libraries
//	response: {"ok": true, "errors": [], "outputType": "bool",
//	           "cost": {"min": 1, "max": 2},
//	           "evalResult": {"type": "bool", "value": "true"}}
//
// Type syntax: bool int uint double string bytes dyn null timestamp duration,
// list(T), map(K, V), optional(T).
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker"

	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/cel/environment"
	"k8s.io/apiserver/pkg/cel/library"
)

type request struct {
	Expr        string                     `json:"expr"`
	Vars        map[string]string          `json:"vars"`
	Values      map[string]json.RawMessage `json:"values"`
	KubeVersion string                     `json:"kubeVersion"`
	EnvType     string                     `json:"envType"`
	Libs        []string                   `json:"libs"`
}

type cost struct {
	Min uint64 `json:"min"`
	Max uint64 `json:"max"`
}

type evalResult struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type response struct {
	OK         bool        `json:"ok"`
	Errors     []string    `json:"errors"`
	OutputType string      `json:"outputType,omitempty"`
	Cost       *cost       `json:"cost,omitempty"`
	EvalResult *evalResult `json:"evalResult,omitempty"`
}

func main() {
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 1<<26)
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	enc := json.NewEncoder(out)
	for in.Scan() {
		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}
		var req request
		var resp response
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			resp = response{Errors: []string{"bad request: " + err.Error()}}
		} else {
			resp = handle(req)
		}
		if resp.Errors == nil {
			resp.Errors = []string{}
		}
		_ = enc.Encode(resp)
		out.Flush()
	}
}

func handle(req request) (resp response) {
	defer func() {
		if r := recover(); r != nil {
			resp = response{Errors: []string{fmt.Sprintf("oracle panic: %v", r)}}
		}
	}()
	ver, err := parseVersion(req.KubeVersion)
	if err != nil {
		return response{Errors: []string{err.Error()}}
	}
	envType := environment.NewExpressions
	if req.EnvType == "stored" {
		envType = environment.StoredExpressions
	}
	base, err := environment.MustBaseEnvSet(ver).Env(envType)
	if err != nil {
		return response{Errors: []string{err.Error()}}
	}
	var opts []cel.EnvOption
	for name, ts := range req.Vars {
		t, err := parseType(ts)
		if err != nil {
			return response{Errors: []string{fmt.Sprintf("variable %s: %v", name, err)}}
		}
		opts = append(opts, cel.Variable(name, t))
	}
	for _, lib := range req.Libs {
		switch lib {
		case "jsonpatch":
			// `hasPatchTypes` of plugin/cel/compile.go.
			opts = append(opts, library.JSONPatch())
		default:
			return response{Errors: []string{"unknown library " + lib}}
		}
	}
	env, err := base.Extend(opts...)
	if err != nil {
		return response{Errors: []string{err.Error()}}
	}
	ast, iss := env.Compile(req.Expr)
	if iss != nil && iss.Err() != nil {
		var msgs []string
		for _, e := range iss.Errors() {
			msgs = append(msgs, e.Message)
		}
		return response{Errors: msgs}
	}
	resp = response{OK: true, OutputType: ast.OutputType().String()}
	est, err := env.EstimateCost(ast, &library.CostEstimator{})
	if err == nil {
		resp.Cost = &cost{Min: est.Min, Max: est.Max}
	}
	if len(req.Values) > 0 {
		resp.EvalResult = evaluate(env, ast, req.Values)
	}
	return resp
}

func evaluate(env *cel.Env, ast *cel.Ast, raw map[string]json.RawMessage) *evalResult {
	prg, err := env.Program(ast)
	if err != nil {
		return &evalResult{Type: "error", Value: err.Error()}
	}
	act := map[string]any{}
	for k, v := range raw {
		var x any
		if err := json.Unmarshal(v, &x); err != nil {
			return &evalResult{Type: "error", Value: err.Error()}
		}
		act[k] = x
	}
	val, _, err := prg.Eval(act)
	if err != nil {
		return &evalResult{Type: "error", Value: err.Error()}
	}
	return &evalResult{Type: val.Type().TypeName(), Value: fmt.Sprintf("%v", val.Value())}
}

func parseVersion(s string) (*version.Version, error) {
	if s == "" {
		return version.MajorMinor(1, 35), nil
	}
	parts := strings.SplitN(s, ".", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("bad kubeVersion %q", s)
	}
	major, err1 := strconv.ParseUint(parts[0], 10, 32)
	minor, err2 := strconv.ParseUint(parts[1], 10, 32)
	if err1 != nil || err2 != nil {
		return nil, fmt.Errorf("bad kubeVersion %q", s)
	}
	return version.MajorMinor(uint(major), uint(minor)), nil
}

// parseType parses the small type syntax documented above.
func parseType(s string) (*cel.Type, error) {
	s = strings.TrimSpace(s)
	switch s {
	case "bool":
		return cel.BoolType, nil
	case "int":
		return cel.IntType, nil
	case "uint":
		return cel.UintType, nil
	case "double":
		return cel.DoubleType, nil
	case "string":
		return cel.StringType, nil
	case "bytes":
		return cel.BytesType, nil
	case "dyn":
		return cel.DynType, nil
	case "null":
		return cel.NullType, nil
	case "timestamp":
		return cel.TimestampType, nil
	case "duration":
		return cel.DurationType, nil
	}
	open := strings.Index(s, "(")
	if open < 0 || !strings.HasSuffix(s, ")") {
		return nil, fmt.Errorf("unknown type %q", s)
	}
	args, err := splitArgs(s[open+1 : len(s)-1])
	if err != nil {
		return nil, err
	}
	var ps []*cel.Type
	for _, a := range args {
		p, err := parseType(a)
		if err != nil {
			return nil, err
		}
		ps = append(ps, p)
	}
	switch s[:open] {
	case "list":
		if len(ps) == 1 {
			return cel.ListType(ps[0]), nil
		}
	case "map":
		if len(ps) == 2 {
			return cel.MapType(ps[0], ps[1]), nil
		}
	case "optional":
		if len(ps) == 1 {
			return cel.OptionalType(ps[0]), nil
		}
	}
	return nil, fmt.Errorf("unknown type %q", s)
}

func splitArgs(s string) ([]string, error) {
	var out []string
	depth, start := 0, 0
	for i, c := range s {
		switch c {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	if strings.TrimSpace(s[start:]) != "" {
		out = append(out, s[start:])
	}
	return out, nil
}

var _ = checker.UnknownCostEstimate
