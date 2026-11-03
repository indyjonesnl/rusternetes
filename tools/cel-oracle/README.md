# cel-oracle

Dev/CI-only Go harness that runs cel-go (checker, cost estimator, interpreter)
in the Kubernetes CEL environment (`k8s.io/apiserver/pkg/cel/environment`,
release-1.35) so the Rust checker (`crates/cel-checker`) can be compared with
the real thing.

- Built only from a local Kubernetes checkout's vendored sources (`GOPROXY=off`,
  `-mod=vendor`); nothing is downloaded.
- Never shipped, never linked into a Rusternetes binary, not part of the Cargo
  workspace or the default `cargo test`.

```bash
# build (set GOTOOLCHAIN=go1.25.x if the system Go is older than the checkout needs)
K8S_SRC=/path/to/kubernetes tools/cel-oracle/build.sh /tmp/celoracle

# regenerate crates/cel-checker/tests/corpus/goldens.jsonl from requests.jsonl
CEL_ORACLE=/tmp/celoracle tools/cel-oracle/gen-goldens.sh

# check that the checked-in goldens are what the oracle says today
CEL_ORACLE=/tmp/celoracle cargo nextest run -p rusternetes-cel-checker --run-ignored only
```

Normal test runs compare the Rust checker with the checked-in goldens
(`goldens::checker_matches_goldens`) and need no Go. Divergences that are not
fixed yet are listed, with reasons, in `KNOWN_DIVERGENCES` in
`crates/cel-checker/tests/it/goldens.rs`.

Protocol: one JSON object per line, see the header of `main.go`. A failed
compile also returns the `common.Location` of each issue (`locations`, parallel
to `errors`) and `Issues.String()` (`display`); the golden test compares them.
