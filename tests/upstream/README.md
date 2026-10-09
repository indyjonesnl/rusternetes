# Vendored upstream Kubernetes test oracles

Files under `k8s-<version>/` are copied verbatim from the upstream Kubernetes
repository (see `k8s-1.35/PROVENANCE` for the exact commit). They are golden
oracles for Go-compatibility tests:

- `api-testdata/`, `apiextensions-testdata/`: the `testdata/HEAD` JSON, YAML and
  protobuf (`.pb`) round-trip fixtures for every API type.
- `openapi-spec/swagger.json` and `openapi-spec/v3/`: the published OpenAPI v2
  and v3 specs.

**Never edit these files by hand.** Refresh with
`scripts/sync-upstream-testdata.sh [path-to-kubernetes-checkout]`, which also
rewrites `PROVENANCE`. They are marked `linguist-vendored`, `-diff` and `-text`
in `.gitattributes` so they are never normalised.

## License

Copied from Kubernetes, Copyright The Kubernetes Authors, licensed under the
Apache License 2.0. A copy of the license is in `LICENSE` in this directory.
