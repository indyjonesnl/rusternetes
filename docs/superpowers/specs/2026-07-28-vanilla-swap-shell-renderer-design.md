# Vanilla-Swap Shell Renderer Design

## Context

The vanilla-swap harness renders YAML recipe templates with `envsubst`, but the
self-hosted conformance runner neither provides that command nor installs
gettext. Both the workflow prerequisite check and `vs_require_tools` therefore
pass before a template-based swap fails after the baseline cluster is ready.

## Decision

Remove the `envsubst` dependency. Add a Bash-native helper that reads a recipe's
existing `template: |` block and replaces only the explicitly named variables
provided by its caller. The static-pod path will allow `VS_IMAGE`; the
kube-proxy DaemonSet path will allow `VS_IMAGE`, `VS_APISERVER_URL`,
`VS_CLUSTER_CIDR`, and `VS_NODEPORT_RANGE`.

The helper will use indirect Bash expansion rather than `eval`. Unlisted shell
references remain unchanged, matching `envsubst` when it receives an explicit
variable allow-list. A requested variable that is unset is an error, preventing
the harness from applying a partially rendered manifest.

Installing gettext in the ARC image was rejected because it couples this
repository's harness to runner-image rollout. Downloading another renderer in
the workflow was rejected because it adds an unnecessary network and supply
chain dependency.

## Code Shape

`scripts/vanilla-swap-common.sh` will expose:

```text
vs_render_recipe_template <recipe-path> <variable-name>...
```

It will reuse `vs_recipe_template`, render exact `${NAME}` tokens, and write the
result to stdout for the existing pipes into `docker exec` or `kubectl apply`.
The two current `envsubst` pipelines will call this helper instead.

## Verification

`scripts/vanilla-swap-common-test.sh` will create a representative recipe and
invoke the renderer with a restricted `PATH` containing `awk` but no
`envsubst`. It will verify values containing URL slashes and CIDR notation,
confirm that explicitly named placeholders are replaced, and confirm that an
unlisted placeholder remains untouched. A second assertion will verify that an
unset requested variable is rejected.

Verification must run `bash scripts/vanilla-swap-common-test.sh`,
`bash scripts/vanilla-swap-guard-test.sh`, and
`bash scripts/tests/test-vanilla-swap-workflows-sync.sh`. A GitHub Actions rerun
remains the end-to-end proof because it exercises the ARC runner and a real kind
cluster.
