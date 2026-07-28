# Remove Probe Termination Grace Period Target

## Context

The dedicated `probe-termination-grace-period` conformance target uses the
focus regex `\[Feature:ProbeTerminationGracePeriod\]`. The Kubernetes v1.35.0
test inventory has no spec carrying that tag, so its scheduled workflow starts
a cluster and then selects zero of 7,348 specs. The broader `sig-node` target
continues to exercise node probe behavior.

## Design

Remove the `probe-termination-grace-period` entry from
`ci/conformance/targets.json` and delete its generated
`.github/workflows/conformance-probe-termination-grace-period.yml` caller.
Do not change the reusable target runner, workflow generator, other feature
targets, or the `sig-node` target.

This removes both scheduled and manually dispatched dedicated runs for the
obsolete target. Existing generic target infrastructure remains unchanged.

## Verification

Run the manifest validation, generated-workflow synchronization, target
coverage, and workflow-generation tests. In particular, the workflow-sync
test must confirm that every remaining manifest entry has a matching generated
workflow and that no stale generated probe workflow remains.

