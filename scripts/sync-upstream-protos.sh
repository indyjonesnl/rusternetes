#!/usr/bin/env bash
# Fetches upstream Kubernetes .proto files for the schema parity test.
#
# Usage:
#   bash scripts/sync-upstream-protos.sh                   # default tag release-1.35
#   bash scripts/sync-upstream-protos.sh release-1.36      # override
#
# Files are written verbatim under
#   crates/api-server/proto/upstream/v1.35/<upstream-path>
# mirroring the upstream layout so that subsequent re-syncs are diff-clean.
#
# When bumping the pinned tag, also bump the destination v1.35 directory name
# (and update crates/api-server/proto/upstream/README.md).

set -euo pipefail

TAG="${1:-release-1.35}"
DEST_VERSION="${DEST_VERSION:-v1.35}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST_ROOT="$ROOT/crates/api-server/proto/upstream/$DEST_VERSION"

# Every generated.proto of the release (k8s.io/api/*, apimachinery, apiextensions,
# kube-aggregator, apiserver audit, metrics) except the testapigroup/example
# fixture groups. Only the files listed in PROTO_FILES of
# crates/api-server/tests/it/protobuf_schema_parity_upstream.rs are a HARD gate;
# the rest feed the warning-only wire-format coverage report
# (scripts/wire-format-coverage.sh).
#
# Each entry is the upstream path *relative to* https://raw.githubusercontent.com/kubernetes/kubernetes/<TAG>/staging/src/
FILES=(
    "k8s.io/api/admission/v1/generated.proto"
    "k8s.io/api/admission/v1beta1/generated.proto"
    "k8s.io/api/admissionregistration/v1/generated.proto"
    "k8s.io/api/admissionregistration/v1alpha1/generated.proto"
    "k8s.io/api/admissionregistration/v1beta1/generated.proto"
    "k8s.io/api/apidiscovery/v2/generated.proto"
    "k8s.io/api/apidiscovery/v2beta1/generated.proto"
    "k8s.io/api/apiserverinternal/v1alpha1/generated.proto"
    "k8s.io/api/apps/v1/generated.proto"
    "k8s.io/api/apps/v1beta1/generated.proto"
    "k8s.io/api/apps/v1beta2/generated.proto"
    "k8s.io/api/authentication/v1/generated.proto"
    "k8s.io/api/authentication/v1alpha1/generated.proto"
    "k8s.io/api/authentication/v1beta1/generated.proto"
    "k8s.io/api/authorization/v1/generated.proto"
    "k8s.io/api/authorization/v1beta1/generated.proto"
    "k8s.io/api/autoscaling/v1/generated.proto"
    "k8s.io/api/autoscaling/v2/generated.proto"
    "k8s.io/api/autoscaling/v2beta1/generated.proto"
    "k8s.io/api/autoscaling/v2beta2/generated.proto"
    "k8s.io/api/batch/v1/generated.proto"
    "k8s.io/api/batch/v1beta1/generated.proto"
    "k8s.io/api/certificates/v1/generated.proto"
    "k8s.io/api/certificates/v1alpha1/generated.proto"
    "k8s.io/api/certificates/v1beta1/generated.proto"
    "k8s.io/api/coordination/v1/generated.proto"
    "k8s.io/api/coordination/v1alpha2/generated.proto"
    "k8s.io/api/coordination/v1beta1/generated.proto"
    "k8s.io/api/core/v1/generated.proto"
    "k8s.io/api/discovery/v1/generated.proto"
    "k8s.io/api/discovery/v1beta1/generated.proto"
    "k8s.io/api/events/v1/generated.proto"
    "k8s.io/api/events/v1beta1/generated.proto"
    "k8s.io/api/extensions/v1beta1/generated.proto"
    "k8s.io/api/flowcontrol/v1/generated.proto"
    "k8s.io/api/flowcontrol/v1beta1/generated.proto"
    "k8s.io/api/flowcontrol/v1beta2/generated.proto"
    "k8s.io/api/flowcontrol/v1beta3/generated.proto"
    "k8s.io/api/imagepolicy/v1alpha1/generated.proto"
    "k8s.io/api/networking/v1/generated.proto"
    "k8s.io/api/networking/v1beta1/generated.proto"
    "k8s.io/api/node/v1/generated.proto"
    "k8s.io/api/node/v1alpha1/generated.proto"
    "k8s.io/api/node/v1beta1/generated.proto"
    "k8s.io/api/policy/v1/generated.proto"
    "k8s.io/api/policy/v1beta1/generated.proto"
    "k8s.io/api/rbac/v1/generated.proto"
    "k8s.io/api/rbac/v1alpha1/generated.proto"
    "k8s.io/api/rbac/v1beta1/generated.proto"
    "k8s.io/api/resource/v1/generated.proto"
    "k8s.io/api/resource/v1alpha3/generated.proto"
    "k8s.io/api/resource/v1beta1/generated.proto"
    "k8s.io/api/resource/v1beta2/generated.proto"
    "k8s.io/api/scheduling/v1/generated.proto"
    "k8s.io/api/scheduling/v1alpha1/generated.proto"
    "k8s.io/api/scheduling/v1beta1/generated.proto"
    "k8s.io/api/storage/v1/generated.proto"
    "k8s.io/api/storage/v1alpha1/generated.proto"
    "k8s.io/api/storage/v1beta1/generated.proto"
    "k8s.io/api/storagemigration/v1beta1/generated.proto"
    "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1/generated.proto"
    "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1/generated.proto"
    "k8s.io/apimachinery/pkg/api/resource/generated.proto"
    "k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto"
    "k8s.io/apimachinery/pkg/apis/meta/v1beta1/generated.proto"
    "k8s.io/apimachinery/pkg/runtime/generated.proto"
    "k8s.io/apimachinery/pkg/runtime/schema/generated.proto"
    "k8s.io/apimachinery/pkg/util/intstr/generated.proto"
    "k8s.io/apiserver/pkg/apis/audit/v1/generated.proto"
    "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1/generated.proto"
    "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1beta1/generated.proto"
    "k8s.io/metrics/pkg/apis/custom_metrics/v1beta1/generated.proto"
    "k8s.io/metrics/pkg/apis/custom_metrics/v1beta2/generated.proto"
    "k8s.io/metrics/pkg/apis/external_metrics/v1beta1/generated.proto"
    "k8s.io/metrics/pkg/apis/metrics/v1alpha1/generated.proto"
    "k8s.io/metrics/pkg/apis/metrics/v1beta1/generated.proto"
)

# Optional: copy from a local checkout instead of fetching, e.g.
#   UPSTREAM_CHECKOUT=../kubernetes bash scripts/sync-upstream-protos.sh
# (the checkout must be on the matching release branch).
UPSTREAM_CHECKOUT="${UPSTREAM_CHECKOUT:-}"

BASE="https://raw.githubusercontent.com/kubernetes/kubernetes/${TAG}/staging/src"

echo "Syncing upstream Kubernetes .proto files from tag '${TAG}' into ${DEST_ROOT}"

for rel in "${FILES[@]}"; do
    url="${BASE}/${rel}"
    out="${DEST_ROOT}/${rel}"
    mkdir -p "$(dirname "$out")"
    echo "  fetch ${rel}"
    if [ -n "$UPSTREAM_CHECKOUT" ]; then
        cp "${UPSTREAM_CHECKOUT}/staging/src/${rel}" "${out}"
    else
        curl --fail --silent --show-error -L "${url}" -o "${out}"
    fi
done

echo "Done. ${#FILES[@]} file(s) written under ${DEST_ROOT}."
