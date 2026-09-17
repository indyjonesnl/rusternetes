//! Port of `pkg/volume/util/types/types.go` — types used only by the volume
//! components, plus `v1.UniqueVolumeName`, which lives in the API package
//! upstream but is only ever produced and consumed here.

use std::fmt;

/// Port of `types.UniquePodName` (`pkg/volume/util/types/types.go:33`):
///
/// ```go
/// // UniquePodName defines the type to key pods off of
/// type UniquePodName types.UID
/// ```
///
/// A distinct named type over the pod UID, not the UID itself — the volume
/// caches key on it and must not silently accept a pod name, namespace/name
/// pair or any other string. Built only by
/// [`super::get_unique_pod_name`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UniquePodName(pub String);

impl fmt::Display for UniquePodName {
    /// Go formats a `UniquePodName` with `%v` (`pkg/volume/util/util.go:271`),
    /// which for a string-kinded named type prints the bare string.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Port of `v1.UniqueVolumeName`
/// (`staging/src/k8s.io/api/core/v1/types.go:6774`):
///
/// ```go
/// type UniqueVolumeName string
/// ```
///
/// The key of the volume-manager caches. Its two construction rules —
/// plugin-scoped and pod-scoped — are in
/// [`super::get_unique_volume_name_from_spec`] and
/// [`super::get_unique_volume_name_from_spec_with_pod`]; which one applies is
/// decided by the caller, and getting that backwards silently breaks
/// multi-pod volume sharing.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UniqueVolumeName(pub String);

impl fmt::Display for UniqueVolumeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
