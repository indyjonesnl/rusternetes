//! The one home for serde helpers that bridge Go's `encoding/json` zero-value
//! and `null` handling, and `metav1.Time` marshalling, to Rust (#2953).
//!
//! * [`null_default`]: Go decodes `null` as a
//!   no-op, leaving the zero value.
//! * [`empty_as_none`]: an unset enum/string-typed field some clients send as `""`.
//! * [`bytes_b64`]: Go `[]byte`, base64 on the wire, `null` when nil.
//! * the `k8s_time` family: `metav1.Time` / `metav1.MicroTime` marshalling
//!   (`staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/time.go`,
//!   `micro_time.go`), plus `k8s_float` and `sorted_opt_map`.
//!
//! `crate::types` and `crate::resources::serde_helpers` re-export these under
//! their historical paths so existing `with = "k8s_time::..."` sites are
//! unchanged.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer};

/// Deserialize `null` (or a missing field, with `#[serde(default)]`) as
/// `T::default()`, as Go's `encoding/json` does (`null` is a no-op on decode).
/// Use with `#[serde(deserialize_with = "crate::serde_helpers::null_default")]`.
pub fn null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

/// Deserialize an `Option<T>` where the JSON value may be `null`, missing,
/// or the empty string. Empty strings are coerced to `None` before falling
/// through to `T`'s own deserializer.
///
/// Kubernetes API clients sometimes serialize unset enum-typed fields as the
/// empty string instead of omitting them. Upstream Go decoders accept the
/// zero-value; strict Rust serde enums reject it as an unknown variant.
pub fn empty_as_none<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) if s.is_empty() => Ok(None),
        Some(v) => T::deserialize(v)
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

/// Deserialize half of the canonical mechanism for a Go plain (non-pointer)
/// scalar with `json:",omitempty"` (#2955): such a field is absent on the wire
/// when zero, so Go cannot tell `0`/`false` from unset. We model it as
/// `Option<T>` and fold the zero value to `None` on decode, so `Some(0)` is
/// unrepresentable after decoding and `None` vs `Some(0)` comparisons (e.g.
/// `generation != old.generation`) agree with Go.
///
/// Use as `#[serde(default, deserialize_with = "zero_as_none",
/// skip_serializing_if = "is_none_or_zero")]`. A Go *pointer* scalar
/// (`*int32`) is different: `Some(0)` is meaningful there, keep a plain
/// `Option<T>`. A plain scalar without `omitempty` is a plain `T`.
pub fn zero_as_none<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Default + PartialEq + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.filter(|v| *v != T::default()))
}

/// Serialize half of the mechanism: Go's `omitempty` drops a zero scalar, so
/// both `None` and `Some(zero)` are omitted.
pub fn is_none_or_zero<T: Default + PartialEq>(value: &Option<T>) -> bool {
    value.as_ref().is_none_or(|v| *v == T::default())
}

/// Historical name of [`empty_as_none`].
pub use self::empty_as_none as empty_string_as_none;

/// Serde for Go `[]byte` fields: standard base64 on the wire; a nil slice
/// marshals as `null`, which decodes to an empty `Vec`.
/// Use with `#[serde(with = "crate::serde_helpers::bytes_b64")]` plus
/// `#[serde(default)]`.
pub mod bytes_b64 {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(data: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            data,
        ))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        // Go marshals a nil `[]byte` as `null`.
        let Some(encoded) = Option::<String>::deserialize(d)? else {
            return Ok(Vec::new());
        };
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
            .map_err(serde::de::Error::custom)
    }
}

/// Go's zero `time.Time` (`0001-01-01T00:00:00Z`), the value a required
/// `metav1.Time` / `metav1.MicroTime` holds when unset.
pub fn go_zero_time() -> DateTime<Utc> {
    use chrono::TimeZone;
    Utc.with_ymd_and_hms(1, 1, 1, 0, 0, 0).unwrap()
}

/// `Time.IsZero()`.
pub(crate) fn is_go_zero_time(t: &DateTime<Utc>) -> bool {
    *t == go_zero_time()
}

pub mod sorted_opt_map {
    //! Serialize an `Option<HashMap<String, String>>` with keys sorted, as
    //! Go's `encoding/json` does for maps. A `HashMap` otherwise emits in
    //! random order, which makes a marshalled bad value
    //! (`BadValue::marshal`, #2190) nondeterministic.
    use serde::{Serialize, Serializer};
    use std::collections::{BTreeMap, HashMap};

    pub fn serialize<S: Serializer>(
        v: &Option<HashMap<String, String>>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        v.as_ref()
            .map(|m| m.iter().collect::<BTreeMap<_, _>>())
            .serialize(s)
    }
}

/// Serde for `metav1.Time` fields (creationTimestamp, deletionTimestamp, ...).
///
/// Upstream `metav1.Time.MarshalJSON` formats as RFC3339 with **second**
/// precision (`time.RFC3339`, no fractional seconds), e.g. `2026-05-31T08:00:43Z`.
/// We emit the same so responses match k8s exactly (sub-second precision is a
/// non-conformance and breaks proto Timestamp round-tripping). Deserialize is
/// lenient and accepts timestamps with or without fractional seconds.
pub mod k8s_time {
    use chrono::{DateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(date: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match date {
            Some(dt) => serializer.serialize_str(&dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<String> = Option::deserialize(deserializer)?;
        match opt {
            Some(s) => {
                if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
                    return Ok(Some(dt.with_timezone(&Utc)));
                }
                s.parse::<DateTime<Utc>>()
                    .map(Some)
                    .map_err(serde::de::Error::custom)
            }
            None => Ok(None),
        }
    }
}

/// Serde for RFC3339 timestamp fields that are stored as `Option<String>`
/// rather than a typed `DateTime` (e.g. `ContainerState` `startedAt` /
/// `finishedAt`, which the kubelet writes as preformatted strings). Normalises
/// to whole-second precision on serialize so JSON matches the protobuf /
/// typed-client representation (`metav1.Time` is second-precision). A value
/// that does not parse as RFC3339 is emitted unchanged.
pub mod k8s_time_str {
    use chrono::{DateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Option<String>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(raw) => {
                let normalized = DateTime::parse_from_rfc3339(raw)
                    .map(|dt| {
                        dt.with_timezone(&Utc)
                            .format("%Y-%m-%dT%H:%M:%SZ")
                            .to_string()
                    })
                    .unwrap_or_else(|_| raw.clone());
                serializer.serialize_str(&normalized)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<String>::deserialize(deserializer)
    }
}

/// [`k8s_time`] for a field that is not an `Option` — upstream's `metav1.Time` is
/// a value type, so `job.status.startTime`-style fields on the Go side are
/// non-pointer and always present on the wire. Same output format as
/// [`k8s_time`]; separate module only because serde's `with` must match the
/// field's exact type.
pub mod k8s_time_required {
    use chrono::{DateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // `Time.MarshalJSON` / `MicroTime.MarshalJSON` (time.go:162-166,
        // micro_time.go:175-178): the zero time encodes as `null`.
        if super::is_go_zero_time(date) {
            return serializer.serialize_none();
        }
        serializer.serialize_str(&date.format("%Y-%m-%dT%H:%M:%SZ").to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        // `Time.UnmarshalJSON` / `MicroTime.UnmarshalJSON` (time.go:100-104,
        // micro_time.go:113-117): `null` is the zero time, not an error.
        let Some(s) = Option::<String>::deserialize(deserializer)? else {
            return Ok(super::go_zero_time());
        };
        if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
            return Ok(dt.with_timezone(&Utc));
        }
        s.parse::<DateTime<Utc>>().map_err(serde::de::Error::custom)
    }
}

/// Serde for `metav1.MicroTime` fields.
///
/// Upstream `MicroTime.MarshalJSON` formats with `RFC3339Micro`
/// (`staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/micro_time.go:26,181`:
/// `"2006-01-02T15:04:05.000000Z07:00"`), i.e. **exactly six** fractional
/// digits — not chrono's variable-width default, and not the second precision
/// a `metav1.Time` gets. Deserialize is lenient and accepts a timestamp with
/// or without a fractional part, mirroring `MicroTime.UnmarshalJSON`.
pub mod k8s_micro_time {
    use chrono::{DateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(date: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match date {
            Some(dt) => serializer.serialize_str(&dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<String> = Option::deserialize(deserializer)?;
        match opt {
            Some(s) => {
                if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
                    return Ok(Some(dt.with_timezone(&Utc)));
                }
                s.parse::<DateTime<Utc>>()
                    .map(Some)
                    .map_err(serde::de::Error::custom)
            }
            None => Ok(None),
        }
    }
}

/// [`k8s_micro_time`] for a non-`Option` field (audit's
/// `requestReceivedTimestamp` / `stageTimestamp`, both `metav1.MicroTime` in
/// `staging/src/k8s.io/apiserver/pkg/apis/audit/types.go:140,142`).
pub mod k8s_micro_time_required {
    use chrono::{DateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // `Time.MarshalJSON` / `MicroTime.MarshalJSON` (time.go:162-166,
        // micro_time.go:175-178): the zero time encodes as `null`.
        if super::is_go_zero_time(date) {
            return serializer.serialize_none();
        }
        serializer.serialize_str(&date.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        // `Time.UnmarshalJSON` / `MicroTime.UnmarshalJSON` (time.go:100-104,
        // micro_time.go:113-117): `null` is the zero time, not an error.
        let Some(s) = Option::<String>::deserialize(deserializer)? else {
            return Ok(super::go_zero_time());
        };
        if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
            return Ok(dt.with_timezone(&Utc));
        }
        s.parse::<DateTime<Utc>>().map_err(serde::de::Error::custom)
    }
}

/// Serde for the `float64` fields of an OpenAPI schema (`maximum`, `minimum`,
/// `multipleOf`), matching how Go writes them.
///
/// Upstream declares these as `*float64`
/// (`staging/src/k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1/types_jsonschema.go`),
/// and Go's `encoding/json` writes a float with
/// `strconv.AppendFloat(b, f, fmt, -1, 64)` — the *shortest* representation
/// that round-trips (`encoding/json/encode.go`, `floatEncoder.encode`). For an
/// integral value that is `10`, not `10.0`.
///
/// serde_json always writes an `f64` with a fractional part, so a CRD created
/// with `maximum: 10` read back as `maximum: 10.0`: a different JSON token for
/// the same schema, which shows up in `kubectl get crd -o yaml` and makes a
/// re-apply diff against itself. Emitting the integral case as an integer
/// restores Go's output. Found by the wide serialization sweep
/// (`crates/api-server/tests/it/conformance_wide_serialization_sweep_test.rs`).
pub mod k8s_float {
    use serde::{self, Deserialize, Deserializer, Serializer};

    /// The largest magnitude for which every integer is exactly representable
    /// in an `f64` (2^53). Above it `fract() == 0` no longer implies the value
    /// is the integer it looks like, so defer to the float form — which is
    /// also what Go's shortest-round-trip encoder produces there.
    const MAX_EXACT_INT: f64 = 9_007_199_254_740_992.0;

    pub fn serialize<S>(value: &Option<f64>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(f) if f.is_finite() && f.fract() == 0.0 && f.abs() <= MAX_EXACT_INT => {
                serializer.serialize_i64(*f as i64)
            }
            Some(f) => serializer.serialize_f64(*f),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::deserialize(deserializer)
    }
}

/// Serde for `map[string]metav1.Time` fields (e.g. `PodDisruptionBudgetStatus.disruptedPods`).
///
/// Upstream `metav1.Time` normally marshals to an RFC3339 string, but apply
/// configurations and some clients emit a *zero* `metav1.Time` as an empty JSON
/// object `{}` (and a zero time marshals to `null`). chrono's `DateTime`
/// deserializer rejects both the object form and `null`, which made the
/// PodDisruptionBudget `UpdateStatus` path reject otherwise-valid requests with
/// a 422. This module accepts a string, an object, or null per value, mirroring
/// `metav1.Time.UnmarshalJSON`'s leniency.
pub mod k8s_time_map {
    use chrono::{DateTime, Utc};
    use serde::de::{MapAccess, Visitor};
    use serde::{self, ser::SerializeMap, Deserialize, Deserializer, Serializer};
    use std::collections::HashMap;
    use std::fmt;

    pub fn serialize<S>(
        map: &Option<HashMap<String, DateTime<Utc>>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match map {
            Some(m) => {
                let mut ser = serializer.serialize_map(Some(m.len()))?;
                for (k, v) in m {
                    ser.serialize_entry(k, &v.format("%Y-%m-%dT%H:%M:%SZ").to_string())?;
                }
                ser.end()
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<HashMap<String, DateTime<Utc>>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        /// Accepts a string (RFC3339), an object (zero `metav1.Time` -> now), or
        /// null (zero time -> now), matching upstream leniency.
        struct TimeValue(DateTime<Utc>);

        impl<'de> serde::Deserialize<'de> for TimeValue {
            fn deserialize<D>(d: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct V;
                impl<'de> Visitor<'de> for V {
                    type Value = DateTime<Utc>;
                    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                        f.write_str("an RFC3339 time string, an object, or null")
                    }
                    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        DateTime::parse_from_rfc3339(s)
                            .map(|dt| dt.with_timezone(&Utc))
                            .or_else(|_| s.parse::<DateTime<Utc>>())
                            .map_err(serde::de::Error::custom)
                    }
                    fn visit_unit<E>(self) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(Utc::now())
                    }
                    fn visit_none<E>(self) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(Utc::now())
                    }
                    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                    where
                        A: MapAccess<'de>,
                    {
                        // Drain the (empty) object form of a zero metav1.Time.
                        while map
                            .next_entry::<serde::de::IgnoredAny, serde::de::IgnoredAny>()?
                            .is_some()
                        {}
                        Ok(Utc::now())
                    }
                }
                d.deserialize_any(V).map(TimeValue)
            }
        }

        let opt: Option<HashMap<String, TimeValue>> =
            Option::deserialize(deserializer).map_err(serde::de::Error::custom)?;
        Ok(opt.map(|m| m.into_iter().map(|(k, v)| (k, v.0)).collect()))
    }
}
