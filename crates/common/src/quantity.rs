//! `Quantity` newtype matching Kubernetes `apimachinery`
//! `pkg/api/resource/quantity.go`.
//!
//! Provides upstream-parity parsing and canonical-form encoding for
//! resource quantity values such as `"100m"`, `"1Gi"`, `"2.5e3"`.
//!
//! Internal representation is `mantissa * 10^scale` for all three
//! formats. `BinarySI` inputs are converted to that representation on
//! parse (multiplying mantissa by `2^suffix_exp`) and re-encoded by
//! factoring out powers of 1024 on canonicalisation.

use std::fmt;

/// The chosen output format for a [`Quantity`]. Mirrors upstream
/// `resource.Format`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Format {
    /// `n`, `u`, `m`, `""`, `k`, `M`, `G`, `T`, `P`, `E` (base 10).
    DecimalSI,
    /// `Ki`, `Mi`, `Gi`, `Ti`, `Pi`, `Ei` (base 2, exponents are
    /// multiples of 10 with suffixes spaced every factor of 1024).
    BinarySI,
    /// `e<int>` / `E<int>` with arbitrary signed integer exponent.
    DecimalExponent,
}

/// Parsed resource quantity. Construct via [`Quantity::parse`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quantity {
    pub mantissa: i128,
    pub scale: i32,
    pub format: Format,
}

/// Error returned by [`Quantity::parse`] for any input that upstream
/// `resource.ParseQuantity` rejects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseQuantityError(String);

impl ParseQuantityError {
    fn new(message: impl Into<String>) -> Self {
        ParseQuantityError(message.into())
    }
}

impl fmt::Display for ParseQuantityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ParseQuantityError {}

impl Quantity {
    /// Parse a quantity string. Rejects empty input, whitespace,
    /// unknown suffixes, mixed suffix letters and bare suffixes with
    /// no mantissa.
    pub fn parse(input: &str) -> Result<Quantity, ParseQuantityError> {
        if input.is_empty() {
            return Err(ParseQuantityError::new(
                "quantities must match the regular expression '^[+-]?(\\d+(\\.\\d*)?|\\.\\d+)([eE][+-]?\\d+|[numkMGTPE]|Ki|Mi|Gi|Ti|Pi|Ei)?$': empty string",
            ));
        }
        let bytes = input.as_bytes();
        let mut pos = 0usize;

        // Optional sign.
        let mut sign: i128 = 1;
        match bytes[pos] {
            b'+' => pos += 1,
            b'-' => {
                sign = -1;
                pos += 1;
            }
            _ => {}
        }

        // Whole-digit run.
        let whole_start = pos;
        while pos < bytes.len() && bytes[pos].is_ascii_digit() {
            pos += 1;
        }
        let whole_end = pos;

        // Optional fractional part introduced by `.`.
        let mut frac_start = pos;
        let mut frac_end = pos;
        if pos < bytes.len() && bytes[pos] == b'.' {
            pos += 1;
            frac_start = pos;
            while pos < bytes.len() && bytes[pos].is_ascii_digit() {
                pos += 1;
            }
            frac_end = pos;
        }

        if whole_start == whole_end && frac_start == frac_end {
            return Err(ParseQuantityError::new(format!(
                "quantity must have at least one digit: {input:?}"
            )));
        }

        let whole = &input[whole_start..whole_end];
        let frac = &input[frac_start..frac_end];
        let suffix = &input[pos..];

        // Build mantissa from concatenated whole+frac digits and parse
        // as `i128`. Empty whole becomes `0` to support `".5"` shape
        // (though upstream rejects bare `".5i"`; the suffix check
        // below catches that).
        let mut combined = String::with_capacity(whole.len() + frac.len());
        if whole.is_empty() {
            combined.push('0');
        } else {
            combined.push_str(whole);
        }
        combined.push_str(frac);
        let mut mantissa = combined.parse::<i128>().map_err(|_| {
            ParseQuantityError::new(format!("quantity mantissa overflows i128: {input:?}"))
        })?;
        mantissa *= sign;
        let mut scale = -(frac.len() as i32);

        let kind = interpret_suffix(suffix).ok_or_else(|| {
            ParseQuantityError::new(format!("unable to parse quantity suffix in {input:?}"))
        })?;
        let format = match kind {
            SuffixKind::DecimalSI(e) => {
                scale = scale.checked_add(e).ok_or_else(|| {
                    ParseQuantityError::new(format!("quantity scale overflow: {input:?}"))
                })?;
                Format::DecimalSI
            }
            SuffixKind::DecimalExponent(e) => {
                scale = scale.checked_add(e).ok_or_else(|| {
                    ParseQuantityError::new(format!("quantity scale overflow: {input:?}"))
                })?;
                Format::DecimalExponent
            }
            SuffixKind::BinarySI(e) => {
                if !(0..=120).contains(&e) {
                    return Err(ParseQuantityError::new(format!(
                        "invalid binary exponent in {input:?}"
                    )));
                }
                let mul = 1i128.checked_shl(e as u32).ok_or_else(|| {
                    ParseQuantityError::new(format!("binary multiplier overflow: {input:?}"))
                })?;
                mantissa = mantissa.checked_mul(mul).ok_or_else(|| {
                    ParseQuantityError::new(format!("quantity value overflow: {input:?}"))
                })?;
                Format::BinarySI
            }
        };

        Ok(Quantity {
            mantissa,
            scale,
            format,
        })
    }

    /// Quantity representing `value` in `format`. Port of upstream
    /// `resource.NewQuantity(value int64, format Format)`
    /// (`quantity.go:786-791`).
    ///
    /// Use this instead of hand-rolling a canonical string from an
    /// accumulated integer: `from_value(bytes, BinarySI).canonical_string()`
    /// is upstream's own round trip, suffix table and all.
    pub fn from_value(value: i64, format: Format) -> Quantity {
        Quantity {
            mantissa: value as i128,
            scale: 0,
            format,
        }
    }

    /// Quantity representing `value * 1/1000` in `format`. Port of upstream
    /// `resource.NewMilliQuantity` (`quantity.go:797-802`).
    ///
    /// The natural inverse of [`Self::milli_value`], so a whole number of cores
    /// canonicalises as `"2"` rather than the `"2000m"` a `format!("{}m", ..)`
    /// would emit.
    pub fn from_milli_value(value: i64, format: Format) -> Quantity {
        Quantity {
            mantissa: value as i128,
            scale: -3,
            format,
        }
    }

    /// Quantity representing `value * 10^scale`, always `DecimalSI`. Port of
    /// upstream `resource.NewScaledQuantity` (`quantity.go:806-811`).
    pub fn from_scaled_value(value: i64, scale: i32) -> Quantity {
        Quantity {
            mantissa: value as i128,
            scale,
            format: Format::DecimalSI,
        }
    }

    /// Format. Preserved from parsed suffix.
    pub fn format(&self) -> Format {
        self.format
    }

    /// True when the underlying value is zero regardless of format /
    /// scale.
    pub fn is_zero(&self) -> bool {
        self.mantissa == 0
    }

    /// Compare two quantities by their numeric value, ignoring
    /// format. Mirrors upstream `Quantity.Cmp`.
    pub fn cmp_value(&self, other: &Quantity) -> std::cmp::Ordering {
        // a*10^sa  vs  b*10^sb
        let (a, sa) = (self.mantissa, self.scale);
        let (b, sb) = (other.mantissa, other.scale);
        if sa == sb {
            return a.cmp(&b);
        }
        // Bring to common scale = min(sa, sb).
        let common = sa.min(sb);
        let shift_a = (sa - common) as u32;
        let shift_b = (sb - common) as u32;
        let a_scaled =
            a.checked_mul(10i128.pow(shift_a))
                .unwrap_or(if a < 0 { i128::MIN } else { i128::MAX });
        let b_scaled =
            b.checked_mul(10i128.pow(shift_b))
                .unwrap_or(if b < 0 { i128::MIN } else { i128::MAX });
        a_scaled.cmp(&b_scaled)
    }

    /// Two quantities are value-equal when they represent the same
    /// numeric value, regardless of format.
    pub fn value_eq(&self, other: &Quantity) -> bool {
        self.cmp_value(other) == std::cmp::Ordering::Equal
    }

    /// Bring `self` and `other` onto a common scale so their mantissas can be
    /// combined directly. Returns `(a, b, scale)`, or `None` if either
    /// mantissa overflows `i128` at that scale.
    fn align(&self, other: &Quantity) -> Option<(i128, i128, i32)> {
        let common = self.scale.min(other.scale);
        let shift_a = (self.scale - common) as u32;
        let shift_b = (other.scale - common) as u32;
        let a = self.mantissa.checked_mul(10i128.checked_pow(shift_a)?)?;
        let b = other.mantissa.checked_mul(10i128.checked_pow(shift_b)?)?;
        Some((a, b, common))
    }

    /// Subtract `other` from `self`, returning a new `Quantity`. Port of
    /// upstream `Quantity.Sub` (`quantity.go:618-627`).
    ///
    /// The result inherits the format of `self` (the "capacity" side) so that
    /// `8Gi - 1Gi` canonicalises as `7Gi` — except when `self` is zero, where
    /// upstream adopts `other`'s format (`quantity.go:620-622`) so that
    /// `0 - 1Gi` prints as `-1Gi` and not `-1073741824`.  Returns `None` on
    /// overflow (i128 mantissa).
    ///
    /// Used by `NodeController` to compute `allocatable = capacity - reserved`.
    pub fn sub(&self, other: &Quantity) -> Option<Quantity> {
        let (a, b, common) = self.align(other)?;
        Some(Quantity {
            mantissa: a.checked_sub(b)?,
            scale: common,
            format: self.effective_format(other),
        })
    }

    /// Add `other` to `self`, returning a new `Quantity`. Port of upstream
    /// `Quantity.Add` (`quantity.go:601-614`).
    ///
    /// The result inherits the format of `self`, unless `self` is zero — then
    /// it adopts `other`'s (`quantity.go:604-606`). That rule is what makes an
    /// accumulator work: folding `0 + 512Mi + 512Mi` yields `1Gi`, whereas an
    /// unconditional `self.format` would print the `DecimalSI` of the seed.
    /// Returns `None` on overflow (i128 mantissa).
    pub fn add(&self, other: &Quantity) -> Option<Quantity> {
        let (a, b, common) = self.align(other)?;
        Some(Quantity {
            mantissa: a.checked_add(b)?,
            scale: common,
            format: self.effective_format(other),
        })
    }

    /// Negate this quantity. Port of upstream `Quantity.Neg`
    /// (`quantity.go:658-665`); the format is untouched.
    pub fn neg(&self) -> Quantity {
        Quantity {
            mantissa: -self.mantissa,
            scale: self.scale,
            format: self.format,
        }
    }

    /// The format an in-place `Add`/`Sub` would leave behind: `self`'s, unless
    /// `self` is zero, in which case upstream adopts the operand's.
    fn effective_format(&self, other: &Quantity) -> Format {
        if self.mantissa == 0 {
            other.format
        } else {
            self.format
        }
    }

    /// True when this quantity represents a whole-number value (no fractional
    /// part). The internal value is `mantissa * 10^scale`, so a non-negative
    /// scale is always integral; a negative scale is integral only when the
    /// mantissa is divisible by `10^(-scale)`.
    ///
    /// Mirrors the integer-resource check in upstream
    /// `ValidateResourceQuantityValue` (`value.MilliValue()%1000 == 0`):
    /// extended (integer) resources must not carry a fractional quantity.
    pub fn is_integer(&self) -> bool {
        if self.scale >= 0 {
            return true;
        }
        let divisor = 10i128.checked_pow((-self.scale) as u32);
        match divisor {
            Some(d) => self.mantissa % d == 0,
            // Scale so negative that 10^(-scale) overflows i128: the magnitude
            // is far below 1, so it cannot be a non-zero integer.
            None => self.mantissa == 0,
        }
    }

    /// True when this quantity represents a negative value. Used to clamp
    /// `allocatable = capacity - reserved` to zero when a node reserves more
    /// than its capacity (upstream `getNodeAllocatableAbsolute` clamps to 0).
    pub fn is_negative(&self) -> bool {
        self.mantissa < 0
    }

    /// Value of `ceil(self / 10^target_scale)` as `i128`. Mirrors upstream
    /// `Quantity.ScaledValue(scale)` (rounding up, away from zero). The
    /// internal representation is `mantissa * 10^self.scale`, so this is
    /// `ceil(mantissa * 10^(self.scale - target_scale))`.
    fn scaled_value(&self, target_scale: i32) -> i128 {
        if self.mantissa == 0 {
            return 0;
        }
        let saturated = if self.mantissa < 0 {
            i128::MIN
        } else {
            i128::MAX
        };
        // `scale` is the parsed exponent, so `"1e2147483647"` can push this
        // subtraction out of `i32` — saturate instead of overflowing.
        let delta = self.scale.saturating_sub(target_scale);
        match delta.cmp(&0) {
            std::cmp::Ordering::Equal => self.mantissa,
            std::cmp::Ordering::Greater => {
                // Multiply up — exact, no rounding needed. `10^delta` alone
                // leaves `i128` from `delta >= 39` (`"1e400"`), so the factor
                // needs the same overflow check as the product; upstream caps
                // at the int64 bound rather than erroring
                // (`quantity.go`, `ScaledValue`).
                let Some(factor) = 10i128.checked_pow(delta as u32) else {
                    return saturated;
                };
                self.mantissa.checked_mul(factor).unwrap_or(saturated)
            }
            std::cmp::Ordering::Less => {
                // Divide by 10^(-delta), rounding up away from zero to match
                // upstream `ScaledValue` ceiling semantics.
                let Some(divisor) = 10i128.checked_pow(delta.unsigned_abs()) else {
                    // The divisor exceeds `i128`, so `|value| < 1` at this
                    // scale. Upstream rounds a non-zero quantity up to the
                    // smallest representable value rather than to zero ("if
                    // you want some resources, you should get some
                    // resources") — `ParseQuantity`'s `inf.RoundUp` call.
                    return if self.mantissa < 0 { -1 } else { 1 };
                };
                let q = self.mantissa / divisor;
                let r = self.mantissa % divisor;
                if r > 0 {
                    q + 1
                } else if r < 0 {
                    q - 1
                } else {
                    q
                }
            }
        }
    }

    /// Unscaled value rounded up to the nearest integer away from zero.
    /// Mirrors upstream `Quantity.Value()`.
    pub fn value(&self) -> i128 {
        self.scaled_value(0)
    }

    /// `ceil(self * 1000)`. Mirrors upstream `Quantity.MilliValue()`.
    pub fn milli_value(&self) -> i128 {
        self.scaled_value(-3)
    }

    /// Canonical-form string for this quantity. Matches upstream
    /// `Quantity.String()`.
    pub fn canonical_string(&self) -> String {
        if self.mantissa == 0 {
            return "0".to_string();
        }
        match self.format {
            Format::DecimalSI => canonical_decimal_si(self.mantissa, self.scale),
            Format::DecimalExponent => canonical_decimal_exponent(self.mantissa, self.scale),
            Format::BinarySI => canonical_binary_si(self.mantissa, self.scale),
        }
    }
}

impl fmt::Display for Quantity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.canonical_string())
    }
}

/// Parse a `resource.Quantity` into the unit Kubernetes accounts the named
/// resource in: **millicores** for `cpu`, the **base unit** (bytes for memory
/// and ephemeral-storage, whole items for scalar/extended resources) for
/// everything else.
///
/// Upstream makes this decision in exactly one place, `Resource.Add`
/// (`pkg/scheduler/framework/types.go:915-925`):
///
/// ```text
/// case v1.ResourceCPU:              r.MilliCPU += rQuant.MilliValue()
/// case v1.ResourceMemory:           r.Memory += rQuant.Value()
/// case v1.ResourceEphemeralStorage: r.EphemeralStorage += rQuant.Value()
/// default:                          r.AddScalar(rName, rQuant.Value())
/// ```
///
/// Prefer this over a local `strip_suffix` chain. Hand-rolled parsers keep
/// getting the `<number>` production wrong — it permits a decimal point with
/// *every* suffix, so `"0.5Gi"` is as valid as `"512Mi"` — and a quantity that
/// silently parses to 0 is indistinguishable from "asks for nothing".
///
/// Both accessors round up away from zero like upstream `ScaledValue`, so a
/// container asking for a sliver of a resource never accounts as asking for
/// none. Values beyond `i64` saturate rather than wrapping.
///
/// Whitespace is trimmed before parsing. Upstream `ParseQuantity` rejects it,
/// but these strings reach us from CLI flags (`--eviction-hard`) as well as
/// from validated API objects, and every hand-rolled parser this replaces
/// trimmed first.
pub fn parse_resource_value(
    quantity: &str,
    resource_name: &str,
) -> Result<i64, ParseQuantityError> {
    let parsed = Quantity::parse(quantity.trim())?;
    let value = if resource_name == "cpu" {
        parsed.milli_value()
    } else {
        parsed.value()
    };
    Ok(value.clamp(i64::MIN as i128, i64::MAX as i128) as i64)
}

/// Strip trailing factors of 10 from `mantissa`, lifting the scale
/// the same number of steps. Leaves the numeric value unchanged.
fn strip_trailing_zeros(mut mantissa: i128, mut scale: i32) -> (i128, i32) {
    if mantissa == 0 {
        return (0, scale);
    }
    while mantissa % 10 == 0 {
        mantissa /= 10;
        scale = scale.saturating_add(1);
    }
    (mantissa, scale)
}

fn canonical_decimal_si(mantissa: i128, scale: i32) -> String {
    let (m, s) = strip_trailing_zeros(mantissa, scale);
    const VALID: [i32; 10] = [-9, -6, -3, 0, 3, 6, 9, 12, 15, 18];
    // Pick the largest valid suffix-scale `t` with `t <= s` and shift
    // mantissa up by `10^(s - t)` so the value is preserved.
    if let Some(t) = VALID.iter().rev().find(|&&v| v <= s).copied() {
        let shift = (s - t) as u32;
        let factor = 10i128
            .checked_pow(shift)
            .expect("decimal SI shift factor overflow");
        let final_m = m.checked_mul(factor).expect("decimal SI mantissa overflow");
        format!("{}{}", final_m, decimal_si_suffix(t))
    } else if let Some(top) = VALID.last().copied().filter(|&top| s > top) {
        // scale > 18: clamp to E and absorb the excess into mantissa.
        let shift = (s - top) as u32;
        let factor = 10i128
            .checked_pow(shift)
            .expect("decimal SI shift factor overflow");
        let final_m = m.checked_mul(factor).expect("decimal SI mantissa overflow");
        format!("{}{}", final_m, decimal_si_suffix(top))
    } else {
        // scale < -9: try to divide mantissa cleanly down to scale=-9;
        // otherwise emit as DecimalExponent (no valid SI suffix).
        let target = -9i32;
        let shift = (target - s) as u32;
        let factor = 10i128
            .checked_pow(shift)
            .expect("decimal SI shift factor overflow");
        if m % factor == 0 {
            format!("{}{}", m / factor, decimal_si_suffix(target))
        } else {
            canonical_decimal_exponent(mantissa, scale)
        }
    }
}

fn decimal_si_suffix(scale: i32) -> &'static str {
    match scale {
        -9 => "n",
        -6 => "u",
        -3 => "m",
        0 => "",
        3 => "k",
        6 => "M",
        9 => "G",
        12 => "T",
        15 => "P",
        18 => "E",
        _ => unreachable!("decimal_si_suffix called with invalid scale {scale}"),
    }
}

/// DecimalExponent canonical form, per upstream `int64Amount.AsCanonicalBytes`
/// (`../kubernetes/staging/src/k8s.io/apimachinery/pkg/api/resource/amount.go:257-281`):
/// strip factors of 10 out of the mantissa, then **force the exponent to a
/// multiple of 3**, shifting the mantissa back up to compensate.
///
/// The second step is easy to miss and not obviously desirable — it is why
/// `80 * 10^-3` prints as `"80e-3"` rather than the simpler `"8e-2"`
/// (`quantity_test.go:723`). Upstream then emits **no suffix at all** when the
/// exponent lands on 0 (`suffix.go:165-167`), so `25 * 10^2` is `"2500"`, not
/// `"25e2"`.
fn canonical_decimal_exponent(mantissa: i128, scale: i32) -> String {
    let (mut m, mut s) = strip_trailing_zeros(mantissa, scale);

    // `i32::rem_euclid` would map -2 to 1; upstream switches on Go's `%`,
    // which keeps the sign, so match on the signed remainder directly.
    match s % 3 {
        1 | -2 => {
            m *= 10;
            s -= 1;
        }
        2 | -1 => {
            m *= 100;
            s -= 2;
        }
        _ => {}
    }

    if s == 0 {
        return format!("{m}");
    }
    format!("{m}e{s}")
}

fn canonical_binary_si(mantissa: i128, scale: i32) -> String {
    // BinarySI must represent an integer value. If the parsed
    // mantissa/scale cannot be reduced to an integer, fall back to
    // DecimalSI canonical form.
    let value = match scale.cmp(&0) {
        std::cmp::Ordering::Greater | std::cmp::Ordering::Equal => {
            let factor = match 10i128.checked_pow(scale as u32) {
                Some(f) => f,
                None => return canonical_decimal_si(mantissa, scale),
            };
            match mantissa.checked_mul(factor) {
                Some(v) => v,
                None => return canonical_decimal_si(mantissa, scale),
            }
        }
        std::cmp::Ordering::Less => {
            let factor = match 10i128.checked_pow((-scale) as u32) {
                Some(f) => f,
                None => return canonical_decimal_si(mantissa, scale),
            };
            if mantissa % factor != 0 {
                return canonical_decimal_si(mantissa, scale);
            }
            mantissa / factor
        }
    };

    if value.unsigned_abs() < 1024 {
        return canonical_decimal_si(mantissa, scale);
    }

    let mut v = value;
    let mut k: u32 = 0;
    while k < 6 && v % 1024 == 0 {
        v /= 1024;
        k += 1;
    }
    if k == 0 {
        return canonical_decimal_si(mantissa, scale);
    }
    let suffix = binary_si_suffix(k);
    format!("{v}{suffix}")
}

fn binary_si_suffix(k: u32) -> &'static str {
    match k {
        1 => "Ki",
        2 => "Mi",
        3 => "Gi",
        4 => "Ti",
        5 => "Pi",
        6 => "Ei",
        _ => unreachable!("binary_si_suffix called with invalid k {k}"),
    }
}

enum SuffixKind {
    DecimalSI(i32),
    DecimalExponent(i32),
    BinarySI(i32),
}

fn interpret_suffix(suffix: &str) -> Option<SuffixKind> {
    if suffix.is_empty() {
        return Some(SuffixKind::DecimalSI(0));
    }
    // DecimalExponent: `e`/`E` followed by an optional sign and at
    // least one digit. The rest must parse as a signed integer; if
    // not, fall through to the static suffix table (so `Ei` / `E`
    // resolve correctly).
    if let Some(rest) = suffix.strip_prefix(['e', 'E']) {
        if !rest.is_empty() {
            if let Ok(exp) = rest.parse::<i32>() {
                return Some(SuffixKind::DecimalExponent(exp));
            }
        }
    }
    if suffix == "E" {
        return Some(SuffixKind::DecimalSI(18));
    }
    match suffix {
        "n" => Some(SuffixKind::DecimalSI(-9)),
        "u" => Some(SuffixKind::DecimalSI(-6)),
        "m" => Some(SuffixKind::DecimalSI(-3)),
        "k" => Some(SuffixKind::DecimalSI(3)),
        "M" => Some(SuffixKind::DecimalSI(6)),
        "G" => Some(SuffixKind::DecimalSI(9)),
        "T" => Some(SuffixKind::DecimalSI(12)),
        "P" => Some(SuffixKind::DecimalSI(15)),
        "Ki" => Some(SuffixKind::BinarySI(10)),
        "Mi" => Some(SuffixKind::BinarySI(20)),
        "Gi" => Some(SuffixKind::BinarySI(30)),
        "Ti" => Some(SuffixKind::BinarySI(40)),
        "Pi" => Some(SuffixKind::BinarySI(50)),
        "Ei" => Some(SuffixKind::BinarySI(60)),
        _ => None,
    }
}

/// serde adapters for fields that hold a `resource.Quantity` as a `String`.
///
/// Port of upstream `Quantity.MarshalJSON` (`quantity.go:694`: emit
/// `CanonicalizeBytes`, so `1024Mi` -> `1Gi`, `1000m` -> `1`, `0Mi` -> `0`) and
/// `Quantity.UnmarshalJSON` (`quantity.go:733`: `null` is a no-op, a string or a
/// bare number is parsed with `ParseQuantity` after `TrimSpace`, anything
/// unparsable is a decode error).
///
/// Serialising an unparsable string emits it unchanged rather than failing:
/// every value that came from the wire was validated on decode, and a
/// serializer cannot report a semantic error the way Go's `MarshalJSON` never
/// can for a parsed `Quantity`.
pub mod wire {
    use super::Quantity;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::{BTreeMap, HashMap};

    /// Canonical string for `s`, or `s` itself when it does not parse.
    pub fn canonicalize(s: &str) -> String {
        match Quantity::parse(s.trim()) {
            Ok(q) => q.canonical_string(),
            Err(_) => s.to_string(),
        }
    }

    /// Validate a decoded JSON scalar; `None` for `null`.
    fn scalar<E: serde::de::Error>(v: serde_json::Value) -> Result<Option<String>, E> {
        let s = match v {
            serde_json::Value::Null => return Ok(None),
            serde_json::Value::String(s) => s,
            serde_json::Value::Number(n) => n.to_string(),
            other => {
                return Err(E::custom(format!(
                    "Quantity must be a string or number, got {other}"
                )))
            }
        };
        Quantity::parse(s.trim()).map_err(|e| E::custom(format!("invalid Quantity {s:?}: {e}")))?;
        Ok(Some(s))
    }

    pub fn serialize<S: Serializer>(v: &str, s: S) -> Result<S::Ok, S::Error> {
        canonicalize(v).serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
        let v = serde_json::Value::deserialize(d)?;
        Ok(scalar(v)?.unwrap_or_default())
    }

    pub fn serialize_opt<S: Serializer>(v: &Option<String>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(v) => s.serialize_str(&canonicalize(v)),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize_opt<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
        let v = serde_json::Value::deserialize(d)?;
        scalar(v)
    }

    pub fn serialize_vec<S: Serializer>(v: &[String], s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(v.iter().map(|x| canonicalize(x)))
    }

    pub fn deserialize_vec<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
        let items = Vec::<serde_json::Value>::deserialize(d)?;
        let mut out = Vec::with_capacity(items.len());
        for v in items {
            if let Some(s) = scalar(v)? {
                out.push(s);
            }
        }
        Ok(out)
    }

    pub fn serialize_map<S: Serializer>(
        m: &HashMap<String, String>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        s.collect_map(m.iter().map(|(k, v)| (k, canonicalize(v))))
    }

    pub fn deserialize_map<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<HashMap<String, String>, D::Error> {
        Ok(crate::types::deserialize_quantity_map(d)?.unwrap_or_default())
    }

    pub fn serialize_opt_map<S: Serializer>(
        m: &Option<HashMap<String, String>>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        match m {
            Some(m) => serialize_map(m, s),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize_opt_map<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Option<HashMap<String, String>>, D::Error> {
        crate::types::deserialize_quantity_map(d)
    }

    pub fn serialize_opt_btree_map<S: Serializer>(
        m: &Option<BTreeMap<String, String>>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        match m {
            Some(m) => serialize_btree_map(m, s),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize_opt_btree_map<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Option<BTreeMap<String, String>>, D::Error> {
        Ok(crate::types::deserialize_quantity_map(d)?.map(|m| m.into_iter().collect()))
    }

    pub fn serialize_btree_map<S: Serializer>(
        m: &BTreeMap<String, String>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        s.collect_map(m.iter().map(|(k, v)| (k, canonicalize(v))))
    }

    pub fn deserialize_btree_map<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<BTreeMap<String, String>, D::Error> {
        Ok(crate::types::deserialize_quantity_map(d)?
            .unwrap_or_default()
            .into_iter()
            .collect())
    }
}
