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
    mantissa: i128,
    scale: i32,
    format: Format,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Quantity {
        Quantity::parse(s).unwrap_or_else(|e| panic!("parse({s:?}) failed: {e}"))
    }

    #[test]
    fn decimal_si_suffix_table_canonical_self() {
        for s in [
            "5n", "10u", "100m", "1", "5k", "1M", "10G", "1T", "1P", "1E",
        ] {
            assert_eq!(parse(s).canonical_string(), s);
        }
    }

    /// `value()`/`milli_value()` used to compute `10^delta` with an
    /// unchecked `pow`, so an exponent past ~39 digits panicked with
    /// "attempt to multiply with overflow" in debug and wrapped to a
    /// nonsense value in release. Upstream `ScaledValue` caps at the
    /// int64 bound instead.
    #[test]
    fn extreme_exponents_saturate_instead_of_overflowing() {
        assert_eq!(parse("1e400").value(), i128::MAX);
        assert_eq!(parse("-1e400").value(), i128::MIN);
        assert_eq!(parse("1e400").milli_value(), i128::MAX);
        // A non-zero quantity smaller than the target scale rounds up away
        // from zero rather than dividing to 0.
        assert_eq!(parse("1e-400").value(), 1);
        assert_eq!(parse("-1e-400").value(), -1);
        assert_eq!(parse("1e-400").milli_value(), 1);
        // Zero stays zero at any scale.
        assert_eq!(parse("0e400").value(), 0);
        assert_eq!(parse("0e-400").value(), 0);
        // Exponent at the i32 bound must not overflow the scale subtraction
        // that `milli_value()` performs.
        assert_eq!(parse("1e2147483647").milli_value(), i128::MAX);
        assert_eq!(parse("1e-2147483648").milli_value(), 1);
    }

    /// Upstream's own canonical-form table, ported row-for-row from
    /// `TestQuantityString`
    /// (`../kubernetes/staging/src/k8s.io/apimachinery/pkg/api/resource/quantity_test.go:696`).
    ///
    /// `expect` is the canonical string upstream's `Quantity.String()` produces
    /// for that `(mantissa, scale, format)`. `alternate`, where non-empty, is a
    /// *non-canonical* spelling of the same value that must canonicalise to
    /// `expect` — upstream asserts all three properties per row, so this does
    /// too. Testing the encoder against upstream's expectations rather than
    /// against a reading of `CanonicalizeBytes` is the point: several of these
    /// rows are counter-intuitive (BinarySI `5` prints `"5"`, not `"5m"`;
    /// BinarySI `1025` stays `"1025"`; DecimalExponent `80e-3` does *not*
    /// simplify to `8e-2`).
    #[test]
    fn upstream_quantity_string_table() {
        use Format::{BinarySI, DecimalExponent, DecimalSI};

        /// Mirrors upstream's `decQuantity(value, scale, format)` test helper.
        fn dec(mantissa: i128, scale: i32, format: Format) -> Quantity {
            Quantity {
                mantissa,
                scale,
                format,
            }
        }

        let table: &[(Quantity, &str, &str)] = &[
            (dec(1024 * 1024 * 1024, 0, BinarySI), "1Gi", "1024Mi"),
            (dec(300 * 1024 * 1024, 0, BinarySI), "300Mi", "307200Ki"),
            (dec(6 * 1024, 0, BinarySI), "6Ki", ""),
            (
                dec(1001 * 1024 * 1024 * 1024, 0, BinarySI),
                "1001Gi",
                "1025024Mi",
            ),
            (dec(1024 * 1024 * 1024 * 1024, 0, BinarySI), "1Ti", "1024Gi"),
            (dec(5, 0, BinarySI), "5", "5000m"),
            (dec(500, -3, BinarySI), "500m", "0.5"),
            (dec(1, 9, DecimalSI), "1G", "1000M"),
            (dec(1000, 6, DecimalSI), "1G", "0.001T"),
            (dec(1000000, 3, DecimalSI), "1G", ""),
            (dec(1000000000, 0, DecimalSI), "1G", ""),
            (dec(1, -3, DecimalSI), "1m", "1000u"),
            (dec(80, -3, DecimalSI), "80m", ""),
            (dec(1080, -3, DecimalSI), "1080m", "1.08"),
            (dec(108, -2, DecimalSI), "1080m", "1080000000n"),
            (dec(10800, -4, DecimalSI), "1080m", ""),
            (dec(300, 6, DecimalSI), "300M", ""),
            (dec(1, 12, DecimalSI), "1T", ""),
            (dec(1234567, 6, DecimalSI), "1234567M", ""),
            (dec(1234567, -3, BinarySI), "1234567m", ""),
            (dec(3, 3, DecimalSI), "3k", ""),
            (dec(1025, 0, BinarySI), "1025", ""),
            (dec(0, 0, DecimalSI), "0", ""),
            (dec(0, 0, BinarySI), "0", ""),
            (dec(1, 9, DecimalExponent), "1e9", ".001e12"),
            (dec(1, -3, DecimalExponent), "1e-3", "0.001e0"),
            (dec(1, -9, DecimalExponent), "1e-9", "1000e-12"),
            (dec(80, -3, DecimalExponent), "80e-3", ""),
            (dec(300, 6, DecimalExponent), "300e6", ""),
            (dec(1, 12, DecimalExponent), "1e12", ""),
            (dec(1, 3, DecimalExponent), "1e3", ""),
            (dec(3, 3, DecimalExponent), "3e3", ""),
            (dec(3, 3, DecimalSI), "3k", ""),
            (dec(0, 0, DecimalExponent), "0", "00"),
            (dec(1, -9, DecimalSI), "1n", ""),
            (dec(80, -9, DecimalSI), "80n", ""),
            (dec(1080, -9, DecimalSI), "1080n", ""),
            (dec(108, -8, DecimalSI), "1080n", ""),
            (dec(10800, -10, DecimalSI), "1080n", ""),
            (dec(1, -6, DecimalSI), "1u", ""),
            (dec(80, -6, DecimalSI), "80u", ""),
            (dec(1080, -6, DecimalSI), "1080u", ""),
        ];

        for (quantity, expect, alternate) in table {
            assert_eq!(
                &quantity.canonical_string(),
                expect,
                "String() for {quantity:?}"
            );

            // Upstream also asserts `expect` is itself canonical: parsing it
            // and re-encoding must be a no-op.
            let reparsed =
                Quantity::parse(expect).unwrap_or_else(|e| panic!("parse({expect:?}) failed: {e}"));
            assert_eq!(
                &reparsed.canonical_string(),
                expect,
                "{expect:?} is not its own canonical form"
            );

            if alternate.is_empty() {
                continue;
            }
            let alt = Quantity::parse(alternate)
                .unwrap_or_else(|e| panic!("parse({alternate:?}) failed: {e}"));
            assert_eq!(
                &alt.canonical_string(),
                expect,
                "alternate {alternate:?} must canonicalise to {expect:?}"
            );
        }
    }

    /// Ports of upstream `NewQuantity` / `NewMilliQuantity` /
    /// `NewScaledQuantity`, which is how upstream builds a quantity from an
    /// accumulated integer before serialising it back
    /// (`quantity.go:786-811`). Without these, callers hand-roll canonical
    /// encoding from a raw byte count.
    #[test]
    fn constructors_round_trip_to_canonical_form() {
        use Format::{BinarySI, DecimalSI};
        // Byte counts, BinarySI — what a ResourceQuota status carries.
        assert_eq!(
            Quantity::from_value(1_073_741_824, BinarySI).canonical_string(),
            "1Gi"
        );
        assert_eq!(
            Quantity::from_value(536_870_912, BinarySI).canonical_string(),
            "512Mi"
        );
        assert_eq!(
            Quantity::from_value(1024, BinarySI).canonical_string(),
            "1Ki"
        );
        // Below 1024, upstream switches BinarySI to DecimalSI formatting
        // (`CanonicalizeBytes`, `quantity.go:434-437`), so 1000 bytes is "1k".
        assert_eq!(
            Quantity::from_value(1000, BinarySI).canonical_string(),
            "1k"
        );
        // At/above 1024 but not a clean power, it stays a bare number.
        assert_eq!(
            Quantity::from_value(1500, BinarySI).canonical_string(),
            "1500"
        );
        assert_eq!(Quantity::from_value(0, BinarySI).canonical_string(), "0");

        // Millicores, DecimalSI — a whole number of cores must not print as
        // "2000m", which is what hand-rolled `format!("{}m", ..)` produces.
        assert_eq!(
            Quantity::from_milli_value(2000, DecimalSI).canonical_string(),
            "2"
        );
        assert_eq!(
            Quantity::from_milli_value(1500, DecimalSI).canonical_string(),
            "1500m"
        );
        assert_eq!(
            Quantity::from_milli_value(100, DecimalSI).canonical_string(),
            "100m"
        );
        assert_eq!(
            Quantity::from_milli_value(0, DecimalSI).canonical_string(),
            "0"
        );

        assert_eq!(Quantity::from_scaled_value(5, 3).canonical_string(), "5k");
    }

    /// The constructors must agree with `parse` on the same value, or the two
    /// halves of a round trip disagree.
    #[test]
    fn constructors_agree_with_parse() {
        for (bytes, text) in [
            (1_073_741_824i64, "1Gi"),
            (536_870_912, "512Mi"),
            (1024, "1Ki"),
        ] {
            let built = Quantity::from_value(bytes, Format::BinarySI);
            assert!(built.value_eq(&parse(text)), "{text}");
            assert_eq!(built.canonical_string(), parse(text).canonical_string());
        }
        for (millis, text) in [(2000i64, "2"), (1500, "1500m"), (100, "100m")] {
            let built = Quantity::from_milli_value(millis, Format::DecimalSI);
            assert!(built.value_eq(&parse(text)), "{text}");
            assert_eq!(built.canonical_string(), parse(text).canonical_string());
        }
    }

    /// A quantity built from an integer reads back as that integer.
    #[test]
    fn constructors_preserve_value_accessors() {
        assert_eq!(Quantity::from_value(4096, Format::BinarySI).value(), 4096);
        assert_eq!(
            Quantity::from_milli_value(1500, Format::DecimalSI).milli_value(),
            1500
        );
        // 1500 millicores rounds up to 2 whole cores, per `Value()`'s ceiling.
        assert_eq!(
            Quantity::from_milli_value(1500, Format::DecimalSI).value(),
            2
        );
        assert_eq!(Quantity::from_value(-5, Format::DecimalSI).value(), -5);
    }

    #[test]
    fn binary_si_suffix_table_canonical_self() {
        for s in ["1Ki", "1Mi", "1Gi", "1Ti", "1Pi", "1Ei"] {
            assert_eq!(parse(s).canonical_string(), s);
        }
    }

    #[test]
    fn decimal_exponent_canonical_form() {
        assert_eq!(parse("1e3").canonical_string(), "1e3");
        assert_eq!(parse("1e-3").canonical_string(), "1e-3");
        // Upstream lowercases `E` -> `e` in DecimalExponent canonical.
        assert_eq!(parse("1E6").canonical_string(), "1e6");
        // Exponent 0 emits NO suffix (`suffix.go:165-167` returns a nil suffix
        // for `DecimalExponent` with `exponent == 0`). Previously asserted as
        // "1e0", which upstream never produces.
        assert_eq!(parse("1e0").canonical_string(), "1");
        // The exponent is forced to a multiple of 3 after zero-stripping
        // (`amount.go:264-279`), so `2.5e3` is mantissa 25 scale 2 -> shifted
        // to 2500 scale 0 -> "2500". Previously asserted as "25e2", which
        // skipped the multiple-of-3 step.
        assert_eq!(parse("2.5e3").canonical_string(), "2500");
        // Non-multiple-of-3 exponents in both directions. Not rows in
        // upstream's table (every DecimalExponent row there already has a
        // multiple-of-3 scale), so these come from `AsCanonicalBytes` directly.
        assert_eq!(parse("8e-2").canonical_string(), "80e-3");
        assert_eq!(parse("1e1").canonical_string(), "10");
        assert_eq!(parse("1e2").canonical_string(), "100");
        assert_eq!(parse("1e4").canonical_string(), "10e3");
        assert_eq!(parse("1e-1").canonical_string(), "100e-3");
        assert_eq!(parse("1e-2").canonical_string(), "10e-3");
    }

    #[test]
    fn fractional_decimal_simplifies_to_millis() {
        assert_eq!(parse("0.5").canonical_string(), "500m");
        assert_eq!(parse("1.5").canonical_string(), "1500m");
        assert_eq!(parse("0.001").canonical_string(), "1m");
    }

    #[test]
    fn thousand_millis_simplifies_to_one() {
        assert_eq!(parse("1000m").canonical_string(), "1");
    }

    #[test]
    fn millis_with_remainder_stays_in_milli_scale() {
        assert_eq!(parse("1024m").canonical_string(), "1024m");
    }

    #[test]
    fn binary_si_factors_out_1024s() {
        // 1024Mi == 1Gi.
        assert_eq!(parse("1024Mi").canonical_string(), "1Gi");
        // 2048Mi == 2Gi.
        assert_eq!(parse("2048Mi").canonical_string(), "2Gi");
    }

    #[test]
    fn binary_si_keeps_format_when_value_is_not_power_of_1024() {
        // 100Mi has 100 left over after factoring 1024^2.
        assert_eq!(parse("100Mi").canonical_string(), "100Mi");
    }

    #[test]
    fn decimal_si_keeps_format_when_value_is_not_power_of_1000() {
        // 1024M is 1024 * 10^6, the canonical DecimalSI form because
        // 1024 has no trailing zeros.
        assert_eq!(parse("1024M").canonical_string(), "1024M");
    }

    #[test]
    fn zero_in_any_format_is_bare_zero() {
        for s in ["0", "0.0", "0Ki", "0m", "0n", "0Mi", "0e0", "-0", "-0m"] {
            assert_eq!(parse(s).canonical_string(), "0", "for {s:?}");
        }
    }

    #[test]
    fn negative_values_canonical() {
        assert_eq!(parse("-100m").canonical_string(), "-100m");
        assert_eq!(parse("-1Ki").canonical_string(), "-1Ki");
        assert_eq!(parse("-1").canonical_string(), "-1");
        assert_eq!(parse("-2.5").canonical_string(), "-2500m");
        assert_eq!(parse("-1e3").canonical_string(), "-1e3");
    }

    #[test]
    fn value_equality_across_formats() {
        // "1Ki" parses to the same numeric value as "1024".
        assert!(parse("1Ki").value_eq(&parse("1024")));
        // "1M" == "1000000".
        assert!(parse("1M").value_eq(&parse("1000000")));
        // "1024Mi" == "1Gi".
        assert!(parse("1024Mi").value_eq(&parse("1Gi")));
    }

    #[test]
    fn very_small_canonicalises_to_nanos() {
        assert_eq!(parse("0.000000001").canonical_string(), "1n");
    }

    #[test]
    fn very_large_boundary_decimal_si() {
        assert_eq!(parse("8E").canonical_string(), "8E");
    }

    #[test]
    fn very_large_boundary_binary_si() {
        assert_eq!(parse("1Ei").canonical_string(), "1Ei");
    }

    #[test]
    fn i64_max_decodes_to_self() {
        let q = parse(&i64::MAX.to_string());
        assert_eq!(q.canonical_string(), i64::MAX.to_string());
    }

    #[test]
    fn rejects_empty() {
        assert!(Quantity::parse("").is_err());
    }

    #[test]
    fn rejects_whitespace_only() {
        assert!(Quantity::parse("   ").is_err());
    }

    #[test]
    fn rejects_suffix_without_mantissa() {
        assert!(Quantity::parse("Ki").is_err());
        assert!(Quantity::parse("m").is_err());
        assert!(Quantity::parse("E").is_err());
    }

    #[test]
    fn rejects_unknown_suffix() {
        for s in ["1Q", "1A", "1B", "1KiB", "1ki", "1MIB"] {
            assert!(
                Quantity::parse(s).is_err(),
                "expected reject for {s:?} but it parsed"
            );
        }
    }

    #[test]
    fn rejects_trailing_whitespace() {
        assert!(Quantity::parse("1ki ").is_err());
        assert!(Quantity::parse("1 ").is_err());
        assert!(Quantity::parse(" 1").is_err());
    }

    #[test]
    fn rejects_dotted_garbage() {
        // ".5i" — i alone is not a valid suffix.
        assert!(Quantity::parse(".5i").is_err());
        // "1.1.M" — second `.` ends the parse with leftover garbage.
        assert!(Quantity::parse("1.1.M").is_err());
        // "1+1.0M" — `+` mid-mantissa is not allowed.
        assert!(Quantity::parse("1+1.0M").is_err());
        // "-3.01e-" — exponent must have at least one digit.
        assert!(Quantity::parse("-3.01e-").is_err());
        // "0.1mi" — lowercase `mi` is not a suffix.
        assert!(Quantity::parse("0.1mi").is_err());
    }

    #[test]
    fn sub_value_and_clamp() {
        let q = |s: &str| Quantity::parse(s).unwrap();
        // Same binary unit.
        assert!(q("8Gi").sub(&q("1Gi")).unwrap().value_eq(&q("7Gi")));
        // Different decimal scales (cores minus millicores).
        assert!(q("4").sub(&q("500m")).unwrap().value_eq(&q("3500m")));
        // Cross binary units.
        assert!(q("1Gi").sub(&q("512Mi")).unwrap().value_eq(&q("512Mi")));
        // Subtracting zero is identity; equal operands net to zero.
        assert!(q("4").sub(&q("0")).unwrap().value_eq(&q("4")));
        assert!(q("1Gi").sub(&q("1Gi")).unwrap().value_eq(&q("0")));
        // Reserved exceeding capacity yields a negative quantity (callers clamp).
        let neg = q("100m").sub(&q("500m")).unwrap();
        assert!(neg.is_negative());
        assert!(!q("500m").sub(&q("100m")).unwrap().is_negative());
    }

    /// The three grammar defects every hand-rolled parser this module replaced
    /// shared, pinned here now that the per-crate wrappers that used to guard
    /// them are gone.
    #[test]
    fn rejects_the_hand_rolled_parser_defects() {
        // `trim_end_matches` strips *repeated* suffixes; `strip_suffix` does not.
        assert!(Quantity::parse("1GiGi").is_err());
        assert!(Quantity::parse("100mm").is_err());
        // Upstream defines `k` (lowercase) and no `K`. The copies had it
        // backwards: `1K` parsed as 1000 and the legal `1k` errored.
        assert_eq!(Quantity::parse("1k").unwrap().value(), 1_000);
        assert!(Quantity::parse("1K").is_err());
        // A decimal point is legal with every suffix — `0.5Gi` is as valid as
        // `512Mi`, and an integer-only mantissa parse read it as nothing.
        assert_eq!(Quantity::parse("0.5Gi").unwrap().value(), 536_870_912);
        assert_eq!(Quantity::parse("0.5").unwrap().milli_value(), 500);
    }

    /// Port of upstream `Quantity.Add` (`quantity.go:601-614`), including its
    /// format rule: the receiver's format wins unless the receiver is zero.
    #[test]
    fn add_value_and_format() {
        let q = |s: &str| Quantity::parse(s).unwrap();
        assert!(q("1Gi").add(&q("1Gi")).unwrap().value_eq(&q("2Gi")));
        assert!(q("500m").add(&q("500m")).unwrap().value_eq(&q("1")));
        // Cross-scale.
        assert!(q("1").add(&q("500m")).unwrap().value_eq(&q("1500m")));
        // Cross-format: the receiver's suffix survives.
        assert_eq!(
            q("1Gi").add(&q("1G")).unwrap().canonical_string(),
            "2073741824"
        );
        // A zero receiver adopts the addend's format, so an accumulator
        // seeded at zero still prints in binary units.
        assert_eq!(q("0").add(&q("512Mi")).unwrap().canonical_string(), "512Mi");
        // Sub follows the same rule (`quantity.go:620-622`).
        assert_eq!(q("0").sub(&q("1Gi")).unwrap().canonical_string(), "-1Gi");
    }

    #[test]
    fn neg_flips_sign_and_keeps_format() {
        let q = |s: &str| Quantity::parse(s).unwrap();
        let n = q("1Gi").neg();
        assert!(n.is_negative());
        assert_eq!(n.canonical_string(), "-1Gi");
        assert_eq!(n.neg().canonical_string(), "1Gi");
        assert!(q("0").neg().is_zero());
    }
}
