//! Go `time.Duration` string parsing/formatting and the `metav1.Duration`
//! serde representation.
//!
//! Ported from:
//! - `time.ParseDuration` — Go `src/time/format.go` (`ParseDuration`,
//!   `leadingInt`, `leadingFraction`, `unitMap`); tests are the
//!   `parseDurationTests` / `parseDurationErrorTests` tables in
//!   `src/time/time_test.go`.
//! - `Duration.String` — Go `src/time/time.go` (`Duration.format`).
//! - `metav1.Duration` (de)serialization —
//!   `staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/duration.go`
//!   (`UnmarshalJSON` accepts only a JSON string and runs `ParseDuration`;
//!   `MarshalJSON` emits `Duration.String()`).
//!
//! Deviation: Rust's `std::time::Duration` is unsigned, so the serde helper
//! rejects negative values. [`parse_go_duration`] itself is signed (`i64`
//! nanoseconds, Go's `Duration` representation).

use std::time::Duration;

const MICRO: u64 = 1_000;
const MILLI: u64 = 1_000_000;
const SECOND: u64 = 1_000_000_000;
const MINUTE: u64 = 60 * SECOND;
const HOUR: u64 = 60 * MINUTE;
const LIMIT: u64 = 1 << 63;

/// Go's `quote` (src/time/format.go): wrap in quotes, escape `"`/`\`, and
/// render U+FFFD as its UTF-8 bytes in `\x` form.
fn quote(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            '\u{FFFD}' => out.push_str("\\xef\\xbf\\xbd"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

fn unit_nanos(u: &str) -> Option<u64> {
    Some(match u {
        "ns" => 1,
        "us" | "\u{00B5}s" | "\u{03BC}s" => MICRO,
        "ms" => MILLI,
        "s" => SECOND,
        "m" => MINUTE,
        "h" => HOUR,
        _ => return None,
    })
}

/// `leadingInt`: consumes `[0-9]*`; `None` on overflow.
fn leading_int(s: &str) -> Option<(u64, &str)> {
    let mut x: u64 = 0;
    let mut i = 0;
    for &c in s.as_bytes() {
        if !c.is_ascii_digit() {
            break;
        }
        if x > LIMIT / 10 {
            return None;
        }
        x = x * 10 + u64::from(c - b'0');
        if x > LIMIT {
            return None;
        }
        i += 1;
    }
    Some((x, &s[i..]))
}

/// `leadingFraction`: consumes `[0-9]*`, never errors (stops accumulating
/// precision on overflow).
fn leading_fraction(s: &str) -> (u64, f64, &str) {
    let mut x: u64 = 0;
    let mut scale = 1f64;
    let mut overflow = false;
    let mut i = 0;
    for &c in s.as_bytes() {
        if !c.is_ascii_digit() {
            break;
        }
        i += 1;
        if overflow {
            continue;
        }
        if x > (LIMIT - 1) / 10 {
            overflow = true;
            continue;
        }
        let y = x * 10 + u64::from(c - b'0');
        if y > LIMIT {
            overflow = true;
            continue;
        }
        x = y;
        scale *= 10.0;
    }
    (x, scale, &s[i..])
}

/// Port of Go `time.ParseDuration`. Returns nanoseconds (Go's `Duration`).
pub fn parse_go_duration(orig: &str) -> Result<i64, String> {
    let invalid = || format!("time: invalid duration {}", quote(orig));
    let mut s = orig;
    let mut d: u64 = 0;
    let mut neg = false;

    if s.starts_with('-') || s.starts_with('+') {
        neg = s.starts_with('-');
        s = &s[1..];
    }
    if s == "0" {
        return Ok(0);
    }
    if s.is_empty() {
        return Err(invalid());
    }
    while !s.is_empty() {
        let first = s.as_bytes()[0];
        if !(first == b'.' || first.is_ascii_digit()) {
            return Err(invalid());
        }
        let before = s.len();
        let (mut v, rest) = leading_int(s).ok_or_else(invalid)?;
        s = rest;
        let pre = before != s.len();

        let (mut f, mut scale, mut post) = (0u64, 1f64, false);
        if let Some(rest) = s.strip_prefix('.') {
            s = rest;
            let pl = s.len();
            let (ff, sc, rest) = leading_fraction(s);
            f = ff;
            scale = sc;
            s = rest;
            post = pl != s.len();
        }
        if !pre && !post {
            return Err(invalid());
        }

        let i = s
            .bytes()
            .position(|c| c == b'.' || c.is_ascii_digit())
            .unwrap_or(s.len());
        if i == 0 {
            return Err(format!("time: missing unit in duration {}", quote(orig)));
        }
        let (u, rest) = s.split_at(i);
        s = rest;
        let unit = unit_nanos(u).ok_or_else(|| {
            format!(
                "time: unknown unit {} in duration {}",
                quote(u),
                quote(orig)
            )
        })?;
        if v > LIMIT / unit {
            return Err(invalid());
        }
        v *= unit;
        if f > 0 {
            // float64 keeps nanosecond accuracy for fractions of hours.
            v += (f as f64 * (unit as f64 / scale)) as u64;
            if v > LIMIT {
                return Err(invalid());
            }
        }
        d += v;
        if d > LIMIT {
            return Err(invalid());
        }
    }
    if neg {
        return Ok((d as i64).wrapping_neg());
    }
    if d > LIMIT - 1 {
        return Err(invalid());
    }
    Ok(d as i64)
}

/// Port of Go `Duration.String` for a nanosecond count.
pub fn format_go_duration(nanos: i64) -> String {
    let neg = nanos < 0;
    let u = nanos.unsigned_abs();
    let mut out = String::new();
    if u < SECOND {
        if u == 0 {
            return "0s".to_string();
        }
        let (prec, unit) = if u < MICRO {
            (0, "ns")
        } else if u < MILLI {
            (3, "\u{00B5}s")
        } else {
            (6, "ms")
        };
        out.push_str(&frac(u, prec));
        out.push_str(unit);
    } else {
        // Seconds with up to 9 fractional digits, then minutes/hours.
        let total_secs = u / SECOND;
        let fraction = frac(u % SECOND, 9);
        // `frac` of a value below 10^9 yields "0" or "0.<digits>".
        let frac_part = fraction.strip_prefix('0').unwrap_or("");
        let secs = total_secs % 60;
        let mins_total = total_secs / 60;
        if mins_total > 0 {
            let hours = mins_total / 60;
            if hours > 0 {
                out.push_str(&format!("{hours}h"));
            }
            out.push_str(&format!("{}m", mins_total % 60));
        }
        out.push_str(&format!("{secs}{frac_part}s"));
    }
    if neg {
        out.insert(0, '-');
    }
    out
}

/// Port of `fmtFrac` + `fmtInt`: `v / 10^prec` with the fraction's trailing
/// zeros omitted.
fn frac(v: u64, prec: u32) -> String {
    let div = 10u64.pow(prec);
    let int = v / div;
    let mut fr = v % div;
    if prec == 0 || fr == 0 {
        return int.to_string();
    }
    let mut digits = vec![0u8; prec as usize];
    for slot in digits.iter_mut().rev() {
        *slot = (fr % 10) as u8;
        fr /= 10;
    }
    while digits.last() == Some(&0) {
        digits.pop();
    }
    let ds: String = digits.iter().map(|d| char::from(b'0' + d)).collect();
    format!("{int}.{ds}")
}

/// serde adapter for `Option<std::time::Duration>` matching
/// `metav1.Duration` (use with `#[serde(default, with = "...")]`).
pub mod option_serde {
    use super::*;
    use serde::{de, Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(d) => {
                let ns = i64::try_from(d.as_nanos()).unwrap_or(i64::MAX);
                s.serialize_str(&format_go_duration(ns))
            }
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
        let Some(s) = Option::<String>::deserialize(d)? else {
            return Ok(None);
        };
        let ns = parse_go_duration(&s).map_err(de::Error::custom)?;
        let ns = u64::try_from(ns)
            .map_err(|_| de::Error::custom(format!("negative duration {s:?} is not supported")))?;
        Ok(Some(Duration::from_nanos(ns)))
    }
}
