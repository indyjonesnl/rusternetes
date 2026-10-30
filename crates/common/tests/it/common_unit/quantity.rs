//! Unit tests moved from `src/quantity.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::fmt;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::quantity::*;

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
