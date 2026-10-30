//! Unit tests moved from `src/go_duration.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::time::Duration;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::go_duration::*;

    const NS: i64 = 1;
    const US: i64 = 1_000;
    const MS: i64 = 1_000_000;
    const S: i64 = 1_000_000_000;
    const M: i64 = 60 * S;
    const H: i64 = 60 * M;

    /// Go `parseDurationTests` (src/time/time_test.go).
    #[test]
    fn parse_duration_table() {
        let cases: &[(&str, i64)] = &[
            ("0", 0),
            ("5s", 5 * S),
            ("30s", 30 * S),
            ("1478s", 1478 * S),
            ("-5s", -5 * S),
            ("+5s", 5 * S),
            ("-0", 0),
            ("+0", 0),
            ("5.0s", 5 * S),
            ("5.6s", 5 * S + 600 * MS),
            ("5.s", 5 * S),
            (".5s", 500 * MS),
            ("1.0s", S),
            ("1.00s", S),
            ("1.004s", S + 4 * MS),
            ("1.0040s", S + 4 * MS),
            ("100.00100s", 100 * S + MS),
            ("10ns", 10 * NS),
            ("11us", 11 * US),
            ("12\u{00B5}s", 12 * US),
            ("12\u{03BC}s", 12 * US),
            ("13ms", 13 * MS),
            ("14s", 14 * S),
            ("15m", 15 * M),
            ("16h", 16 * H),
            ("3h30m", 3 * H + 30 * M),
            ("10.5s4m", 4 * M + 10 * S + 500 * MS),
            ("-2m3.4s", -(2 * M + 3 * S + 400 * MS)),
            (
                "1h2m3s4ms5us6ns",
                H + 2 * M + 3 * S + 4 * MS + 5 * US + 6 * NS,
            ),
            ("39h9m14.425s", 39 * H + 9 * M + 14 * S + 425 * MS),
            ("52763797000ns", 52763797000),
            ("0.3333333333333333333h", 20 * M),
            ("9007199254740993ns", (1 << 53) + 1),
            ("9223372036854775807ns", i64::MAX),
            ("9223372036854775.807us", i64::MAX),
            ("9223372036s854ms775us807ns", i64::MAX),
            ("-9223372036854775808ns", i64::MIN),
            ("-9223372036854775.808us", i64::MIN),
            ("-9223372036s854ms775us808ns", i64::MIN),
            ("-2562047h47m16.854775808s", i64::MIN),
            ("0.100000000000000000000h", 6 * M),
            ("0.830103483285477580700h", 49 * M + 48 * S + 372539827),
        ];
        for (input, want) in cases {
            assert_eq!(
                parse_go_duration(input),
                Ok(*want),
                "ParseDuration({input:?})"
            );
        }
    }

    /// Go `parseDurationErrorTests` (valid-UTF-8 entries; Go's invalid-byte
    /// cases cannot be expressed as a Rust `&str`).
    #[test]
    fn parse_duration_error_table() {
        let cases: &[(&str, &str)] = &[
            ("", "\"\""),
            ("3", "\"3\""),
            ("-", "\"-\""),
            ("s", "\"s\""),
            (".", "\".\""),
            ("-.", "\"-.\""),
            (".s", "\".s\""),
            ("+.s", "\"+.s\""),
            ("1d", "\"1d\""),
            ("\u{FFFD}", "\"\\xef\\xbf\\xbd\""),
            (
                "\u{FFFD} hello \u{FFFD} world",
                "\"\\xef\\xbf\\xbd hello \\xef\\xbf\\xbd world\"",
            ),
            ("9223372036854775810ns", "\"9223372036854775810ns\""),
            ("9223372036854775808ns", "\"9223372036854775808ns\""),
            ("-9223372036854775809ns", "\"-9223372036854775809ns\""),
            ("9223372036854776us", "\"9223372036854776us\""),
            ("3000000h", "\"3000000h\""),
            ("9223372036854775.808us", "\"9223372036854775.808us\""),
            ("9223372036854ms775us808ns", "\"9223372036854ms775us808ns\""),
        ];
        for (input, expect) in cases {
            let err = parse_go_duration(input).expect_err(input);
            assert!(err.contains(expect), "{input:?}: {err:?} lacks {expect}");
        }
    }

    /// Go `Duration.String` and `TestParseDurationRoundTrip` (issue 48629).
    #[test]
    fn format_duration_and_round_trip() {
        let cases: &[(i64, &str)] = &[
            (0, "0s"),
            (S, "1s"),
            (90 * S, "1m30s"),
            (H, "1h0m0s"),
            (3 * H + 30 * M, "3h30m0s"),
            (500 * MS, "500ms"),
            (1500 * US, "1.5ms"),
            (12 * US, "12\u{00B5}s"),
            (10, "10ns"),
            (-2 * M - 3 * S - 400 * MS, "-2m3.4s"),
            (S + 4 * MS, "1.004s"),
            (i64::MAX, "2562047h47m16.854775807s"),
            (i64::MIN, "-2562047h47m16.854775808s"),
        ];
        for (ns, want) in cases {
            assert_eq!(&format_go_duration(*ns), want);
            assert_eq!(parse_go_duration(want), Ok(*ns));
        }
    }
}
