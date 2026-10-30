//! Unit tests moved from `src/validation/go_regexp.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::validation::go_regexp::*;

    /// `regexp.Compile(pattern)` outcomes recorded under Go 1.25.9
    /// (`None` = accepted, else `err.Error()`).
    const GO_OUTCOMES: &[(&str, Option<&str>)] = &[
        (
            r#"("#,
            Some(r#"error parsing regexp: missing closing ): `(`"#),
        ),
        (
            r#"(abc"#,
            Some(r#"error parsing regexp: missing closing ): `(abc`"#),
        ),
        (
            r#"abc)"#,
            Some(r#"error parsing regexp: unexpected ): `abc)`"#),
        ),
        (r#")"#, Some(r#"error parsing regexp: unexpected ): `)`"#)),
        (
            r#"a**"#,
            Some(r#"error parsing regexp: invalid nested repetition operator: `**`"#),
        ),
        (
            r#"a++"#,
            Some(r#"error parsing regexp: invalid nested repetition operator: `++`"#),
        ),
        (
            r#"a*+"#,
            Some(r#"error parsing regexp: invalid nested repetition operator: `*+`"#),
        ),
        (
            r#"a???"#,
            Some(r#"error parsing regexp: invalid nested repetition operator: `???`"#),
        ),
        (
            r#"*"#,
            Some(r#"error parsing regexp: missing argument to repetition operator: `*`"#),
        ),
        (
            r#"+a"#,
            Some(r#"error parsing regexp: missing argument to repetition operator: `+`"#),
        ),
        (
            r#"?"#,
            Some(r#"error parsing regexp: missing argument to repetition operator: `?`"#),
        ),
        (
            r#"(*)"#,
            Some(r#"error parsing regexp: missing argument to repetition operator: `*`"#),
        ),
        (
            r#"|*"#,
            Some(r#"error parsing regexp: missing argument to repetition operator: `*`"#),
        ),
        (r#"^*"#, None),
        (
            r#"a{2}{3}"#,
            Some(r#"error parsing regexp: invalid nested repetition operator: `{2}{3}`"#),
        ),
        (
            r#"a{1001}"#,
            Some(r#"error parsing regexp: invalid repeat count: `{1001}`"#),
        ),
        (
            r#"a{1,1001}"#,
            Some(r#"error parsing regexp: invalid repeat count: `{1,1001}`"#),
        ),
        (
            r#"a{2,1}"#,
            Some(r#"error parsing regexp: invalid repeat count: `{2,1}`"#),
        ),
        (
            r#"a{99999999999}"#,
            Some(r#"error parsing regexp: invalid repeat count: `{99999999999}`"#),
        ),
        (
            r#"(a{600}){2}"#,
            Some(r#"error parsing regexp: invalid repeat count: `{2}`"#),
        ),
        (
            r#"(a{20}){20}{3}"#,
            Some(r#"error parsing regexp: invalid nested repetition operator: `{20}{3}`"#),
        ),
        (r#"a{1000}"#, None),
        (r#"a{,5}"#, None),
        (r#"a{"#, None),
        (r#"a{x}"#, None),
        (r#"{"#, None),
        (r#"a{1"#, None),
        (r#"a{1,"#, None),
        (r#"a{01}"#, None),
        (
            r#"[a"#,
            Some(r#"error parsing regexp: missing closing ]: `[a`"#),
        ),
        (
            r#"[a-"#,
            Some(r#"error parsing regexp: missing closing ]: `[a-`"#),
        ),
        (
            r#"[]"#,
            Some(r#"error parsing regexp: missing closing ]: `[]`"#),
        ),
        (r#"[]a]"#, None),
        (
            r#"[^]"#,
            Some(r#"error parsing regexp: missing closing ]: `[^]`"#),
        ),
        (
            r#"[z-a]"#,
            Some(r#"error parsing regexp: invalid character class range: `z-a`"#),
        ),
        (
            r#"[a-\d]"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\d`"#),
        ),
        (
            r#"[[:foo:]]"#,
            Some(r#"error parsing regexp: invalid character class range: `[:foo:]`"#),
        ),
        (
            r#"[[:alpha:]"#,
            Some(r#"error parsing regexp: missing closing ]: `[[:alpha:]`"#),
        ),
        (r#"[[:alpha:]]"#, None),
        (r#"[[:^alpha:]]"#, None),
        (
            r#"[[:alpha:"#,
            Some(r#"error parsing regexp: missing closing ]: `[[:alpha:`"#),
        ),
        (
            r#"[\q]"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\q`"#),
        ),
        (
            r#"[\"#,
            Some(r#"error parsing regexp: trailing backslash at end of expression: ``"#),
        ),
        (
            r#"\"#,
            Some(r#"error parsing regexp: trailing backslash at end of expression: ``"#),
        ),
        (
            r#"\q"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\q`"#),
        ),
        (
            r#"\8"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\8`"#),
        ),
        (
            r#"\1"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\1`"#),
        ),
        (r#"\12"#, None),
        (r#"\0"#, None),
        (r#"\01"#, None),
        (
            r#"\x"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\x`"#),
        ),
        (
            r#"\x4"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\x4`"#),
        ),
        (
            r#"\x4g"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\x4g`"#),
        ),
        (r#"\x41"#, None),
        (r#"\x{41}"#, None),
        (
            r#"\x{}"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\x{}`"#),
        ),
        (
            r#"\x{110000}"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\x{110000`"#),
        ),
        (
            r#"\x{41"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\x{41`"#),
        ),
        (
            r#"\x{zz}"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\x{z`"#),
        ),
        (
            r#"\C"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\C`"#),
        ),
        (
            r#"\Z"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\Z`"#),
        ),
        (r#"\z"#, None),
        (r#"\A"#, None),
        (r#"\b"#, None),
        (r#"\B"#, None),
        (r#"\Qab(\E+"#, None),
        (r#"\Qab("#, None),
        (r#"\Q"#, None),
        (
            r#"\p"#,
            Some(r#"error parsing regexp: invalid character class range: `\p`"#),
        ),
        (r#"\pL"#, None),
        (
            r#"\pX"#,
            Some(r#"error parsing regexp: invalid character class range: `\pX`"#),
        ),
        (r#"\p{L}"#, None),
        (r#"\p{^L}"#, None),
        (r#"\P{Greek}"#, None),
        (
            r#"\p{Foo}"#,
            Some(r#"error parsing regexp: invalid character class range: `\p{Foo}`"#),
        ),
        (
            r#"\p{Old_Italic}"#,
            Some(r#"error parsing regexp: invalid character class range: `\p{Old_Italic}`"#),
        ),
        (
            r#"\p{OldItalic}"#,
            Some(r#"error parsing regexp: invalid character class range: `\p{OldItalic}`"#),
        ),
        (r#"\p{letter}"#, None),
        (r#"\p{Letter}"#, None),
        (r#"\p{ascii}"#, None),
        (r#"\p{Any}"#, None),
        (r#"\p{Assigned}"#, None),
        (r#"\p{Lc}"#, None),
        (r#"\p{LC}"#, None),
        (
            r#"\p{L"#,
            Some(r#"error parsing regexp: invalid character class range: `\p{L`"#),
        ),
        (
            r#"\p{}"#,
            Some(r#"error parsing regexp: invalid character class range: `\p{}`"#),
        ),
        (
            r#"[\p{Foo}]"#,
            Some(r#"error parsing regexp: invalid character class range: `\p{Foo}`"#),
        ),
        (r#"[\p{L}\d]"#, None),
        (
            r#"(?"#,
            Some(r#"error parsing regexp: invalid or unsupported Perl syntax: `(?`"#),
        ),
        (
            r#"(?i"#,
            Some(r#"error parsing regexp: invalid or unsupported Perl syntax: `(?i`"#),
        ),
        (r#"(?i)"#, None),
        (r#"(?i:abc)"#, None),
        (r#"(?-i)"#, None),
        (
            r#"(?-)"#,
            Some(r#"error parsing regexp: invalid or unsupported Perl syntax: `(?-)`"#),
        ),
        (
            r#"(?--i)"#,
            Some(r#"error parsing regexp: invalid or unsupported Perl syntax: `(?--`"#),
        ),
        (
            r#"(?z)"#,
            Some(r#"error parsing regexp: invalid or unsupported Perl syntax: `(?z`"#),
        ),
        (r#"(?P<name>x)"#, None),
        (
            r#"(?P<name"#,
            Some(r#"error parsing regexp: invalid named capture: `(?P<name`"#),
        ),
        (
            r#"(?P<>x)"#,
            Some(r#"error parsing regexp: invalid named capture: `(?P<>`"#),
        ),
        (
            r#"(?P<a-b>x)"#,
            Some(r#"error parsing regexp: invalid named capture: `(?P<a-b>`"#),
        ),
        (r#"(?<name>x)"#, None),
        (
            r#"(?<>x)"#,
            Some(r#"error parsing regexp: invalid named capture: `(?<>`"#),
        ),
        (
            r#"(?'name'x)"#,
            Some(r#"error parsing regexp: invalid or unsupported Perl syntax: `(?'`"#),
        ),
        (
            r#"(?=x)"#,
            Some(r#"error parsing regexp: invalid or unsupported Perl syntax: `(?=`"#),
        ),
        (
            r#"(?!x)"#,
            Some(r#"error parsing regexp: invalid or unsupported Perl syntax: `(?!`"#),
        ),
        (
            r#"(?<=x)"#,
            Some(r#"error parsing regexp: invalid named capture: `(?<=x)`"#),
        ),
        (
            r#"(?#c)"#,
            Some(r#"error parsing regexp: invalid or unsupported Perl syntax: `(?#`"#),
        ),
        (
            r#"(?:"#,
            Some(r#"error parsing regexp: missing closing ): `(?:`"#),
        ),
        (
            r#"(?:a"#,
            Some(r#"error parsing regexp: missing closing ): `(?:a`"#),
        ),
        (r#"(?:a)"#, None),
        (r#"(?i-s:a)"#, None),
        (r#"(?is-m:a)"#, None),
        (r#"(?U)a*"#, None),
        (r#"a|"#, None),
        (r#"|a"#, None),
        (r#"||"#, None),
        (r#"(|)"#, None),
        (
            r#"(a|b"#,
            Some(r#"error parsing regexp: missing closing ): `(a|b`"#),
        ),
        (
            r#"a|b)"#,
            Some(r#"error parsing regexp: unexpected ): `a|b)`"#),
        ),
        (
            r#"((a)"#,
            Some(r#"error parsing regexp: missing closing ): `((a)`"#),
        ),
        (
            r#"(a))"#,
            Some(r#"error parsing regexp: unexpected ): `(a))`"#),
        ),
        (r#"^abc$"#, None),
        (r#"é"#, None),
        (
            r#"[é-a]"#,
            Some(r#"error parsing regexp: invalid character class range: `é-a`"#),
        ),
        (
            r#"\é"#,
            Some(r#"error parsing regexp: invalid escape sequence: `\é`"#),
        ),
        (r#"."#, None),
        (r#"a.*b"#, None),
        (r#"[a-z]+@[a-z]+"#, None),
        (r#"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"#, None),
        (r#"(?i)\p{Greek}+"#, None),
        (
            r#"x{2}{3}"#,
            Some(r#"error parsing regexp: invalid nested repetition operator: `{2}{3}`"#),
        ),
        (r#"(x{2}){3}"#, None),
        (
            r#"(((((((((((a{2}){2}){2}){2}){2}){2}){2}){2}){2}){2}){2})"#,
            Some(r#"error parsing regexp: invalid repeat count: `{2}`"#),
        ),
        (r#"a{0}"#, None),
        (r#"a{0,0}"#, None),
        (
            r#"(a{0}){100000}"#,
            Some(r#"error parsing regexp: invalid repeat count: `{100000}`"#),
        ),
        (r#"[a-b-c]"#, None),
        (r#"[a-]"#, None),
        (r#"[-a]"#, None),
        (r#"[a\-z]"#, None),
        (r#"[\d-z]"#, None),
        (r#"(?P<n>a)(?P<n>b)"#, None),
        (r#"\_"#, None),
        (r#"\-"#, None),
        (r#"\ "#, None),
        (r#"\Qabc\Edef"#, None),
    ];

    #[test]
    fn compile_matches_go_regexp_compile() {
        for (pattern, want) in GO_OUTCOMES {
            let got = compile(pattern).err().map(|e| e.to_string());
            assert_eq!(got.as_deref(), *want, "pattern {pattern:?}");
        }
    }

    /// `checkSize`/`checkHeight` (parse.go) outcomes recorded under Go 1.25.9.
    #[test]
    fn size_and_height_limits_match_go() {
        let nest = |n: usize| format!("{}a{}", "(".repeat(n), ")".repeat(n));
        let cases: Vec<(String, Option<&str>)> = vec![
            (nest(1100), Some(ERR_NESTING_DEPTH)),
            (nest(999), None),
            (nest(400), None),
            (format!("{}a{}", "(?:".repeat(1100), ")".repeat(1100)), None),
            ("a{1000}".repeat(3300), None),
            ("a{1000}".repeat(3400), Some(ERR_LARGE)),
            ("(a{1000}|b{1000})".repeat(1700), Some(ERR_LARGE)),
            ("(a{1000}|b{1000})".repeat(1600), None),
            ("[a-z]{1000}".repeat(3400), Some(ERR_LARGE)),
            ("x*".repeat(1_700_000), Some(ERR_LARGE)),
            ("x*".repeat(1_200_000), None),
            ("(((a*)*)*)*".repeat(1000), None),
            (format!("{}b", "a|".repeat(5000)), None),
        ];
        for (pattern, want) in cases {
            let got = compile(&pattern).err().map(|e| e.code);
            assert_eq!(got, want, "pattern of {} bytes", pattern.len());
        }
    }
}
