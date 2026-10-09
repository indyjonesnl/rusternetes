//! Port of apimachinery's `diff.Diff` (the non-`usegocmp` build):
//! `staging/src/k8s.io/apimachinery/pkg/util/diff/diff.go:35-56`
//! (release-1.35) — a unified diff, context 3, of the two objects'
//! `json.MarshalIndent(obj, "", " ")` — over `github.com/pmezard/go-difflib`
//! `difflib.go` (`SplitLines` :768, `SequenceMatcher` :103-452,
//! `WriteUnifiedDiff` :559, `formatRangeUnified` :515).
//!
//! Go marshals a struct in declaration order, which `serde_json::Value`
//! (sorted keys) cannot express, so callers build a [`GoJson`] tree in the
//! order Go would emit.

use std::collections::HashMap;

/// An ordered JSON value, rendered exactly as Go's
/// `json.MarshalIndent(v, "", " ")` does (one-space indent, `"k": v`, empty
/// object `{}` and empty array `[]`).
#[derive(Debug, Clone, PartialEq)]
pub enum GoJson {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<GoJson>),
    Object(Vec<(String, GoJson)>),
}

impl GoJson {
    pub fn str(s: impl Into<String>) -> Self {
        GoJson::String(s.into())
    }

    /// `*string` field: `null` when absent.
    pub fn opt_str(s: &Option<String>) -> Self {
        s.as_ref().map_or(GoJson::Null, |v| GoJson::str(v.clone()))
    }

    /// Go `map[string]string` (keys sorted by `encoding/json`); `None` is a
    /// nil map, i.e. `null`.
    pub fn string_map<'a>(m: Option<impl IntoIterator<Item = (&'a String, &'a String)>>) -> Self {
        match m {
            None => GoJson::Null,
            Some(it) => {
                let mut v: Vec<(String, GoJson)> = it
                    .into_iter()
                    .map(|(k, v)| (k.clone(), GoJson::str(v.clone())))
                    .collect();
                v.sort_by(|a, b| a.0.cmp(&b.0));
                GoJson::Object(v)
            }
        }
    }

    /// Render like `json.MarshalIndent(v, "", " ")`.
    pub fn marshal_indent(&self) -> String {
        let mut out = String::new();
        self.write(&mut out, 0);
        out
    }

    fn write(&self, out: &mut String, depth: usize) {
        match self {
            GoJson::Null => out.push_str("null"),
            GoJson::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            GoJson::Number(n) => out.push_str(n),
            GoJson::String(s) => out.push_str(&go_quote(s)),
            GoJson::Array(a) if a.is_empty() => out.push_str("[]"),
            GoJson::Array(a) => {
                out.push('[');
                for (i, v) in a.iter().enumerate() {
                    out.push_str(if i == 0 { "\n" } else { ",\n" });
                    out.push_str(&" ".repeat(depth + 1));
                    v.write(out, depth + 1);
                }
                out.push('\n');
                out.push_str(&" ".repeat(depth));
                out.push(']');
            }
            GoJson::Object(o) if o.is_empty() => out.push_str("{}"),
            GoJson::Object(o) => {
                out.push('{');
                for (i, (k, v)) in o.iter().enumerate() {
                    out.push_str(if i == 0 { "\n" } else { ",\n" });
                    out.push_str(&" ".repeat(depth + 1));
                    out.push_str(&go_quote(k));
                    out.push_str(": ");
                    v.write(out, depth + 1);
                }
                out.push('\n');
                out.push_str(&" ".repeat(depth));
                out.push('}');
            }
        }
    }
}

/// `encoding/json` string quoting with HTML escaping on (`<`, `>`, `&`,
/// U+2028/9 become `\u00XX`), which `json.MarshalIndent` uses.
fn go_quote(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('"');
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            '\t' => o.push_str("\\t"),
            '<' | '>' | '&' | '\u{2028}' | '\u{2029}' => {
                o.push_str(&format!("\\u{:04x}", c as u32))
            }
            c if (c as u32) < 0x20 => o.push_str(&format!("\\u{:04x}", c as u32)),
            c => o.push(c),
        }
    }
    o.push('"');
    o
}

/// `diff.Diff(a, b)` over two already-marshalled documents
/// (`GetUnifiedDiffString` with `Context: 3` and no file names).
pub fn diff(a: &GoJson, b: &GoJson) -> String {
    unified_diff(
        &split_lines(&a.marshal_indent()),
        &split_lines(&b.marshal_indent()),
        3,
    )
}

/// `difflib.SplitLines`: split after each `\n`, then append `\n` to the last
/// element (:768-772).
pub fn split_lines(s: &str) -> Vec<String> {
    let mut lines: Vec<String> = s.split_inclusive('\n').map(str::to_string).collect();
    if s.is_empty() || s.ends_with('\n') {
        lines.push(String::new());
    }
    let last = lines.len() - 1;
    lines[last].push('\n');
    lines
}

#[derive(Clone, Copy, Debug)]
struct Match {
    a: usize,
    b: usize,
    size: usize,
}

#[derive(Clone, Copy, Debug)]
struct OpCode {
    tag: u8,
    i1: usize,
    i2: usize,
    j1: usize,
    j2: usize,
}

/// `difflib.SequenceMatcher` with `autoJunk` on and no `IsJunk`.
struct SequenceMatcher<'a> {
    a: &'a [String],
    b: &'a [String],
    b2j: HashMap<&'a str, Vec<usize>>,
}

impl<'a> SequenceMatcher<'a> {
    fn new(a: &'a [String], b: &'a [String]) -> Self {
        // chainB (:150-190)
        let mut b2j: HashMap<&str, Vec<usize>> = HashMap::new();
        for (i, s) in b.iter().enumerate() {
            b2j.entry(s.as_str()).or_default().push(i);
        }
        let n = b.len();
        if n >= 200 {
            let ntest = n / 100 + 1;
            b2j.retain(|_, v| v.len() <= ntest);
        }
        SequenceMatcher { a, b, b2j }
    }

    /// `findLongestMatch` (:215-290); with no junk only the first pair of
    /// extension loops can fire (they extend over "popular" lines).
    fn find_longest_match(&self, alo: usize, ahi: usize, blo: usize, bhi: usize) -> Match {
        let (mut besti, mut bestj, mut bestsize) = (alo, blo, 0usize);
        let mut j2len: HashMap<usize, usize> = HashMap::new();
        for i in alo..ahi {
            let mut newj2len: HashMap<usize, usize> = HashMap::new();
            if let Some(js) = self.b2j.get(self.a[i].as_str()) {
                for &j in js {
                    if j < blo {
                        continue;
                    }
                    if j >= bhi {
                        break;
                    }
                    let k = if j == 0 {
                        0
                    } else {
                        j2len.get(&(j - 1)).copied().unwrap_or(0)
                    } + 1;
                    newj2len.insert(j, k);
                    if k > bestsize {
                        besti = i + 1 - k;
                        bestj = j + 1 - k;
                        bestsize = k;
                    }
                }
            }
            j2len = newj2len;
        }
        while besti > alo && bestj > blo && self.a[besti - 1] == self.b[bestj - 1] {
            besti -= 1;
            bestj -= 1;
            bestsize += 1;
        }
        while besti + bestsize < ahi
            && bestj + bestsize < bhi
            && self.a[besti + bestsize] == self.b[bestj + bestsize]
        {
            bestsize += 1;
        }
        Match {
            a: besti,
            b: bestj,
            size: bestsize,
        }
    }

    /// `GetMatchingBlocks` (:299-352).
    fn matching_blocks(&self) -> Vec<Match> {
        let mut matched = Vec::new();
        self.match_blocks(0, self.a.len(), 0, self.b.len(), &mut matched);
        let mut non_adjacent = Vec::new();
        let (mut i1, mut j1, mut k1) = (0usize, 0usize, 0usize);
        for m in &matched {
            if i1 + k1 == m.a && j1 + k1 == m.b {
                k1 += m.size;
            } else {
                if k1 > 0 {
                    non_adjacent.push(Match {
                        a: i1,
                        b: j1,
                        size: k1,
                    });
                }
                i1 = m.a;
                j1 = m.b;
                k1 = m.size;
            }
        }
        if k1 > 0 {
            non_adjacent.push(Match {
                a: i1,
                b: j1,
                size: k1,
            });
        }
        non_adjacent.push(Match {
            a: self.a.len(),
            b: self.b.len(),
            size: 0,
        });
        non_adjacent
    }

    fn match_blocks(&self, alo: usize, ahi: usize, blo: usize, bhi: usize, out: &mut Vec<Match>) {
        let m = self.find_longest_match(alo, ahi, blo, bhi);
        if m.size > 0 {
            if alo < m.a && blo < m.b {
                self.match_blocks(alo, m.a, blo, m.b, out);
            }
            out.push(m);
            if m.a + m.size < ahi && m.b + m.size < bhi {
                self.match_blocks(m.a + m.size, ahi, m.b + m.size, bhi, out);
            }
        }
    }

    /// `GetOpCodes` (:358-390).
    fn op_codes(&self) -> Vec<OpCode> {
        let (mut i, mut j) = (0usize, 0usize);
        let mut out = Vec::new();
        for m in self.matching_blocks() {
            let tag = if i < m.a && j < m.b {
                b'r'
            } else if i < m.a {
                b'd'
            } else if j < m.b {
                b'i'
            } else {
                0
            };
            if tag > 0 {
                out.push(OpCode {
                    tag,
                    i1: i,
                    i2: m.a,
                    j1: j,
                    j2: m.b,
                });
            }
            i = m.a + m.size;
            j = m.b + m.size;
            if m.size > 0 {
                out.push(OpCode {
                    tag: b'e',
                    i1: m.a,
                    i2: i,
                    j1: m.b,
                    j2: j,
                });
            }
        }
        out
    }

    /// `GetGroupedOpCodes` (:413-452).
    fn grouped_op_codes(&self, n: usize) -> Vec<Vec<OpCode>> {
        let mut codes = self.op_codes();
        if codes.is_empty() {
            codes.push(OpCode {
                tag: b'e',
                i1: 0,
                i2: 1,
                j1: 0,
                j2: 1,
            });
        }
        if codes[0].tag == b'e' {
            let c = codes[0];
            codes[0] = OpCode {
                tag: c.tag,
                i1: c.i1.max(c.i2.saturating_sub(n)),
                i2: c.i2,
                j1: c.j1.max(c.j2.saturating_sub(n)),
                j2: c.j2,
            };
        }
        let last = codes.len() - 1;
        if codes[last].tag == b'e' {
            let c = codes[last];
            codes[last] = OpCode {
                tag: c.tag,
                i1: c.i1,
                i2: c.i2.min(c.i1 + n),
                j1: c.j1,
                j2: c.j2.min(c.j1 + n),
            };
        }
        let nn = n + n;
        let mut groups = Vec::new();
        let mut group: Vec<OpCode> = Vec::new();
        for c in codes {
            let (mut i1, i2, mut j1, j2) = (c.i1, c.i2, c.j1, c.j2);
            if c.tag == b'e' && i2 - i1 > nn {
                group.push(OpCode {
                    tag: c.tag,
                    i1,
                    i2: i2.min(i1 + n),
                    j1,
                    j2: j2.min(j1 + n),
                });
                groups.push(std::mem::take(&mut group));
                i1 = i1.max(i2.saturating_sub(n));
                j1 = j1.max(j2.saturating_sub(n));
            }
            group.push(OpCode {
                tag: c.tag,
                i1,
                i2,
                j1,
                j2,
            });
        }
        if !group.is_empty() && !(group.len() == 1 && group[0].tag == b'e') {
            groups.push(group);
        }
        groups
    }
}

fn format_range_unified(start: usize, stop: usize) -> String {
    let mut beginning = start + 1;
    let length = stop - start;
    if length == 1 {
        return beginning.to_string();
    }
    if length == 0 {
        beginning -= 1;
    }
    format!("{beginning},{length}")
}

/// `difflib.GetUnifiedDiffString` with no file names (so no `---`/`+++`
/// header, :588) and the given context.
pub fn unified_diff(a: &[String], b: &[String], context: usize) -> String {
    let m = SequenceMatcher::new(a, b);
    let mut out = String::new();
    for g in m.grouped_op_codes(context) {
        let (first, last) = (g[0], g[g.len() - 1]);
        out.push_str(&format!(
            "@@ -{} +{} @@\n",
            format_range_unified(first.i1, last.i2),
            format_range_unified(first.j1, last.j2)
        ));
        for c in g {
            if c.tag == b'e' {
                for l in &a[c.i1..c.i2] {
                    out.push(' ');
                    out.push_str(l);
                }
                continue;
            }
            if c.tag == b'r' || c.tag == b'd' {
                for l in &a[c.i1..c.i2] {
                    out.push('-');
                    out.push_str(l);
                }
            }
            if c.tag == b'r' || c.tag == b'i' {
                for l in &b[c.j1..c.j2] {
                    out.push('+');
                    out.push_str(l);
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(a: &str, b: &str) -> String {
        unified_diff(&split_lines(a), &split_lines(b), 3)
    }

    #[test]
    fn identical_is_empty() {
        assert_eq!(d("a\nb", "a\nb"), "");
    }

    #[test]
    fn split_lines_appends_newline_to_last() {
        assert_eq!(split_lines("a\nb"), vec!["a\n", "b\n"]);
        assert_eq!(split_lines(""), vec!["\n"]);
    }

    #[test]
    fn single_line_change_uses_short_range() {
        // difflib/python: `@@ -1 +1 @@`
        assert_eq!(d("a", "b"), "@@ -1 +1 @@\n-a\n+b\n");
    }

    #[test]
    fn context_of_three_and_hunk_split() {
        let a: Vec<String> = (0..20).map(|i| format!("l{i}")).collect();
        let mut b = a.clone();
        b[1] = "X".into();
        b[18] = "Y".into();
        let got = d(&a.join("\n"), &b.join("\n"));
        let want = "@@ -1,5 +1,5 @@\n l0\n-l1\n+X\n l2\n l3\n l4\n@@ -16,5 +16,5 @@\n l15\n l16\n l17\n-l18\n+Y\n l19\n";
        assert_eq!(got, want);
    }

    #[test]
    fn insertion_and_deletion() {
        assert_eq!(d("a\nb", "a\nx\nb"), "@@ -1,2 +1,3 @@\n a\n+x\n b\n");
        assert_eq!(d("a\nx\nb", "a\nb"), "@@ -1,3 +1,2 @@\n a\n-x\n b\n");
    }

    #[test]
    fn marshal_indent_matches_go() {
        let v = GoJson::Object(vec![
            ("B".into(), GoJson::Array(vec![])),
            ("A".into(), GoJson::Array(vec![GoJson::str("x<y")])),
            ("C".into(), GoJson::Object(vec![])),
            ("D".into(), GoJson::Null),
        ]);
        assert_eq!(
            v.marshal_indent(),
            "{\n \"B\": [],\n \"A\": [\n  \"x\\u003cy\"\n ],\n \"C\": {},\n \"D\": null\n}"
        );
    }
}
