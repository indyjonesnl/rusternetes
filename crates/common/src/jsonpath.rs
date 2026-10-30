//! Port of the PARSER half of `k8s.io/client-go/util/jsonpath`
//! (`staging/src/k8s.io/client-go/util/jsonpath/parser.go` and `node.go`,
//! release-1.35). Only parsing is ported: callers such as the
//! admissionregistration `validateFieldRef` (`pkg/apis/admissionregistration/
//! validation/validation.go:1292-1295`) need the parse verdict and the exact
//! error text, not evaluation.
//!
//! Mechanism notes:
//! - `Parse` (`jsonpath.go:61-65`) is `parser.Parse(name, text)`; a template is
//!   plain text with `{...}` actions.
//! - Go's `unicode.IsLetter`/`IsDigit`/`strconv.IsPrint` are matched through
//!   the regex crate's Unicode classes (`\p{L}`, `\p{Nd}`, and
//!   `[\p{L}\p{M}\p{N}\p{P}\p{S}]`) rather than std approximations.
//! - Go recursion between `parseInsideAction` and its helpers is expressed as
//!   a loop; the verdicts and messages are the same.

use regex::Regex;
use std::fmt;
use std::sync::OnceLock;

/// `NodeType` + the node payloads (`node.go`). `Display` reproduces each
/// node's Go `String()` so the upstream parser tests can be ported verbatim.
#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    Text(String),
    Array([ParamsEntry; 3]),
    List(ListNode),
    Field(String),
    Identifier(String),
    Filter {
        left: ListNode,
        right: ListNode,
        operator: String,
    },
    Int(i64),
    Float(f64),
    Wildcard,
    Recursive,
    Union(Vec<ListNode>),
    Bool(bool),
}

/// `ListNode` (`node.go:73-86`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ListNode {
    pub nodes: Vec<Node>,
}

/// `ParamsEntry` (`node.go:150-154`).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct ParamsEntry {
    pub value: i64,
    pub known: bool,
    pub derived: bool,
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Node::Text(t) => write!(f, "NodeText: {t}"),
            Node::Array(p) => write!(
                f,
                "NodeArray: [{} {} {}]",
                fmt_entry(&p[0]),
                fmt_entry(&p[1]),
                fmt_entry(&p[2])
            ),
            Node::List(_) => write!(f, "NodeList"),
            Node::Field(v) => write!(f, "NodeField: {v}"),
            Node::Identifier(v) => write!(f, "NodeIdentifier: {v}"),
            Node::Filter { operator, .. } => {
                write!(f, "NodeFilter: NodeList {operator} NodeList")
            }
            Node::Int(i) => write!(f, "NodeInt: {i}"),
            Node::Float(x) => write!(f, "NodeFloat: {x:.6}"),
            Node::Wildcard => write!(f, "NodeWildcard"),
            Node::Recursive => write!(f, "NodeRecursive"),
            Node::Union(_) => write!(f, "NodeUnion"),
            Node::Bool(b) => write!(f, "NodeBool: {b}"),
        }
    }
}

fn fmt_entry(p: &ParamsEntry) -> String {
    format!("{{{} {} {}}}", p.value, p.known, p.derived)
}

/// `Parser` (`parser.go:36-43`).
#[derive(Debug)]
pub struct Parser {
    pub name: String,
    pub root: ListNode,
    input: String,
    pos: usize,
    start: usize,
    width: usize,
}

/// A parse failure; `Display` is the Go `err.Error()` text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError(pub String);

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ParseError {}

type PResult<T> = Result<T, ParseError>;

fn err<T>(msg: impl Into<String>) -> PResult<T> {
    Err(ParseError(msg.into()))
}

const LEFT_DELIM: &str = "{";
const RIGHT_DELIM: &str = "}";
/// `ErrSyntax = errors.New("invalid syntax")` (`parser.go:46`).
const ERR_SYNTAX: &str = "invalid syntax";

fn dict_key_rex() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"^'([^']*)'$").unwrap())
}

/// Go's `\d` is ASCII-only, so `[0-9]` (Rust's `\d` is Unicode).
fn slice_operator_rex() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"^(-?[0-9]*)(:-?[0-9]*)?(:-?[0-9]*)?$").unwrap())
}

fn filter_rex() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"^([^!<>=]+)([!<>=]+)(.+?)$").unwrap())
}

fn letter_rex() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"^\p{L}$").unwrap())
}

fn digit_rex() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"^\p{Nd}$").unwrap())
}

fn print_rex() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"^[\p{L}\p{M}\p{N}\p{P}\p{S}]$").unwrap())
}

fn matches_char(re: &Regex, c: char) -> bool {
    let mut buf = [0u8; 4];
    re.is_match(c.encode_utf8(&mut buf))
}

/// `unicode.IsLetter`.
fn is_letter(c: char) -> bool {
    if c.is_ascii() {
        c.is_ascii_alphabetic()
    } else {
        matches_char(letter_rex(), c)
    }
}

/// `unicode.IsDigit`.
fn is_digit(c: char) -> bool {
    if c.is_ascii() {
        c.is_ascii_digit()
    } else {
        matches_char(digit_rex(), c)
    }
}

/// Go `%#U`: `U+002A '*'` (the quoted form only when `strconv.IsPrint`).
fn sharp_u(c: char) -> String {
    let printable = c == ' ' || matches_char(print_rex(), c);
    if printable {
        format!("U+{:04X} '{c}'", c as u32)
    } else {
        format!("U+{:04X}", c as u32)
    }
}

/// `Parse(name, text)` (`parser.go:54-61`).
pub fn parse(name: &str, text: &str) -> PResult<Parser> {
    let mut p = Parser::new(name);
    p.parse(text)?;
    Ok(p)
}

/// `parseAction` (`parser.go:70-78`).
fn parse_action(name: &str, text: &str) -> PResult<Parser> {
    let mut p = parse(name, &format!("{LEFT_DELIM}{text}{RIGHT_DELIM}"))?;
    match p.root.nodes.first() {
        Some(Node::List(l)) => {
            p.root = l.clone();
            Ok(p)
        }
        _ => err("invalid syntax"),
    }
}

impl Parser {
    /// `NewParser`.
    pub fn new(name: &str) -> Self {
        Parser {
            name: name.to_string(),
            root: ListNode::default(),
            input: String::new(),
            pos: 0,
            start: 0,
            width: 0,
        }
    }

    /// `(*Parser).Parse` (`parser.go:80-85`).
    pub fn parse(&mut self, text: &str) -> PResult<()> {
        self.input = text.to_string();
        self.root = ListNode::default();
        self.pos = 0;
        self.parse_text()
    }

    /// `consumeText`.
    fn consume_text(&mut self) -> String {
        let value = self.input[self.start..self.pos].to_string();
        self.start = self.pos;
        value
    }

    /// `next`; `None` is Go's `eof`.
    fn next(&mut self) -> Option<char> {
        if self.pos >= self.input.len() {
            self.width = 0;
            return None;
        }
        let c = self.input[self.pos..].chars().next().unwrap();
        self.width = c.len_utf8();
        self.pos += self.width;
        Some(c)
    }

    fn peek(&mut self) -> Option<char> {
        let r = self.next();
        self.backup();
        r
    }

    fn backup(&mut self) {
        self.pos -= self.width;
    }

    fn has_prefix(&self, prefix: &str) -> bool {
        self.input[self.pos..].starts_with(prefix)
    }

    /// `parseText` (`parser.go:118-135`) with `parseLeftDelim` folded in.
    fn parse_text(&mut self) -> PResult<()> {
        loop {
            if self.has_prefix(LEFT_DELIM) {
                if self.pos > self.start {
                    let t = self.consume_text();
                    self.root.nodes.push(Node::Text(t));
                }
                // parseLeftDelim (:138-145)
                self.pos += LEFT_DELIM.len();
                self.consume_text();
                let mut cur = ListNode::default();
                self.parse_inside_action(&mut cur)?;
                // parseRightDelim reached: the action list joins the root.
                // (Go appends it before parsing; order in the tree is the same
                // and an error discards the tree either way.)
                self.root.nodes.push(Node::List(cur));
                continue;
            }
            if self.next().is_none() {
                break;
            }
        }
        // Correctly reached EOF.
        if self.pos > self.start {
            let t = self.consume_text();
            self.root.nodes.push(Node::Text(t));
        }
        Ok(())
    }

    /// `parseInsideAction` (`parser.go:147-182`); returns on `}`.
    fn parse_inside_action(&mut self, cur: &mut ListNode) -> PResult<()> {
        loop {
            if self.has_prefix(RIGHT_DELIM) {
                // parseRightDelim (:185-189)
                self.pos += RIGHT_DELIM.len();
                self.consume_text();
                return Ok(());
            }
            if self.has_prefix("[?(") {
                self.parse_filter(cur)?;
                continue;
            }
            if self.has_prefix("..") {
                self.parse_recursive(cur)?;
                continue;
            }

            match self.next() {
                None => return err("unclosed action"),
                Some(r) if is_end_of_line(r) => return err("unclosed action"),
                Some(' ') => {
                    self.consume_text();
                }
                Some('@') | Some('$') => {
                    // the current object, just pass it
                    self.consume_text();
                }
                Some('[') => self.parse_array(cur)?,
                Some(r) if r == '"' || r == '\'' => self.parse_quote(cur, r)?,
                Some('.') => self.parse_field(cur)?,
                Some(r) if r == '+' || r == '-' || is_digit(r) => {
                    self.backup();
                    self.parse_number(cur)?;
                }
                Some(r) if is_alpha_numeric(Some(r)) => {
                    self.backup();
                    self.parse_identifier(cur)?;
                }
                Some(r) => return err(format!("unrecognized character in action: {}", sharp_u(r))),
            }
        }
    }

    /// `parseIdentifier` (`parser.go:192-215`).
    fn parse_identifier(&mut self, cur: &mut ListNode) -> PResult<()> {
        loop {
            let r = self.next();
            if is_terminator(r) {
                self.backup();
                break;
            }
        }
        let value = self.consume_text();
        if value == "true" || value == "false" {
            cur.nodes.push(Node::Bool(value == "true"));
        } else {
            cur.nodes.push(Node::Identifier(value));
        }
        Ok(())
    }

    /// `parseRecursive` (`parser.go:218-229`).
    fn parse_recursive(&mut self, cur: &mut ListNode) -> PResult<()> {
        if matches!(cur.nodes.last(), Some(Node::Recursive)) {
            return err("invalid multiple recursive descent");
        }
        self.pos += "..".len();
        self.consume_text();
        cur.nodes.push(Node::Recursive);
        if is_alpha_numeric(self.peek()) {
            return self.parse_field(cur);
        }
        Ok(())
    }

    /// `parseNumber` (`parser.go:232-256`).
    fn parse_number(&mut self, cur: &mut ListNode) -> PResult<()> {
        let r = self.peek();
        if r == Some('+') || r == Some('-') {
            self.next();
        }
        loop {
            let r = self.next();
            let keep = matches!(r, Some(c) if c == '.' || is_digit(c));
            if !keep {
                self.backup();
                break;
            }
        }
        let value = self.consume_text();
        // strconv.Atoi
        if let Ok(i) = value.parse::<i64>() {
            cur.nodes.push(Node::Int(i));
            return Ok(());
        }
        // strconv.ParseFloat (an out-of-range result is an error in Go)
        if let Ok(d) = value.parse::<f64>() {
            if d.is_finite() {
                cur.nodes.push(Node::Float(d));
                return Ok(());
            }
        }
        err(format!("cannot parse number {value}"))
    }

    /// `parseArray` (`parser.go:259-338`).
    fn parse_array(&mut self, cur: &mut ListNode) -> PResult<()> {
        loop {
            match self.next() {
                None | Some('\n') => return err("unterminated array"),
                Some(']') => break,
                _ => {}
            }
        }
        let text = self.consume_text();
        let mut text = text[1..text.len() - 1].to_string();
        if text == "*" {
            text = ":".to_string();
        }

        // union operator
        let strs: Vec<&str> = text.split(',').collect();
        if strs.len() > 1 {
            let mut union = Vec::new();
            for s in strs {
                let parser = parse_action("union", &format!("[{}]", s.trim_matches(' ')))?;
                union.push(parser.root);
            }
            cur.nodes.push(Node::Union(union));
            return Ok(());
        }

        // dict key
        if let Some(m) = dict_key_rex().captures(&text) {
            let parser = parse_action("arraydict", &format!(".{}", &m[1]))?;
            for node in parser.root.nodes {
                cur.nodes.push(node);
            }
            return Ok(());
        }

        // slice operator
        let Some(m) = slice_operator_rex().captures(&text) else {
            return err(format!("invalid array index {text}"));
        };
        let mut value: Vec<String> = (1..=3)
            .map(|i| m.get(i).map_or("", |g| g.as_str()).to_string())
            .collect();
        let mut params = [ParamsEntry::default(); 3];
        for i in 0..3 {
            if !value[i].is_empty() {
                if i > 0 {
                    value[i] = value[i][1..].to_string();
                }
                if i > 0 && value[i].is_empty() {
                    params[i].known = false;
                } else {
                    params[i].known = true;
                    match value[i].parse::<i64>() {
                        Ok(v) => params[i].value = v,
                        Err(_) => return err(format!("array index {} is not a number", value[i])),
                    }
                }
            } else if i == 1 {
                params[i].known = true;
                params[i].value = params[0].value + 1;
                params[i].derived = true;
            } else {
                params[i].known = false;
                params[i].value = 0;
            }
        }
        cur.nodes.push(Node::Array(params));
        Ok(())
    }

    /// `parseFilter` (`parser.go:341-398`).
    fn parse_filter(&mut self, cur: &mut ListNode) -> PResult<()> {
        self.pos += "[?(".len();
        self.consume_text();
        let mut begin = false;
        let mut end = false;
        let mut pair = '\0';

        loop {
            match self.next() {
                None | Some('\n') => return err("unterminated filter"),
                Some(r) if r == '"' || r == '\'' => {
                    if !begin {
                        // save the paired rune
                        begin = true;
                        pair = r;
                        continue;
                    }
                    // only add when met paired rune
                    if self.input.as_bytes()[self.pos - 2] != b'\\' && r == pair {
                        end = true;
                    }
                }
                // in rightParser below quotes only appear zero or once
                // and must be paired at the beginning and end
                Some(')') if begin == end => break,
                _ => {}
            }
        }
        if self.next() != Some(']') {
            return err("unclosed array expect ]");
        }
        let text = self.consume_text();
        let text = &text[..text.len() - 2];
        match filter_rex().captures(text) {
            None => {
                let parser = parse_action("text", text)?;
                cur.nodes.push(Node::Filter {
                    left: parser.root,
                    right: ListNode::default(),
                    operator: "exists".to_string(),
                });
            }
            Some(m) => {
                let left = parse_action("left", &m[1])?;
                let right = parse_action("right", &m[3])?;
                cur.nodes.push(Node::Filter {
                    left: left.root,
                    right: right.root,
                    operator: m[2].to_string(),
                });
            }
        }
        Ok(())
    }

    /// `parseQuote` (`parser.go:401-421`).
    fn parse_quote(&mut self, cur: &mut ListNode, end: char) -> PResult<()> {
        loop {
            match self.next() {
                None | Some('\n') => return err("unterminated quoted string"),
                // if it's not escape break the Loop
                Some(c) if c == end && self.input.as_bytes()[self.pos - 2] != b'\\' => break,
                _ => {}
            }
        }
        let value = self.consume_text();
        match unquote_extend(value.as_bytes()) {
            Ok(s) => {
                cur.nodes.push(Node::Text(s));
                Ok(())
            }
            Err(e) => err(format!("unquote string {value} error {e}")),
        }
    }

    /// `parseField` (`parser.go:424-435`).
    fn parse_field(&mut self, cur: &mut ListNode) -> PResult<()> {
        self.consume_text();
        while self.advance() {}
        let value = self.consume_text();
        if value == "*" {
            cur.nodes.push(Node::Wildcard);
        } else {
            cur.nodes.push(Node::Field(value.replace('\\', "")));
        }
        Ok(())
    }

    /// `advance` (`parser.go:438-447`).
    fn advance(&mut self) -> bool {
        let r = self.next();
        if r == Some('\\') {
            self.next();
        } else if is_terminator(r) {
            self.backup();
            return false;
        }
        true
    }
}

/// `isTerminator` (`parser.go:450-459`).
fn is_terminator(r: Option<char>) -> bool {
    match r {
        None => true,
        Some(c) => {
            is_space(c)
                || is_end_of_line(c)
                || matches!(c, '.' | ',' | '[' | ']' | '$' | '@' | '{' | '}')
        }
    }
}

fn is_space(c: char) -> bool {
    c == ' ' || c == '\t'
}

fn is_end_of_line(c: char) -> bool {
    c == '\r' || c == '\n'
}

/// `isAlphaNumeric` (`parser.go:472-474`).
fn is_alpha_numeric(r: Option<char>) -> bool {
    matches!(r, Some(c) if c == '_' || is_letter(c) || is_digit(c))
}

/// `UnquoteExtend` (`parser.go:482-518`): `strconv.Unquote` that also accepts
/// single quotes as a string delimiter. Returns Go's error text.
fn unquote_extend(s: &[u8]) -> Result<String, &'static str> {
    let n = s.len();
    if n < 2 {
        return Err(ERR_SYNTAX);
    }
    let quote = s[0];
    if quote != s[n - 1] {
        return Err(ERR_SYNTAX);
    }
    let mut s = &s[1..n - 1];
    if quote != b'"' && quote != b'\'' {
        return Err(ERR_SYNTAX);
    }
    // Is it trivial?  Avoid allocation.
    if !s.contains(&b'\\') && !s.contains(&quote) {
        return Ok(String::from_utf8_lossy(s).into_owned());
    }
    let mut buf: Vec<u8> = Vec::with_capacity(3 * s.len() / 2);
    while !s.is_empty() {
        let (c, multibyte, ss) = unquote_char(s, quote)?;
        s = ss;
        if c < 0x80 || !multibyte {
            buf.push(c as u8);
        } else {
            let ch = char::from_u32(c).ok_or(ERR_SYNTAX)?;
            let mut tmp = [0u8; 4];
            buf.extend_from_slice(ch.encode_utf8(&mut tmp).as_bytes());
        }
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

fn unhex(b: u8) -> Option<u32> {
    (b as char).to_digit(16)
}

/// `strconv.UnquoteChar` (Go stdlib `strconv/quote.go`): decodes the first
/// (possibly escaped) character of `s`, returning (value, multibyte, rest).
fn unquote_char(s: &[u8], quote: u8) -> Result<(u32, bool, &[u8]), &'static str> {
    // easy cases
    if s.is_empty() {
        return Err(ERR_SYNTAX);
    }
    let c = s[0];
    if c == quote && (quote == b'\'' || quote == b'"') {
        return Err(ERR_SYNTAX);
    }
    if c >= 0x80 {
        let st = std::str::from_utf8(&s[..s.len().min(4)])
            .or_else(|e| std::str::from_utf8(&s[..e.valid_up_to()]))
            .map_err(|_| ERR_SYNTAX)?;
        return match st.chars().next() {
            // Go decodes invalid UTF-8 as U+FFFD width 1.
            None => Ok((0xFFFD, true, &s[1..])),
            Some(ch) => Ok((ch as u32, true, &s[ch.len_utf8()..])),
        };
    }
    if c != b'\\' {
        return Ok((c as u32, false, &s[1..]));
    }

    // hard case: c is backslash
    if s.len() <= 1 {
        return Err(ERR_SYNTAX);
    }
    let c = s[1];
    let mut s = &s[2..];
    let mut multibyte = false;
    let value: u32;
    match c {
        b'a' => value = 7,
        b'b' => value = 8,
        b'f' => value = 12,
        b'n' => value = 10,
        b'r' => value = 13,
        b't' => value = 9,
        b'v' => value = 11,
        b'x' | b'u' | b'U' => {
            let n = match c {
                b'x' => 2,
                b'u' => 4,
                _ => 8,
            };
            if s.len() < n {
                return Err(ERR_SYNTAX);
            }
            let mut v: u32 = 0;
            for &b in &s[..n] {
                let x = unhex(b).ok_or(ERR_SYNTAX)?;
                v = (v << 4) | x;
            }
            s = &s[n..];
            if c == b'x' {
                // single-byte string, possibly not UTF-8
                value = v;
            } else {
                // surrogate halves and out-of-range values are invalid
                if (0xD800..0xE000).contains(&v) || v > 0x10FFFF {
                    return Err(ERR_SYNTAX);
                }
                value = v;
                multibyte = true;
            }
        }
        b'0'..=b'7' => {
            let mut v = (c - b'0') as u32;
            if s.len() < 2 {
                return Err(ERR_SYNTAX);
            }
            for &b in &s[..2] {
                let x = (b as u32).wrapping_sub(b'0' as u32);
                if x > 7 {
                    return Err(ERR_SYNTAX);
                }
                v = (v << 3) | x;
            }
            s = &s[2..];
            if v > 255 {
                return Err(ERR_SYNTAX);
            }
            value = v;
        }
        b'\\' => value = b'\\' as u32,
        b'\'' | b'"' => {
            if c != quote {
                return Err(ERR_SYNTAX);
            }
            value = c as u32;
        }
        _ => return Err(ERR_SYNTAX),
    }
    Ok((value, multibyte, s))
}
