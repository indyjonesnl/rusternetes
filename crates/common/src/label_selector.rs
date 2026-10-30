// Label Selector implementation for server-side filtering
//
// Faithful port of upstream `labels.Parse`
// (staging/src/k8s.io/apimachinery/pkg/labels/selector.go, release-1.35):
// the Lexer (selector.go `Lexer`), the Parser (selector.go `Parser`),
// `NewRequirement` (selector.go:171-222), `Requirement.Matches` (:257-298)
// and `Requirement.String`. Error wording is upstream's verbatim.
//
// Grammar (selector.go Parse doc comment):
//   <selector-syntax>         ::= <requirement> | <requirement> "," <selector-syntax>
//   <requirement>             ::= [!] KEY [ <set-based-restriction> | <exact-match-restriction> ]
//   <set-based-restriction>   ::= "" | <inclusion-exclusion> <value-set>
//   <exact-match-restriction> ::= ["="|"=="|"!="|">"|"<"] VALUE

use crate::validation::field::{BadValue, Error as FieldError, Path};
use crate::validation::metav1::{is_qualified_name, is_valid_label_value};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::fmt;

/// Label selector for filtering resources by labels
#[derive(Debug, Clone, PartialEq)]
pub struct LabelSelector {
    /// Individual selector requirements, sorted by key (upstream `ByKey`).
    pub requirements: Vec<LabelRequirement>,
}

impl LabelSelector {
    /// Parse a label selector string. Port of upstream `labels.Parse`
    /// (selector.go `Parse`/`parse`). Requirements are sorted by key "to
    /// grant deterministic parsing" (selector.go `parse`).
    pub fn parse(selector: &str) -> Result<Self, LabelSelectorError> {
        let mut p = Parser {
            l: Lexer {
                s: selector.as_bytes(),
                pos: 0,
            },
            scanned_items: Vec::new(),
            position: 0,
        };
        let mut requirements = p.parse().map_err(LabelSelectorError::InvalidSelector)?;
        // sort.Sort(ByKey(items))
        requirements.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(Self { requirements })
    }

    /// Check if a resource matches this label selector
    pub fn matches(&self, labels: &HashMap<String, String>) -> bool {
        // internalSelector.Matches: all requirements must match.
        self.requirements.iter().all(|req| req.matches(labels))
    }

    /// Check if a resource (as JSON) matches this label selector
    pub fn matches_resource(&self, resource: &Value) -> bool {
        // Extract labels from metadata.labels
        let labels = resource
            .get("metadata")
            .and_then(|m| m.get("labels"))
            .and_then(|l| l.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect::<HashMap<String, String>>()
            })
            .unwrap_or_default();

        self.matches(&labels)
    }

    /// Get the individual requirements
    pub fn requirements(&self) -> &[LabelRequirement] {
        &self.requirements
    }

    /// Check if this is an empty selector
    pub fn is_empty(&self) -> bool {
        self.requirements.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Lexer (selector.go `Token`, `string2token`, `Lexer`)
// ---------------------------------------------------------------------------

/// Lexer token. Port of upstream `Token`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Token {
    Error,
    EndOfString,
    ClosedPar,
    Comma,
    DoesNotExist,
    DoubleEquals,
    Equals,
    GreaterThan,
    Identifier,
    In,
    LessThan,
    NotEquals,
    NotIn,
    OpenPar,
}

/// Port of `string2token`.
fn string2token(s: &str) -> Option<Token> {
    Some(match s {
        ")" => Token::ClosedPar,
        "," => Token::Comma,
        "!" => Token::DoesNotExist,
        "==" => Token::DoubleEquals,
        "=" => Token::Equals,
        ">" => Token::GreaterThan,
        "in" => Token::In,
        "<" => Token::LessThan,
        "!=" => Token::NotEquals,
        "notin" => Token::NotIn,
        "(" => Token::OpenPar,
        _ => return None,
    })
}

/// Port of `isWhitespace`.
fn is_whitespace(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\r' | b'\n')
}

/// Port of `isSpecialSymbol`.
fn is_special_symbol(ch: u8) -> bool {
    matches!(ch, b'=' | b'!' | b'(' | b')' | b',' | b'>' | b'<')
}

/// Port of upstream `Lexer`.
pub struct Lexer<'a> {
    s: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(s: &'a str) -> Self {
        Self {
            s: s.as_bytes(),
            pos: 0,
        }
    }

    /// `read`: 0 at end of input (as upstream).
    fn read(&mut self) -> u8 {
        if self.pos < self.s.len() {
            let b = self.s[self.pos];
            self.pos += 1;
            b
        } else {
            0
        }
    }

    /// `unread`
    fn unread(&mut self) {
        self.pos -= 1;
    }

    /// `scanIDOrKeyword`
    fn scan_id_or_keyword(&mut self) -> (Token, String) {
        let mut buffer: Vec<u8> = Vec::new();
        loop {
            let ch = self.read();
            if ch == 0 {
                break;
            }
            if is_special_symbol(ch) || is_whitespace(ch) {
                self.unread();
                break;
            }
            buffer.push(ch);
        }
        // Splits only happen at ASCII bytes, so this stays valid UTF-8.
        let s = String::from_utf8_lossy(&buffer).into_owned();
        match string2token(&s) {
            Some(tok) => (tok, s),
            None => (Token::Identifier, s),
        }
    }

    /// `scanSpecialSymbol`
    fn scan_special_symbol(&mut self) -> (Token, String) {
        let mut last: Option<(Token, String)> = None;
        let mut buffer: Vec<u8> = Vec::new();
        loop {
            let ch = self.read();
            if ch == 0 {
                break;
            }
            if is_special_symbol(ch) {
                buffer.push(ch);
                let s = String::from_utf8_lossy(&buffer).into_owned();
                if let Some(token) = string2token(&s) {
                    last = Some((token, s));
                } else if last.is_some() {
                    self.unread();
                    break;
                }
            } else {
                self.unread();
                break;
            }
        }
        match last {
            Some(item) => item,
            None => (
                Token::Error,
                format!(
                    "error expected: keyword found '{}'",
                    String::from_utf8_lossy(&buffer)
                ),
            ),
        }
    }

    /// `skipWhiteSpaces`
    fn skip_white_spaces(&mut self, mut ch: u8) -> u8 {
        while is_whitespace(ch) {
            ch = self.read();
        }
        ch
    }

    /// `Lex`
    pub fn lex(&mut self) -> (Token, String) {
        let c = self.read();
        let ch = self.skip_white_spaces(c);
        if ch == 0 {
            (Token::EndOfString, String::new())
        } else if is_special_symbol(ch) {
            self.unread();
            self.scan_special_symbol()
        } else {
            self.unread();
            self.scan_id_or_keyword()
        }
    }
}

// ---------------------------------------------------------------------------
// Parser (selector.go `Parser`)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum ParserContext {
    KeyAndOperator,
    Values,
}

struct Parser<'a> {
    l: Lexer<'a>,
    scanned_items: Vec<(Token, String)>,
    position: usize,
}

impl Parser<'_> {
    fn item(&self, idx: usize) -> (Token, String) {
        // Upstream indexes scannedItems directly; clamp so a malformed
        // selector can never panic (the last item is always EndOfString).
        match self.scanned_items.get(idx) {
            Some(i) => i.clone(),
            None => (Token::EndOfString, String::new()),
        }
    }

    fn adjust(tok: Token, context: ParserContext) -> Token {
        if context == ParserContext::Values && matches!(tok, Token::In | Token::NotIn) {
            Token::Identifier
        } else {
            tok
        }
    }

    /// `lookahead`
    fn lookahead(&self, context: ParserContext) -> (Token, String) {
        let (tok, lit) = self.item(self.position);
        (Self::adjust(tok, context), lit)
    }

    /// `consume`
    fn consume(&mut self, context: ParserContext) -> (Token, String) {
        self.position += 1;
        let (tok, lit) = self.item(self.position - 1);
        (Self::adjust(tok, context), lit)
    }

    /// `scan`
    fn scan(&mut self) {
        loop {
            let (token, literal) = self.l.lex();
            self.scanned_items.push((token, literal));
            if token == Token::EndOfString {
                break;
            }
        }
    }

    /// `parse`: the left recursive descent algorithm.
    fn parse(&mut self) -> Result<Vec<LabelRequirement>, String> {
        use ParserContext::Values;
        self.scan();

        let mut requirements = Vec::new();
        loop {
            let (tok, lit) = self.lookahead(Values);
            match tok {
                Token::Identifier | Token::DoesNotExist => {
                    let r = self
                        .parse_requirement()
                        .map_err(|e| format!("unable to parse requirement: {e}"))?;
                    requirements.push(r);
                    let (t, l) = self.consume(Values);
                    match t {
                        Token::EndOfString => return Ok(requirements),
                        Token::Comma => {
                            let (t2, l2) = self.lookahead(Values);
                            if t2 != Token::Identifier && t2 != Token::DoesNotExist {
                                return Err(format!(
                                    "found '{l2}', expected: identifier after ','"
                                ));
                            }
                        }
                        _ => {
                            return Err(format!("found '{l}', expected: ',' or 'end of string'"));
                        }
                    }
                }
                Token::EndOfString => return Ok(requirements),
                _ => {
                    return Err(format!(
                        "found '{lit}', expected: !, identifier, or 'end of string'"
                    ));
                }
            }
        }
    }

    /// `parseRequirement`
    fn parse_requirement(&mut self) -> Result<LabelRequirement, String> {
        let (key, operator) = self.parse_key_and_infer_operator()?;
        if let Some(op @ (LabelOperator::Exists | LabelOperator::DoesNotExist)) = operator {
            return LabelRequirement::new(&key, op, Vec::new());
        }
        let operator = self.parse_operator()?;
        let values = match operator {
            LabelOperator::In | LabelOperator::NotIn => self.parse_values()?,
            _ => self.parse_exact_value()?,
        };
        // values.List(): sorted, de-duplicated (sets.String)
        LabelRequirement::new(&key, operator, values.into_iter().collect())
    }

    /// `parseKeyAndInferOperator`: in case no operator '!, in, notin, ==, =,
    /// !=' is found the 'exists' operator is inferred.
    fn parse_key_and_infer_operator(&mut self) -> Result<(String, Option<LabelOperator>), String> {
        use ParserContext::Values;
        let mut operator = None;
        let (mut tok, mut literal) = self.consume(Values);
        if tok == Token::DoesNotExist {
            operator = Some(LabelOperator::DoesNotExist);
            (tok, literal) = self.consume(Values);
        }
        if tok != Token::Identifier {
            return Err(format!("found '{literal}', expected: identifier"));
        }
        validate_label_key(&literal, &Path::default()).map_err(|e| e.to_string())?;
        let (t, _) = self.lookahead(Values);
        if (t == Token::EndOfString || t == Token::Comma)
            && operator != Some(LabelOperator::DoesNotExist)
        {
            operator = Some(LabelOperator::Exists);
        }
        Ok((literal, operator))
    }

    /// `parseOperator`
    fn parse_operator(&mut self) -> Result<LabelOperator, String> {
        let (tok, lit) = self.consume(ParserContext::KeyAndOperator);
        Ok(match tok {
            // DoesNotExistToken shouldn't be here because it's a unary operator
            Token::In => LabelOperator::In,
            Token::Equals => LabelOperator::Equals,
            Token::DoubleEquals => LabelOperator::DoubleEquals,
            Token::GreaterThan => LabelOperator::GreaterThan,
            Token::LessThan => LabelOperator::LessThan,
            Token::NotIn => LabelOperator::NotIn,
            Token::NotEquals => LabelOperator::NotEquals,
            _ => {
                return Err(format!(
                    "found '{lit}', expected: {}",
                    BINARY_OPERATORS.join(", ")
                ));
            }
        })
    }

    /// `parseValues`: the values for set based matching (x,y,z)
    fn parse_values(&mut self) -> Result<BTreeSet<String>, String> {
        use ParserContext::Values;
        let (tok, lit) = self.consume(Values);
        if tok != Token::OpenPar {
            return Err(format!("found '{lit}' expected: '('"));
        }
        let (tok, lit) = self.lookahead(Values);
        match tok {
            Token::Identifier | Token::Comma => {
                let s = self.parse_identifiers_list()?;
                let (tok, _) = self.consume(Values);
                if tok != Token::ClosedPar {
                    return Err(format!("found '{lit}', expected: ')'"));
                }
                Ok(s)
            }
            // handles "()"
            Token::ClosedPar => {
                self.consume(Values);
                Ok(BTreeSet::from([String::new()]))
            }
            _ => Err(format!("found '{lit}', expected: ',', ')' or identifier")),
        }
    }

    /// `parseIdentifiersList`: a (possibly empty) list of comma separated
    /// (possibly empty) identifiers.
    fn parse_identifiers_list(&mut self) -> Result<BTreeSet<String>, String> {
        use ParserContext::Values;
        let mut s = BTreeSet::new();
        loop {
            let (tok, lit) = self.consume(Values);
            match tok {
                Token::Identifier => {
                    s.insert(lit);
                    let (tok2, lit2) = self.lookahead(Values);
                    match tok2 {
                        Token::Comma => continue,
                        Token::ClosedPar => return Ok(s),
                        _ => return Err(format!("found '{lit2}', expected: ',' or ')'")),
                    }
                }
                // handled here since we can have "(,"
                Token::Comma => {
                    if s.is_empty() {
                        s.insert(String::new()); // to handle (,
                    }
                    let (tok2, _) = self.lookahead(Values);
                    if tok2 == Token::ClosedPar {
                        s.insert(String::new()); // to handle ,)
                        return Ok(s);
                    }
                    if tok2 == Token::Comma {
                        s.insert(String::new()); // to handle ,,
                    }
                }
                // it can be operator
                _ => return Err(format!("found '{lit}', expected: ',', or identifier")),
            }
        }
    }

    /// `parseExactValue`: the only value for exact match style
    fn parse_exact_value(&mut self) -> Result<BTreeSet<String>, String> {
        use ParserContext::Values;
        let mut s = BTreeSet::new();
        let (tok, _) = self.lookahead(Values);
        if tok == Token::EndOfString || tok == Token::Comma {
            s.insert(String::new());
            return Ok(s);
        }
        let (tok, lit) = self.consume(Values);
        if tok == Token::Identifier {
            s.insert(lit);
            return Ok(s);
        }
        Err(format!("found '{lit}', expected: identifier"))
    }
}

/// `binaryOperators` (selector.go:39-43), in upstream's order.
const BINARY_OPERATORS: [&str; 7] = ["in", "notin", "=", "==", "!=", "gt", "lt"];

/// `validateLabelKey` (selector.go): `content.IsLabelKey`.
fn validate_label_key(k: &str, path: &Path) -> Result<(), FieldError> {
    let errs = is_qualified_name(k);
    if errs.is_empty() {
        Ok(())
    } else {
        Err(FieldError::invalid(path, k, errs.join("; ")))
    }
}

/// `validateLabelValue` (selector.go): `validation.IsValidLabelValue`.
fn validate_label_value(k: &str, v: &str, path: &Path) -> Result<(), FieldError> {
    let errs = is_valid_label_value(v);
    if errs.is_empty() {
        Ok(())
    } else {
        Err(FieldError::invalid(&path.key(k), v, errs.join("; ")))
    }
}

/// `ErrorList.ToAggregate().Error()`: duplicates (by message) dropped, one
/// message verbatim, several rendered as `[a, b]`.
fn aggregate_message(errs: &[FieldError]) -> String {
    let mut seen: Vec<String> = Vec::new();
    for e in errs {
        let msg = e.to_string();
        if !seen.contains(&msg) {
            seen.push(msg);
        }
    }
    match seen.len() {
        1 => seen.remove(0),
        _ => format!("[{}]", seen.join(", ")),
    }
}

/// A single label selector requirement. Port of upstream `Requirement`.
#[derive(Debug, Clone, PartialEq)]
pub struct LabelRequirement {
    /// Label key
    pub key: String,
    /// Operator
    pub operator: LabelOperator,
    /// Values (for In/NotIn/Equals/NotEquals/Gt/Lt operators)
    pub values: Option<Vec<String>>,
}

impl LabelRequirement {
    /// The error list of `NewRequirement(key, op, vals, field.WithPath(path))`
    /// (selector.go:185-225), before `ToAggregate`: callers that report field
    /// paths (e.g. `nodeaffinity.NewNodeSelector`) need the typed errors.
    pub fn validate_at(
        path: &Path,
        key: &str,
        op: LabelOperator,
        vals: &[String],
    ) -> Vec<FieldError> {
        let mut all_errs: Vec<FieldError> = Vec::new();
        if let Err(e) = validate_label_key(key, &path.child("key")) {
            all_errs.push(e);
        }
        let value_path = path.child("values");
        match op {
            LabelOperator::In | LabelOperator::NotIn => {
                if vals.is_empty() {
                    all_errs.push(FieldError::invalid(
                        &value_path,
                        BadValue::from(vals),
                        "for 'in', 'notin' operators, values set can't be empty",
                    ));
                }
            }
            LabelOperator::Equals | LabelOperator::DoubleEquals | LabelOperator::NotEquals => {
                if vals.len() != 1 {
                    all_errs.push(FieldError::invalid(
                        &value_path,
                        BadValue::from(vals),
                        "exact-match compatibility requires one single value",
                    ));
                }
            }
            LabelOperator::Exists | LabelOperator::DoesNotExist => {
                if !vals.is_empty() {
                    all_errs.push(FieldError::invalid(
                        &value_path,
                        BadValue::from(vals),
                        "values set must be empty for exists and does not exist",
                    ));
                }
            }
            LabelOperator::GreaterThan | LabelOperator::LessThan => {
                if vals.len() != 1 {
                    all_errs.push(FieldError::invalid(
                        &value_path,
                        BadValue::from(vals),
                        "for 'Gt', 'Lt' operators, exactly one value is required",
                    ));
                }
                for (i, v) in vals.iter().enumerate() {
                    if v.parse::<i64>().is_err() {
                        all_errs.push(FieldError::invalid(
                            &value_path.index(i),
                            v.as_str(),
                            "for 'Gt', 'Lt' operators, the value must be an integer",
                        ));
                    }
                }
            }
        }
        for (i, v) in vals.iter().enumerate() {
            if let Err(e) = validate_label_value(key, v, &value_path.index(i)) {
                all_errs.push(e);
            }
        }
        all_errs
    }

    /// Port of `NewRequirement` (selector.go:171-222). Returns the aggregated
    /// field-error message (upstream `allErrs.ToAggregate()`) on failure.
    /// The unsupported-operator arm is unreachable: the operator is a closed enum.
    pub fn new(key: &str, op: LabelOperator, vals: Vec<String>) -> Result<Self, String> {
        let all_errs = Self::validate_at(&Path::default(), key, op, &vals);
        if !all_errs.is_empty() {
            return Err(aggregate_message(&all_errs));
        }
        let values = match op {
            LabelOperator::Exists | LabelOperator::DoesNotExist => None,
            _ => Some(vals),
        };
        Ok(Self {
            key: key.to_string(),
            operator: op,
            values,
        })
    }

    fn has_value(&self, value: &str) -> bool {
        self.values
            .as_ref()
            .is_some_and(|v| v.iter().any(|s| s == value))
    }

    /// Port of `Requirement.Matches` (selector.go:257-298).
    pub fn matches(&self, labels: &HashMap<String, String>) -> bool {
        match self.operator {
            LabelOperator::In | LabelOperator::Equals | LabelOperator::DoubleEquals => {
                labels.get(&self.key).is_some_and(|v| self.has_value(v))
            }
            LabelOperator::NotIn | LabelOperator::NotEquals => {
                labels.get(&self.key).is_none_or(|v| !self.has_value(v))
            }
            LabelOperator::Exists => labels.contains_key(&self.key),
            LabelOperator::DoesNotExist => !labels.contains_key(&self.key),
            LabelOperator::GreaterThan | LabelOperator::LessThan => {
                let Some(ls_value) = labels.get(&self.key).and_then(|v| v.parse::<i64>().ok())
                else {
                    return false;
                };
                let Some(values) = self.values.as_ref().filter(|v| v.len() == 1) else {
                    return false;
                };
                let Ok(r_value) = values[0].parse::<i64>() else {
                    return false;
                };
                (self.operator == LabelOperator::GreaterThan && ls_value > r_value)
                    || (self.operator == LabelOperator::LessThan && ls_value < r_value)
            }
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Get the operator
    pub fn operator(&self) -> LabelOperator {
        self.operator
    }

    /// Get the values
    pub fn values(&self) -> Option<&[String]> {
        self.values.as_deref()
    }

    pub fn new_for_test(
        key: impl Into<String>,
        operator: LabelOperator,
        values: Option<Vec<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            operator,
            values,
        }
    }
}

/// Label selector operators (upstream `selection.Operator`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelOperator {
    /// `=`
    Equals,
    /// `==`
    DoubleEquals,
    /// `!=`
    NotEquals,
    /// `key in (val1, val2)`
    In,
    /// `key notin (val1, val2)`
    NotIn,
    /// Key exists
    Exists,
    /// Key does not exist
    DoesNotExist,
    /// `key>N`
    GreaterThan,
    /// `key<N`
    LessThan,
}

impl fmt::Display for LabelOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LabelOperator::Equals => write!(f, "="),
            LabelOperator::DoubleEquals => write!(f, "=="),
            LabelOperator::NotEquals => write!(f, "!="),
            LabelOperator::In => write!(f, "in"),
            LabelOperator::NotIn => write!(f, "notin"),
            LabelOperator::Exists => write!(f, "exists"),
            LabelOperator::DoesNotExist => write!(f, "!"),
            LabelOperator::GreaterThan => write!(f, "gt"),
            LabelOperator::LessThan => write!(f, "lt"),
        }
    }
}

/// Errors that can occur during label selector operations. The message is
/// upstream's verbatim (e.g. `unable to parse requirement: found '!', ...`).
#[derive(Debug, thiserror::Error)]
pub enum LabelSelectorError {
    #[error("{0}")]
    InvalidSelector(String),
}

impl fmt::Display for LabelRequirement {
    /// Port of `Requirement.String` (selector.go).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.operator == LabelOperator::DoesNotExist {
            f.write_str("!")?;
        }
        f.write_str(&self.key)?;
        match self.operator {
            LabelOperator::Equals => f.write_str("=")?,
            LabelOperator::DoubleEquals => f.write_str("==")?,
            LabelOperator::NotEquals => f.write_str("!=")?,
            LabelOperator::In => f.write_str(" in ")?,
            LabelOperator::NotIn => f.write_str(" notin ")?,
            LabelOperator::GreaterThan => f.write_str(">")?,
            LabelOperator::LessThan => f.write_str("<")?,
            LabelOperator::Exists | LabelOperator::DoesNotExist => return Ok(()),
        }
        let in_set = matches!(self.operator, LabelOperator::In | LabelOperator::NotIn);
        if in_set {
            f.write_str("(")?;
        }
        let vals = self.values.as_deref().unwrap_or(&[]);
        if vals.len() == 1 {
            f.write_str(&vals[0])?;
        } else {
            // safeSort: normalizes value order on output only.
            let mut sorted = vals.to_vec();
            sorted.sort();
            f.write_str(&sorted.join(","))?;
        }
        if in_set {
            f.write_str(")")?;
        }
        Ok(())
    }
}

impl fmt::Display for LabelSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<String> = self.requirements.iter().map(|r| r.to_string()).collect();
        write!(f, "{}", parts.join(","))
    }
}
