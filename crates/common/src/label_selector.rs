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
    requirements: Vec<LabelRequirement>,
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
pub(crate) enum Token {
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
pub(crate) struct Lexer<'a> {
    s: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    #[cfg(test)]
    pub(crate) fn new(s: &'a str) -> Self {
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
    pub(crate) fn lex(&mut self) -> (Token, String) {
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
    key: String,
    /// Operator
    operator: LabelOperator,
    /// Values (for In/NotIn/Equals/NotEquals/Gt/Lt operators)
    values: Option<Vec<String>>,
}

impl LabelRequirement {
    /// Port of `NewRequirement` (selector.go:171-222). Returns the aggregated
    /// field-error message (upstream `allErrs.ToAggregate()`) on failure.
    /// The unsupported-operator arm is unreachable: the operator is a closed enum.
    pub fn new(key: &str, op: LabelOperator, vals: Vec<String>) -> Result<Self, String> {
        let mut all_errs: Vec<FieldError> = Vec::new();
        let path = Path::default();
        if let Err(e) = validate_label_key(key, &path.child("key")) {
            all_errs.push(e);
        }
        let value_path = path.child("values");
        match op {
            LabelOperator::In | LabelOperator::NotIn => {
                if vals.is_empty() {
                    all_errs.push(FieldError::invalid(
                        &value_path,
                        BadValue::from(vals.clone()),
                        "for 'in', 'notin' operators, values set can't be empty",
                    ));
                }
            }
            LabelOperator::Equals | LabelOperator::DoubleEquals | LabelOperator::NotEquals => {
                if vals.len() != 1 {
                    all_errs.push(FieldError::invalid(
                        &value_path,
                        BadValue::from(vals.clone()),
                        "exact-match compatibility requires one single value",
                    ));
                }
            }
            LabelOperator::Exists | LabelOperator::DoesNotExist => {
                if !vals.is_empty() {
                    all_errs.push(FieldError::invalid(
                        &value_path,
                        BadValue::from(vals.clone()),
                        "values set must be empty for exists and does not exist",
                    ));
                }
            }
            LabelOperator::GreaterThan | LabelOperator::LessThan => {
                if vals.len() != 1 {
                    all_errs.push(FieldError::invalid(
                        &value_path,
                        BadValue::from(vals.clone()),
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

    #[cfg(test)]
    pub(crate) fn new_for_test(
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_display_does_not_panic_on_missing_values() {
        // A LabelRequirement built without values (e.g. via a code path that
        // bypasses the parser) used to panic in the Display impl. Now it
        // renders as an empty value string.
        for op in [
            LabelOperator::Equals,
            LabelOperator::NotEquals,
            LabelOperator::In,
            LabelOperator::NotIn,
        ] {
            let req = LabelRequirement::new_for_test("app", op, None);
            let sel = LabelSelector {
                requirements: vec![req],
            };
            let _ = format!("{}", sel); // must not panic
        }
    }

    #[test]
    fn test_parse_equality_simple() {
        let selector = LabelSelector::parse("app=nginx").unwrap();
        assert_eq!(selector.requirements.len(), 1);
        assert_eq!(selector.requirements[0].key, "app");
        assert_eq!(selector.requirements[0].operator, LabelOperator::Equals);
        assert_eq!(
            selector.requirements[0].values,
            Some(vec!["nginx".to_string()])
        );
    }

    #[test]
    fn test_parse_equality_double_equals() {
        let selector = LabelSelector::parse("app==nginx").unwrap();
        assert_eq!(
            selector.requirements[0].operator,
            LabelOperator::DoubleEquals
        );
    }

    #[test]
    fn test_parse_not_equals() {
        let selector = LabelSelector::parse("tier!=backend").unwrap();
        assert_eq!(selector.requirements[0].operator, LabelOperator::NotEquals);
        assert_eq!(
            selector.requirements[0].values,
            Some(vec!["backend".to_string()])
        );
    }

    #[test]
    fn test_parse_multiple_equality() {
        let selector = LabelSelector::parse("app=nginx,tier=frontend").unwrap();
        assert_eq!(selector.requirements.len(), 2);
        assert_eq!(selector.requirements[0].key, "app");
        assert_eq!(selector.requirements[1].key, "tier");
    }

    #[test]
    fn test_parse_in_operator() {
        let selector = LabelSelector::parse("environment in (production, qa)").unwrap();
        assert_eq!(selector.requirements.len(), 1);
        assert_eq!(selector.requirements[0].key, "environment");
        assert_eq!(selector.requirements[0].operator, LabelOperator::In);
        assert_eq!(
            selector.requirements[0].values,
            Some(vec!["production".to_string(), "qa".to_string()])
        );
    }

    #[test]
    fn test_parse_notin_operator() {
        let selector = LabelSelector::parse("tier notin (frontend,backend)").unwrap();
        assert_eq!(selector.requirements.len(), 1);
        assert_eq!(selector.requirements[0].operator, LabelOperator::NotIn);
        assert_eq!(
            selector.requirements[0].values,
            Some(vec!["backend".to_string(), "frontend".to_string()])
        );
    }

    #[test]
    fn test_parse_exists() {
        let selector = LabelSelector::parse("hasFeature").unwrap();
        assert_eq!(selector.requirements.len(), 1);
        assert_eq!(selector.requirements[0].key, "hasFeature");
        assert_eq!(selector.requirements[0].operator, LabelOperator::Exists);
        assert_eq!(selector.requirements[0].values, None);
    }

    #[test]
    fn test_parse_does_not_exist() {
        let selector = LabelSelector::parse("!deprecated").unwrap();
        assert_eq!(selector.requirements.len(), 1);
        assert_eq!(selector.requirements[0].key, "deprecated");
        assert_eq!(
            selector.requirements[0].operator,
            LabelOperator::DoesNotExist
        );
    }

    #[test]
    fn test_parse_mixed() {
        let selector =
            LabelSelector::parse("app=nginx,environment in (production, qa),tier!=backend")
                .unwrap();
        assert_eq!(selector.requirements.len(), 3);
        assert_eq!(selector.requirements[0].operator, LabelOperator::Equals);
        assert_eq!(selector.requirements[1].operator, LabelOperator::In);
        assert_eq!(selector.requirements[2].operator, LabelOperator::NotEquals);
    }

    #[test]
    fn test_matches_equality() {
        let selector = LabelSelector::parse("app=nginx").unwrap();
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "nginx".to_string());

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_not_matches_equality() {
        let selector = LabelSelector::parse("app=nginx").unwrap();
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "apache".to_string());

        assert!(!selector.matches(&labels));
    }

    #[test]
    fn test_matches_not_equals() {
        let selector = LabelSelector::parse("tier!=backend").unwrap();
        let mut labels = HashMap::new();
        labels.insert("tier".to_string(), "frontend".to_string());

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_matches_in() {
        let selector = LabelSelector::parse("environment in (production, qa)").unwrap();
        let mut labels = HashMap::new();
        labels.insert("environment".to_string(), "production".to_string());

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_not_matches_in() {
        let selector = LabelSelector::parse("environment in (production, qa)").unwrap();
        let mut labels = HashMap::new();
        labels.insert("environment".to_string(), "staging".to_string());

        assert!(!selector.matches(&labels));
    }

    #[test]
    fn test_matches_notin() {
        let selector = LabelSelector::parse("tier notin (frontend, backend)").unwrap();
        let mut labels = HashMap::new();
        labels.insert("tier".to_string(), "middleware".to_string());

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_matches_exists() {
        let selector = LabelSelector::parse("hasFeature").unwrap();
        let mut labels = HashMap::new();
        labels.insert("hasFeature".to_string(), "true".to_string());

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_not_matches_exists() {
        let selector = LabelSelector::parse("hasFeature").unwrap();
        let labels = HashMap::new();

        assert!(!selector.matches(&labels));
    }

    #[test]
    fn test_matches_does_not_exist() {
        let selector = LabelSelector::parse("!deprecated").unwrap();
        let labels = HashMap::new();

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_not_matches_does_not_exist() {
        let selector = LabelSelector::parse("!deprecated").unwrap();
        let mut labels = HashMap::new();
        labels.insert("deprecated".to_string(), "true".to_string());

        assert!(!selector.matches(&labels));
    }

    #[test]
    fn test_matches_multiple_all_match() {
        let selector = LabelSelector::parse("app=nginx,tier=frontend").unwrap();
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "nginx".to_string());
        labels.insert("tier".to_string(), "frontend".to_string());

        assert!(selector.matches(&labels));
    }

    #[test]
    fn test_matches_multiple_one_fails() {
        let selector = LabelSelector::parse("app=nginx,tier=frontend").unwrap();
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "nginx".to_string());
        labels.insert("tier".to_string(), "backend".to_string());

        assert!(!selector.matches(&labels));
    }

    #[test]
    fn test_matches_resource() {
        let selector = LabelSelector::parse("app=nginx").unwrap();
        let resource = json!({
            "metadata": {
                "labels": {
                    "app": "nginx",
                    "tier": "frontend"
                }
            }
        });

        assert!(selector.matches_resource(&resource));
    }

    #[test]
    fn test_empty_selector_matches_all() {
        let selector = LabelSelector::parse("").unwrap();
        let labels = HashMap::new();

        assert!(selector.matches(&labels));
        assert!(selector.is_empty());
    }

    #[test]
    fn test_display_equality() {
        let selector = LabelSelector::parse("app=nginx").unwrap();
        assert_eq!(format!("{}", selector), "app=nginx");
    }

    #[test]
    fn test_display_in() {
        let selector = LabelSelector::parse("environment in (production, qa)").unwrap();
        assert_eq!(format!("{}", selector), "environment in (production,qa)");
    }

    #[test]
    fn test_display_exists() {
        let selector = LabelSelector::parse("hasFeature").unwrap();
        assert_eq!(format!("{}", selector), "hasFeature");
    }

    #[test]
    fn test_display_does_not_exist() {
        let selector = LabelSelector::parse("!deprecated").unwrap();
        assert_eq!(format!("{}", selector), "!deprecated");
    }

    // ---- Ported from upstream selector_test.go (release-1.35) ----

    fn parse_err(s: &str) -> String {
        LabelSelector::parse(s).unwrap_err().to_string()
    }

    /// selector_test.go TestSelectorParse (:36-70).
    #[test]
    fn upstream_test_selector_parse() {
        for good in [
            "x=a,y=b,z=c",
            "",
            "x!=a,y=b",
            "x=",
            "x= ",
            "x=,z= ",
            "x= ,z= ",
            "!x",
            "x>1",
            "x>1,z<5",
        ] {
            let sel =
                LabelSelector::parse(good).unwrap_or_else(|e| panic!("{good:?} should parse: {e}"));
            assert_eq!(good.replace(' ', ""), sel.to_string(), "restring {good:?}");
        }
        for bad in ["x=a||y=b", "x==a==b", "!x=a", "x<a"] {
            assert!(LabelSelector::parse(bad).is_err(), "{bad:?} must fail");
        }
    }

    /// The issue's own examples (#2181).
    #[test]
    fn issue_2181_malformed_selectors_rejected() {
        assert_eq!(
            parse_err("!!!"),
            "unable to parse requirement: found '!', expected: identifier"
        );
        assert_eq!(
            parse_err("app in ("),
            "unable to parse requirement: found '', expected: ',', ')' or identifier"
        );
    }

    /// selector_test.go TestLexer (:193-225).
    #[test]
    fn upstream_test_lexer() {
        let cases = [
            ("", Token::EndOfString),
            (",", Token::Comma),
            ("notin", Token::NotIn),
            ("in", Token::In),
            ("=", Token::Equals),
            ("==", Token::DoubleEquals),
            (">", Token::GreaterThan),
            ("<", Token::LessThan),
            ("!", Token::DoesNotExist),
            ("!=", Token::NotEquals),
            ("(", Token::OpenPar),
            (")", Token::ClosedPar),
            ("~", Token::Identifier),
            ("||", Token::Identifier),
        ];
        for (s, t) in cases {
            let (tok, lit) = Lexer::new(s).lex();
            assert_eq!(tok, t, "token for {s:?}");
            assert_eq!(lit, s, "literal for {s:?}");
        }
    }

    /// selector_test.go TestLexerSequence (:227-261).
    #[test]
    fn upstream_test_lexer_sequence() {
        use Token::*;
        let cases: Vec<(&str, Vec<Token>)> = vec![
            (
                "key in ( value )",
                vec![Identifier, In, OpenPar, Identifier, ClosedPar],
            ),
            (
                "key notin ( value )",
                vec![Identifier, NotIn, OpenPar, Identifier, ClosedPar],
            ),
            (
                "key in ( value1, value2 )",
                vec![
                    Identifier, In, OpenPar, Identifier, Comma, Identifier, ClosedPar,
                ],
            ),
            ("key", vec![Identifier]),
            ("!key", vec![DoesNotExist, Identifier]),
            ("()", vec![OpenPar, ClosedPar]),
            (
                "x in (),y",
                vec![Identifier, In, OpenPar, ClosedPar, Comma, Identifier],
            ),
            (
                "== != (), = notin",
                vec![
                    DoubleEquals,
                    NotEquals,
                    OpenPar,
                    ClosedPar,
                    Comma,
                    Equals,
                    NotIn,
                ],
            ),
            ("key>2", vec![Identifier, GreaterThan, Identifier]),
        ];
        for (s, expected) in cases {
            let mut l = Lexer::new(s);
            let mut got = Vec::new();
            loop {
                let (tok, _) = l.lex();
                if tok == EndOfString {
                    break;
                }
                got.push(tok);
            }
            assert_eq!(got, expected, "sequence for {s:?}");
        }
    }

    type Want = (&'static str, LabelOperator, Option<Vec<&'static str>>);

    /// selector_test.go TestSetSelectorParser (:604-690).
    #[test]
    fn upstream_test_set_selector_parser() {
        use LabelOperator::*;
        let rows: Vec<(&str, bool, Vec<Want>)> = vec![
            ("", true, vec![]),
            ("\rx", true, vec![("x", Exists, None)]),
            (
                "this-is-a-dns.domain.com/key-with-dash",
                true,
                vec![("this-is-a-dns.domain.com/key-with-dash", Exists, None)],
            ),
            (
                "this-is-another-dns.domain.com/key-with-dash in (so,what)",
                true,
                vec![(
                    "this-is-another-dns.domain.com/key-with-dash",
                    In,
                    Some(vec!["so", "what"]),
                )],
            ),
            (
                "0.1.2.domain/99 notin (10.10.100.1, tick.tack.clock)",
                true,
                vec![(
                    "0.1.2.domain/99",
                    NotIn,
                    Some(vec!["10.10.100.1", "tick.tack.clock"]),
                )],
            ),
            (
                "foo  in\t (abc)",
                true,
                vec![("foo", In, Some(vec!["abc"]))],
            ),
            (
                "x notin\n (abc)",
                true,
                vec![("x", NotIn, Some(vec!["abc"]))],
            ),
            (
                "x  notin\t\t(abc,def)",
                true,
                vec![("x", NotIn, Some(vec!["abc", "def"]))],
            ),
            (
                "x in (abc,def)",
                true,
                vec![("x", In, Some(vec!["abc", "def"]))],
            ),
            ("x in (abc,)", true, vec![("x", In, Some(vec!["", "abc"]))]),
            ("x in (abc,abc)", true, vec![("x", In, Some(vec!["abc"]))]),
            ("x in ()", true, vec![("x", In, Some(vec![""]))]),
            ("x in (a,,)", true, vec![("x", In, Some(vec!["", "a"]))]),
            ("x in (a,,,)", true, vec![("x", In, Some(vec!["", "a"]))]),
            ("x in (a,,,,,,)", true, vec![("x", In, Some(vec!["", "a"]))]),
            (
                "x in (a,,a,,a,,a,,)",
                true,
                vec![("x", In, Some(vec!["", "a"]))],
            ),
            (
                "x notin (abc,,def),bar,z in (),w",
                true,
                vec![
                    ("bar", Exists, None),
                    ("w", Exists, None),
                    ("x", NotIn, Some(vec!["", "abc", "def"])),
                    ("z", In, Some(vec![""])),
                ],
            ),
            (
                "x,y in (a)",
                true,
                vec![("x", Exists, None), ("y", In, Some(vec!["a"]))],
            ),
            ("x=a", true, vec![("x", Equals, Some(vec!["a"]))]),
            ("x>1", true, vec![("x", GreaterThan, Some(vec!["1"]))]),
            ("x<7", true, vec![("x", LessThan, Some(vec!["7"]))]),
            (
                "x=a,y!=b",
                true,
                vec![
                    ("x", Equals, Some(vec!["a"])),
                    ("y", NotEquals, Some(vec!["b"])),
                ],
            ),
            (
                "x=a,y!=b,z in (h,i,j)",
                true,
                vec![
                    ("x", Equals, Some(vec!["a"])),
                    ("y", NotEquals, Some(vec!["b"])),
                    ("z", In, Some(vec!["h", "i", "j"])),
                ],
            ),
            ("x=a||y=b", false, vec![]),
            ("x,,y", false, vec![]),
            (",x,y", false, vec![]),
            ("x nott in (y)", false, vec![]),
            ("x notin ( )", true, vec![("x", NotIn, Some(vec![""]))]),
            (
                "x notin (, a)",
                true,
                vec![("x", NotIn, Some(vec!["", "a"]))],
            ),
            ("a in (xyz),", false, vec![]),
            ("a in (xyz)b notin ()", false, vec![]),
            ("a ", true, vec![("a", Exists, None)]),
            (
                "a in (x,y,notin, z,in)",
                true,
                vec![("a", In, Some(vec!["in", "notin", "x", "y", "z"]))],
            ),
            ("a in (xyz abc)", false, vec![]),
            ("a notin(", false, vec![]),
            ("a (", false, vec![]),
            ("(", false, vec![]),
        ];
        for (input, valid, expected) in rows {
            match LabelSelector::parse(input) {
                Err(e) => assert!(!valid, "Parse({input:?}) => {e}, expected no error"),
                Ok(sel) => {
                    assert!(valid, "Parse({input:?}) => {sel}, expected error");
                    let got: Vec<_> = sel
                        .requirements()
                        .iter()
                        .map(|r| {
                            (
                                r.key().to_string(),
                                r.operator(),
                                r.values().map(|v| v.to_vec()),
                            )
                        })
                        .collect();
                    let want: Vec<_> = expected
                        .into_iter()
                        .map(|(k, o, v)| {
                            (
                                k.to_string(),
                                o,
                                v.map(|v| v.into_iter().map(String::from).collect::<Vec<_>>()),
                            )
                        })
                        .collect();
                    assert_eq!(got, want, "Parse({input:?})");
                }
            }
        }
    }

    /// selector_test.go TestRequirementConstructor (:327-507).
    #[test]
    fn upstream_test_requirement_constructor() {
        use LabelOperator::*;
        let v = |xs: &[&str]| xs.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let long = "a".repeat(254);
        let cases: Vec<(String, LabelOperator, Vec<String>, Option<&str>)> = vec![
            ("x1".into(), In, v(&[]), Some("values: Invalid value: []")),
            (
                "x2".into(),
                NotIn,
                v(&[]),
                Some("values: Invalid value: []"),
            ),
            ("x3".into(), In, v(&["foo"]), None),
            ("x4".into(), NotIn, v(&["foo"]), None),
            (
                "x5".into(),
                Equals,
                v(&["bar", "foo"]),
                Some("values: Invalid value: [\"bar\",\"foo\"]"),
            ),
            ("x6".into(), Exists, v(&[]), None),
            ("x7".into(), DoesNotExist, v(&[]), None),
            (
                "x8".into(),
                Exists,
                v(&["foo"]),
                Some("values: Invalid value: [\"foo\"]"),
            ),
            ("x11".into(), GreaterThan, v(&["1"]), None),
            ("x12".into(), LessThan, v(&["6"]), None),
            (
                "x13".into(),
                GreaterThan,
                v(&[]),
                Some("values: Invalid value: []"),
            ),
            (
                "x14".into(),
                GreaterThan,
                v(&["bar"]),
                Some("values[0]: Invalid value: \"bar\""),
            ),
            (
                "x15".into(),
                LessThan,
                v(&["bar"]),
                Some("values[0]: Invalid value: \"bar\""),
            ),
            (
                long.clone(),
                Exists,
                v(&[]),
                Some("key: Invalid value: \"aaaa"),
            ),
            (
                "x16".into(),
                Equals,
                vec![long.clone()],
                Some("values[0][x16]: Invalid value: \"aaaa"),
            ),
            (
                "x17".into(),
                Equals,
                v(&["a b"]),
                Some("values[0][x17]: Invalid value: \"a b\""),
            ),
        ];
        for (key, op, vals, want) in cases {
            let got = LabelRequirement::new(&key, op, vals);
            match want {
                None => assert!(got.is_ok(), "{key}: {got:?}"),
                Some(prefix) => {
                    let e = got.expect_err(&key);
                    assert!(
                        e.starts_with(prefix),
                        "{key}: got {e:?}, want prefix {prefix:?}"
                    );
                }
            }
        }
    }

    /// Requirement.Matches for Gt/Lt (selector.go:257-298).
    #[test]
    fn gt_lt_matching() {
        let sel = LabelSelector::parse("x>5,y<3").unwrap();
        let m = |x: &str, y: &str| {
            let mut l = HashMap::new();
            l.insert("x".to_string(), x.to_string());
            l.insert("y".to_string(), y.to_string());
            sel.matches(&l)
        };
        assert!(m("6", "2"));
        assert!(!m("5", "2"));
        assert!(!m("6", "3"));
        assert!(!m("abc", "2"));
    }

    /// Operator list in the parseOperator error (selector.go:39-43,794).
    #[test]
    fn parse_operator_error_lists_binary_operators() {
        assert_eq!(
            parse_err("x ("),
            "unable to parse requirement: found '(', expected: in, notin, =, ==, !=, gt, lt"
        );
    }

    /// Requirement.String forms: `==` kept, set values sorted.
    #[test]
    fn requirement_string_forms() {
        assert_eq!(LabelSelector::parse("x==a").unwrap().to_string(), "x==a");
        assert_eq!(
            LabelSelector::parse("x notin (b,a)").unwrap().to_string(),
            "x notin (a,b)"
        );
        assert_eq!(LabelSelector::parse("!x").unwrap().to_string(), "!x");
    }
}
