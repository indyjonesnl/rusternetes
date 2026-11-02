//! `checker/errors.go` (the message templates of `typeErrors`) and the
//! reporting of `common/errors.go` (`Errors`, `Error.ToDisplayString`).

use crate::types::DType;
use crate::unify::format_signature;

/// `common.Location`: 1-based line, 0-based column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Location {
    pub line: usize,
    pub column: usize,
}

/// Where an expression id sits in the source: `SourceInfo.GetStartLocation`.
/// The `cel` crate exposes this from 0.15 (`Program::source_info`); until
/// then checks run without locations.
pub trait Locator {
    fn location(&self, id: u64) -> Option<Location>;
}

/// A [`Locator`] with no source information.
pub struct NoLocations;

impl Locator for NoLocations {
    fn location(&self, _: u64) -> Option<Location> {
        None
    }
}

/// `common.Error`.
#[derive(Clone, Debug, PartialEq)]
pub struct CheckError {
    pub expr_id: u64,
    pub location: Option<Location>,
    pub message: String,
}

impl CheckError {
    /// `Error.ToDisplayString`: `ERROR: <input>:L:C: msg`, then the source
    /// line and a caret when the source is known.
    pub fn to_display_string(&self, source: Option<&str>) -> String {
        let Some(loc) = self.location else {
            return format!("ERROR: <input>:-1:0: {}", self.message);
        };
        let mut out = format!(
            "ERROR: <input>:{}:{}: {}",
            loc.line,
            loc.column + 1,
            self.message
        );
        if let Some(line) = source.and_then(|s| s.lines().nth(loc.line.saturating_sub(1))) {
            // maxSnippetLength in common/error.go.
            if line.len() <= 1000 {
                let line = line.replace('\t', " ");
                let mut ind = String::new();
                for c in line.chars().take(loc.column) {
                    ind.push_str(if c.len_utf8() > 1 { "\u{ff0e}" } else { "." });
                }
                ind.push_str(
                    if line
                        .chars()
                        .nth(loc.column)
                        .is_some_and(|c| c.len_utf8() > 1)
                    {
                        "\u{ff3e}"
                    } else {
                        "^"
                    },
                );
                out.push_str(&format!("\n | {line}\n | {ind}"));
            }
        }
        out
    }
}

/// `common.Errors` with the checker's `typeErrors` reporters.
#[derive(Clone, Debug, Default)]
pub struct Errors {
    errors: Vec<CheckError>,
    num_errors: usize,
}

/// `maxErrorsToReport` of `common.NewErrors`.
const MAX_ERRORS: usize = 100;

impl Errors {
    pub fn errors(&self) -> &[CheckError] {
        &self.errors
    }

    pub fn is_empty(&self) -> bool {
        self.num_errors == 0
    }

    /// `Errors.ToDisplayString`: the errors, one per line group.
    pub fn to_display_string(&self, source: Option<&str>) -> String {
        let mut parts: Vec<String> = self
            .errors
            .iter()
            .map(|e| e.to_display_string(source))
            .collect();
        if self.num_errors > MAX_ERRORS {
            parts.push(format!(
                "{} more errors were truncated",
                self.num_errors - MAX_ERRORS
            ));
        }
        parts.join("\n")
    }

    fn report(&mut self, id: u64, loc: &dyn Locator, message: String) {
        self.num_errors += 1;
        if self.num_errors > MAX_ERRORS {
            return;
        }
        self.errors.push(CheckError {
            expr_id: id,
            location: loc.location(id),
            message,
        });
    }

    pub(crate) fn field_type_mismatch(
        &mut self,
        id: u64,
        l: &dyn Locator,
        name: &str,
        field: &DType,
        value: &DType,
    ) {
        self.report(
            id,
            l,
            format!(
                "expected type of field '{name}' is '{}' but provided type is '{}'",
                field.name(),
                value.name()
            ),
        );
    }

    /// `incompatibleType`: an internal invariant error. cel-go prints the
    /// expression (`%v`); this port prints the node id only.
    pub(crate) fn incompatible_type(&mut self, id: u64, l: &dyn Locator, old: &DType, new: &DType) {
        self.report(
            id,
            l,
            format!(
                "incompatible type already exists for expression: ({id}) old:{}, new:{}",
                old.name(),
                new.name()
            ),
        );
    }

    pub(crate) fn no_matching_overload(
        &mut self,
        id: u64,
        l: &dyn Locator,
        name: &str,
        args: &[DType],
        is_instance: bool,
    ) {
        let signature = format_signature(args, is_instance);
        self.report(
            id,
            l,
            format!("found no matching overload for '{name}' applied to '{signature}'"),
        );
    }

    pub(crate) fn not_a_comprehension_range(&mut self, id: u64, l: &dyn Locator, t: &DType) {
        self.report(
            id,
            l,
            format!(
                "expression of type '{}' cannot be range of a comprehension (must be list, map, or dynamic)",
                t.name()
            ),
        );
    }

    pub(crate) fn not_an_optional_field_selection_call(
        &mut self,
        id: u64,
        l: &dyn Locator,
        err: &str,
    ) {
        self.report(
            id,
            l,
            format!("unsupported optional field selection: {err}"),
        );
    }

    /// `notAnOptionalFieldSelection`: cel-go prints the field expression
    /// (`%v`), which this port renders from its kind.
    pub(crate) fn not_an_optional_field_selection(
        &mut self,
        id: u64,
        l: &dyn Locator,
        field: &str,
    ) {
        self.report(
            id,
            l,
            format!("unsupported optional field selection: {field}"),
        );
    }

    pub(crate) fn not_a_type(&mut self, id: u64, l: &dyn Locator, type_name: &str) {
        self.report(id, l, format!("'{type_name}' is not a type"));
    }

    pub(crate) fn not_a_message_type(&mut self, id: u64, l: &dyn Locator, type_name: &str) {
        self.report(id, l, format!("'{type_name}' is not a message type"));
    }

    pub(crate) fn type_does_not_support_field_selection(
        &mut self,
        id: u64,
        l: &dyn Locator,
        t: &DType,
    ) {
        self.report(
            id,
            l,
            format!("type '{}' does not support field selection", t.name()),
        );
    }

    pub(crate) fn type_mismatch(
        &mut self,
        id: u64,
        l: &dyn Locator,
        expected: &DType,
        actual: &DType,
    ) {
        self.report(
            id,
            l,
            format!(
                "expected type '{}' but found '{}'",
                expected.name(),
                actual.name()
            ),
        );
    }

    pub(crate) fn undefined_field(&mut self, id: u64, l: &dyn Locator, field: &str) {
        self.report(id, l, format!("undefined field '{field}'"));
    }

    pub(crate) fn undeclared_reference(
        &mut self,
        id: u64,
        l: &dyn Locator,
        container: &str,
        name: &str,
    ) {
        self.report(
            id,
            l,
            format!("undeclared reference to '{name}' (in container '{container}')"),
        );
    }

    pub(crate) fn unexpected_failed_resolution(&mut self, id: u64, l: &dyn Locator, name: &str) {
        self.report(id, l, format!("unexpected failed resolution of '{name}'"));
    }

    pub(crate) fn unexpected_ast_type(&mut self, id: u64, l: &dyn Locator, kind: &str, name: &str) {
        self.report(id, l, format!("unexpected {kind} type: {name}"));
    }
}
