//! Issue locations: cel-go `checker.go` `location(id)` is
//! `sourceInfo.GetStartLocation(id)` (`common/ast/ast.go`), reported through
//! `Errors.ReportErrorAtID` (`common/errors.go`) and rendered by
//! `Errors.ToDisplayString` / `Error.ToDisplayString` (`common/error.go`):
//! `ERROR: <input>:LINE:COLUMN+1: message`, the source line, and a caret.
//! `Errors.ToDisplayString` sorts the errors by line, then column.

use cel::parser::Parser;
use k3rs_cel_checker::{
    check, Container, EnvSet, EnvType, Features, MapProvider, SourceLocator, Version,
};
use std::sync::Arc;

fn display(src: &str) -> String {
    let (expr, info) = Parser::new()
        .enable_optional_syntax(true)
        .parse_with_source_info(src)
        .unwrap_or_else(|e| panic!("{src}: {e}"));
    let set = EnvSet::must_base_env_set(Version::major_minor(1, 35), &Features::default());
    let mut env = set.env(
        EnvType::NewExpressions,
        Container::default(),
        Arc::new(MapProvider::new()),
    );
    let locator = SourceLocator::new(&info, src);
    let (_, errs) = check(&expr, &mut env, &locator);
    errs.to_display_string(Some(src))
}

#[test]
fn undeclared_reference_is_at_the_identifier() {
    assert_eq!(
        display("foo == 1"),
        "ERROR: <input>:1:1: undeclared reference to 'foo' (in container '')\n | foo == 1\n | ^"
    );
}

/// The call is located at its operator, as in cel-go.
#[test]
fn call_is_at_the_operator() {
    assert_eq!(
        display("1 + \"x\" == 2"),
        "ERROR: <input>:1:3: found no matching overload for '_+_' applied to '(int, string)'\n | 1 + \"x\" == 2\n | ..^"
    );
}

#[test]
fn line_and_column_restart_after_a_newline() {
    assert_eq!(
        display("foo ||\n  bar"),
        "ERROR: <input>:1:1: undeclared reference to 'foo' (in container '')\n | foo ||\n | ^\n\
         ERROR: <input>:2:3: undeclared reference to 'bar' (in container '')\n |   bar\n | ..^"
    );
}

/// cel-go counts characters, the parser records bytes: the multi-byte `é`
/// is one column, and a full-width dot marks it in the indicator line.
#[test]
fn columns_count_characters_not_bytes() {
    assert_eq!(
        display("'é' + 1"),
        "ERROR: <input>:1:5: found no matching overload for '_+_' applied to '(string, int)'\n | 'é' + 1\n | .\u{ff0e}..^"
    );
}

/// `Errors.ToDisplayString` sorts by location, not by the order of reporting
/// (the checker reports the operands before the call that failed).
#[test]
fn errors_are_sorted_by_location() {
    let out = display("1 + 'a' && bar");
    let at: Vec<(usize, usize)> = out
        .lines()
        .filter_map(|l| l.strip_prefix("ERROR: <input>:"))
        .map(|l| {
            let mut p = l.split(':');
            (
                p.next().unwrap().parse().unwrap(),
                p.next().unwrap().parse().unwrap(),
            )
        })
        .collect();
    let mut sorted = at.clone();
    sorted.sort();
    assert_eq!(at, sorted, "{out}");
    assert!(out.contains("found no matching overload"), "{out}");
    assert!(out.contains("undeclared reference to 'bar'"), "{out}");
}
