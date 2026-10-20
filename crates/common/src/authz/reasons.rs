//! RBAC source descriptions: pkg/registry/rbac/validation/rule.go:139-176.
use super::{Subject, UserInfo};
use std::fmt::Write;
use std::sync::LazyLock;

/// fmt's %q uses strconv.Quote: Go src/strconv/quote.go:68-130,519-522.
/// Rust debug quoting has different control/Unicode escape syntax.
pub(super) fn quoted(value: &str) -> String {
    static PRINTABLE: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"^[\p{L}\p{M}\p{N}\p{P}\p{S} ]$")
            .expect("Go printable character classes")
    });
    let mut output = String::from("\"");
    for c in value.chars() {
        match c {
            '"' | '\\' => {
                output.push('\\');
                output.push(c);
            }
            '\u{7}' => output.push_str("\\a"),
            '\u{8}' => output.push_str("\\b"),
            '\u{c}' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{b}' => output.push_str("\\v"),
            c if (c as u32) < 32 || c == '\u{7f}' => {
                let _ = write!(output, "\\x{:02x}", c as u32);
            }
            c => {
                let mut bytes = [0; 4];
                if PRINTABLE.is_match(c.encode_utf8(&mut bytes)) {
                    output.push(c);
                } else if (c as u32) < 0x10000 {
                    let _ = write!(output, "\\u{:04x}", c as u32);
                } else {
                    let _ = write!(output, "\\U{:08x}", c as u32);
                }
            }
        }
    }
    output.push('"');
    output
}

/// appliesToUser (rule.go:281-303) uses the binding namespace as the default
/// for an unqualified service account; unsupported subject kinds never match.
pub(super) fn applicable_subject<'a>(
    subjects: &'a [Subject],
    user: &UserInfo,
    namespace: &str,
) -> Option<&'a Subject> {
    subjects.iter().find(|subject| match subject.kind.as_str() {
        "User" => subject.name == user.username,
        "Group" => user.groups.contains(&subject.name),
        "ServiceAccount" => {
            let namespace = subject
                .namespace
                .as_deref()
                .filter(|namespace| !namespace.is_empty())
                .unwrap_or(namespace);
            !namespace.is_empty()
                && user.username == format!("system:serviceaccount:{namespace}:{}", subject.name)
        }
        _ => false,
    })
}

pub(super) fn describe_subject(subject: &Subject, binding_namespace: &str) -> String {
    // Keep upstream's NAME/NAMESPACE ordering, even though service-account
    // usernames put namespace before name (rule.go:139-148).
    let name = if subject.kind == "ServiceAccount" {
        format!(
            "{}/{}",
            subject.name,
            subject
                .namespace
                .as_deref()
                .filter(|namespace| !namespace.is_empty())
                .unwrap_or(binding_namespace)
        )
    } else {
        subject.name.clone()
    };
    format!("{} {}", subject.kind, quoted(&name))
}

/// apimachinery/pkg/util/errors/errors.go:70-97: stable, unique messages;
/// only a multiple-message aggregate has square brackets.
pub(super) fn aggregate_errors(errors: &[String]) -> Option<String> {
    let mut unique = Vec::new();
    for error in errors {
        if !unique.contains(error) {
            unique.push(error.clone());
        }
    }
    match unique.len() {
        0 => None,
        1 => unique.pop(),
        _ => Some(format!("[{}]", unique.join(", "))),
    }
}
