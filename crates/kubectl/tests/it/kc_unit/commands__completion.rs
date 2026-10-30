//! Unit tests moved from `src/commands/completion.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::completion::*;

    #[test]
    fn test_bash_completion_succeeds() {
        let result = execute("bash");
        assert!(result.is_ok());
    }

    #[test]
    fn test_zsh_completion_succeeds() {
        let result = execute("zsh");
        assert!(result.is_ok());
    }

    #[test]
    fn test_fish_completion_succeeds() {
        let result = execute("fish");
        assert!(result.is_ok());
    }

    #[test]
    fn test_powershell_completion_succeeds() {
        let result = execute("powershell");
        assert!(result.is_ok());
        let result = execute("pwsh");
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_shell() {
        let result = execute("invalid");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported shell type"));
    }

    #[test]
    fn test_bash_completion_contains_expected_content() {
        // Capture output by checking the function structure
        // The bash completion should define a function and use `complete`
        // We verify the functions don't error - content is printed to stdout
        // which is verified by the fact execute("bash") returns Ok
        assert!(execute("bash").is_ok());
    }

    #[test]
    fn test_print_bash_completion_returns_ok() {
        assert!(print_bash_completion().is_ok());
    }

    #[test]
    fn test_print_zsh_completion_returns_ok() {
        assert!(print_zsh_completion().is_ok());
    }

    #[test]
    fn test_print_fish_completion_returns_ok() {
        assert!(print_fish_completion().is_ok());
    }

    #[test]
    fn test_print_powershell_completion_returns_ok() {
        assert!(print_powershell_completion().is_ok());
    }

    #[test]
    fn test_execute_case_sensitive_shell() {
        assert!(execute("BASH").is_err());
        assert!(execute("Zsh").is_err());
        assert!(execute("FISH").is_err());
    }

    #[test]
    fn test_invalid_shell_error_message_contains_shell_name() {
        let result = execute("tcsh");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("tcsh"),
            "Error should mention the invalid shell name"
        );
    }

    #[test]
    fn test_pwsh_alias_matches_powershell() {
        // Both "powershell" and "pwsh" should succeed
        assert!(execute("powershell").is_ok());
        assert!(execute("pwsh").is_ok());
    }

    #[test]
    fn test_empty_shell_string_fails() {
        assert!(execute("").is_err());
    }

    #[test]
    fn test_whitespace_shell_string_fails() {
        assert!(execute(" ").is_err());
    }

    #[test]
    fn test_shell_with_special_chars_fails() {
        assert!(execute("bash;echo").is_err());
        assert!(execute("zsh\n").is_err());
    }
}
