//! Unit tests moved from `src/commands/help.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use clap::CommandFactory;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::help::*;

    #[derive(clap::Parser)]
    #[command(name = "test-cli")]
    #[command(about = "A test CLI")]
    struct TestCli {
        #[command(subcommand)]
        command: TestCommands,
    }

    #[derive(clap::Subcommand)]
    enum TestCommands {
        /// Get resources
        Get,
        /// Delete resources
        Delete,
    }

    #[test]
    fn test_help_no_subcommand() {
        // Calling with None should print general help without error
        let result = execute::<TestCli>(None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_help_valid_subcommand() {
        let result = execute::<TestCli>(Some("get"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_help_valid_subcommand_delete() {
        let result = execute::<TestCli>(Some("delete"));
        assert!(result.is_ok());
    }
}
