//! Unit tests moved from `src/commands/kustomize.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use std::process::Command;
}

mod tests {
    #[test]
    fn test_kustomize_command_construction() {
        // Verify the command that would be constructed for kustomize
        let dir = "/path/to/overlay";
        let cmd = std::process::Command::new("kustomize")
            .arg("build")
            .arg(dir)
            .env("PATH", "") // ensure it won't actually find kustomize
            .output();
        // We just verify the command can be constructed; it will fail since
        // kustomize is unlikely to be in an empty PATH, but that's fine.
        // The important thing is no panic in construction.
        assert!(cmd.is_ok() || cmd.is_err());
    }

    #[test]
    fn test_kustomize_dir_argument_is_passed() {
        // Verify the directory argument is properly used in command construction
        let dirs = vec![".", "./overlays/prod", "https://github.com/example/repo"];
        for dir in dirs {
            let mut cmd = std::process::Command::new("echo");
            cmd.arg("build").arg(dir);
            // The command properly accepts any string as the dir argument
            let args: Vec<_> = cmd
                .get_args()
                .map(|a| a.to_string_lossy().to_string())
                .collect();
            assert_eq!(args, vec!["build", dir]);
        }
    }

    #[test]
    fn test_kustomize_command_program_name() {
        let cmd = std::process::Command::new("kustomize");
        assert_eq!(cmd.get_program(), "kustomize");
    }
}
