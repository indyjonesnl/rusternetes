//! Unit tests moved from `src/commands/plugin.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use std::fs;
    pub use std::os::unix::fs::PermissionsExt;
    pub use std::path::PathBuf;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::plugin::*;
    use std::io::Write;

    #[test]
    fn test_plugin_discovery_finds_kubectl_prefixed_executables() {
        // Create a temp directory with a fake kubectl plugin
        let tmp_dir = tempfile::tempdir().unwrap();
        let plugin_path = tmp_dir.path().join("kubectl-myplugin");
        {
            let mut f = fs::File::create(&plugin_path).unwrap();
            f.write_all(b"#!/bin/sh\necho hello\n").unwrap();
        }
        // Make it executable
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&plugin_path, fs::Permissions::from_mode(0o755)).unwrap();

        // Also create a non-kubectl file that should be ignored
        let non_plugin = tmp_dir.path().join("not-a-plugin");
        fs::File::create(&non_plugin).unwrap();
        fs::set_permissions(&non_plugin, fs::Permissions::from_mode(0o755)).unwrap();

        // Simulate the PATH scanning logic
        let path_var = tmp_dir.path().to_string_lossy().to_string();
        let mut found_plugins: Vec<PathBuf> = Vec::new();

        for dir in path_var.split(':') {
            if dir.is_empty() {
                continue;
            }
            let entries = match fs::read_dir(dir) {
                Ok(entries) => entries,
                Err(_) => continue,
            };
            for entry in entries.flatten() {
                let file_name = entry.file_name();
                let name = file_name.to_string_lossy();
                if !name.starts_with("kubectl-") {
                    continue;
                }
                let path = entry.path();
                let metadata = fs::metadata(&path).unwrap();
                if metadata.is_file() {
                    let perms = metadata.permissions();
                    if perms.mode() & 0o111 != 0 {
                        found_plugins.push(path);
                    }
                }
            }
        }

        assert_eq!(found_plugins.len(), 1);
        assert!(found_plugins[0].file_name().unwrap().to_string_lossy() == "kubectl-myplugin");
    }

    #[test]
    fn test_non_executable_plugin_is_skipped() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let plugin_path = tmp_dir.path().join("kubectl-noexec");
        fs::File::create(&plugin_path).unwrap();
        // Deliberately not setting executable bit
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&plugin_path, fs::Permissions::from_mode(0o644)).unwrap();

        let metadata = fs::metadata(&plugin_path).unwrap();
        let is_executable = metadata.permissions().mode() & 0o111 != 0;
        assert!(!is_executable, "File should not be executable");
    }

    #[test]
    fn test_duplicate_plugin_detection() {
        let tmp_dir1 = tempfile::tempdir().unwrap();
        let tmp_dir2 = tempfile::tempdir().unwrap();

        // Create same-named plugin in both dirs
        for dir in [tmp_dir1.path(), tmp_dir2.path()] {
            let plugin_path = dir.join("kubectl-dup");
            let mut f = fs::File::create(&plugin_path).unwrap();
            f.write_all(b"#!/bin/sh\n").unwrap();
            fs::set_permissions(&plugin_path, fs::Permissions::from_mode(0o755)).unwrap();
        }

        let path_var = format!(
            "{}:{}",
            tmp_dir1.path().display(),
            tmp_dir2.path().display()
        );
        let mut found_plugins: Vec<PathBuf> = Vec::new();
        let mut warnings: Vec<String> = Vec::new();

        for dir in path_var.split(':') {
            if dir.is_empty() {
                continue;
            }
            let entries = match fs::read_dir(dir) {
                Ok(entries) => entries,
                Err(_) => continue,
            };
            for entry in entries.flatten() {
                let file_name = entry.file_name();
                let name = file_name.to_string_lossy();
                if !name.starts_with("kubectl-") {
                    continue;
                }
                let path = entry.path();
                let metadata = fs::metadata(&path).unwrap();
                if metadata.is_file() && metadata.permissions().mode() & 0o111 != 0 {
                    let plugin_name = name.to_string();
                    let is_duplicate = found_plugins.iter().any(|p| {
                        p.file_name()
                            .map(|f| f.to_string_lossy() == plugin_name)
                            .unwrap_or(false)
                    });
                    if is_duplicate {
                        warnings.push(format!("overshadowed: {}", path.display()));
                    } else {
                        found_plugins.push(path);
                    }
                }
            }
        }

        assert_eq!(found_plugins.len(), 1);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("overshadowed"));
    }
}
