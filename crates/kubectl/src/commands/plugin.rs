use anyhow::Result;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

/// Provides utilities for interacting with plugins.
///
/// Equivalent to:
///   kubectl plugin list
///
/// Searches PATH for executables named kubectl-* and lists them.
pub fn execute_list() -> Result<()> {
    let path_var = std::env::var("PATH").unwrap_or_default();
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

            // Check if it's a file (not a directory)
            let metadata = match fs::metadata(&path) {
                Ok(m) => m,
                Err(_) => continue,
            };

            if !metadata.is_file() {
                continue;
            }

            // Check if it's executable
            let permissions = metadata.permissions();
            let is_executable = permissions.mode() & 0o111 != 0;

            if is_executable {
                // Check for duplicates (same plugin name in earlier PATH entry)
                let plugin_name = name.to_string();
                let is_duplicate = found_plugins.iter().any(|p| {
                    p.file_name()
                        .map(|f| f.to_string_lossy() == plugin_name)
                        .unwrap_or(false)
                });

                if is_duplicate {
                    warnings.push(format!(
                        "warning: {} is overshadowed by a similarly named plugin: {}",
                        path.display(),
                        found_plugins
                            .iter()
                            .find(|p| p
                                .file_name()
                                .map(|f| f.to_string_lossy() == plugin_name)
                                .unwrap_or(false))
                            .map(|p| p.display().to_string())
                            .unwrap_or_default()
                    ));
                } else {
                    found_plugins.push(path);
                }
            } else {
                warnings.push(format!(
                    "warning: {} identified as a kubectl plugin, but it is not executable",
                    path.display()
                ));
            }
        }
    }

    if found_plugins.is_empty() && warnings.is_empty() {
        eprintln!("error: unable to find any kubectl plugins in your PATH");
        std::process::exit(1);
    }

    // Print found plugins
    for plugin in &found_plugins {
        println!("{}", plugin.display());
    }

    // Print warnings to stderr
    if !warnings.is_empty() {
        for warning in &warnings {
            eprintln!("{}", warning);
        }
    }

    Ok(())
}
