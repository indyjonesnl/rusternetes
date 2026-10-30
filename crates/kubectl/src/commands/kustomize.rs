use anyhow::Result;
use std::process::Command;

/// Build a set of KRM resources using a kustomization.yaml file.
///
/// Equivalent to:
///   kubectl kustomize <dir>
///   kubectl kustomize <url>
pub fn execute(dir: &str) -> Result<()> {
    // Check if the kustomize binary is available
    let kustomize_result = Command::new("kustomize").arg("build").arg(dir).output();

    match kustomize_result {
        Ok(output) => {
            if output.status.success() {
                print!("{}", String::from_utf8_lossy(&output.stdout));
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                eprintln!("{}", stderr);
                std::process::exit(output.status.code().unwrap_or(1));
            }
        }
        Err(_) => {
            // kustomize binary not found, print an informative message
            eprintln!("error: kustomize is not installed or not found in PATH");
            eprintln!();
            eprintln!("To use 'kubectl kustomize', install kustomize:");
            eprintln!("  https://kubectl.docs.kubernetes.io/installation/kustomize/");
            std::process::exit(1);
        }
    }

    Ok(())
}
