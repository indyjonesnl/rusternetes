use crate::types::KubercCommands;
use anyhow::Result;

/// KubeRC is an alpha feature for user-level kubectl configuration.
///
/// Equivalent to:
///   kubectl kuberc
///
/// KubeRC allows users to configure kubectl behavior via a
/// configuration file (~/.kube/kuberc) including default command
/// flags, aliases, and overrides.
pub fn execute(subcommand: Option<&KubercCommands>) -> Result<()> {
    match subcommand {
        Some(KubercCommands::Set { property, value }) => {
            if let (Some(prop), Some(val)) = (property, value) {
                eprintln!("kuberc set {} = {}", prop, val);
                eprintln!("kuberc is an alpha feature and is not enabled by default.");
            } else {
                eprintln!("Usage: kubectl kuberc set <property> <value>");
            }
        }
        Some(KubercCommands::View {}) => {
            eprintln!("kuberc view");
            eprintln!("kuberc is an alpha feature and is not enabled by default.");
            eprintln!();
            eprintln!("No kuberc configuration found.");
            eprintln!("To use kuberc, set the KUBECTL_KUBERC environment variable to 'true'");
            eprintln!("and create a configuration file at ~/.kube/kuberc.");
        }
        None => {
            eprintln!("kuberc is an alpha feature and is not enabled by default.");
            eprintln!();
            eprintln!("To use kuberc, set the KUBECTL_KUBERC environment variable to 'true'");
            eprintln!("and create a configuration file at ~/.kube/kuberc.");
            eprintln!();
            eprintln!("For more information, see:");
            eprintln!("  https://kubernetes.io/docs/reference/kubectl/kuberc/");
        }
    }

    Ok(())
}
