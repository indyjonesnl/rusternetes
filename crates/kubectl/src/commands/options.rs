use anyhow::Result;

/// Print the list of flags inherited by all commands.
///
/// Equivalent to:
///   kubectl options
pub fn execute() -> Result<()> {
    println!("The following options can be passed to any command:");
    println!();
    println!("      --kubeconfig='': Path to the kubeconfig file to use for CLI requests.");
    println!("      --context='': The name of the kubeconfig context to use.");
    println!("      --server='': The address and port of the Kubernetes API server.");
    println!("      --insecure-skip-tls-verify=false: If true, the server's certificate will not be checked for validity.");
    println!("      --token='': Bearer token for authentication to the API server.");
    println!("  -n, --namespace='': If present, the namespace scope for this CLI request.");

    Ok(())
}
