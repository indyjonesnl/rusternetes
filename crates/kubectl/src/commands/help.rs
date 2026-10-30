use anyhow::Result;
use clap::CommandFactory;

/// Print help for a specific command or general help.
///
/// Equivalent to:
///   kubectl help
///   kubectl help <command>
pub fn execute<C: CommandFactory>(command: Option<&str>) -> Result<()> {
    let mut cmd = C::command();

    if let Some(subcmd_name) = command {
        // Find the subcommand and print its help
        if let Some(subcmd) = cmd.find_subcommand_mut(subcmd_name) {
            subcmd.print_help()?;
        } else {
            // Try kebab-case conversion (e.g., "cluster-info" -> "ClusterInfo")
            let found = cmd
                .get_subcommands_mut()
                .find(|s| s.get_name() == subcmd_name);
            if let Some(subcmd) = found {
                subcmd.print_help()?;
            } else {
                eprintln!("Unknown command: {}", subcmd_name);
                eprintln!();
                cmd.print_help()?;
                std::process::exit(1);
            }
        }
    } else {
        cmd.print_help()?;
    }

    Ok(())
}
