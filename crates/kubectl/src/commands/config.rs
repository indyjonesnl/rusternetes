use crate::types::ConfigCommands;
use anyhow::Result;
use rusternetes_client::kubeconfig::{
    Cluster, ClusterEntry, Context, ContextEntry, KubeConfig, User, UserEntry,
};
use std::path::PathBuf;

/// Execute config commands for kubeconfig management
pub async fn execute(command: ConfigCommands, kubeconfig_path: Option<&str>) -> Result<()> {
    let config_path = if let Some(path) = kubeconfig_path {
        PathBuf::from(path)
    } else {
        KubeConfig::default_path()?
    };

    match command {
        ConfigCommands::CurrentContext {} => {
            let config = KubeConfig::load_from_file(&config_path)?;
            println!("{}", config.current_context);
        }
        ConfigCommands::View {
            minify: _,
            flatten: _,
        } => {
            let config = KubeConfig::load_from_file(&config_path)?;
            let yaml = serde_yaml::to_string(&config)?;
            println!("{}", yaml);
        }
        ConfigCommands::UseContext { name } => {
            let mut config = KubeConfig::load_from_file(&config_path)?;
            if !config.contexts.iter().any(|ctx| ctx.name == name) {
                anyhow::bail!("Context '{}' not found in kubeconfig", name);
            }
            config.current_context = name.clone();
            let yaml = serde_yaml::to_string(&config)?;
            std::fs::write(&config_path, yaml)?;
            println!("Switched to context \"{}\"", name);
        }
        ConfigCommands::GetContexts { output: _ } => {
            let config = KubeConfig::load_from_file(&config_path)?;
            println!("CURRENT   NAME                CLUSTER             AUTHINFO");
            for ctx in &config.contexts {
                let current = if ctx.name == config.current_context {
                    "*"
                } else {
                    " "
                };
                println!(
                    "{}         {:<20} {:<20} {}",
                    current, ctx.name, ctx.context.cluster, ctx.context.user
                );
            }
        }
        ConfigCommands::GetClusters {} => {
            let config = KubeConfig::load_from_file(&config_path)?;
            println!("NAME");
            for cluster in &config.clusters {
                println!("{}", cluster.name);
            }
        }
        ConfigCommands::GetUsers {} => {
            let config = KubeConfig::load_from_file(&config_path)?;
            println!("NAME");
            for user in &config.users {
                println!("{}", user.name);
            }
        }
        ConfigCommands::Set { property, value } => {
            let mut config = KubeConfig::load_from_file(&config_path)?;
            let parts: Vec<&str> = property.split('.').collect();
            match parts.as_slice() {
                ["current-context"] => {
                    config.current_context = value.clone();
                    println!("Property \"current-context\" set");
                }
                ["contexts", ctx_name, "namespace"] => {
                    let mut found = false;
                    for ctx in &mut config.contexts {
                        if ctx.name == *ctx_name {
                            ctx.context.namespace = value.clone();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        anyhow::bail!("Context '{}' not found", ctx_name);
                    }
                    println!("Property \"contexts.{}.namespace\" set", ctx_name);
                }
                ["contexts", ctx_name, "cluster"] => {
                    let mut found = false;
                    for ctx in &mut config.contexts {
                        if ctx.name == *ctx_name {
                            ctx.context.cluster = value.clone();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        anyhow::bail!("Context '{}' not found", ctx_name);
                    }
                    println!("Property \"contexts.{}.cluster\" set", ctx_name);
                }
                ["contexts", ctx_name, "user"] => {
                    let mut found = false;
                    for ctx in &mut config.contexts {
                        if ctx.name == *ctx_name {
                            ctx.context.user = value.clone();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        anyhow::bail!("Context '{}' not found", ctx_name);
                    }
                    println!("Property \"contexts.{}.user\" set", ctx_name);
                }
                ["clusters", cluster_name, "server"] => {
                    let mut found = false;
                    for cluster in &mut config.clusters {
                        if cluster.name == *cluster_name {
                            cluster.cluster.server = value.clone();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        anyhow::bail!("Cluster '{}' not found", cluster_name);
                    }
                    println!("Property \"clusters.{}.server\" set", cluster_name);
                }
                _ => {
                    anyhow::bail!("Unsupported property path: {}. Supported: current-context, contexts.<name>.namespace, contexts.<name>.cluster, contexts.<name>.user, clusters.<name>.server", property);
                }
            }
            let yaml = serde_yaml::to_string(&config)?;
            std::fs::write(&config_path, yaml)?;
        }
        ConfigCommands::Unset { property } => {
            let mut config = KubeConfig::load_from_file(&config_path)?;
            let parts: Vec<&str> = property.split('.').collect();
            match parts.as_slice() {
                ["current-context"] => {
                    config.current_context = String::new();
                    println!("Property \"current-context\" unset");
                }
                ["contexts", ctx_name, "namespace"] => {
                    let mut found = false;
                    for ctx in &mut config.contexts {
                        if ctx.name == *ctx_name {
                            ctx.context.namespace = String::new();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        anyhow::bail!("Context '{}' not found", ctx_name);
                    }
                    println!("Property \"contexts.{}.namespace\" unset", ctx_name);
                }
                ["contexts", ctx_name, "cluster"] => {
                    let mut found = false;
                    for ctx in &mut config.contexts {
                        if ctx.name == *ctx_name {
                            ctx.context.cluster = String::new();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        anyhow::bail!("Context '{}' not found", ctx_name);
                    }
                    println!("Property \"contexts.{}.cluster\" unset", ctx_name);
                }
                ["contexts", ctx_name, "user"] => {
                    let mut found = false;
                    for ctx in &mut config.contexts {
                        if ctx.name == *ctx_name {
                            ctx.context.user = String::new();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        anyhow::bail!("Context '{}' not found", ctx_name);
                    }
                    println!("Property \"contexts.{}.user\" unset", ctx_name);
                }
                ["clusters", cluster_name, "server"] => {
                    let mut found = false;
                    for cluster in &mut config.clusters {
                        if cluster.name == *cluster_name {
                            cluster.cluster.server = String::new();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        anyhow::bail!("Cluster '{}' not found", cluster_name);
                    }
                    println!("Property \"clusters.{}.server\" unset", cluster_name);
                }
                _ => {
                    anyhow::bail!(
                        "Unsupported property path for unset: {}. Supported: current-context, contexts.<name>.namespace, contexts.<name>.cluster, contexts.<name>.user, clusters.<name>.server",
                        property
                    );
                }
            }
            let yaml = serde_yaml::to_string(&config)?;
            std::fs::write(&config_path, yaml)?;
        }
        ConfigCommands::DeleteCluster { name } => {
            let mut config = KubeConfig::load_from_file(&config_path)?;
            let initial_len = config.clusters.len();
            config.clusters.retain(|c| c.name != name);
            if config.clusters.len() == initial_len {
                anyhow::bail!(
                    "cannot delete cluster \"{}\", not in {}",
                    name,
                    config_path.display()
                );
            }
            let yaml = serde_yaml::to_string(&config)?;
            std::fs::write(&config_path, yaml)?;
            println!(
                "deleted cluster \"{}\" from {}",
                name,
                config_path.display()
            );
        }
        ConfigCommands::DeleteContext { name } => {
            let mut config = KubeConfig::load_from_file(&config_path)?;
            let initial_len = config.contexts.len();
            config.contexts.retain(|c| c.name != name);
            if config.contexts.len() == initial_len {
                anyhow::bail!(
                    "cannot delete context \"{}\", not in {}",
                    name,
                    config_path.display()
                );
            }
            let yaml = serde_yaml::to_string(&config)?;
            std::fs::write(&config_path, yaml)?;
            println!(
                "deleted context \"{}\" from {}",
                name,
                config_path.display()
            );
        }
        ConfigCommands::DeleteUser { name } => {
            let mut config = KubeConfig::load_from_file(&config_path)?;
            let initial_len = config.users.len();
            config.users.retain(|u| u.name != name);
            if config.users.len() == initial_len {
                anyhow::bail!(
                    "cannot delete user \"{}\", not in {}",
                    name,
                    config_path.display()
                );
            }
            let yaml = serde_yaml::to_string(&config)?;
            std::fs::write(&config_path, yaml)?;
            println!("deleted user \"{}\" from {}", name, config_path.display());
        }
        ConfigCommands::RenameContext { old_name, new_name } => {
            let mut config = KubeConfig::load_from_file(&config_path)?;
            if config.contexts.iter().any(|c| c.name == new_name) {
                anyhow::bail!(
                    "cannot rename context \"{}\": context \"{}\" already exists",
                    old_name,
                    new_name
                );
            }
            let mut found = false;
            for ctx in &mut config.contexts {
                if ctx.name == old_name {
                    ctx.name = new_name.clone();
                    found = true;
                    break;
                }
            }
            if !found {
                anyhow::bail!("cannot rename context \"{}\": context not found", old_name);
            }
            if config.current_context == old_name {
                config.current_context = new_name.clone();
            }
            let yaml = serde_yaml::to_string(&config)?;
            std::fs::write(&config_path, yaml)?;
            println!("Context \"{}\" renamed to \"{}\".", old_name, new_name);
        }
        ConfigCommands::SetCluster {
            name,
            server,
            certificate_authority,
            certificate_authority_data,
            insecure_skip_tls_verify,
        } => {
            let mut config = KubeConfig::load_from_file(&config_path)?;
            let cluster_entry = config.clusters.iter_mut().find(|c| c.name == name);
            if let Some(entry) = cluster_entry {
                if let Some(s) = server {
                    entry.cluster.server = s;
                }
                if certificate_authority.is_some() {
                    entry.cluster.certificate_authority = certificate_authority;
                }
                if certificate_authority_data.is_some() {
                    entry.cluster.certificate_authority_data = certificate_authority_data;
                }
                if insecure_skip_tls_verify.is_some() {
                    entry.cluster.insecure_skip_tls_verify = insecure_skip_tls_verify;
                }
            } else {
                config.clusters.push(ClusterEntry {
                    name: name.clone(),
                    cluster: Cluster {
                        server: server.unwrap_or_default(),
                        certificate_authority,
                        certificate_authority_data,
                        insecure_skip_tls_verify,
                    },
                });
            }
            let yaml = serde_yaml::to_string(&config)?;
            std::fs::write(&config_path, yaml)?;
            println!("Cluster \"{}\" set.", name);
        }
        ConfigCommands::SetContext {
            name,
            cluster,
            user,
            namespace,
        } => {
            let mut config = KubeConfig::load_from_file(&config_path)?;
            let ctx_entry = config.contexts.iter_mut().find(|c| c.name == name);
            if let Some(entry) = ctx_entry {
                if let Some(c) = cluster {
                    entry.context.cluster = c;
                }
                if let Some(u) = user {
                    entry.context.user = u;
                }
                if let Some(ns) = namespace {
                    entry.context.namespace = ns;
                }
            } else {
                config.contexts.push(ContextEntry {
                    name: name.clone(),
                    context: Context {
                        cluster: cluster.unwrap_or_default(),
                        user: user.unwrap_or_default(),
                        namespace: namespace.unwrap_or_else(|| "default".to_string()),
                    },
                });
            }
            let yaml = serde_yaml::to_string(&config)?;
            std::fs::write(&config_path, yaml)?;
            println!("Context \"{}\" modified.", name);
        }
        ConfigCommands::SetCredentials {
            name,
            token,
            username,
            password,
            client_certificate,
            client_key,
            client_certificate_data,
            client_key_data,
        } => {
            let mut config = KubeConfig::load_from_file(&config_path)?;
            let user_entry = config.users.iter_mut().find(|u| u.name == name);
            if let Some(entry) = user_entry {
                if token.is_some() {
                    entry.user.token = token;
                }
                if username.is_some() {
                    entry.user.username = username;
                }
                if password.is_some() {
                    entry.user.password = password;
                }
                if client_certificate.is_some() {
                    entry.user.client_certificate = client_certificate;
                }
                if client_key.is_some() {
                    entry.user.client_key = client_key;
                }
                if client_certificate_data.is_some() {
                    entry.user.client_certificate_data = client_certificate_data;
                }
                if client_key_data.is_some() {
                    entry.user.client_key_data = client_key_data;
                }
            } else {
                config.users.push(UserEntry {
                    name: name.clone(),
                    user: User {
                        client_certificate,
                        client_certificate_data,
                        client_key,
                        client_key_data,
                        token,
                        username,
                        password,
                        auth_provider: None,
                        exec: None,
                    },
                });
            }
            let yaml = serde_yaml::to_string(&config)?;
            std::fs::write(&config_path, yaml)?;
            println!("User \"{}\" set.", name);
        }
    }

    Ok(())
}
