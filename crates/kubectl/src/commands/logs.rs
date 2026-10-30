use anyhow::Result;
use rusternetes_client::http::ApiClient;

#[allow(clippy::too_many_arguments)]
pub async fn execute_enhanced(
    client: &ApiClient,
    pod_name: &str,
    namespace: &str,
    container: Option<&str>,
    follow: bool,
    tail: Option<i64>,
    timestamps: bool,
    since_time: Option<&str>,
    since: Option<&str>,
    previous: bool,
) -> Result<()> {
    execute_full(
        client,
        pod_name,
        Some(namespace),
        container,
        follow,
        tail,
        timestamps,
        since_time,
        since,
        previous,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_full(
    client: &ApiClient,
    pod_name: &str,
    namespace: Option<&str>,
    container: Option<&str>,
    follow: bool,
    tail: Option<i64>,
    timestamps: bool,
    since_time: Option<&str>,
    since: Option<&str>,
    previous: bool,
) -> Result<()> {
    let default_namespace = "default";
    let ns = namespace.unwrap_or(default_namespace);

    // Build the logs URL
    let mut url = format!("/api/v1/namespaces/{}/pods/{}/log", ns, pod_name);

    // Add query parameters
    let mut params = Vec::new();
    if let Some(container_name) = container {
        params.push(format!("container={}", container_name));
    }
    if follow {
        params.push("follow=true".to_string());
    }
    if let Some(lines) = tail {
        params.push(format!("tailLines={}", lines));
    }
    if timestamps {
        params.push("timestamps=true".to_string());
    }
    if let Some(st) = since_time {
        params.push(format!("sinceTime={}", st));
    }
    if let Some(s) = since {
        // Parse duration (e.g., "5s", "2m", "1h") and convert to seconds
        let seconds = parse_duration_to_seconds(s)?;
        params.push(format!("sinceSeconds={}", seconds));
    }
    if previous {
        params.push("previous=true".to_string());
    }

    if !params.is_empty() {
        url.push('?');
        url.push_str(&params.join("&"));
    }

    // Fetch logs (the API returns plain text)
    let logs = client.get_text(&url).await?;

    print!("{}", logs);

    Ok(())
}

pub fn parse_duration_to_seconds(duration: &str) -> Result<i64> {
    let duration = duration.trim();
    if duration.is_empty() {
        anyhow::bail!("Empty duration");
    }

    let (value_str, unit) = if let Some(stripped) = duration.strip_suffix("ms") {
        (stripped, "ms")
    } else {
        let last_char = duration.chars().last().unwrap();
        if last_char.is_alphabetic() {
            (
                &duration[..duration.len() - 1],
                &duration[duration.len() - 1..],
            )
        } else {
            (duration, "s") // Default to seconds
        }
    };

    let value: i64 = value_str
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid duration value: {}", value_str))?;

    let seconds = match unit {
        "s" => value,
        "m" => value * 60,
        "h" => value * 3600,
        "d" => value * 86400,
        "ms" => value / 1000, // Convert milliseconds to seconds
        _ => anyhow::bail!(
            "Unknown duration unit: {}. Supported units: s, m, h, d",
            unit
        ),
    };

    Ok(seconds)
}
