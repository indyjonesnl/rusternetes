use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use serde_json::{json, Value};

/// Approve or deny certificate signing requests (CSRs).
///
/// Equivalent to:
///   kubectl certificate approve csr-name
///   kubectl certificate deny csr-name
pub async fn execute(
    client: &ApiClient,
    action: &str,
    csr_names: &[String],
    force: bool,
) -> Result<()> {
    if csr_names.is_empty() {
        anyhow::bail!("one or more CSRs must be specified as <name>");
    }

    for csr_name in csr_names {
        match action {
            "approve" => approve_csr(client, csr_name, force).await?,
            "deny" => deny_csr(client, csr_name, force).await?,
            _ => anyhow::bail!(
                "unknown certificate action: {}. Use 'approve' or 'deny'",
                action
            ),
        }
    }

    Ok(())
}

async fn approve_csr(client: &ApiClient, name: &str, force: bool) -> Result<()> {
    let csr_path = format!(
        "/apis/certificates.k8s.io/v1/certificatesigningrequests/{}",
        name
    );

    // Get the current CSR
    let csr: Value = client
        .get(&csr_path)
        .await
        .map_err(|e| anyhow::anyhow!("certificatesigningrequest \"{}\" not found: {}", name, e))?;

    // Check if already approved
    if !force {
        if let Some(conditions) = csr
            .get("status")
            .and_then(|s| s.get("conditions"))
            .and_then(|c| c.as_array())
        {
            for condition in conditions {
                let ctype = condition.get("type").and_then(|t| t.as_str()).unwrap_or("");
                if ctype == "Approved" {
                    println!(
                        "certificatesigningrequest.certificates.k8s.io/{} already approved",
                        name
                    );
                    return Ok(());
                }
            }
        }
    }

    // Build the approval - update the status with an Approved condition
    let mut updated_csr = csr.clone();
    let conditions = updated_csr
        .get_mut("status")
        .and_then(|s| s.as_object_mut())
        .map(|s| {
            s.entry("conditions")
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .unwrap()
                .clone()
        })
        .unwrap_or_default();

    // Remove any existing Denied condition if force
    let mut new_conditions: Vec<Value> = if force {
        conditions
            .into_iter()
            .filter(|c| c.get("type").and_then(|t| t.as_str()).unwrap_or("") != "Denied")
            .collect()
    } else {
        conditions
    };

    // Add the Approved condition
    new_conditions.push(json!({
        "type": "Approved",
        "status": "True",
        "reason": "KubectlApprove",
        "message": "This CSR was approved by kubectl certificate approve.",
        "lastUpdateTime": chrono::Utc::now().to_rfc3339(),
    }));

    if let Some(status) = updated_csr.get_mut("status") {
        status["conditions"] = json!(new_conditions);
    } else {
        updated_csr["status"] = json!({
            "conditions": new_conditions,
        });
    }

    let approval_path = format!(
        "/apis/certificates.k8s.io/v1/certificatesigningrequests/{}/approval",
        name
    );

    let _result: Value = client
        .put(&approval_path, &updated_csr)
        .await
        .context("Failed to approve CSR")?;

    println!(
        "certificatesigningrequest.certificates.k8s.io/{} approved",
        name
    );
    Ok(())
}

async fn deny_csr(client: &ApiClient, name: &str, force: bool) -> Result<()> {
    let csr_path = format!(
        "/apis/certificates.k8s.io/v1/certificatesigningrequests/{}",
        name
    );

    // Get the current CSR
    let csr: Value = client
        .get(&csr_path)
        .await
        .map_err(|e| anyhow::anyhow!("certificatesigningrequest \"{}\" not found: {}", name, e))?;

    // Check if already denied
    if !force {
        if let Some(conditions) = csr
            .get("status")
            .and_then(|s| s.get("conditions"))
            .and_then(|c| c.as_array())
        {
            for condition in conditions {
                let ctype = condition.get("type").and_then(|t| t.as_str()).unwrap_or("");
                if ctype == "Denied" {
                    println!(
                        "certificatesigningrequest.certificates.k8s.io/{} already denied",
                        name
                    );
                    return Ok(());
                }
            }
        }
    }

    // Build the denial
    let mut updated_csr = csr.clone();
    let conditions = updated_csr
        .get_mut("status")
        .and_then(|s| s.as_object_mut())
        .map(|s| {
            s.entry("conditions")
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .unwrap()
                .clone()
        })
        .unwrap_or_default();

    // Remove any existing Approved condition if force
    let mut new_conditions: Vec<Value> = if force {
        conditions
            .into_iter()
            .filter(|c| c.get("type").and_then(|t| t.as_str()).unwrap_or("") != "Approved")
            .collect()
    } else {
        conditions
    };

    new_conditions.push(json!({
        "type": "Denied",
        "status": "True",
        "reason": "KubectlDeny",
        "message": "This CSR was denied by kubectl certificate deny.",
        "lastUpdateTime": chrono::Utc::now().to_rfc3339(),
    }));

    if let Some(status) = updated_csr.get_mut("status") {
        status["conditions"] = json!(new_conditions);
    } else {
        updated_csr["status"] = json!({
            "conditions": new_conditions,
        });
    }

    let approval_path = format!(
        "/apis/certificates.k8s.io/v1/certificatesigningrequests/{}/approval",
        name
    );

    let _result: Value = client
        .put(&approval_path, &updated_csr)
        .await
        .context("Failed to deny CSR")?;

    println!(
        "certificatesigningrequest.certificates.k8s.io/{} denied",
        name
    );
    Ok(())
}
