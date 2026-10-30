/// Table output format support for kubectl get commands
///
/// This module implements the Table output format that kubectl uses to display
/// resources in a human-readable table format.
use rusternetes_common::types::ObjectMeta;
use serde::{Deserialize, Serialize};

/// Table is the response format for kubectl get requests with Accept: application/json;as=Table
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Table {
    /// APIVersion defines the versioned schema
    pub api_version: String,

    /// Kind is always "Table"
    pub kind: String,

    /// Standard list metadata
    pub metadata: TableMetadata,

    /// Column definitions for the table
    pub column_definitions: Vec<ColumnDefinition>,

    /// Rows of data
    pub rows: Vec<TableRow>,
}

/// Metadata for the table
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableMetadata {
    /// Resource version for the list
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,

    /// Continue token for pagination
    #[serde(skip_serializing_if = "Option::is_none", rename = "continue")]
    pub continue_token: Option<String>,

    /// Remaining items count
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remaining_item_count: Option<i64>,
}

/// Column definition in the table
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnDefinition {
    /// Name of the column
    pub name: String,

    /// Type of the column (e.g., "string", "integer", "date")
    #[serde(rename = "type")]
    pub column_type: String,

    /// Format hint (e.g., "name", "date-time")
    pub format: String,

    /// Description of the column
    pub description: String,

    /// Priority determines visibility (0 = always shown)
    pub priority: i32,
}

/// Single row in the table
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableRow {
    /// Cells contain the actual data
    pub cells: Vec<serde_json::Value>,

    /// Object contains the full resource (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<serde_json::Value>,
}

impl Table {
    /// Create a new Table
    pub fn new() -> Self {
        Self {
            api_version: "meta.k8s.io/v1".to_string(),
            kind: "Table".to_string(),
            metadata: TableMetadata {
                resource_version: None,
                continue_token: None,
                remaining_item_count: None,
            },
            column_definitions: Vec::new(),
            rows: Vec::new(),
        }
    }

    /// Add a column definition
    pub fn add_column(
        mut self,
        name: &str,
        column_type: &str,
        format: &str,
        description: &str,
        priority: i32,
    ) -> Self {
        self.column_definitions.push(ColumnDefinition {
            name: name.to_string(),
            column_type: column_type.to_string(),
            format: format.to_string(),
            description: description.to_string(),
            priority,
        });
        self
    }

    /// Add a row of data
    pub fn add_row(
        mut self,
        cells: Vec<serde_json::Value>,
        object: Option<serde_json::Value>,
    ) -> Self {
        self.rows.push(TableRow { cells, object });
        self
    }

    /// Set metadata
    pub fn with_metadata(
        mut self,
        resource_version: Option<String>,
        continue_token: Option<String>,
        remaining: Option<i64>,
    ) -> Self {
        self.metadata.resource_version = resource_version;
        self.metadata.continue_token = continue_token;
        self.metadata.remaining_item_count = remaining;
        self
    }
}

impl Default for Table {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper function to create a table for Pods.
///
/// Delegates column and cell definitions to [`printer_columns`] /
/// [`printer_row_cells`] so the LIST path here stays byte-for-byte consistent
/// with the single-resource GET path served by the response middleware,
/// including the `-o wide` columns (IP, NODE, ...).
pub fn pods_table<T>(pods: Vec<T>, resource_version: Option<String>) -> Table
where
    T: Serialize,
{
    let objects: Vec<serde_json::Value> = pods
        .iter()
        .map(|p| serde_json::to_value(p).unwrap_or(serde_json::Value::Null))
        .collect();
    kinded_table("Pod", objects, resource_version)
}

/// Build a Table for a known kind from already-serialized objects, using the
/// canonical [`printer_columns`] / [`printer_row_cells`]. Falls back to a
/// minimal NAME/AGE table for kinds without a rich printer.
pub fn kinded_table(
    kind: &str,
    objects: Vec<serde_json::Value>,
    resource_version: Option<String>,
) -> Table {
    let mut table = Table::new();
    if let Some(columns) = printer_columns(kind) {
        table.column_definitions = columns;
    } else {
        table = table
            .add_column("NAME", "string", "name", "Name of the resource", 0)
            .add_column("AGE", "string", "", "Age of the resource", 0);
    }

    for obj in objects {
        let cells = printer_row_cells(kind, &obj).unwrap_or_else(|| {
            let name = obj
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            vec![
                serde_json::Value::String(name),
                serde_json::Value::String(json_age(&obj)),
            ]
        });
        table = table.add_row(cells, Some(obj));
    }

    table.with_metadata(resource_version, None, None)
}

/// Helper function to create a table for generic resources with just NAME and AGE
pub fn generic_table<T>(
    resources: Vec<T>,
    resource_version: Option<String>,
    resource_kind: &str,
) -> Table
where
    T: Serialize + HasMetadata,
{
    let mut table = Table::new()
        .add_column(
            "NAME",
            "string",
            "name",
            &format!(
                "Name must be unique within a namespace for {}",
                resource_kind
            ),
            0,
        )
        .add_column(
            "AGE",
            "string",
            "",
            &format!("Age of the {}", resource_kind),
            0,
        );

    for resource in resources {
        let metadata = resource.metadata();
        let name = metadata.name.clone();
        let age = format_age(metadata);

        let cells = vec![
            serde_json::Value::String(name),
            serde_json::Value::String(age),
        ];
        let object = serde_json::to_value(&resource).ok();
        table = table.add_row(cells, object);
    }

    table.with_metadata(resource_version, None, None)
}

/// Trait for extracting metadata from resources
pub trait HasMetadata {
    fn metadata(&self) -> &ObjectMeta;
}

/// Humanize a creation timestamp the way the upstream table printers do:
/// `duration.HumanDuration(time.Since(ts))`. `None` renders as `<unknown>`
/// (printers.go `translateTimestampSince`).
pub fn humanize_age(creation: Option<chrono::DateTime<chrono::Utc>>) -> String {
    match creation {
        Some(creation_time) => {
            human_duration(chrono::Utc::now().signed_duration_since(creation_time))
        }
        None => "<unknown>".to_string(),
    }
}

/// Port of `HumanDuration` in
/// k8s.io/apimachinery/pkg/util/duration/duration.go:48-93: a duration
/// rendered with at most two units, switching unit at fixed thresholds.
pub fn human_duration(d: chrono::Duration) -> String {
    let ms = d.num_milliseconds();
    // Go: `int(d.Seconds())` truncates toward zero.
    let seconds = ms / 1000;
    if seconds < -1 {
        return "<invalid>".to_string();
    } else if seconds < 0 {
        return "0s".to_string();
    } else if seconds < 60 * 2 {
        return format!("{seconds}s");
    }
    let minutes = ms / 60_000;
    if minutes < 10 {
        let s = seconds % 60;
        if s == 0 {
            return format!("{minutes}m");
        }
        return format!("{minutes}m{s}s");
    } else if minutes < 60 * 3 {
        return format!("{minutes}m");
    }
    let hours = ms / 3_600_000;
    if hours < 8 {
        let m = minutes % 60;
        if m == 0 {
            return format!("{hours}h");
        }
        return format!("{hours}h{m}m");
    } else if hours < 48 {
        return format!("{hours}h");
    } else if hours < 24 * 8 {
        let h = hours % 24;
        if h == 0 {
            return format!("{}d", hours / 24);
        }
        return format!("{}d{h}h", hours / 24);
    } else if hours < 24 * 365 * 2 {
        return format!("{}d", hours / 24);
    } else if hours < 24 * 365 * 8 {
        let dy = (hours / 24) % 365;
        if dy == 0 {
            return format!("{}y", hours / 24 / 365);
        }
        return format!("{}y{dy}d", hours / 24 / 365);
    }
    format!("{}y", hours / 24 / 365)
}

/// Format age from typed metadata.
fn format_age(metadata: &ObjectMeta) -> String {
    humanize_age(metadata.creation_timestamp)
}

/// Format age from a JSON object's `metadata.creationTimestamp`.
fn json_age(obj: &serde_json::Value) -> String {
    let creation = obj
        .get("metadata")
        .and_then(|m| m.get("creationTimestamp"))
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc));
    humanize_age(creation)
}

/// Strip a trailing `List` so `PodList`/`NodeList` map to `Pod`/`Node`.
pub fn normalize_kind(kind: &str) -> &str {
    kind.strip_suffix("List").unwrap_or(kind)
}

/// `ObjectMeta` swagger doc for `name`.
const OBJECT_META_NAME_DOC: &str = "Name must be unique within a namespace. Is required when creating resources, although some resources may allow a client to request the generation of an appropriate name automatically. Name is primarily intended for creation idempotence and configuration definition. Cannot be updated. More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names#names";

/// `ObjectMeta` swagger doc for `creationTimestamp`.
const OBJECT_META_CREATION_TIMESTAMP_DOC: &str = "CreationTimestamp is a timestamp representing the server time when this object was created. It is not guaranteed to be set in happens-before order across separate operations. Clients may not set this value. It is represented in RFC3339 form and is in UTC.\n\nPopulated by the system. Read-only. Null for lists. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata";

/// `PodCertificateRequestSpec` swagger doc for `signerName`
/// (certificates/v1beta1 types_swagger_doc_generated.go:131).
const PCR_SIGNER_NAME_DOC: &str = "signerName indicates the requested signer.\n\nAll signer names beginning with `kubernetes.io` are reserved for use by the Kubernetes project.  There is currently one well-known signer documented by the Kubernetes project, `kubernetes.io/kube-apiserver-client-pod`, which will issue client certificates understood by kube-apiserver.  It is currently unimplemented.";

/// `PodCertificateRequestSpec` swagger doc for `unverifiedUserAnnotations`
/// (certificates/v1beta1 types_swagger_doc_generated.go:141).
const PCR_ANNOTATIONS_DOC: &str = "unverifiedUserAnnotations allow pod authors to pass additional information to the signer implementation.  Kubernetes does not restrict or validate this metadata in any way.\n\nEntries are subject to the same validation as object metadata annotations, with the addition that all keys must be domain-prefixed. No restrictions are placed on values, except an overall size limitation on the entire field.\n\nSigners should document the keys and values they support.  Signers should deny requests that contain keys they do not recognize.";

/// `ClusterTrustBundleSpec` swagger doc for `signerName`
/// (certificates/v1beta1 types_swagger_doc_generated.go:100).
const CTB_SIGNER_NAME_DOC: &str = "signerName indicates the associated signer, if any.\n\nIn order to create or update a ClusterTrustBundle that sets signerName, you must have the following cluster-scoped permission: group=certificates.k8s.io resource=signers resourceName=<the signer name> verb=attest.\n\nIf signerName is not empty, then the ClusterTrustBundle object must be named with the signer name as a prefix (translating slashes to colons). For example, for the signer name `example.com/foo`, valid ClusterTrustBundle object names include `example.com:foo:abc` and `example.com:foo:v1`.\n\nIf signerName is empty, then the ClusterTrustBundle object's name must not have such a prefix.\n\nList/watch requests for ClusterTrustBundles can filter on this field using a `spec.signerName=NAME` field selector.";

fn col(
    name: &str,
    column_type: &str,
    format: &str,
    description: &str,
    priority: i32,
) -> ColumnDefinition {
    ColumnDefinition {
        name: name.to_string(),
        column_type: column_type.to_string(),
        format: format.to_string(),
        description: description.to_string(),
        priority,
    }
}

/// Canonical kubectl printer columns for a resource kind, or `None` when the
/// kind has no rich printer (callers fall back to the generic NAME/AGE table).
///
/// This is the single source of truth shared by the resource LIST handlers
/// (e.g. [`pods_table`]) and the middleware's on-demand Table conversion, so
/// `kubectl get` and `kubectl get -o wide` agree on every code path. Columns
/// with `priority: 1` are the wide columns kubectl only shows under `-o wide`.
pub fn printer_columns(kind: &str) -> Option<Vec<ColumnDefinition>> {
    let cols = match normalize_kind(kind) {
        "Pod" => vec![
            col(
                "NAME",
                "string",
                "name",
                "Name must be unique within a namespace",
                0,
            ),
            col(
                "READY",
                "string",
                "",
                "The aggregate readiness state of this pod for accepting traffic",
                0,
            ),
            col(
                "STATUS",
                "string",
                "",
                "The aggregate state of the containers in this pod",
                0,
            ),
            col(
                "RESTARTS",
                "integer",
                "",
                "The number of times the containers in this pod have been restarted",
                0,
            ),
            col("AGE", "string", "", "Age of the pod", 0),
            col("IP", "string", "", "The IP address assigned to the pod", 1),
            col("NODE", "string", "", "The node this pod is running on", 1),
            col(
                "NOMINATED NODE",
                "string",
                "",
                "The node nominated for preemption to schedule this pod",
                1,
            ),
            col(
                "READINESS GATES",
                "string",
                "",
                "The readiness gate status for the pod",
                1,
            ),
        ],
        "Node" => vec![
            col("NAME", "string", "name", "Name of the node", 0),
            col(
                "STATUS",
                "string",
                "",
                "The readiness status of the node",
                0,
            ),
            col("ROLES", "string", "", "The roles assigned to the node", 0),
            col("AGE", "string", "", "Age of the node", 0),
            col(
                "VERSION",
                "string",
                "",
                "The kubelet version running on the node",
                0,
            ),
            col(
                "INTERNAL-IP",
                "string",
                "",
                "The internal IP address of the node",
                1,
            ),
            col(
                "EXTERNAL-IP",
                "string",
                "",
                "The external IP address of the node",
                1,
            ),
            col("OS-IMAGE", "string", "", "The OS image of the node", 1),
            col(
                "KERNEL-VERSION",
                "string",
                "",
                "The kernel version of the node",
                1,
            ),
            col(
                "CONTAINER-RUNTIME",
                "string",
                "",
                "The container runtime of the node",
                1,
            ),
        ],
        "Namespace" => vec![
            col("NAME", "string", "name", "Name of the namespace", 0),
            col(
                "STATUS",
                "string",
                "",
                "The lifecycle phase of the namespace",
                0,
            ),
            col("AGE", "string", "", "Age of the namespace", 0),
        ],
        // kube-aggregator pkg/registry/apiservice/etcd/etcd.go:85-92
        // (`REST.ConvertToTable` column definitions). The Name and Age
        // descriptions are the `ObjectMeta` swagger docs
        // (`swaggerMetadataDescriptions["name"]` / `["creationTimestamp"]`,
        // apimachinery meta/v1/types_swagger_doc_generated.go:255,262).
        "APIService" => vec![
            col("Name", "string", "name", OBJECT_META_NAME_DOC, 0),
            col(
                "Service",
                "string",
                "",
                "The reference to the service that hosts this API endpoint.",
                0,
            ),
            col(
                "Available",
                "string",
                "",
                "Whether this service is available.",
                0,
            ),
            col("Age", "string", "", OBJECT_META_CREATION_TIMESTAMP_DOC, 0),
        ],
        // pkg/printers/internalversion/printers.go:419-424
        // (`clusterTrustBundleColumnDefinitions`).
        "ClusterTrustBundle" => vec![
            col("Name", "string", "name", OBJECT_META_NAME_DOC, 0),
            col("SignerName", "string", "", CTB_SIGNER_NAME_DOC, 0),
        ],
        // pkg/printers/internalversion/printers.go:426-436
        // (`podCertificateRequestColumnDefinitions`); descriptions are the
        // certificates/v1beta1 `PodCertificateRequestSpec` swagger docs
        // (types_swagger_doc_generated.go:131-141).
        "PodCertificateRequest" => vec![
            col("Name", "string", "name", OBJECT_META_NAME_DOC, 0),
            col(
                "PodName",
                "string",
                "",
                "podName is the name of the pod into which the certificate will be mounted.",
                0,
            ),
            col(
                "ServiceAccountName",
                "string",
                "",
                "serviceAccountName is the name of the service account the pod is running as.",
                0,
            ),
            col(
                "NodeName",
                "string",
                "",
                "nodeName is the name of the node the pod is assigned to.",
                0,
            ),
            col("SignerName", "string", "", PCR_SIGNER_NAME_DOC, 0),
            col(
                "State",
                "string",
                "",
                "Is the request Pending, Issued, Denied, or Failed?",
                0,
            ),
            col(
                "UnverifiedUserAnnotations",
                "string",
                "",
                PCR_ANNOTATIONS_DOC,
                0,
            ),
        ],
        _ => return None,
    };
    Some(cols)
}

/// Roles for a node, derived from `node-role.kubernetes.io/<role>` (and the
/// legacy `kubernetes.io/role`) labels, comma-joined and sorted. `<none>`
/// when the node carries no role labels.
fn node_roles(obj: &serde_json::Value) -> String {
    let mut roles: Vec<String> = Vec::new();
    if let Some(labels) = obj
        .get("metadata")
        .and_then(|m| m.get("labels"))
        .and_then(|v| v.as_object())
    {
        for (key, value) in labels {
            if let Some(role) = key.strip_prefix("node-role.kubernetes.io/") {
                if !role.is_empty() {
                    roles.push(role.to_string());
                }
            } else if key == "kubernetes.io/role" {
                if let Some(role) = value.as_str() {
                    if !role.is_empty() {
                        roles.push(role.to_string());
                    }
                }
            }
        }
    }
    if roles.is_empty() {
        "<none>".to_string()
    } else {
        roles.sort();
        roles.join(",")
    }
}

/// Row cells for one serialized object of `kind`, matching the order of
/// [`printer_columns`] for that kind. Returns `None` for kinds without a rich
/// printer. Defensive against missing fields so partially-populated objects
/// (e.g. a freshly created pod with no status) still render.
pub fn printer_row_cells(kind: &str, obj: &serde_json::Value) -> Option<Vec<serde_json::Value>> {
    use serde_json::Value;

    let name = obj
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let age = json_age(obj);

    let str_at = |obj: &Value, path: &[&str]| -> Option<String> {
        let mut cur = obj;
        for key in path {
            cur = cur.get(key)?;
        }
        cur.as_str().map(|s| s.to_string())
    };

    let cells = match normalize_kind(kind) {
        "Pod" => {
            let status = obj.get("status");
            let phase = status
                .and_then(|s| s.get("phase"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let container_statuses = status
                .and_then(|s| s.get("containerStatuses"))
                .and_then(|v| v.as_array());
            let ready = container_statuses
                .map(|cs| {
                    cs.iter()
                        .filter(|c| c.get("ready").and_then(|v| v.as_bool()).unwrap_or(false))
                        .count()
                })
                .unwrap_or(0);
            let total = container_statuses.map(|cs| cs.len()).unwrap_or(0);
            let restarts: i64 = container_statuses
                .map(|cs| {
                    cs.iter()
                        .map(|c| c.get("restartCount").and_then(|v| v.as_i64()).unwrap_or(0))
                        .sum()
                })
                .unwrap_or(0);
            let ip = str_at(obj, &["status", "podIP"]).unwrap_or_else(|| "<none>".to_string());
            let node = str_at(obj, &["spec", "nodeName"]).unwrap_or_else(|| "<none>".to_string());
            let nominated = str_at(obj, &["status", "nominatedNodeName"])
                .unwrap_or_else(|| "<none>".to_string());
            vec![
                Value::String(name),
                Value::String(format!("{}/{}", ready, total)),
                Value::String(phase.to_string()),
                Value::Number(restarts.into()),
                Value::String(age),
                Value::String(ip),
                Value::String(node),
                Value::String(nominated),
                Value::String("<none>".to_string()),
            ]
        }
        "Node" => {
            let status = obj.get("status");
            let ready = status
                .and_then(|s| s.get("conditions"))
                .and_then(|v| v.as_array())
                .map(|conds| {
                    conds.iter().any(|c| {
                        c.get("type").and_then(|v| v.as_str()) == Some("Ready")
                            && c.get("status").and_then(|v| v.as_str()) == Some("True")
                    })
                })
                .unwrap_or(false);
            let mut node_status = if ready {
                "Ready".to_string()
            } else {
                "NotReady".to_string()
            };
            if obj
                .get("spec")
                .and_then(|s| s.get("unschedulable"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                node_status.push_str(",SchedulingDisabled");
            }
            let roles = node_roles(obj);
            let version = str_at(obj, &["status", "nodeInfo", "kubeletVersion"])
                .unwrap_or_else(|| "<unknown>".to_string());
            let addr_of = |kind: &str| -> String {
                status
                    .and_then(|s| s.get("addresses"))
                    .and_then(|v| v.as_array())
                    .and_then(|addrs| {
                        addrs
                            .iter()
                            .find(|a| a.get("type").and_then(|v| v.as_str()) == Some(kind))
                            .and_then(|a| a.get("address"))
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    })
                    .unwrap_or_else(|| "<none>".to_string())
            };
            let os_image = str_at(obj, &["status", "nodeInfo", "osImage"])
                .unwrap_or_else(|| "<unknown>".to_string());
            let kernel = str_at(obj, &["status", "nodeInfo", "kernelVersion"])
                .unwrap_or_else(|| "<unknown>".to_string());
            let runtime = str_at(obj, &["status", "nodeInfo", "containerRuntimeVersion"])
                .unwrap_or_else(|| "<unknown>".to_string());
            vec![
                Value::String(name),
                Value::String(node_status),
                Value::String(roles),
                Value::String(age),
                Value::String(version),
                Value::String(addr_of("InternalIP")),
                Value::String(addr_of("ExternalIP")),
                Value::String(os_image),
                Value::String(kernel),
                Value::String(runtime),
            ]
        }
        "Namespace" => {
            let phase = str_at(obj, &["status", "phase"]).unwrap_or_default();
            vec![
                Value::String(name),
                Value::String(phase),
                Value::String(age),
            ]
        }
        // etcd.go:96-117 (the `MetaToTableRow` callback).
        "APIService" => {
            let service = match (
                str_at(obj, &["spec", "service", "namespace"]),
                str_at(obj, &["spec", "service", "name"]),
            ) {
                (None, None) if obj.pointer("/spec/service").is_none_or(|s| s.is_null()) => {
                    "Local".to_string()
                }
                (ns, name) => format!("{}/{}", ns.unwrap_or_default(), name.unwrap_or_default()),
            };
            let mut status = "Unknown".to_string();
            let available = obj
                .pointer("/status/conditions")
                .and_then(|c| c.as_array())
                .and_then(|conds| {
                    conds
                        .iter()
                        .find(|c| c.get("type").and_then(|v| v.as_str()) == Some("Available"))
                });
            if let Some(cond) = available {
                let cond_status = str_at(cond, &["status"]).unwrap_or_default();
                let reason = str_at(cond, &["reason"]).unwrap_or_default();
                status = if cond_status != "True" && !reason.is_empty() {
                    format!("{} ({})", cond_status, reason)
                } else {
                    cond_status
                };
            }
            vec![
                Value::String(name),
                Value::String(service),
                Value::String(status),
                Value::String(age),
            ]
        }
        // printers.go:2332-2342 (`printClusterTrustBundle`): an empty
        // signerName renders as `<none>`.
        "ClusterTrustBundle" => {
            let signer = str_at(obj, &["spec", "signerName"]).unwrap_or_default();
            vec![
                Value::String(name),
                Value::String(if signer.is_empty() {
                    "<none>".to_string()
                } else {
                    signer
                }),
            ]
        }
        // printers.go:2356-2381 (`printPodCertificateRequest`): "Issued",
        // "Denied" and "Failed" are mutually exclusive; the last matching
        // condition wins, as in upstream's loop. The server renders the Wide
        // cell (labels.FormatLabels) always.
        "PodCertificateRequest" => {
            let mut state = "Pending";
            if let Some(conds) = obj.pointer("/status/conditions").and_then(|c| c.as_array()) {
                for c in conds {
                    match c.get("type").and_then(|v| v.as_str()) {
                        Some("Issued") => state = "Issued",
                        Some("Denied") => state = "Denied",
                        Some("Failed") => state = "Failed",
                        _ => {}
                    }
                }
            }
            let annotations = match obj
                .pointer("/spec/unverifiedUserAnnotations")
                .and_then(|a| a.as_object())
            {
                Some(m) if !m.is_empty() => {
                    let mut kv: Vec<String> = m
                        .iter()
                        .map(|(k, v)| format!("{}={}", k, v.as_str().unwrap_or("")))
                        .collect();
                    kv.sort();
                    kv.join(",")
                }
                _ => "<none>".to_string(),
            };
            vec![
                Value::String(name),
                Value::String(str_at(obj, &["spec", "podName"]).unwrap_or_default()),
                Value::String(str_at(obj, &["spec", "serviceAccountName"]).unwrap_or_default()),
                Value::String(str_at(obj, &["spec", "nodeName"]).unwrap_or_default()),
                Value::String(str_at(obj, &["spec", "signerName"]).unwrap_or_default()),
                Value::String(state.to_string()),
                Value::String(annotations),
            ]
        }
        _ => return None,
    };
    Some(cells)
}

/// Check if the request wants table format
pub fn wants_table(accept_header: Option<&str>) -> bool {
    if let Some(accept) = accept_header {
        accept.contains("as=Table") || accept.contains("application/json;as=Table")
    } else {
        false
    }
}

// Trait implementations for common resource types

impl HasMetadata for rusternetes_common::resources::Pod {
    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Deployment {
    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::Service {
    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ReplicationController {
    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }
}

impl HasMetadata for rusternetes_common::resources::ReplicaSet {
    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }
}
