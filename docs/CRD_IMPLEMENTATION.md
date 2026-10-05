# Custom Resource Definitions (CRDs)

Rusternetes supports Custom Resource Definitions (CRDs), allowing you to extend the Kubernetes API with your own resource types. This includes full CRUD operations, watch, status/scale subresources, OpenAPI v3 schema validation, and defaulting/pruning.

> **Tip:** CRDs and custom resources are fully browsable in the [web console](CONSOLE_USER_GUIDE.md) — the Resource Explorer auto-discovers them and lets you create, view, edit, and delete instances.

## Overview

## Features Implemented

### 1. CRD Resource Types ✅

**Files Created:**
- `crates/common/src/resources/crd.rs` (700+ lines)

**Key Types:**
- `CustomResourceDefinition` - Main CRD resource
- `CustomResourceDefinitionSpec` - CRD specification
- `CustomResourceDefinitionVersion` - Version definitions
- `JSONSchemaProps` - OpenAPI v3 schema validation
- `CustomResourceSubresources` - Status and scale subresources
- `CustomResource` - Generic custom resource instance

**Features:**
- Multiple version support with storage version selection
- Namespaced and cluster-scoped resources
- OpenAPI v3 schema validation
- Status and scale subresources
- Additional printer columns for kubectl
- Short names and categories
- Conversion webhooks (framework ready)

### 2. OpenAPI v3 Schema Validation ✅

**File Created:**
- `crates/common/src/schema_validation.rs` (540+ lines)

**Validation Features:**
- Type validation (object, array, string, number, boolean, null)
- Required fields
- Min/max properties for objects
- Min/max items for arrays
- String length and pattern validation
- Number range validation (min/max with exclusive support)
- Enum validation
- oneOf, anyOf, allOf, not validation
- Nested schema validation
- Additional properties control
- Format validation (date-time, email, uri, uuid)

**Test Coverage:**
- 7 unit tests for schema validation
- Type validation tests
- Required fields tests
- String/number constraints tests
- Array validation tests
- Enum validation tests
- Pattern matching tests

### 3. CRD API Handlers ✅

**File Created:**
- `crates/api-server/src/handlers/crd.rs` (370+ lines)

**Endpoints:**
- `POST /apis/apiextensions.k8s.io/v1/customresourcedefinitions` - Create CRD
- `GET /apis/apiextensions.k8s.io/v1/customresourcedefinitions` - List CRDs
- `GET /apis/apiextensions.k8s.io/v1/customresourcedefinitions/:name` - Get CRD
- `PUT /apis/apiextensions.k8s.io/v1/customresourcedefinitions/:name` - Update CRD
- `DELETE /apis/apiextensions.k8s.io/v1/customresourcedefinitions/:name` - Delete CRD

**Validation:**
- At least one version must be defined
- Exactly one storage version required
- Group and plural names cannot be empty
- CRD name must follow `<plural>.<group>` convention
- Version uniqueness validation

**Authorization:**
- RBAC integration with `customresourcedefinitions` resource
- API group: `apiextensions.k8s.io`
- Verbs: create, get, list, update, delete

**Test Coverage:**
- 6 unit tests for CRD validation
- Success case test
- No versions error test
- No storage version error test
- Multiple storage versions error test
- Empty group error test
- Wrong name format error test

### 4. Custom Resource Handlers ✅

**File Created:**
- `crates/api-server/src/handlers/custom_resource.rs` (1900+ lines)

**Dynamic Endpoints (created per CRD):**
- Namespaced resources:
  - `POST /apis/{group}/{version}/namespaces/{namespace}/{plural}`
  - `GET /apis/{group}/{version}/namespaces/{namespace}/{plural}`
  - `GET /apis/{group}/{version}/namespaces/{namespace}/{plural}/{name}`
  - `PUT /apis/{group}/{version}/namespaces/{namespace}/{plural}/{name}`
  - `DELETE /apis/{group}/{version}/namespaces/{namespace}/{plural}/{name}`

- Cluster-scoped resources:
  - `POST /apis/{group}/{version}/{plural}`
  - `GET /apis/{group}/{version}/{plural}`
  - `GET /apis/{group}/{version}/{plural}/{name}`
  - `PUT /apis/{group}/{version}/{plural}/{name}`
  - `DELETE /apis/{group}/{version}/{plural}/{name}`

**Features:**
- Schema validation against CRD schema
- Version validation (served check)
- RBAC authorization per custom resource
- Automatic API version and kind assignment
- Generic storage with type-safe retrieval
- Watch support via router fallback (`?watch=true`)
- Status subresource (get/update/patch)
- Scale subresource (get/update)
- Schema defaulting on read
- Structural pruning of unknown fields
- Strict field validation (`fieldValidation=Strict`)
- Admission webhook integration (mutating + validating)
- Server-side apply support

### 5. Router Integration ✅

**Modified:**
- `crates/api-server/src/router.rs` - Added CRD routes
- `crates/api-server/src/handlers/mod.rs` - Registered new handlers

**Routes Added:**
```rust
.route(
    "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
    get(handlers::crd::list_crds).post(handlers::crd::create_crd),
)
.route(
    "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/:name",
    get(handlers::crd::get_crd)
        .put(handlers::crd::update_crd)
        .delete(handlers::crd::delete_crd),
)
```

## Usage

### Creating a CRD

```yaml
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: crontabs.stable.example.com
spec:
  group: stable.example.com
  names:
    plural: crontabs
    singular: crontab
    kind: CronTab
    shortNames:
      - ct
  scope: Namespaced
  versions:
    - name: v1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              properties:
                cronSpec:
                  type: string
                  pattern: '^(\*|[0-9]+) (\*|[0-9]+) (\*|[0-9]+) (\*|[0-9]+) (\*|[0-9]+)$'
                image:
                  type: string
                replicas:
                  type: integer
                  minimum: 1
                  maximum: 10
              required:
                - cronSpec
                - image
```

### Creating a Custom Resource

```yaml
apiVersion: stable.example.com/v1
kind: CronTab
metadata:
  name: my-cron-job
  namespace: default
spec:
  cronSpec: "*/5 * * * *"
  image: my-cron-image:latest
  replicas: 3
```

### Using kubectl

```bash
# Create the CRD
kubectl apply -f examples/crd-example.yaml

# List CRDs
kubectl get customresourcedefinitions
kubectl get crds  # short form

# Get a specific CRD
kubectl get crd crontabs.stable.example.com

# Create a custom resource
kubectl apply -f my-crontab.yaml

# List custom resources
kubectl get crontabs -n default
kubectl get ct -n default  # using short name

# Get a custom resource
kubectl get crontab my-cron-job -n default -o yaml

# Delete custom resources
kubectl delete crontab my-cron-job -n default

# Delete the CRD (also deletes all custom resources)
kubectl delete crd crontabs.stable.example.com
```

## Architecture

### Storage Structure

CRDs are stored in etcd with the key pattern:
```
/registry/customresourcedefinitions/<crd-name>
```

Custom resources are stored with the key pattern:
```
# Namespaced
/registry/<group>_<plural>/<namespace>/<name>

# Cluster-scoped
/registry/<group>_<plural>/<name>
```

### Validation Flow

```
1. User creates custom resource
        ↓
2. API server finds CRD by group + plural
        ↓
3. Check version exists and is served
        ↓
4. Validate spec against OpenAPI v3 schema
        ↓
5. Check RBAC authorization
        ↓
6. Store in etcd
        ↓
7. Return created resource
```

### Schema Validation Example

Given this schema:
```json
{
  "type": "object",
  "properties": {
    "spec": {
      "type": "object",
      "properties": {
        "replicas": {
          "type": "integer",
          "minimum": 1,
          "maximum": 10
        },
        "name": {
          "type": "string",
          "minLength": 3,
          "maxLength": 50
        }
      },
      "required": ["replicas", "name"]
    }
  }
}
```

The validator will:
- ✅ Accept: `{"spec": {"replicas": 5, "name": "test"}}`
- ❌ Reject: `{"spec": {"replicas": 0, "name": "test"}}` (min: 1)
- ❌ Reject: `{"spec": {"replicas": 5, "name": "ab"}}` (minLength: 3)
- ❌ Reject: `{"spec": {"replicas": 5}}` (required: name)

## Examples

See `examples/crd-example.yaml` for complete examples including:
1. CRD definition with OpenAPI schema
2. Custom resource instance
3. Schema validation examples
4. Subresources configuration
5. Additional printer columns

## Limitations

### Current Limitations

1. **No Conversion Webhooks**: Version conversion is not implemented. Only one storage version is supported.

2. **No Strategic Merge Patch Directives**: CRD-specific merge key directives are not yet implemented.

### Future Enhancements

1. **Version Conversion**
   - Webhook-based version conversion
   - Automatic conversion between versions

2. **Strategic Merge Patch**
   - CRD-specific merge key directives

## Testing

### Unit Tests

**CRD Validation** (`crates/api-server/src/handlers/crd.rs`):
```bash
cargo test --package rusternetes-api-server --lib crd::tests
```

**Schema Validation** (`crates/common/src/schema_validation.rs`):
```bash
cargo test --package rusternetes-common --lib schema_validation::tests
```

**Custom Resource Validation** (`crates/api-server/src/handlers/custom_resource.rs`):
```bash
cargo test --package rusternetes-api-server --lib custom_resource::tests
```

### Integration Tests

*To be implemented*

Test scenarios:
1. Create CRD and verify in etcd
2. Create custom resource and validate schema
3. Update custom resource with invalid data (should fail)
4. Delete CRD and verify custom resources are cleaned up
5. Multiple versions with different schemas

### Manual Testing

```bash
# 1. Start the cluster
podman-compose -f compose.yml up -d

# 2. Create a CRD
kubectl apply -f examples/crd-example.yaml

# 3. Verify CRD is created
kubectl get crd crontabs.stable.example.com -o yaml

# 4. Create a custom resource
cat <<EOF | kubectl apply -f -
apiVersion: stable.example.com/v1
kind: CronTab
metadata:
  name: test-crontab
  namespace: default
spec:
  cronSpec: "*/5 * * * *"
  image: my-image:latest
  replicas: 3
EOF

# 5. Verify custom resource (currently requires direct API call)
curl -k https://localhost:6443/apis/stable.example.com/v1/namespaces/default/crontabs/test-crontab

# 6. Test schema validation (should fail - replicas > 10)
cat <<EOF | kubectl apply -f -
apiVersion: stable.example.com/v1
kind: CronTab
metadata:
  name: invalid-crontab
  namespace: default
spec:
  cronSpec: "*/5 * * * *"
  image: my-image:latest
  replicas: 15  # Should fail validation
EOF
```

## Code Structure

```
crates/
├── common/src/
│   ├── resources/
│   │   └── crd.rs                    # CRD types (700+ lines)
│   └── schema_validation.rs          # OpenAPI v3 validation (540+ lines)
├── api-server/src/
│   ├── handlers/
│   │   ├── crd.rs                    # CRD CRUD handlers (800+ lines)
│   │   └── custom_resource.rs        # CR CRUD + status/scale/watch (1900+ lines)
│   └── dynamic_routes.rs             # Dynamic route registration (350+ lines)
├── controller-manager/src/controllers/
└── examples/
    └── crd-example.yaml              # Example CRD and CR
```

## Kubernetes Compatibility

### Compatible Features

- ✅ CRD API group (`apiextensions.k8s.io/v1`)
- ✅ Multiple versions with storage version
- ✅ OpenAPI v3 schema validation
- ✅ Required fields
- ✅ Type validation
- ✅ Min/max constraints
- ✅ Pattern validation
- ✅ Enum validation
- ✅ Nested object validation
- ✅ Array validation
- ✅ Scope (Namespaced/Cluster)
- ✅ Short names and categories
- ✅ Additional printer columns
- ✅ CRD naming convention

### Implemented Since Initial Release

- ✅ Watch API for CRs (router.rs fallback handles `?watch=true` for custom resources)
- ✅ Status subresource (get/update/patch at `/{plural}/{name}/status`)
- ✅ Scale subresource (get/update at `/{plural}/{name}/scale`)
- ✅ Defaulting (schema defaults applied on read)
- ✅ Pruning unknown fields (structural pruning based on schema)
- ✅ Dynamic route registration (`dynamic_routes.rs`, 350+ lines)

### Not Yet Implemented

- ⏹️ Conversion webhooks
- ⏹️ Strategic merge patch directives

## Performance Considerations

- **Schema Validation**: O(n) where n = object size
  - Recursive validation for nested objects
  - May be slow for very large resources

- **Storage**: Custom resources stored with type prefix
  - Efficient list operations per resource type
  - Namespace isolation for namespaced resources

- **Memory**: Each CRD definition loaded into memory
  - Schema trees can be large for complex schemas
  - Consider caching validated resources

## Security Considerations

1. **Schema Validation**: Prevents malformed data from being stored
2. **RBAC Integration**: Full authorization for CRDs and custom resources
3. **Version Validation**: Only served versions can be used
4. **Namespace Isolation**: Namespaced resources respect namespace boundaries

## Comparison with Kubernetes

| Feature | Kubernetes | Rusternetes |
|---------|-----------|-------------|
| CRD Definition | ✅ | ✅ |
| Multiple Versions | ✅ | ✅ |
| OpenAPI v3 Schema | ✅ | ✅ |
| Status Subresource | ✅ | ✅ |
| Scale Subresource | ✅ | ✅ |
| Conversion Webhooks | ✅ | ⏹️ |
| Dynamic Routes | ✅ | ✅ |
| Watch API | ✅ | ✅ |
| Defaulting | ✅ | ✅ |
| Pruning | ✅ | ✅ |
| Categories | ✅ | ✅ |
| Short Names | ✅ | ✅ |
| Printer Columns | ✅ | ✅ |
| Strategic Merge Patch | ✅ | ⏹️ |

## Migration Notes

If migrating from Kubernetes:
1. CRDs can be copied directly (YAML compatible)
2. Custom resources work with standard tooling (kubectl, kube-rs, client-go)
3. Watch, status subresource, scale subresource all work
4. Conversion webhooks not implemented (single storage version only)

## Troubleshooting

### CRD Won't Create

**Error:** "CRD must have exactly one storage version"
- **Fix:** Ensure only one version has `storage: true`

**Error:** "CRD name must be '<plural>.<group>'"
- **Fix:** Set `metadata.name` to `{spec.names.plural}.{spec.group}`

### Custom Resource Validation Fails

**Error:** "Field at spec.replicas must be at least 1"
- **Fix:** Check schema min/max constraints

**Error:** "Version v2 not found in CRD"
- **Fix:** Create CRD version before creating resources with that version

**Error:** "Version v1 is not served"
- **Fix:** Set `versions[].served: true` in CRD

### RBAC Errors

**Error:** "Forbidden: user cannot create customresourcedefinitions"
- **Fix:** Grant `apiextensions.k8s.io` API group permissions

**Error:** "Forbidden: user cannot create crontabs"
- **Fix:** Grant permissions for the custom resource's API group

## Conclusion

Custom Resource Definitions are fully functional in Rusternetes, including watch, status/scale subresources, schema validation with defaulting and pruning, RBAC integration, and Kubernetes-compatible API conventions.

**Next Steps:**
1. Conversion webhooks for multi-version CRDs
2. Strategic merge patch directives

**Status:** ✅ Full CRD functionality — CRUD, watch, status/scale subresources, schema validation, defaulting, pruning
