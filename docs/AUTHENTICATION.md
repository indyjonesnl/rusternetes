# Rūsternetes Authentication & Authorization Guide

This guide covers how to configure authentication and authorization in rusternetes, how to create user tokens, and how to move from insecure development mode to a properly secured cluster.

## The Problem: `--skip-auth`

By default, rusternetes runs with `--skip-auth` enabled. This means:

- Every request is treated as coming from `admin` in the `system:masters` group
- No tokens are required
- All RBAC checks are bypassed (AlwaysAllowAuthorizer)
- The console, kubectl, and any network client have full cluster access

**This is intentional for development but must not be used in any environment where the API server is network-accessible.**

## How Authentication Works

When `--skip-auth` is disabled, the API server authenticates requests through the `auth_middleware`:

```
Request with Authorization: Bearer <token>
    │
    ├── Try ServiceAccount JWT validation (TokenManager)
    │     └── Valid? → Extract UserInfo from JWT claims
    │
    ├── Try Bootstrap Token validation (BootstrapTokenManager)
    │     └── Valid? → Extract UserInfo from bootstrap token
    │
    └── No token or invalid token?
          └── Anonymous user (system:anonymous)
```

After authentication, requests pass through the RBAC authorizer, which checks Roles, ClusterRoles, RoleBindings, and ClusterRoleBindings.

## Enabling Authentication

### Step 1: Generate signing keys

The API server needs an RSA key pair for signing and validating JWT tokens. These are the same keys used for ServiceAccount token signing.

```bash
# Generate RSA key pair
mkdir -p .rusternetes/certs
openssl genrsa -out .rusternetes/certs/sa.key 2048
openssl rsa -in .rusternetes/certs/sa.key -pubout -out .rusternetes/certs/sa.pub
```

The API server searches for keys in these locations (in order):
1. `/etc/kubernetes/pki/sa.key` + `sa.pub`
2. `~/.rusternetes/certs/sa.key` + `sa.pub`
3. `~/.rusternetes/keys/sa-signing-key.pem` + `sa-signing-key.pub`

If no RSA keys are found, it falls back to HMAC-SHA256 using the `--jwt-secret` value.

### Step 2: Create an admin user (while still in skip-auth mode)

Before disabling skip-auth, create an admin ServiceAccount so you can authenticate after the switch.

```bash
export KUBECONFIG=~/.kube/rusternetes-config

# Create an admin ServiceAccount
kubectl create serviceaccount cluster-admin -n kube-system

# Create a ClusterRoleBinding granting cluster-admin permissions
cat <<EOF | kubectl apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: cluster-admin-binding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-admin
subjects:
- kind: ServiceAccount
  name: cluster-admin
  namespace: kube-system
EOF

# Create a long-lived token Secret for the ServiceAccount
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: cluster-admin-token
  namespace: kube-system
  annotations:
    kubernetes.io/service-account.name: cluster-admin
type: kubernetes.io/service-account-token
EOF
```

After the controller-manager generates the token, retrieve it:

```bash
kubectl get secret cluster-admin-token -n kube-system -o jsonpath='{.data.token}' | base64 -d
```

### Step 3: Configure kubectl

Update your kubeconfig to use the token:

```bash
TOKEN=$(kubectl get secret cluster-admin-token -n kube-system -o jsonpath='{.data.token}' | base64 -d)

kubectl config set-credentials rusternetes-admin --token="$TOKEN"
kubectl config set-context rusternetes --user=rusternetes-admin
```

### Step 4: Restart without `--skip-auth`

Now restart the API server without `--skip-auth`, with `--authorization-mode=Node,RBAC`. All requests will require a valid token. The default `--authorization-mode` is `AlwaysAllow` (as in upstream kube-apiserver), so without that flag every authenticated user is authorized for everything.

**Compose cluster:** Edit the compose file, replace the `--skip-auth` line in the api-server command with `--authorization-mode=Node,RBAC`, then:

```bash
podman compose build api-server
podman compose up -d api-server
```

**All-in-one binary:**

```bash
rusternetes --skip-auth=false --authorization-mode=Node,RBAC --data-dir ./cluster.db
```

**Verify:** `curl -k https://localhost:6443/api/v1/pods` should return 401/403.

## Token Types

### ServiceAccount Tokens (JWT)

The primary token type. Signed with RS256 (RSA + SHA256) for OIDC compatibility, or HS256 (HMAC) as a fallback.

JWT claims include:
- `sub`: `system:serviceaccount:<namespace>:<name>`
- `iss`: `https://kubernetes.default.svc.cluster.local`
- `aud`: `["rusternetes"]`
- `kubernetes.io`: namespace, serviceaccount name/uid, optional pod/node binding

Tokens are validated against the signing key on every request.

### Bootstrap Tokens

Short-lived tokens in the format `<token-id>.<token-secret>` (6 + 16 characters). Used for node bootstrapping and cluster join operations. Stored in-memory on the API server.

### Anonymous Access

Requests without a token are treated as `system:anonymous` in the `system:unauthenticated` group. By default, anonymous users have no permissions when RBAC is enabled. You can grant limited access by creating a ClusterRoleBinding:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: anonymous-read
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: view
subjects:
- kind: User
  name: system:anonymous
```

## RBAC Authorization

When auth is enabled, the RBAC authorizer checks permissions for every request:

1. Extracts the user, groups, and resource being accessed from the request
2. Searches for matching RoleBindings (namespaced) and ClusterRoleBindings (cluster-wide)
3. Checks if any bound Role or ClusterRole contains a matching rule
4. Returns Allow or Deny

The `system:masters` group always gets full access (like the `admin` user in skip-auth mode).

### Built-in ClusterRoles

Rusternetes bootstraps with the standard Kubernetes ClusterRoles:

| ClusterRole | Access |
|-------------|--------|
| `cluster-admin` | Full access to all resources |
| `admin` | Full access within a namespace |
| `edit` | Read/write to most resources (no RBAC) |
| `view` | Read-only access to most resources |

### Example: Read-Only User

```yaml
# Create a ServiceAccount for the read-only user
apiVersion: v1
kind: ServiceAccount
metadata:
  name: viewer
  namespace: default
---
# Bind it to the view ClusterRole
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: viewer-binding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: view
subjects:
- kind: ServiceAccount
  name: viewer
  namespace: default
```

## Console Authentication

When the web console is enabled (`--console-dir`), authentication depends on the API server mode:

| Mode | Console Access |
|------|---------------|
| `--skip-auth` | Open to anyone — no token needed |
| Auth enabled | Console loads but API calls require a valid token |

In auth-enabled mode, the console reads a JWT from the browser's `sessionStorage` (key: `rusternetes-token`) and sends it as a `Bearer` token on all API requests. You can set this manually in the browser console:

```javascript
sessionStorage.setItem('rusternetes-token', '<your-jwt-token>');
```

A login page that prompts for a token and stores it in sessionStorage is planned but not yet implemented.

## Security Checklist

Before exposing a rusternetes cluster to a network:

- [ ] Remove `--skip-auth` from the API server command and pass `--authorization-mode=Node,RBAC` (the default is `AlwaysAllow`)
- [ ] Generate RSA signing keys for ServiceAccount tokens
- [ ] Enable TLS (`--tls` with cert/key files or `--tls-self-signed`)
- [ ] Create an admin ServiceAccount and ClusterRoleBinding
- [ ] Verify you can authenticate with `kubectl get pods --token=<token>`
- [ ] Verify anonymous access is denied: `curl -k https://localhost:6443/api/v1/pods` returns 403
- [ ] Create limited-scope ServiceAccounts for non-admin users

## Client Certificate Authentication (mTLS)

Rusternetes supports mutual TLS for client certificate authentication, just like real Kubernetes. When `--client-ca-file` is set, the TLS layer requires clients to present a certificate signed by the specified CA.

### Setup

```bash
# Generate a CA
openssl genrsa -out ca.key 2048
openssl req -new -x509 -key ca.key -out ca.crt -days 3650 -subj "/CN=rusternetes-ca"

# Generate a client certificate for an admin user
# CN becomes the username, O becomes a group
openssl genrsa -out admin.key 2048
openssl req -new -key admin.key -out admin.csr -subj "/CN=admin/O=system:masters"
openssl x509 -req -in admin.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out admin.crt -days 365

# Start the API server with client CA
rusternetes \
  --tls \
  --tls-cert-file server.crt \
  --tls-key-file server.key \
  --client-ca-file ca.crt \
  --authorization-mode=Node,RBAC \
  --skip-auth=false

# Configure kubectl
kubectl config set-credentials admin \
  --client-certificate=admin.crt \
  --client-key=admin.key
```

The `--client-ca-file` flag tells the API server to require client certificates and verify them against the provided CA. The TLS layer rejects connections from clients without a valid certificate.

**Current limitation:** The TLS layer enforces that clients present valid certificates, but the CN/O fields are not yet extracted into the authentication middleware's UserInfo. This means mTLS currently acts as a network-level access control (only holders of certs signed by the CA can connect), but RBAC decisions still use JWT token identity. Full CN/O-to-UserInfo extraction is planned.

## CLI Flags Reference

| Flag | Default | Description |
|------|---------|-------------|
| `--skip-auth` | `true` (all-in-one) / `false` (standalone) | Skip authentication and authorization |
| `--authorization-mode` | `AlwaysAllow` | Ordered, comma-delimited authorizers: AlwaysAllow,AlwaysDeny,ABAC,Webhook,RBAC,Node. Pass `Node,RBAC` for a secured cluster |
| `--jwt-secret` | `rusternetes-secret-change-in-production` | HMAC secret for JWT signing (fallback when no RSA keys) |
| `--tls` | `false` | Enable TLS/HTTPS |
| `--tls-cert-file` | — | Path to TLS certificate (PEM) |
| `--tls-key-file` | — | Path to TLS private key (PEM) |
| `--tls-self-signed` | `false` | Generate self-signed certificate |
| `--client-ca-file` | — | Client CA certificate for mTLS authentication |

## Signing Key Locations

The TokenManager searches for RSA keys in this order:

1. `/etc/kubernetes/pki/sa.key` + `sa.pub` (Docker Compose / production)
2. `~/.rusternetes/certs/sa.key` + `sa.pub` (local development)
3. `~/.rusternetes/keys/sa-signing-key.pem` + `sa-signing-key.pub` (legacy)

If none are found, HMAC-SHA256 is used with the `--jwt-secret` value. RSA is strongly preferred because it enables OIDC token verification by external systems (the public key can be distributed without exposing the signing secret).

## Not Yet Implemented

These authentication methods have framework code in the codebase but are not yet wired into the authentication middleware:

- **OIDC Token Validation** — `OIDCTokenValidator` in `crates/common/src/auth.rs` can validate tokens against an external OIDC provider's JWKS endpoint
- **Webhook Token Authentication** — `WebhookTokenAuthenticator` in `crates/common/src/auth.rs` can delegate token validation to an external webhook
- **Client Certificate Identity Extraction** — mTLS enforcement works via `--client-ca-file`, but CN/O fields from the client certificate are not yet extracted into UserInfo for RBAC decisions. The plumbing exists in `UserInfo::from_client_cert()` but needs to be connected to the TLS connection layer.
- **Static Token File** — Not implemented; K8s supports a `--token-auth-file` CSV but rusternetes does not
