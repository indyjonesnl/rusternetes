# TLS/HTTPS Configuration Guide

The k3rs API server supports HTTPS with TLS 1.3, self-signed or custom certificates, and mutual TLS (mTLS) for client certificate authentication.

> **Tip:** The [web console](CONSOLE_USER_GUIDE.md) is also served over TLS at `https://localhost:6443/console/`. For authentication beyond TLS, see [AUTHENTICATION.md](AUTHENTICATION.md).

## Overview

The API server runs with HTTPS enabled using TLS 1.3 encryption, providing secure communication between clients (kubectl, console, controllers) and the API server.

## Current Configuration

### Development Setup (Self-Signed Certificates)

The cluster is configured with auto-generated self-signed certificates:

- **Protocol:** TLS 1.3
- **Cipher:** AEAD-CHACHA20-POLY1305-SHA256
- **Certificate Type:** Self-signed (generated at startup)
- **Subject:** CN=k3rs-api; O=k3rs; C=US
- **SANs:** localhost, 127.0.0.1, api-server, k3rs-api-server

### Configuration in docker-compose.yml

```yaml
api-server:
  command:
    - "--tls"                    # Enable TLS
    - "--tls-self-signed"        # Auto-generate self-signed cert
    - "--tls-san"                # Subject Alternative Names
    - "localhost,127.0.0.1,api-server,k3rs-api-server"
```

## Connecting to the API Server

### Using curl

```bash
# With self-signed cert (skip verification)
curl -k https://localhost:6443/healthz

# Verbose output to see TLS details
curl -kv https://localhost:6443/api/v1
```

### Using kubectl

```bash
# Build kubectl
cargo build --bin kubectl

# Connect with TLS verification skipped (for self-signed cert)
./target/debug/kubectl \
  --server https://localhost:6443 \
  --insecure-skip-tls-verify \
  get pods
```

## Production Configuration

For production environments, use proper certificates from a trusted Certificate Authority.

### Option 1: Using External Certificates

1. **Obtain certificates** from a trusted CA (Let's Encrypt, corporate CA, etc.)

2. **Create certificates directory:**
   ```bash
   mkdir -p certs
   # Place your certificate files:
   # certs/tls.crt - Certificate chain
   # certs/tls.key - Private key
   ```

3. **Update docker-compose.yml:**
   ```yaml
   api-server:
     volumes:
       - ./certs/tls.crt:/certs/tls.crt:ro
       - ./certs/tls.key:/certs/tls.key:ro
     command:
       - "--bind-address"
       - "0.0.0.0:6443"
       - "--etcd-servers"
       - "http://etcd:2379"
       - "--tls"
       - "--tls-cert-file"
       - "/certs/tls.crt"
       - "--tls-key-file"
       - "/certs/tls.key"
   ```

4. **Restart the cluster:**
   ```bash
   podman-compose -f compose.yml down
   podman-compose -f compose.yml up -d
   ```

### Option 2: Using cert-manager (Kubernetes-style)

For a more Kubernetes-like approach, you could integrate cert-manager for automatic certificate management.

## Generating Certificates Manually

### Using OpenSSL

```bash
# Generate private key
openssl genrsa -out tls.key 2048

# Generate certificate signing request
openssl req -new -key tls.key -out tls.csr \
  -subj "/CN=k3rs-api/O=k3rs/C=US"

# Generate self-signed certificate
openssl x509 -req -in tls.csr -signkey tls.key -out tls.crt \
  -days 365 \
  -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1,DNS:api-server")
```

### Using cfssl

```bash
# Install cfssl
go install github.com/cloudflare/cfssl/cmd/cfssl@latest
go install github.com/cloudflare/cfssl/cmd/cfssljson@latest

# Create CA config
cat > ca-config.json <<EOF
{
  "signing": {
    "default": {
      "expiry": "8760h"
    },
    "profiles": {
      "k3rs": {
        "usages": ["signing", "key encipherment", "server auth"],
        "expiry": "8760h"
      }
    }
  }
}
EOF

# Create certificate request
cat > api-server-csr.json <<EOF
{
  "CN": "k3rs-api",
  "hosts": [
    "localhost",
    "127.0.0.1",
    "api-server",
    "k3rs-api-server"
  ],
  "key": {
    "algo": "rsa",
    "size": 2048
  },
  "names": [{
    "O": "k3rs",
    "C": "US"
  }]
}
EOF

# Generate certificates
cfssl gencert -initca ca-csr.json | cfssljson -bare ca
cfssl gencert \
  -ca=ca.pem \
  -ca-key=ca-key.pem \
  -config=ca-config.json \
  -profile=k3rs \
  api-server-csr.json | cfssljson -bare api-server
```

## API Server TLS Options

The API server supports these TLS-related flags:

| Flag | Description | Default |
|------|-------------|---------|
| `--tls` | Enable TLS/HTTPS | false |
| `--tls-cert-file` | Path to TLS certificate file | - |
| `--tls-key-file` | Path to TLS private key file | - |
| `--tls-self-signed` | Generate self-signed certificate | false |
| `--tls-san` | Subject Alternative Names (comma-separated) | localhost,127.0.0.1 |

## Verifying TLS Configuration

### Check Certificate Details

```bash
# Using openssl
echo | openssl s_client -connect localhost:6443 -servername localhost 2>/dev/null | \
  openssl x509 -noout -text | grep -E "Subject:|DNS:|IP Address:"

# Using curl verbose output
curl -kv https://localhost:6443/healthz 2>&1 | grep -E "SSL|TLS|subject|issuer"
```

### Test TLS Connection

```bash
# Basic connectivity
curl -k https://localhost:6443/healthz

# Should return empty response (no error = success)

# Full API test
curl -k https://localhost:6443/api/v1 | jq
```

## Troubleshooting

### "SSL certificate problem: self signed certificate"

This is expected with self-signed certificates. Use `-k` flag with curl or `--insecure-skip-tls-verify` with kubectl.

### "connection refused" after enabling TLS

Make sure to use `https://` instead of `http://`:
```bash
# Wrong
curl http://localhost:6443/healthz

# Correct
curl -k https://localhost:6443/healthz
```

### Certificate doesn't include required SANs

Update the `--tls-san` flag in docker-compose.yml to include all hostnames and IPs you'll use to access the API server:

```yaml
- "--tls-san"
- "localhost,127.0.0.1,api-server,my-domain.com,192.168.1.100"
```

### API server fails to start with custom certificates

Check:
1. Certificate and key files exist and are readable
2. Certificate and key match (same keypair)
3. Certificate is in PEM format
4. File paths in docker-compose.yml are correct
5. Volume mounts are configured correctly

## Security Best Practices

### Development

✅ Self-signed certificates are acceptable
✅ Use `--insecure-skip-tls-verify` for testing
✅ Keep certificates in the repository (they're self-signed)

### Production

❌ Never use self-signed certificates
❌ Never use `--insecure-skip-tls-verify`
❌ Never commit private keys to version control

✅ Use certificates from trusted CA
✅ Rotate certificates regularly (e.g., every 90 days)
✅ Use certificate monitoring/alerting
✅ Store private keys securely (secrets management)
✅ Use strong cipher suites (TLS 1.3)
✅ Enable certificate revocation checking

## Integration with Other Components

### Components that Connect to API Server

The following components need to be updated to use HTTPS:

1. **kubectl** - Add `--insecure-skip-tls-verify` or configure CA certificate
2. **Scheduler** - Currently uses etcd directly, no API server connection
3. **Controller Manager** - May need updates to use HTTPS
4. **Kubelet** - May need updates to use HTTPS

### Updating Component Configurations

If components need to connect to the API server over HTTPS, they'll need:

```rust
// Example: Configure HTTP client with TLS
let client = reqwest::Client::builder()
    .danger_accept_invalid_certs(true)  // Only for dev!
    .build()?;
```

## Monitoring TLS

### Log Messages

When TLS is enabled, the API server logs:

```
INFO TLS enabled - starting HTTPS server
WARN Generating self-signed certificate - NOT suitable for production!
INFO Self-signed cert SANs: ["localhost", "127.0.0.1", "api-server", "k3rs-api-server"]
INFO HTTPS server listening on 0.0.0.0:6443
```

When TLS is disabled:

```
INFO TLS disabled - starting HTTP server (not recommended for production)
INFO API Server listening on 0.0.0.0:6443
```

### Metrics

The API server exposes TLS-related metrics:
- Connection counts
- TLS version distribution
- Cipher suite usage

## Advanced Configuration

### Custom Cipher Suites

The API server uses rustls which automatically selects secure cipher suites. TLS 1.3 is preferred.

### Certificate Rotation

For production, implement certificate rotation:

1. **Generate new certificates before expiry**
2. **Update mounted certificate files**
3. **Restart API server:** `podman-compose -f compose.yml restart api-server`

The API server will load the new certificates on restart.

### Mutual TLS (mTLS)

For client authentication, you can extend the API server to require client certificates. This is not currently implemented but could be added.

## References

- [rustls Documentation](https://docs.rs/rustls/)
- [TLS 1.3 Specification](https://datatracker.ietf.org/doc/html/rfc8446)
- [Let's Encrypt](https://letsencrypt.org/)
- [cert-manager](https://cert-manager.io/)

## Summary

✅ **TLS is now enabled** in the k3rs development environment
✅ **Self-signed certificates** are auto-generated for convenience
✅ **TLS 1.3** provides modern, secure encryption
✅ **Easy to upgrade** to production certificates
✅ **Well documented** with examples and troubleshooting

For production deployments, replace self-signed certificates with proper certificates from a trusted CA.
