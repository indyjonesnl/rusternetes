# Network Policies in k3rs


> **Tip:** You can manage related resources through the [web console](../CONSOLE_USER_GUIDE.md).
## Overview

k3rs implements Kubernetes NetworkPolicy resources following the standard Kubernetes networking model. Network policies are enforced by **CNI (Container Network Interface) plugins** that support the NetworkPolicy API.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   k3rs Cluster                   │
│                                                          │
│  ┌──────────────────────────────────────────────────┐  │
│  │          NetworkPolicy Controller                 │  │
│  │  • Validates NetworkPolicy resources             │  │
│  │  • Tracks affected pods                          │  │
│  │  • Stores policies in etcd                       │  │
│  └──────────────────────────────────────────────────┘  │
│                       │                                  │
│                       │ (watches etcd)                   │
│                       ▼                                  │
│  ┌──────────────────────────────────────────────────┐  │
│  │               CNI Plugin                          │  │
│  │  (Calico, Cilium, Weave, etc.)                   │  │
│  │  • Watches NetworkPolicy from etcd/API           │  │
│  │  • Translates to iptables/eBPF rules             │  │
│  │  • Enforces traffic filtering                    │  │
│  └──────────────────────────────────────────────────┘  │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

## How It Works

### 1. NetworkPolicy Controller (k3rs)

The NetworkPolicy controller in `crates/controller-manager/src/controllers/network_policy.rs`:

**Responsibilities:**
- **Validates** NetworkPolicy resources against Kubernetes spec
- **Tracks** which pods are affected by each policy
- **Stores** validated policies in etcd for CNI plugins to consume
- **Provides** status information (planned feature)

**What It Does NOT Do:**
- Does **NOT** configure iptables directly
- Does **NOT** implement packet filtering
- Does **NOT** manage network interfaces

This follows the standard Kubernetes pattern where policy enforcement is delegated to CNI plugins.

### 2. CNI Plugin (External Component)

CNI plugins like Calico, Cilium, or Weave:

**Responsibilities:**
- **Watch** NetworkPolicy resources from etcd/API server
- **Translate** policies to low-level networking rules (iptables, eBPF, OVS flows)
- **Enforce** traffic filtering at the network layer
- **Update** policy status (if supported)

## Supported CNI Plugins

k3rs is compatible with any NetworkPolicy-capable CNI plugin:

### Recommended Plugins

#### 1. **Calico** (Most Popular)
- **Technology**: iptables + BGP
- **Performance**: Excellent
- **Features**: Full NetworkPolicy support, global policies, encryption
- **Best For**: Production deployments, complex network policies

**Installation:**
```bash
kubectl apply -f https://docs.projectcalico.org/manifests/calico.yaml
```

#### 2. **Cilium** (eBPF-based)
- **Technology**: eBPF (Linux kernel)
- **Performance**: Highest performance
- **Features**: Full NetworkPolicy support, L7 policies, observability
- **Best For**: Performance-critical deployments, modern kernels

**Installation:**
```bash
kubectl apply -f https://raw.githubusercontent.com/cilium/cilium/v1.14/install/kubernetes/quick-install.yaml
```

#### 3. **Weave Net**
- **Technology**: VXLAN + iptables
- **Performance**: Good
- **Features**: Automatic mesh networking, encryption
- **Best For**: Simple deployments, multi-cloud

**Installation:**
```bash
kubectl apply -f https://github.com/weaveworks/weave/releases/download/v2.8.1/weave-daemonset-k8s.yaml
```

### Comparison Matrix

| Feature | Calico | Cilium | Weave |
|---------|--------|--------|-------|
| NetworkPolicy Support | ✅ Full | ✅ Full | ✅ Full |
| Performance | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| L7 Policies | ❌ | ✅ | ❌ |
| Encryption | ✅ (WireGuard) | ✅ (IPSec/WireGuard) | ✅ (IPSec) |
| Observability | Good | Excellent (Hubble) | Basic |
| Complexity | Medium | High | Low |
| Production Ready | ✅ | ✅ | ✅ |

## NetworkPolicy Validation

k3rs validates NetworkPolicy resources before storage:

### Policy Types
- **Ingress**: Controls incoming traffic to pods
- **Egress**: Controls outgoing traffic from pods

### Selector Support
- **matchLabels**: Key-value label matching
- **matchExpressions**: Advanced selector expressions
  - `In`: Label value in list
  - `NotIn`: Label value not in list
  - `Exists`: Label exists (any value)
  - `DoesNotExist`: Label does not exist

### Port Specifications
- **Protocol**: TCP, UDP, or SCTP
- **Port**: Specific port number or name
- **endPort**: Port range support

### Peer Specifications
- **podSelector**: Match pods by labels
- **namespaceSelector**: Match namespaces by labels
- **ipBlock**: Match IP CIDR blocks (with exceptions)

## Testing Network Policies

### Prerequisites

1. **Install a NetworkPolicy-capable CNI plugin** (see above)
2. **Verify CNI plugin is running**:
   ```bash
   kubectl get pods -n kube-system | grep -E 'calico|cilium|weave'
   ```

### Example: Deny All Ingress

```yaml
# deny-all.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: deny-all-ingress
  namespace: default
spec:
  podSelector: {}  # Matches all pods in namespace
  policyTypes:
  - Ingress
  # No ingress rules = deny all ingress
```

Apply:
```bash
kubectl apply -f deny-all.yaml
```

### Example: Allow Specific Traffic

```yaml
# allow-app-to-db.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-app-to-db
  namespace: default
spec:
  podSelector:
    matchLabels:
      app: database
  policyTypes:
  - Ingress
  ingress:
  - from:
    - podSelector:
        matchLabels:
          app: web
    ports:
    - protocol: TCP
      port: 5432
```

This allows:
- Pods with label `app=web` → Pods with label `app=database` on port 5432

### Example: Egress Control

```yaml
# restrict-egress.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: restrict-egress
  namespace: default
spec:
  podSelector:
    matchLabels:
      tier: frontend
  policyTypes:
  - Egress
  egress:
  - to:
    - podSelector:
        matchLabels:
          tier: backend
    ports:
    - protocol: TCP
      port: 8080
  - to:  # Allow DNS
    - namespaceSelector:
        matchLabels:
          name: kube-system
      podSelector:
        matchLabels:
          k8s-app: kube-dns
    ports:
    - protocol: UDP
      port: 53
```

### Testing Policy Enforcement

1. **Create test pods**:
   ```bash
   # Client pod
   kubectl run client --image=nicolaka/netshoot --command -- sleep infinity

   # Server pod
   kubectl run server --image=nginx --labels="app=web"
   ```

2. **Test connectivity before policy**:
   ```bash
   kubectl exec client -- curl http://server
   # Should succeed
   ```

3. **Apply restrictive policy**:
   ```yaml
   apiVersion: networking.k8s.io/v1
   kind: NetworkPolicy
   metadata:
     name: deny-client-to-server
     namespace: default
   spec:
     podSelector:
       matchLabels:
         app: web
     policyTypes:
     - Ingress
     # No ingress rules = deny all
   ```

4. **Test connectivity after policy**:
   ```bash
   kubectl exec client -- curl --max-time 5 http://server
   # Should timeout (blocked by NetworkPolicy)
   ```

## Validation Examples

k3rs validates NetworkPolicy resources and provides clear error messages:

### Valid Policy

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: valid-policy
  namespace: default
spec:
  podSelector:
    matchLabels:
      app: myapp
    matchExpressions:
    - key: environment
      operator: In
      values: [prod, staging]
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - podSelector:
        matchLabels:
          role: frontend
    ports:
    - protocol: TCP
      port: 8080
```

### Invalid Policy (Will Be Rejected)

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: invalid-policy
spec:
  podSelector: {}
  policyTypes:
  - Invalid  # ❌ Must be "Ingress" or "Egress"
  ingress:
  - ports:
    - protocol: HTTP  # ❌ Must be TCP, UDP, or SCTP
```

**Error Message:**
```
NetworkPolicy validation failed: Invalid policy type 'Invalid', must be 'Ingress' or 'Egress'
```

## Troubleshooting

### Policies Not Working

**1. Check CNI Plugin is Installed**
```bash
kubectl get pods -n kube-system
# Look for calico, cilium, or weave pods
```

**2. Verify CNI Plugin Supports NetworkPolicy**
```bash
# For Calico
kubectl get pods -n kube-system -l k8s-app=calico-node

# For Cilium
kubectl get pods -n kube-system -l k8s-app=cilium

# For Weave
kubectl get pods -n kube-system -l name=weave-net
```

**3. Check NetworkPolicy Exists**
```bash
kubectl get networkpolicies --all-namespaces
kubectl describe networkpolicy <name> -n <namespace>
```

**4. Check Controller Logs**
```bash
# k3rs controller-manager logs
kubectl logs -n kube-system deployment/controller-manager | grep NetworkPolicy

# Or with docker-compose
docker-compose logs controller-manager | grep NetworkPolicy
```

**5. Check CNI Plugin Logs**
```bash
# Calico
kubectl logs -n kube-system -l k8s-app=calico-node

# Cilium
kubectl logs -n kube-system -l k8s-app=cilium

# Weave
kubectl logs -n kube-system -l name=weave-net
```

### Policy Not Affecting Pods

**Common Causes:**
1. **Selector mismatch**: Pod labels don't match policy selector
2. **Wrong namespace**: Policy only affects pods in same namespace
3. **CNI plugin not running**: No enforcement without CNI
4. **Default allow**: Without policies, all traffic is allowed

**Debug Steps:**
```bash
# Check pod labels
kubectl get pod <pod-name> --show-labels

# Check which policies select the pod
kubectl get networkpolicies -o yaml | grep -A10 podSelector

# Verify CNI plugin sees the policy (Calico example)
kubectl exec -n kube-system <calico-node-pod> -- calicoctl get networkPolicy
```

### Performance Issues

If network policies cause performance problems:

1. **Reduce policy complexity**:
   - Fewer rules per policy
   - Simpler selectors
   - Avoid wildcard matches

2. **Use namespace selectors** instead of pod selectors when possible

3. **Consider Cilium** for better performance (eBPF vs iptables)

4. **Monitor CNI plugin resource usage**:
   ```bash
   kubectl top pods -n kube-system
   ```

## Advanced Features

### Default Policies

Create default deny policies in each namespace:

```yaml
# default-deny-all.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-all
  namespace: production
spec:
  podSelector: {}
  policyTypes:
  - Ingress
  - Egress
```

Then explicitly allow required traffic.

### Combined Ingress and Egress

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: strict-app-policy
spec:
  podSelector:
    matchLabels:
      app: secure-app
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - podSelector:
        matchLabels:
          allowed: "true"
  egress:
  - to:
    - podSelector:
        matchLabels:
          app: database
```

### IP Block Rules

Allow/deny specific IP ranges:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-external-ips
spec:
  podSelector:
    matchLabels:
      app: api-gateway
  policyTypes:
  - Ingress
  ingress:
  - from:
    - ipBlock:
        cidr: 203.0.113.0/24
        except:
        - 203.0.113.5/32  # Block specific IP
```

## Integration with k3rs

### Controller Status

Check NetworkPolicy controller status:

```bash
# Controller-manager logs
docker-compose logs controller-manager | grep "NetworkPolicy controller"
```

Expected output:
```
INFO NetworkPolicy controller starting reconciliation
INFO Found 3 network policies to reconcile
INFO NetworkPolicy default/allow-app-to-db affects 5 pods
INFO NetworkPolicy default/allow-app-to-db reconciled (enforcement delegated to CNI plugin)
```

### matchExpressions Support

k3rs fully supports matchExpressions in NetworkPolicy selectors:

```yaml
spec:
  podSelector:
    matchExpressions:
    - key: tier
      operator: In
      values: [frontend, backend]
    - key: environment
      operator: NotIn
      values: [development]
    - key: security
      operator: Exists
    - key: deprecated
      operator: DoesNotExist
```

Supported operators:
- `In`: Label value must be in the provided list
- `NotIn`: Label value must not be in the provided list
- `Exists`: Label key must exist (value doesn't matter)
- `DoesNotExist`: Label key must not exist

## Production Best Practices

1. **Start with default deny**:
   - Create deny-all policies in each namespace
   - Explicitly allow required traffic
   - Reduces attack surface

2. **Use namespace isolation**:
   - Separate environments (dev, staging, prod) by namespace
   - Use namespace selectors for cross-namespace communication

3. **Document policies**:
   - Add annotations explaining policy purpose
   - Keep policies in version control
   - Review regularly

4. **Test before applying**:
   - Test in dev/staging first
   - Use `kubectl describe` to verify selectors
   - Monitor CNI plugin logs

5. **Monitor policy violations**:
   - Configure CNI plugin logging
   - Set up alerts for blocked traffic
   - Review logs regularly

6. **Keep policies simple**:
   - One policy per application/tier
   - Clear, descriptive names
   - Avoid overly complex selectors

## References

- **Kubernetes NetworkPolicy Docs**: https://kubernetes.io/docs/concepts/services-networking/network-policies/
- **Calico NetworkPolicy Guide**: https://docs.projectcalico.org/security/kubernetes-network-policy
- **Cilium NetworkPolicy Tutorial**: https://docs.cilium.io/en/stable/gettingstarted/http/
- **Weave NetworkPolicy Docs**: https://www.weave.works/docs/net/latest/kubernetes/kube-addon/#npc

## Files

- **Controller Implementation**: `crates/controller-manager/src/controllers/network_policy.rs`
- **Validation Logic**: Lines 97-255 (policy validation)
- **Selector Matching**: Lines 275-337 (pod selector and matchExpressions)
- **Tests**: Lines 340-561 (comprehensive unit tests)
