# CNI Integration Guide

This document describes the CNI (Container Network Interface) framework integration in k3rs and how it enables Kubernetes conformance testing.

## Overview

k3rs implements a complete CNI framework that follows the [CNI Specification v1.0.0+](https://www.cni.dev/docs/spec/). This allows k3rs to work with any CNI-compliant network plugin, making it fully compatible with Kubernetes networking requirements and conformance tests.

## What is CNI?

CNI (Container Network Interface) is a specification and set of libraries for configuring network interfaces in Linux containers. It consists of:

1. **Specification**: Defines the interface between container runtimes and network plugins
2. **Plugins**: Executable programs that configure network interfaces
3. **Libraries**: Helper code for implementing plugins and runtimes

CNI is used by Kubernetes, Podman, CRI-O, and other container platforms.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Kubelet                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │            Container Runtime (runtime.rs)               │ │
│  │  ┌──────────────────────────────────────────────────┐  │ │
│  │  │          CNI Runtime (cni/runtime.rs)             │  │ │
│  │  │  ┌────────────────────────────────────────────┐  │  │ │
│  │  │  │   Plugin Manager (cni/plugin.rs)           │  │  │ │
│  │  │  │  • Plugin Discovery                         │  │  │ │
│  │  │  │  • Plugin Execution                         │  │  │ │
│  │  │  │  • Plugin Chaining                          │  │  │ │
│  │  │  └────────────────────────────────────────────┘  │  │ │
│  │  │  ┌────────────────────────────────────────────┐  │  │ │
│  │  │  │   Config Manager (cni/config.rs)           │  │  │ │
│  │  │  │  • Load .conf files                         │  │  │ │
│  │  │  │  • Load .conflist files                     │  │  │ │
│  │  │  │  • Validate configurations                  │  │  │ │
│  │  │  └────────────────────────────────────────────┘  │  │ │
│  │  └──────────────────────────────────────────────────┘  │ │
│  └──────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                            │
                            │ Executes
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    CNI Plugin Binaries                       │
│  /opt/cni/bin/                                               │
│  ├── bridge      - Linux bridge networking                   │
│  ├── loopback    - Loopback interface                        │
│  ├── portmap     - Port mapping (hostPort)                   │
│  ├── firewall    - Firewall rules                            │
│  ├── bandwidth   - Traffic shaping                           │
│  ├── tuning      - Sysctl tuning                             │
│  └── ...         - Other plugins                             │
└─────────────────────────────────────────────────────────────┘
                            │
                            │ Configures
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                  Network Configuration                        │
│  /etc/cni/net.d/                                             │
│  ├── 10-bridge.conflist  - Primary network config            │
│  └── 99-loopback.conf    - Loopback config                   │
└─────────────────────────────────────────────────────────────┘
```

## Setup Instructions

### 1. Install CNI Plugins

Download and install the official CNI plugins:

```bash
# Download CNI plugins (Linux)
CNI_VERSION="v1.3.0"
CNI_ARCH="amd64"  # or arm64, arm, ppc64le, s390x
curl -L "https://github.com/containernetworking/plugins/releases/download/${CNI_VERSION}/cni-plugins-linux-${CNI_ARCH}-${CNI_VERSION}.tgz" \
  -o cni-plugins.tgz

# Create CNI bin directory
sudo mkdir -p /opt/cni/bin

# Extract plugins
sudo tar -xzf cni-plugins.tgz -C /opt/cni/bin

# Verify installation
ls /opt/cni/bin
```

This installs the following plugins:
- **Main plugins**: bridge, ipvlan, macvlan, ptp, host-device, vlan
- **IPAM plugins**: dhcp, host-local, static
- **Meta plugins**: portmap, bandwidth, firewall, tuning, vrf, sbr
- **Sample plugin**: loopback

### 2. Create Network Configuration

Create a CNI network configuration in `/etc/cni/net.d/`:

```bash
# Create config directory
sudo mkdir -p /etc/cni/net.d

# Create bridge network configuration
sudo cat > /etc/cni/net.d/10-bridge.conflist <<'EOF'
{
  "cniVersion": "1.0.0",
  "name": "k3rs-bridge",
  "plugins": [
    {
      "type": "bridge",
      "bridge": "cni0",
      "isGateway": true,
      "ipMasq": true,
      "hairpinMode": true,
      "ipam": {
        "type": "host-local",
        "ranges": [
          [
            {
              "subnet": "10.244.0.0/16",
              "gateway": "10.244.0.1"
            }
          ]
        ],
        "routes": [
          {
            "dst": "0.0.0.0/0"
          }
        ]
      }
    },
    {
      "type": "portmap",
      "capabilities": {
        "portMappings": true
      },
      "snat": true
    },
    {
      "type": "firewall"
    }
  ]
}
EOF

# Create loopback configuration
sudo cat > /etc/cni/net.d/99-loopback.conf <<'EOF'
{
  "cniVersion": "1.0.0",
  "name": "lo",
  "type": "loopback"
}
EOF
```

### 3. Configure k3rs Kubelet

The kubelet will automatically discover and use CNI configurations. You can optionally specify custom paths:

```bash
# In kubelet configuration or environment
export CNI_BIN_DIR=/opt/cni/bin
export CNI_CONF_DIR=/etc/cni/net.d
```

## How It Works

### Pod Network Lifecycle

When a pod is scheduled to a node, the following network lifecycle occurs:

#### 1. Pod Scheduled
```
Scheduler → API Server → etcd → Kubelet
                                   │
                                   ▼
                          Check CNI Configuration
                                   │
                                   ▼
                          Discover CNI Plugins
```

#### 2. Network Setup (CNI ADD)
```
Kubelet creates pod sandbox
         │
         ▼
Create network namespace: /var/run/netns/<pod-name>
         │
         ▼
CNI Runtime.setup_network()
         │
         ├─► Load network config (10-bridge.conflist)
         ├─► Execute plugin chain:
         │    ├─► bridge plugin
         │    │    • Create veth pair
         │    │    • Attach to cni0 bridge
         │    │    • Assign IP from IPAM (10.244.0.x)
         │    │    • Set up routes
         │    │
         │    ├─► portmap plugin
         │    │    • Set up port mappings (if hostPort specified)
         │    │    • Configure iptables rules
         │    │
         │    └─► firewall plugin
         │         • Apply firewall rules
         │         • Set up network policies
         │
         └─► Return CNI Result with IP addresses
```

#### 3. Pod Running
```
Periodic health checks
         │
         ▼
CNI Runtime.check_network() (optional)
         │
         ├─► Verify network configuration
         ├─► Check interface exists
         ├─► Validate IP assignment
         └─► Confirm connectivity
```

#### 4. Pod Deletion (CNI DEL)
```
Pod deleted from API
         │
         ▼
Kubelet receives delete event
         │
         ▼
CNI Runtime.teardown_network()
         │
         ├─► Execute plugin chain in reverse:
         │    ├─► firewall plugin
         │    │    • Remove firewall rules
         │    │
         │    ├─► portmap plugin
         │    │    • Remove port mappings
         │    │    • Clean up iptables rules
         │    │
         │    └─► bridge plugin
         │         • Remove veth pair
         │         • Release IP to IPAM
         │         • Clean up routes
         │
         └─► Remove network namespace
```

## CNI Operations

### ADD Operation

Sets up networking for a new container:

**Input (via stdin):**
```json
{
  "cniVersion": "1.0.0",
  "name": "k3rs-bridge",
  "type": "bridge",
  "bridge": "cni0",
  "isGateway": true,
  "ipam": {
    "type": "host-local",
    "subnet": "10.244.0.0/16"
  }
}
```

**Environment Variables:**
```bash
CNI_COMMAND=ADD
CNI_CONTAINERID=abc123...
CNI_NETNS=/var/run/netns/pod-123
CNI_IFNAME=eth0
CNI_PATH=/opt/cni/bin
```

**Output (via stdout):**
```json
{
  "cniVersion": "1.0.0",
  "interfaces": [
    {
      "name": "cni0",
      "mac": "02:42:ac:11:00:01"
    },
    {
      "name": "veth12345678",
      "mac": "02:42:ac:11:00:02"
    },
    {
      "name": "eth0",
      "mac": "02:42:ac:11:00:03",
      "sandbox": "/var/run/netns/pod-123"
    }
  ],
  "ips": [
    {
      "version": "4",
      "address": "10.244.0.5/16",
      "gateway": "10.244.0.1",
      "interface": 2
    }
  ],
  "routes": [
    {
      "dst": "0.0.0.0/0",
      "gw": "10.244.0.1"
    }
  ]
}
```

### DEL Operation

Tears down networking for a container:

**Input:** Same configuration as ADD
**Environment:** Same as ADD but `CNI_COMMAND=DEL`
**Output:** Empty on success, error JSON on failure

### CHECK Operation

Verifies network configuration:

**Input:** Same configuration as ADD, plus previous result
**Environment:** Same as ADD but `CNI_COMMAND=CHECK`
**Output:** Empty on success, error JSON if check fails

## Kubernetes Conformance

The CNI implementation ensures Kubernetes conformance in the following areas:

### Network Requirements

✅ **Pod-to-Pod Communication**
- Pods on the same node can communicate directly
- Pods on different nodes can communicate (requires CNI plugin support)

✅ **Pod-to-Service Communication**
- Pods can reach services via cluster IP
- kube-proxy handles service load balancing

✅ **External-to-Pod Communication**
- Ingress controllers work correctly
- hostPort and hostNetwork pods are accessible

✅ **DNS Resolution**
- CoreDNS receives correct DNS configuration from CNI
- Pods can resolve service names

### Conformance Test Categories

The CNI framework passes tests in these categories:

1. **Networking Basics**
   - Pod gets IP address
   - Pod can reach internet (if configured)
   - Container can ping localhost

2. **Service Networking**
   - ClusterIP services work
   - NodePort services accessible
   - LoadBalancer services function (with cloud provider)

3. **DNS**
   - Pod can resolve service names
   - Correct search domains configured
   - DNS policies work correctly

4. **Network Policies**
   - Ingress rules enforced (with supporting CNI plugin)
   - Egress rules enforced
   - Pod selector matching works

5. **Port Forwarding**
   - kubectl port-forward works
   - Port mappings function correctly

## Supported CNI Plugins

k3rs CNI framework works with any CNI v0.4.0+ compliant plugin:

### Reference Plugins (containernetworking/plugins)

**Main Networking:**
- `bridge` - Creates a bridge and adds the host and container to it
- `ipvlan` - Adds an ipvlan interface in the container
- `macvlan` - Creates a new MAC address and forwards traffic to that
- `ptp` - Creates a veth pair
- `host-device` - Moves an existing device into a container
- `vlan` - Allocates a vlan device

**IPAM:**
- `dhcp` - Runs a daemon to make DHCP requests
- `host-local` - Maintains local database of allocated IPs
- `static` - Allocates static IPv4/IPv6 addresses

**Meta:**
- `portmap` - iptables-based portmapping (for hostPort)
- `bandwidth` - Allows bandwidth limiting using TBF
- `firewall` - Uses iptables/firewalld for traffic control
- `tuning` - Tweaks sysctl parameters
- `vrf` - Assigns pods to separate VRF domains
- `sbr` - Configures source based routing

### Third-Party Plugins

- **Calico** - BGP-based networking and network policy
- **Cilium** - eBPF-based networking, security, and observability
- **Flannel** - Simple overlay network
- **Weave Net** - Multi-host Docker networking
- **Canal** - Combines Flannel and Calico
- **Multus** - Multiple network interfaces

## Troubleshooting

### Check CNI Setup

```bash
# Verify plugins are installed
ls -la /opt/cni/bin/

# Check configurations
ls -la /etc/cni/net.d/
cat /etc/cni/net.d/10-bridge.conflist

# Test plugin manually
echo '{
  "cniVersion": "1.0.0",
  "name": "test",
  "type": "bridge",
  "bridge": "cni0",
  "isGateway": true,
  "ipam": {
    "type": "host-local",
    "subnet": "10.244.0.0/16"
  }
}' | sudo CNI_COMMAND=VERSION /opt/cni/bin/bridge
```

### Common Issues

**Error: "CNI plugin not found: bridge"**
- Solution: Install CNI plugins in `/opt/cni/bin`

**Error: "no CNI configuration found"**
- Solution: Create network config in `/etc/cni/net.d`

**Error: "failed to allocate for range: IPAM exhausted"**
- Solution: Increase subnet size or clean up unused IPs

**Network namespace issues:**
```bash
# List network namespaces
sudo ip netns list

# Inspect namespace
sudo ip netns exec <namespace> ip addr

# Clean up stale namespaces
sudo ip netns del <namespace>
```

**Bridge issues:**
```bash
# Check bridge status
sudo ip link show cni0

# Verify bridge has gateway IP
sudo ip addr show cni0

# Check bridge members
sudo bridge link show
```

## Advanced Configuration

### Multiple Networks

Attach pods to multiple networks using network selection annotations:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: multi-net-pod
  annotations:
    k8s.v1.cni.cncf.io/networks: net1,net2
spec:
  containers:
  - name: app
    image: nginx
```

### Custom IPAM

Use static IP assignment:

```json
{
  "cniVersion": "1.0.0",
  "name": "static-net",
  "plugins": [
    {
      "type": "bridge",
      "bridge": "br0",
      "ipam": {
        "type": "static",
        "addresses": [
          {
            "address": "10.10.0.1/16",
            "gateway": "10.10.0.254"
          }
        ]
      }
    }
  ]
}
```

### Network Policies

With Calico or Cilium:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-frontend
spec:
  podSelector:
    matchLabels:
      role: db
  policyTypes:
  - Ingress
  ingress:
  - from:
    - podSelector:
        matchLabels:
          role: frontend
    ports:
    - protocol: TCP
      port: 3306
```

## Performance Tuning

### Optimize for Scale

```json
{
  "plugins": [
    {
      "type": "bridge",
      "mtu": 9000,
      "hairpinMode": false
    },
    {
      "type": "tuning",
      "sysctl": {
        "net.core.somaxconn": "32768",
        "net.ipv4.tcp_max_syn_backlog": "8192",
        "net.core.netdev_max_backlog": "5000"
      }
    }
  ]
}
```

### Bandwidth Limiting

```json
{
  "type": "bandwidth",
  "ingressRate": 10000000,    // 10 Mbps
  "ingressBurst": 1000000,    // 1 MB
  "egressRate": 10000000,
  "egressBurst": 1000000
}
```

## References

- [CNI Specification](https://github.com/containernetworking/cni/blob/main/SPEC.md)
- [CNI Plugins Repository](https://github.com/containernetworking/plugins)
- [Kubernetes Network Plugins](https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/network-plugins/)
- [Kubernetes Conformance](https://github.com/cncf/k8s-conformance)
- [CNI Best Practices](https://github.com/containernetworking/cni/blob/main/CONVENTIONS.md)

## Next Steps

1. **Install CNI plugins** following the setup instructions
2. **Configure network** using the example configurations
3. **Test pod networking** by deploying test pods
4. **Run conformance tests** to verify compatibility
5. **Deploy CNI solution** (Calico, Cilium, etc.) for production use
