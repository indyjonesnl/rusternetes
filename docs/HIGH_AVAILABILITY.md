# High Availability (HA) in Rusternetes


> **Tip:** The web console deploys on all API server instances in HA mode. Access it via any API server or the HAProxy load balancer.
This document describes the High Availability features implemented in Rusternetes, providing production-grade reliability and fault tolerance.

## Overview

Rusternetes supports full HA deployment with:

- **Multi-node etcd cluster** (3-5 nodes with quorum)
- **Multi-master API servers** behind a load balancer
- **Leader election** for controller-manager and scheduler
- **Automatic failover** on component failure
- **Health checks** for all critical components

## Architecture

```
                    ┌─────────────────┐
                    │   HAProxy LB    │
                    │   (port 6443)   │
                    └────────┬────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
   ┌────▼────┐         ┌────▼────┐         ┌────▼────┐
   │API Srv 1│         │API Srv 2│         │API Srv 3│
   └────┬────┘         └────┬────┘         └────┬────┘
        │                   │                   │
        └───────────────────┼───────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
   ┌────▼────┐         ┌───▼────┐         ┌───▼────┐
   │ etcd-1  │◄───────►│ etcd-2 │◄───────►│ etcd-3 │
   │(Leader) │         │        │         │        │
   └─────────┘         └────────┘         └────────┘

   ┌──────────────────┐     ┌──────────────────┐
   │Controller-Mgr 1  │     │Controller-Mgr 2  │
   │    (Leader)      │     │   (Standby)      │
   └──────────────────┘     └──────────────────┘

   ┌──────────────────┐     ┌──────────────────┐
   │  Scheduler 1     │     │  Scheduler 2     │
   │    (Leader)      │     │   (Standby)      │
   └──────────────────┘     └──────────────────┘
```

## Components

### 1. etcd Cluster

**Purpose**: Distributed, consistent key-value store for all cluster state.

**HA Configuration**:
- **Cluster size**: 3 or 5 nodes (odd number for quorum)
- **Quorum**: Majority must be available (2/3 or 3/5)
- **Fault tolerance**: Can lose 1 node (3-node) or 2 nodes (5-node)

**Setup** (docker-compose.ha.yml):
```yaml
etcd-1:
  command:
    - --initial-cluster=etcd-1=http://etcd-1:2380,etcd-2=http://etcd-2:2380,etcd-3=http://etcd-3:2380
    - --initial-cluster-state=new
```

**Health Check**:
```bash
etcdctl --endpoints=http://localhost:2379 endpoint health
```

### 2. API Server Cluster

**Purpose**: Provides the Kubernetes API and serves as the front-end to the cluster.

**HA Configuration**:
- **Instances**: 3+ API servers (active-active)
- **Load Balancer**: HAProxy distributing traffic
- **Stateless**: No local state, all data in etcd

**Features**:
- Round-robin load balancing
- Automatic health checks
- Session affinity (optional)
- TLS termination support

**Endpoints**:
- Main API: `https://haproxy:6443`
- HAProxy stats: `http://haproxy:8404/stats`
- Individual servers: `https://api-server-{1,2,3}:6443`

**Health Endpoints**:
- `/healthz` - Liveness probe (server running)
- `/healthz/verbose` - Detailed health status
- `/readyz` - Readiness probe (ready for traffic)
- `/metrics` - Prometheus metrics

### 3. Controller Manager

**Purpose**: Runs controller loops for managing resources (Deployments, StatefulSets, etc.)

**HA Configuration**:
- **Instances**: 2+ controller managers
- **Mode**: Active-standby via leader election
- **Leader election**: `coordination.k8s.io/v1` Lease lock (client-go `leaderelection` semantics)

**How it works**:
1. All instances attempt to acquire leadership
2. One instance becomes leader, others standby
3. Leader runs all controllers
4. If leader fails, standby acquires leadership within ~15 seconds
5. Automatic re-election on leadership loss

**Configuration**:
```bash
controller-manager \
  --enable-leader-election \
  --leader-election-identity=controller-manager-1 \
  --leader-election-resource-name=kube-controller-manager \
  --leader-election-lease-duration=15
```

**Monitoring**:
```bash
# Check current leader
kubectl -n kube-system get lease kube-controller-manager -o jsonpath='{.spec.holderIdentity}'
```

### 4. Scheduler

**Purpose**: Assigns Pods to Nodes based on resource requirements and constraints.

**HA Configuration**:
- **Instances**: 2+ schedulers
- **Mode**: Active-standby via leader election
- **Leader election**: `coordination.k8s.io/v1` Lease lock (client-go `leaderelection` semantics)

**How it works**:
- Same leader election mechanism as controller-manager
- Only the leader schedules pods
- Automatic failover on leader failure

**Configuration**:
```bash
scheduler \
  --enable-leader-election \
  --leader-election-identity=scheduler-1 \
  --leader-election-lock-key=/rusternetes/scheduler/leader \
  --leader-election-lease-duration=15
```

## Leader Election

### Implementation

Leader election is implemented using etcd's lease and transaction primitives:

1. **Lease Creation**: Each instance creates a lease with TTL (default: 15s)
2. **Lock Acquisition**: Atomic transaction to set lock key if not exists
3. **Lease Renewal**: Leader renews lease every ~5s (1/3 of TTL)
4. **Failure Detection**: Standby instances detect leadership loss when lease expires
5. **Re-election**: Standby instances compete for leadership

### Key Features

- **Fast failover**: ~15 second detection + election time
- **Automatic recovery**: No manual intervention needed
- **Split-brain prevention**: etcd quorum prevents multiple leaders
- **Graceful shutdown**: Leaders release lock on clean shutdown

### Configuration

```rust
LeaderElectionConfig {
    identity: "unique-instance-id",
    lock_key: "/rusternetes/component/leader",
    lease_duration: 15,      // How long lease is valid (seconds)
    renew_interval: 5,       // How often to renew (seconds)
    retry_interval: 2,       // How often followers check (seconds)
}
```

## Deployment

### Using docker-compose (HA mode)

```bash
# Start HA cluster
docker-compose -f docker-compose.ha.yml up -d

# Check cluster health
./scripts/check-ha-health.sh

# View HAProxy stats
open http://localhost:8404/stats
```

### Component Scaling

**etcd**:
```bash
# Start with 3 nodes (recommended)
docker-compose -f docker-compose.ha.yml up -d etcd-1 etcd-2 etcd-3

# Scale to 5 nodes (optional, for higher availability)
# Add etcd-4 and etcd-5 to docker-compose.ha.yml
```

**API Servers**:
```bash
# Scale API servers (3+ recommended)
docker-compose -f docker-compose.ha.yml up -d --scale api-server=5
```

**Controllers & Scheduler**:
```bash
# Run 2+ instances (leader election ensures only one is active)
docker-compose -f docker-compose.ha.yml up -d \
  controller-manager-1 controller-manager-2 \
  scheduler-1 scheduler-2
```

## Health Checks

### API Server Health

```bash
# Liveness check (is the server running?)
curl http://localhost:6443/healthz

# Readiness check (is the server ready for traffic?)
curl http://localhost:6443/readyz

# Verbose health check
curl http://localhost:6443/healthz/verbose
```

Example response:
```json
{
  "status": "healthy",
  "checks": [
    {
      "name": "storage",
      "status": "ok",
      "message": "etcd connection healthy"
    },
    {
      "name": "metrics",
      "status": "ok",
      "message": "metrics collection active"
    }
  ]
}
```

### etcd Cluster Health

```bash
# Check endpoint health
docker exec rusternetes-etcd-1 etcdctl endpoint health

# Check cluster status
docker exec rusternetes-etcd-1 etcdctl endpoint status --cluster -w table

# Check member list
docker exec rusternetes-etcd-1 etcdctl member list
```

### HAProxy Health

```bash
# Stats page
curl http://localhost:8404/stats

# Check backend status
docker exec rusternetes-haproxy cat /proc/$(pidof haproxy)/status
```

## Failure Scenarios & Recovery

### Scenario 1: Single etcd Node Failure

**Impact**: None (cluster has quorum 2/3)

**Recovery**: Automatic
- Cluster continues operating
- Reads/writes succeed
- Failed node can rejoin when recovered

```bash
# Simulate failure
docker stop rusternetes-etcd-2

# Verify cluster still healthy
docker exec rusternetes-etcd-1 etcdctl endpoint health --cluster

# Recover
docker start rusternetes-etcd-2
```

### Scenario 2: API Server Failure

**Impact**: Minimal (HAProxy routes to healthy servers)

**Recovery**: Immediate
- HAProxy detects failure in ~5 seconds
- Traffic routed to remaining servers
- No client interruption

```bash
# Simulate failure
docker stop rusternetes-api-server-1

# Check HAProxy stats
curl http://localhost:8404/stats | grep api-server

# Recover
docker start rusternetes-api-server-1
```

### Scenario 3: Controller Manager Leader Failure

**Impact**: ~15 second delay in controller operations

**Recovery**: Automatic
- Standby detects leader loss
- New leader elected within lease duration
- Controllers resume operation

```bash
# Check current leader
docker exec rusternetes-etcd-1 etcdctl get /rusternetes/controller-manager/leader

# Simulate failure
docker stop rusternetes-controller-manager-1

# Watch re-election (should complete in ~15 seconds)
docker logs -f rusternetes-controller-manager-2
```

### Scenario 4: Scheduler Leader Failure

**Impact**: ~15 second delay in pod scheduling

**Recovery**: Same as controller-manager
- Standby becomes leader
- Pod scheduling resumes

### Scenario 5: Network Partition

**Impact**: Depends on partition

**Majority partition**:
- Cluster continues operating
- Minority partition becomes read-only

**Even split**:
- All partitions become unavailable (no quorum)
- Cluster restores when partition heals

**Prevention**:
- Use 5-node etcd cluster for better partition tolerance
- Deploy across multiple availability zones

## Monitoring

### Metrics

All components expose Prometheus metrics:

```bash
# API Server metrics
curl http://localhost:6443/metrics

# Scheduler metrics
curl http://localhost:8081/metrics

# etcd metrics
curl http://localhost:2379/metrics
```

### Key Metrics to Monitor

**etcd**:
- `etcd_server_has_leader` - Cluster has a leader (1 = yes)
- `etcd_server_leader_changes_seen_total` - Leader election count
- `etcd_disk_backend_commit_duration_seconds` - Disk performance

**API Server**:
- `apiserver_request_total` - Total API requests
- `apiserver_request_duration_seconds` - Request latency
- `storage_operations_total` - etcd operation count

**Leader Election**:
- Check logs for "Acquired leadership" / "Lost leadership"
- Monitor etcd lease TTL

### Logging

Enable debug logging for troubleshooting:

```yaml
environment:
  - RUST_LOG=debug
```

## Best Practices

### etcd Cluster

1. **Use odd numbers**: 3 or 5 nodes for proper quorum
2. **Fast storage**: Use SSDs for etcd data
3. **Low latency**: Co-locate etcd nodes or use fast networking
4. **Regular backups**: Backup etcd data periodically
5. **Monitor disk**: etcd is sensitive to disk latency

### API Servers

1. **Load balancing**: Use HAProxy or cloud load balancer
2. **Health checks**: Configure appropriate intervals (5-10s)
3. **TLS**: Always use TLS in production
4. **Resource limits**: Set appropriate CPU/memory limits
5. **Monitoring**: Monitor request latency and error rates

### Controllers & Scheduler

1. **Leader election**: Always enable in HA deployments
2. **Unique identities**: Use unique IDs for each instance
3. **Lease duration**: 15s is a good balance (faster = more etcd load)
4. **Graceful shutdown**: Handle SIGTERM properly
5. **Idempotency**: Ensure controllers are idempotent

### Production Checklist

- [ ] 3+ etcd nodes deployed
- [ ] 3+ API servers behind load balancer
- [ ] 2+ controller-managers with leader election
- [ ] 2+ schedulers with leader election
- [ ] Health checks configured
- [ ] Monitoring and alerting setup
- [ ] TLS certificates configured
- [ ] etcd backups scheduled
- [ ] Resource limits set
- [ ] Network policies configured

## Troubleshooting

### etcd Issues

**Problem**: etcd cluster unhealthy

```bash
# Check cluster status
docker exec rusternetes-etcd-1 etcdctl endpoint health --cluster

# Check for disk latency
docker exec rusternetes-etcd-1 etcdctl check perf

# View member list
docker exec rusternetes-etcd-1 etcdctl member list
```

**Problem**: Split brain / multiple leaders

```bash
# This should not happen with proper quorum
# Check if cluster has proper odd number of nodes
docker exec rusternetes-etcd-1 etcdctl member list

# Check for network partitions
# Ensure all nodes can communicate
```

### Leader Election Issues

**Problem**: No leader elected

```bash
# Check etcd connectivity
docker exec rusternetes-controller-manager-1 cat /proc/net/tcp

# Check logs
docker logs rusternetes-controller-manager-1 | grep -i leader

# Manually check lock key
docker exec rusternetes-etcd-1 etcdctl get /rusternetes/controller-manager/leader
```

**Problem**: Frequent leader changes

```bash
# Check network stability
# Check etcd latency
# Increase lease duration if necessary

# View leader election history
docker logs rusternetes-controller-manager-1 | grep "leadership"
```

### API Server Issues

**Problem**: API requests timing out

```bash
# Check HAProxy backend status
curl http://localhost:8404/stats

# Check individual API servers
for i in 1 2 3; do
  curl -k https://localhost:644$((3+i))/healthz
done

# Check etcd connectivity
docker logs rusternetes-api-server-1 | grep -i etcd
```

## Performance Tuning

### etcd

```bash
# Increase snapshot count (default: 100000)
--snapshot-count=200000

# Adjust heartbeat and election timeout
--heartbeat-interval=100
--election-timeout=1000
```

### Leader Election

```rust
// Faster failover (more etcd load)
LeaderElectionConfig {
    lease_duration: 10,
    renew_interval: 3,
    retry_interval: 1,
}

// Slower failover (less etcd load)
LeaderElectionConfig {
    lease_duration: 30,
    renew_interval: 10,
    retry_interval: 5,
}
```

## References

- [etcd Clustering Guide](https://etcd.io/docs/latest/op-guide/clustering/)
- [Kubernetes HA Best Practices](https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/ha-topology/)
- [HAProxy Configuration](http://www.haproxy.org/download/2.9/doc/configuration.txt)
