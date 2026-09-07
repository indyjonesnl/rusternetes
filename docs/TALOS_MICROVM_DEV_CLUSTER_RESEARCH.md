# Research: `talosctl cluster create` (QEMU provisioner) → a Rusternetes local multi-node dev cluster

> Research + design note. **No code.** Goal: understand exactly how Talos spins
> up a multi-node cluster of local VMs from one command on one Linux box, and
> decide what Rusternetes should port, reuse, or shell out to in order to get
> `1 control-plane + 2 workers` as *real nodes* (own kernel, own disk, own
> network stack) for local development and testing.
>
> Companion to [`TALOS_WORKER_USB_RESEARCH.md`](TALOS_WORKER_USB_RESEARCH.md),
> which covers the Talos *install/OS* model (#1036). That note answers "how does
> a Rusternetes node boot from a USB stick"; this one answers "how do I get three
> of them on my laptop in under a minute."

**Sources read** (verbatim, on this machine): `../talos-reference` @ `f6058a1`
(2026-06-10), plus the installed `talosctl v1.13.0`. Talos is **MPL-2.0**;
Rusternetes is **Apache-2.0** — see [§7 Licensing](#7-licensing-constraint-mpl-20).

---

## 1. Why we want this

The current dev/test substrate is `compose.sqlite.yml`: every "node" is a
container sharing the host kernel. That has taken us a long way (sig-network
47/47, NC 191/191), but it structurally cannot exercise:

- **Node lifecycle** — reboot, power-off, `NotReady` → `Ready` transitions,
  graceful shutdown, disk wipe and rejoin. A container restart is not a reboot.
- **Per-node kernel state** — cgroup v2 hierarchy per node, real
  `/proc/meminfo` and `/proc/pressure` for eviction, sysctls, kernel modules,
  a real `/dev` and real block devices for CSI / local PV work.
- **Honest kube-proxy** — today kube-proxy runs in the host netns and writes
  host iptables, which is why parallel local conformance runs must use DinD
  (see the local-conformance harness notes). Each VM owning its own netfilter
  tables removes that whole class of interference.
- **Real node isolation for scheduler/eviction tests** — memory pressure,
  disk pressure, taints from actual node conditions rather than fabricated
  status patches.
- **The footprint story** — the ROADMAP's north star is idle RAM of a node.
  A VM with a fixed memory allotment measures that honestly; a container on a
  62 GB host does not.

Talos already solved "N local nodes from one command", and its provisioner is
**~5.1k LOC of Go** for the whole VM + QEMU stack. That is portable.

---

## 2. What the command actually is

```
talosctl cluster create           # dispatches to a provider
talosctl cluster create qemu      # local QEMU VMs   ← the interesting one
talosctl cluster create docker    # containers (≈ our compose stack)
talosctl cluster create dev       # qemu, using locally-built artifacts
```

Defaults (`cmd/talosctl/cmd/mgmt/cluster/create/clusterops/options.go:171-182`):
1 control-plane, 1 worker, **2 GiB RAM / 2 vCPU per node**, two virtio disks
(`10GiB,6GiB`), network `10.5.0.0/24`, MTU 1500, a 20-minute cluster
health-check wait, state in `~/.talos/clusters/<name>`.

Everything is one process tree rooted at the CLI — **no libvirt, no
virt-manager, no daemon**. `talosctl` re-execs *itself* as a per-VM supervisor.

There are three providers behind one `provision.Provisioner` interface
(`pkg/provision/provision.go:19`): `Create`, `Destroy`, `Reflect` (re-attach to
an existing cluster from its state dir), plus endpoint accessors. The QEMU
provider is `qemu.provisioner` embedding a shared `vm.Provisioner` — i.e. the
generic "VM-ish" machinery (network, dhcpd, dnsd, LB, disks, pidfiles) is
already factored out from the QEMU-specific parts (args, pflash, TPM).

---

## 3. Anatomy of `create` (the part worth stealing)

Sequence, from `pkg/provision/providers/qemu/create.go:19`:

1. **Preflight** (`qemu/preflight.go:43`) — *root required*
   ("please run as root user (CNI, qemu hvf requirement), we recommend
   `sudo -E`"), CNI dirs exist, required CNI plugins present, iptables
   reachable, `/dev/kvm` openable (a **warning**, not fatal — falls back to TCG).
2. **State directory** `~/.talos/clusters/<name>/` — the single source of truth.
   Holds per-node `.disk`, `.log`, `.pid`, `.config`, `.monitor` socket, the
   IPAM db, and a serialized `ClusterInfo` so a later `talosctl cluster
   show/destroy` can re-attach without any daemon.
3. **Network** (`vm/network_linux.go:41`) — bridge name is
   `"talos" + sha256(networkName)[:8]` so it is deterministic per cluster and
   collision-free across clusters. It is created by invoking the **standard CNI
   `bridge` plugin** once against a throwaway netns (just to get the gateway IP
   assigned), then the real per-VM config list is stored in state:
   `bridge` → `firewall` → **`tc-redirect-tap`**. It also inserts
   `-i br -o br -j ACCEPT` into `DOCKER-USER`, because Docker turns on
   `br-netfilter` and would otherwise filter L2 bridge traffic.
4. **Bridge services**, each a daemonized re-exec of the same binary with a
   pidfile: **TCP load balancer** on `gateway:6443` fanning out to the
   control-plane IPs (`vm/loadbalancer.go:26`), **DHCPv4/v6 server**
   (`vm/dhcpd.go`), **DNS** (`vm/dnsd.go`), optional **TFTP/iPXE**, **KMS**,
   **JSON log sink**, **virtiofsd**, and an **image cache**.
5. **Nodes**, control-plane first then workers, created **concurrently**
   (`qemu/node.go:286`, one goroutine per node, errors joined).
6. **`ClusterInfo` saved**, then `ShowCluster` prints the table.

### 3.1 Per-node: disks, boot, config injection

- **Disks** (`vm/disk.go:29`): plain `os.Create` + `Truncate` to a 4 MiB-aligned
  size + `fallocate` (skippable → sparse). No qcow2, no qemu-img. Drivers
  supported per disk: `virtio`, `ide`, `ahci`, `scsi`, `nvme`, `megaraid`,
  `virtiofs` — that breadth exists so Talos can test disk-selector logic.
- **Boot** is one of: ISO (default preset), USB image, UKI, or direct
  `-kernel`/`-initrd`. Disk bootability is probed with `blkid` (`gpt` + at
  least one partition) so a wiped node re-attaches install media automatically.
- **Config injection** — two mechanisms:
  - `talos.config=http://<host>:<port>/config.yaml` on the kernel cmdline,
    served by an **in-memory HTTP server inside the per-node supervisor**
    (`vm/launch.go:102`). The cmdline carries a `{TALOS_CONFIG_URL}` placeholder
    that the supervisor patches once it knows its own port.
  - `metal-iso`: a one-file ISO built with `mkisofs` and attached as a cdrom.

### 3.2 Per-node: the supervisor process (the key design idea)

`createNode` writes a JSON `LaunchConfig` and then
(`qemu/node.go:265`):

```go
cmd := exec.Command(clusterReq.SelfExecutable, "qemu-launch")
cmd.Stdin = launchConfigFile          // config over stdin
cmd.Stdout, cmd.Stderr = logFile, logFile
cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}   // daemonize
```

…and records the PID. The hidden `talosctl qemu-launch` command
(`qemu/launch.go:503`) is a **long-lived, per-VM supervisor** that:

- reads its config from stdin, installs SIGTERM/SIGINT handlers, ignores SIGHUP;
- starts the in-memory HTTP server that both **serves the machine config** and
  exposes a tiny control API: `POST /poweron`, `/poweroff?grace-period=…`,
  `/reboot`, `/pxeboot`, `GET /status`. *This* is the mechanism behind
  `talosctl cluster` power operations — there is no hypervisor daemon to ask;
- **creates a network namespace, runs CNI `ADD` into it**, and extracts the
  `(vmIface, tapIface)` pair from the CNI result (`cniutils.VMTapPair`,
  requires `tc-redirect-tap`); takes the VM MAC from the CNI result;
- dumps an **IPAM record** (`MAC → IP/netmask/gateway/hostname/MTU/DNS`) into
  the shared state dir (`vm/ipam.go:38`) — that file *is* the DHCP database the
  bridge-level dhcpd serves from. Elegant: address assignment is CNI's job,
  DHCP is just a delivery mechanism, and no component needs to talk to another;
- **starts `qemu` inside that netns** (`launch_linux.go:213`,
  `ns.WithNetNSPath(... cmd.Start())`) and supervises it in a `for` loop —
  `-no-reboot` plus relaunch means a guest reboot is a real cold boot;
- serializes concurrent CNI calls with a **file mutex** in the state dir,
  because the CNI plugins race.

QEMU arg choices worth copying verbatim (`qemu/launch.go:109`):
`-netdev tap,ifname=<cni tap>,script=no,downscript=no` +
`virtio-net-pci,host_mtu=`, `virtio-blk-pci` with explicit
logical/physical block size, `virtio-rng-pci`,
**`virtio-balloon,deflate-on-oom=on`**, `-monitor unix:…` (used to send
`system_powerdown` for graceful shutdown before falling back to `kill`,
`launch.go:443`), an **`i6300esb` watchdog with `-watchdog-action pause`**,
`-smbios type=1,uuid=<node uuid>` for stable node identity, and a
`virtio-serial` + qemu-guest-agent channel.

### 3.3 Destroy

Stop each pidfile'd process with SIGTERM + poll (`vm/process.go:19`), delete
the bridge via rtnetlink, drop the `DOCKER-USER` rule, remove the state dir.
CNI teardown happens in the supervisor's `defer` — and `withNetworkContext`
also runs a `DEL` *before* `ADD` to clean up a previous crashed run.

---

## 4. Path A — Talos as the substrate, and what it actually costs

Talos runs the Kubernetes control plane as **static pods whose images come from
the machine config**, and the kubelet from a configurable image. Image
validation only checks that the **tag** parses as a Kubernetes version inside
the Talos↔K8s compatibility window — the repository name is **not** constrained
(`pkg/machinery/compatibility/kubernetes_image.go:15-29`,
`KubernetesVersionFromImageRef` splits on the last `:v`):

```yaml
# rusternetes-swap.yaml
cluster:
  apiServer:         { image: ghcr.io/indyjonesnl/rusternetes/api-server:v1.35.0 }
  controllerManager: { image: ghcr.io/indyjonesnl/rusternetes/controller-manager:v1.35.0 }
  scheduler:         { image: ghcr.io/indyjonesnl/rusternetes/scheduler:v1.35.0 }
```

```bash
sudo -E talosctl cluster create qemu \
  --controlplanes 1 --workers 2 \
  --config-patch @rusternetes-swap.yaml
```

### 4.1 Why this is NOT free (corrects an earlier draft of this note)

**Talos hard-codes the static pod's `command`.** The generated pod is
`Command: args` with `args[0]` a literal
(`control_plane_static_pod.go:290,483-484` for the api-server;
`control_plane_final.go:58,149` for controller-manager and scheduler):

```go
args := []string{"/usr/local/bin/kube-apiserver"}   // then ~40 upstream flags
...
Container{Image: cfg.Image, Command: args}
```

Only `image`, `extraArgs`, `extraVolumes`, `resources` and `environmentVariables`
are configurable. So a swapped image gets **exec'd at
`/usr/local/bin/kube-apiserver` with the full upstream flag set** — there is no
seam to pass our own argv.

This is exactly the seam our existing kind-based vanilla-swap harness relies on
and Talos does not offer: `ci/vanilla-swap/kind/apiserver-patch.yaml` **rewrites
the whole static-pod manifest** (`command: ["/app/api-server"]` plus only the
seven flags we support) straight into `/etc/kubernetes/manifests/`. Under Talos
that file is machine-generated inside `machined`; there is nothing to overwrite.

Two things therefore have to exist before Path A boots:

1. **The image must expose `/usr/local/bin/kube-apiserver`** (and the
   `kube-controller-manager` / `kube-scheduler` paths) — a symlink or copy in
   the Dockerfile, since our images ship `/app/<component>`.
2. **The binaries must tolerate the upstream flag set.** Measured against the
   current `clap` definitions in `crates/*/src/main.rs`:

   | component | flags Talos passes | we accept today | **rejected** |
   |---|---|---|---|
   | api-server | 41 | 4 (`bind-address`, `client-ca-file`, `etcd-servers`, `tls-cert-file`) | **37** |
   | controller-manager | 18 | 3 (`allocate-node-cidrs`, `cluster-cidr`, `kubeconfig`) | **15** |
   | scheduler | 8 | 0 | **8** |

   Two of the rejects are pure renames — upstream `--tls-private-key-file` is
   our `--tls-key-file`, upstream `--leader-elect` is our
   `--enable-leader-election` — and one *accepted* flag is a semantic trap:
   our `--bind-address` takes a full socket address (`0.0.0.0:6443`) while
   upstream takes an IP and gets the port from `--secure-port`, which we do not
   have. Everything else is genuinely absent: the whole
   `requestheader-*` / `proxy-client-*` aggregation set, `etcd-{ca,cert,key}file`,
   `service-account-{issuer,key-file,signing-key-file}`,
   `authorization-{mode,config}`, `authentication-config`,
   `encryption-provider-config`, the `audit-log-*` set, `enable-admission-plugins`,
   `admission-control-config-file`, `kubelet-client-{certificate,key}`,
   `service-cluster-ip-range`, `tls-min-version`, `profiling`.

**A flag-compatibility layer is worth building on its own merits** — "drop-in
replacement" means accepting upstream argv — so this is not wasted work, but it
is *the* prerequisite for Path A rather than a footnote. Minimum viable shape:
per component, accept the full upstream flag set, map the ones we implement,
and **explicitly ignore-with-a-warning** the ones we do not (never silently:
an ignored `--encryption-provider-config` is a security surprise, not a
convenience).

### 4.2 Other frictions

- **Storage.** Talos runs **etcd** (started by `machined`, outside Kubernetes)
  and points the api-server at it via `--etcd-servers` + client certs. Our etcd
  backend covers this; SQLite/Rhino does not participate in this path. Note the
  kind harness dodges this by running SQLite with empty state — under Talos the
  api-server inherits a populated etcd, which is a *better* test and a harder one.
- **Kubelet swap is the hard one.** Talos's kubelet is a *system service* with
  Talos-authored args, Talos's own CRI containerd and a specific mount set — a
  drop-in `rusternetes/kubelet` image has to satisfy that contract, not just
  boot. Do the control plane first, the kubelet second.
- **Prereqs on this box:** `/opt/cni/bin` has `bridge`, `firewall`, `static`
  but **not `tc-redirect-tap`** — preflight fetches `talosctl-cni-bundle` for it
  (`qemu/preflight_linux.go:72`). `virtiofsd`, `swtpm` and `mkisofs` are absent
  (needed only for virtiofs disks, TPM, and `metal-iso` config injection).
  `/dev/kvm` is accessible (user is in `kvm`), 24 cores / 62 GB — three 2 GiB
  VMs is nothing. **`sudo` requires a password here**, so the run is a human
  action, not an agent one.

**Value:** honest multi-node testing of our control plane against a populated
etcd, plus a flag-compat layer we want regardless. **Limit:** the *node* is
still Talos — this is not "Rusternetes on a VM" and cannot be our shipped dev
UX (users would need Talos). That is Path B.

---

## 5. Path B — port the provisioner: `rusternetes dev cluster create`

The thing the user actually asked for: **our own** one-command local cluster of
microVMs. The port is small and the design maps almost 1:1.

| Talos (Go) | Rusternetes (Rust) | Notes |
|---|---|---|
| `pkg/provision/provision.go` (`Provisioner` trait) | `crates/dev-cluster/src/provider.rs` | trait with `create` / `destroy` / `reflect` |
| `providers/vm/network_linux.go` | `net/bridge.rs` | shell out to CNI plugins, or drive rtnetlink directly |
| `providers/vm/{dhcpd,dnsd}.go` | `services/{dhcp,dns}.rs` | tiny servers; IPAM db is a file, MAC→IP |
| `providers/vm/loadbalancer.go` | `services/lb.rs` | plain TCP proxy `gateway:6443` → CP IPs |
| `providers/vm/disk.go` | `disk.rs` | `File::create` + `set_len` + `fallocate` |
| `providers/vm/process.go`, `state.go` | `state.rs` | state dir + pidfiles, no daemon |
| `providers/qemu/{launch,node}.go` | `vmm/{cloud_hypervisor,qemu}.rs` + `rusternetes dev vm-launch` | re-exec self as per-VM supervisor; CH is the default backend (§5.2) |
| `providers/qemu/arch.go` | `vmm/arch.rs` | machine type / accel / console per arch |

Rust crates that cover the Go deps: `rtnetlink` (bridge/link),
`iptables`-equivalent via `nftables`/`iptables` CLI or `rustables`,
`dhcproto` + `tokio` (DHCP), `hickory-dns`/`hickory-proto` (DNS), `nix` for
`setsid`/netns (`setns`), `fs4`/`fd-lock` for the CNI file mutex, `serde_json`
for the launch config over stdin. Nothing exotic.

**Structural choices to copy, not re-derive:**

1. **No daemon.** State dir + pidfiles + a per-VM supervisor process. Survives
   the CLI exiting; `reflect` re-attaches; `destroy` is "read pids, SIGTERM".
2. **CNI does IPAM and the tap.** `bridge` + `firewall` + `tc-redirect-tap`
   gives a spec-compliant path from VM tap → host bridge, and keeps our
   CNI-is-a-hard-contract rule intact instead of hand-rolling `ip tuntap`.
3. **The supervisor owns the netns** and starts the VMM inside it.
4. **The supervisor's HTTP API is the node's out-of-band power control** —
   power on/off/reboot without any hypervisor management layer.
5. **Config over kernel cmdline URL**, served by that same supervisor.

### 5.1 The one genuinely open question: what does the guest boot?

Path A gets a guest OS for free. Path B needs one. Three options:

| Option | How | Cost | Fits ROADMAP? |
|---|---|---|---|
| **B1. Generic cloud image + inject binaries** | Debian/Alpine cloud image, `cloud-init`/`ignition` NoCloud seed drops in `rusternetes` + a systemd unit + kubeconfig; containerd from the distro | Lowest. Days. Kernel/init are someone else's problem | Neutral — measures *our* RAM but with a distro's baseline |
| **B2. Purpose-built minimal image** | Our own kernel + initramfs, `rusternetes` as the only service, read-only squashfs + `/var` overlay | Highest. Weeks | **Yes** — this *is* #33 / #1036's USB image, and the honest idle-RAM number |
| **B3. Talos guest, swapped images** | = Path A | Days | No — node isn't ours |

Recommendation: **B1 first** (it de-risks the whole provisioner and is a real
dev UX in a week), then **B2** reusing the same provisioner once the image
work from #1036 lands. B1's image is throwaway; the provisioner is not.

### 5.2 Which VMM — and a note on "QEMU vs KVM"

They are not alternatives. **KVM is the kernel accelerator; QEMU is a VMM that
drives it.** Talos runs `-machine q35,accel=kvm` (`qemu/arch_linux.go:9`,
`qemu/arch.go:245-248`), falling back to TCG only when `/dev/kvm` cannot be
opened. Guest instructions execute on the host CPU through VT-x in both the
QEMU and the cloud-hypervisor case — every option below is KVM-accelerated. The
axis that actually costs us is the **VMM's device model and control plane**, not
the accelerator:

| Backend | Boot to userspace | Per-VM VMM overhead | Device model |
|---|---|---|---|
| QEMU `q35` (what Talos uses) | seconds — firmware, ACPI, PCI enumeration | ~100 MB+ RSS | full: UEFI/ISO/TPM/NVMe/AHCI |
| QEMU `-machine microvm` | ~100–200 ms | tens of MB | virtio-mmio, direct kernel boot |
| **cloud-hypervisor** (v52.0, installed here) | ~50–150 ms | ~10 MB | virtio-{net,blk,fs,console} |
| firecracker | ~50–125 ms | ~5 MB | virtio-{net,blk,vsock} |

Talos needs the q35 breadth because it tests its *own installer* — ISO boot,
UEFI variable stores, TPM measured boot, disk selectors across
ide/ahci/scsi/nvme/megaraid. **We need none of that for a dev cluster:** we
control the guest and direct-kernel-boot it.

**So cloud-hypervisor should be the default backend, not QEMU.** Beyond the
footprint it fits the design better in three concrete ways:

1. **It is Rust and Apache-2.0** — no MPL question (§7), and it can eventually
   be a library rather than a subprocess.
2. **Its `--api-socket` HTTP API replaces machinery we would otherwise port.**
   Talos needs an in-supervisor HTTP server *plus* a QEMU monitor socket to
   offer `vm.boot` / `vm.shutdown` / `vm.reboot` / `vm.info`; cloud-hypervisor
   exposes exactly those as REST over a unix socket, and `--event-monitor`
   gives a state-change stream instead of polling. Our supervisor then keeps
   only the config-serving job.
3. **`tc-redirect-tap` was written for firecracker**, so the CNI network path
   (§3 step 3) is identical across all four backends — it just hands a tap
   device name to whatever VMM.

Costs of dropping QEMU, all avoidable: no cdrom, so config injection uses a
small **vfat NoCloud seed disk** (build with `mtools`, no `mkisofs` needed)
instead of `metal-iso`; no UEFI/TPM, which a dev cluster does not want anyway;
and cloud-hypervisor needs an **uncompressed `vmlinux`-style kernel** — a
distro's compressed `bzImage` will not boot it, so either run
`extract-vmlinux` on it or ship our own kernel, which the B2 image does
regardless.

Therefore: `vmm::Backend` is a trait from day one, **`CloudHypervisor` is the
default implementation**, and `Qemu` (microvm, `accel=kvm`) is a fallback kept
only for what CH cannot do — UEFI, ISO boot, TPM, exotic disk buses — i.e. for
testing the USB/installer image from #1036, not for everyday dev clusters.

Note this is a *different* axis from **#1045** (microVM as a **CRI runtime**,
i.e. VM-per-pod). Same tooling, different layer — worth keeping the low-level
launch code shareable between them.

---

## 6. Recommended phasing

- **Phase 0 (now, no code):** run Path A locally — `sudo -E talosctl cluster
  create qemu --controlplanes 1 --workers 2` with the control-plane image swap.
  Deliverable: a written list of what breaks, which is the real backlog for
  node-lifecycle parity.
- **Phase 1:** `rusternetes dev cluster create` skeleton — state dir, bridge via
  CNI, dhcpd/dns/LB, disks, per-VM supervisor, **cloud-hypervisor backend**
  (`--api-socket` for power control), B1 guest image + vfat NoCloud seed.
  Target: `1 CP + 2 workers` reachable via a generated kubeconfig in under
  60 s, `dev cluster destroy` leaving nothing behind.
- **Phase 2:** `--vmm qemu` fallback (microvm, `accel=kvm`) for the UEFI/ISO
  cases only, then swap in the B2 purpose-built image (#33/#1036) and publish
  the idle-RAM-per-node number the ROADMAP wants (#35).

---

## 7. Licensing constraint (MPL-2.0)

Talos is **MPL-2.0**; Rusternetes is **Apache-2.0**. MPL is *file-level*
copyleft: a Rust translation of an MPL file is a derivative of that file, so
either

- **keep the ported files under MPL-2.0** with the original notice (MPL-2.0
  §3.3 explicitly permits shipping a Larger Work under other terms as long as
  the MPL-covered files stay MPL) and note it in `NOTICE`, or
- **reimplement from the specs** — CNI spec, DHCP RFCs, the QEMU/CH command-line
  contracts — and cite Talos only as prior art in prose.

For the *architecture* (state dir, supervisor-per-VM, CNI-for-tap) there is no
issue at all: ideas are not covered. Decide this **before** writing
`crates/dev-cluster`, not after. Shelling out to `talosctl` (Path A) raises no
licensing question whatsoever.

---

## 8. Verbatim references

| Concept | File:line (`../talos-reference` @ `f6058a1`) |
|---|---|
| `Provisioner` interface | `pkg/provision/provision.go:19` |
| Create sequence | `pkg/provision/providers/qemu/create.go:19` |
| Root requirement | `pkg/provision/providers/qemu/preflight.go:43` |
| Required CNI plugins | `pkg/provision/providers/qemu/preflight_linux.go:72` |
| Bridge naming + CNI config | `pkg/provision/providers/vm/network_linux.go:41,43,404` |
| Per-node create + re-exec | `pkg/provision/providers/qemu/node.go:36,265,286` |
| VM supervisor entrypoint | `pkg/provision/providers/qemu/launch.go:503` |
| QEMU args | `pkg/provision/providers/qemu/launch.go:109-140` |
| Graceful `system_powerdown` | `pkg/provision/providers/qemu/launch.go:443` |
| netns + CNI ADD + tap | `pkg/provision/providers/qemu/launch_linux.go:104,213` |
| tap/VM iface from CNI result | `pkg/provision/internal/cniutils/cniutils.go:58` |
| Supervisor HTTP control API | `pkg/provision/providers/vm/launch.go:102` |
| IPAM record → DHCP db | `pkg/provision/providers/vm/ipam.go:38` |
| Load balancer | `pkg/provision/providers/vm/loadbalancer.go:26` |
| Disk creation | `pkg/provision/providers/vm/disk.go:29` |
| Pidfile stop | `pkg/provision/providers/vm/process.go:19` |
| Image tag validation (repo unconstrained) | `pkg/machinery/compatibility/kubernetes_image.go:15-29` |
| api-server static pod: hard-coded command + upstream flags | `internal/app/machined/pkg/controllers/k8s/control_plane_static_pod.go:290,483-484` |
| controller-manager / scheduler: same | `internal/app/machined/pkg/controllers/k8s/control_plane_final.go:58,149` |
| our kind-based swap, which rewrites `command` (Talos cannot) | `ci/vanilla-swap/kind/apiserver-patch.yaml` |
| Cluster defaults | `cmd/talosctl/cmd/mgmt/cluster/create/clusterops/options.go:171-182` |
