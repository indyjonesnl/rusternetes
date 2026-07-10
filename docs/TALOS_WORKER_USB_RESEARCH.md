# Research: Talos Linux install model → a Rusternetes worker USB image

> Research + design note for [#1036]. **No code** — this establishes how Talos
> ships Kubernetes on an immutable OS, then proposes a concrete Rusternetes
> worker-node USB image that follows the same shape without reinventing it.

## 1. Why look at Talos

Talos Linux is the reference design for "the OS *is* the Kubernetes node": a
minimal, immutable, API-driven Linux distribution with no shell and no SSH,
whose entire job is to boot and run a kubelet. That is exactly the shape we
want for a single-USB Rusternetes worker — plug it in, it boots, it joins.
Rather than invent a bespoke installer, we mirror Talos's proven mechanism and
swap the Kubernetes payload for Rusternetes' own binaries.

## 2. How Talos works

### 2.1 Process model — `machined` + two containerds

- **`machined` is PID 1.** It is the init system: it provides the Talos gRPC
  API, applies machine configuration, and orchestrates the whole boot
  sequence, launching every system service. There is no systemd, no shell.
- **Two separate containerd instances**, deliberately isolated so a workload
  problem can't take down the node:
  - a **system containerd** runs Talos's own services (`apid`, `trustd`,
    `etcd`, and the **kubelet**) as containerd tasks from OCI images;
  - a **CRI containerd** is the runtime the kubelet talks to for actual
    Kubernetes pods.
- **The kubelet is a container, not a host binary.** Talos runs it as a system
  service from a published image (`ghcr.io/siderolabs/kubelet:v1.x`). Same for
  the control-plane components.
- **Control plane = static pods; etcd = system service.** On control-plane
  nodes the kubelet runs `kube-apiserver` / `kube-scheduler` /
  `kube-controller-manager` as **static pods**, while **etcd is started
  directly by `machined`** outside Kubernetes. Workers run only the kubelet.
- **Only ingress is `apid` on `:50000`**, gRPC over mTLS, client cert
  required. No SSH, no console login.

> Talos advertises "fewer than 50 binaries" on the box: kernel, containerd,
> kubelet, etcd, machined — "if it's not on that list, it's not on the box."

### 2.2 Immutable root filesystem

- Core rootfs is a **read-only squashfs**, mounted as a loop device **into
  RAM**.
- On top: a **tmpfs layer** for the runtime pseudo-filesystems (`/dev`,
  `/proc`, `/run`, `/sys`, `/tmp`) plus a special `/system` for small writable
  essentials (e.g. `/etc/hosts`).
- Anything that must **persist across reboots** is an **overlayfs backed by an
  XFS filesystem mounted at `/var`**.

### 2.3 On-disk partition layout

| Partition   | Purpose |
|-------------|---------|
| `EFI`       | EFI boot data |
| `BIOS`      | GRUB second-stage boot (legacy BIOS) |
| `BOOT`      | bootloader — stores initramfs + kernel |
| `META`      | node metadata (node IDs, install state) |
| `STATE`     | **machine configuration**, node identity for cluster discovery, KubeSpan info |
| `EPHEMERAL` | mounted at `/var`; everything reconstructable — container images, pod `emptyDir` volumes, and (control plane) etcd data |

The split matters: `STATE` is the small, precious partition (config + identity);
`EPHEMERAL` is the large, disposable one (images + volumes). Wiping `EPHEMERAL`
and rebooting rebuilds the node; wiping `STATE` de-identifies it.

### 2.4 Install + join flow

1. **Boot medium loads to RAM.** An ISO/PXE image boots Talos entirely into
   memory. There is *no interactive installer* — the node comes up in
   **maintenance mode** awaiting configuration.
2. **Apply machine config.** A single YAML (`machine:` + `cluster:` sections)
   is delivered — via `talosctl apply-config`, a platform metadata source, or
   a config URL on the kernel cmdline. It carries the cluster endpoint, CA,
   join token, kubelet settings, etc.
3. **Install to disk.** The **installer image** (which bundles the chosen
   system extensions) writes the OS to disk; the installer reference lives in
   the machine config and is reused for upgrades (`talosctl upgrade --image`).
4. **Reboot into the installed system.**
5. **Bootstrap (control plane only, once):** `talosctl bootstrap` tells one
   control-plane node to initialise a single-member etcd and generate the
   control-plane static pods. **Workers never bootstrap** — they read the
   endpoint + token/CA from their machine config and join automatically.

### 2.5 Boot-asset build pipeline

- **Image Factory** (`factory.talos.dev`, [siderolabs/image-factory]) generates
  assets on demand from a **schematic** (extra kernel args + official system
  extensions + hardware overlays). The schematic ID is content-addressed →
  reproducible. It emits ISO, UEFI-UKI (Secure Boot), installer image, raw disk
  images, and PXE assets, per arch/version.
- **Imager container** (`ghcr.io/siderolabs/imager:{version}`) does the same
  build locally from source/custom extensions, with profiles (`iso`, `metal`,
  `aws`, `secureboot`, SBC overlays).
- System extensions are baked at **build time** so they persist across
  reboots/upgrades with no re-download.

## 3. Proposal: a Rusternetes worker USB image

### 3.1 Scope — what a *worker* actually needs

A Rusternetes worker node is far smaller than the control plane. It needs only:

- **`rusternetes-kubelet`** and **`rusternetes-kube-proxy`** (existing crates);
- a **CRI runtime** — the project's target stack is **containerd + youki**
  (see `CLAUDE.md` / the CRI migration), talked to over the CRI gRPC socket;
- **CNI plugins** + a CNI network config (per the non-negotiable CNI contract);
- a **join/identity mechanism** (api-server endpoint + cluster CA + a
  client credential);
- a minimal Linux userland (kernel, init/supervisor, containerd, iptables for
  kube-proxy).

Explicitly **out of scope for the worker image**: the api-server, scheduler,
controller-manager, and storage backend. (The all-in-one `rusternetes` binary
targets the *control-plane-in-a-box* case, not a pure worker — see §3.6.)

### 3.2 Partition layout (mirror Talos)

```
EFI        — EFI boot data (systemd-boot or GRUB)
BOOT       — kernel + initramfs (+ the squashfs rootfs image)
STATE      — worker machine config, node identity: kubelet kubeconfig / client
             cert + key, cluster CA. Small, persistent, precious.
EPHEMERAL  — mounted at /var: containerd image store, pod volumes, kubelet
             working dir (/var/lib/kubelet). Large, disposable.
```

Same STATE-vs-EPHEMERAL discipline: re-imaging keeps the OS read-only in
squashfs; only STATE carries identity, only EPHEMERAL carries bulk.

### 3.3 Root filesystem model

Adopt Talos's read-only-squashfs-into-RAM + overlay design:

- Read-only **squashfs** rootfs built from an existing Rusternetes container
  image (we already build `rusternetes-kubelet` / `-kube-proxy` / containerd
  images — §3.5), plus busybox/Alpine userland, containerd, youki, CNI plugins,
  and iptables.
- `tmpfs` for `/run`, `/tmp`; **overlayfs on `EPHEMERAL` (`/var`)** for
  persistence (containerd state, kubelet state).
- `STATE` mounted read-only-until-needed at a fixed path (e.g.
  `/etc/rusternetes`).

### 3.4 How the worker binaries run — pick the pragmatic path first

Two options, mirroring the two Talos models:

- **(A) Host services (recommended for v1).** Rusternetes binaries are Rust and
  can be built as **static musl** binaries (tracked in **#1041**). Ship
  `rusternetes-kubelet` and `rusternetes-kube-proxy` directly in the squashfs
  and start them from a tiny supervisor (a minimal init, or systemd if the base
  distro has it). No system-containerd needed; the *only* containerd is the CRI
  one the kubelet drives. Simplest to build and reason about.
- **(B) System-containerd services (Talos-faithful, later).** Run the kubelet
  and kube-proxy as tasks in a **second, system containerd** from our published
  GHCR images, isolating them from the workload runtime. More moving parts;
  defer until (A) works end-to-end.

Recommendation: **start with (A)**, keep the door open to (B). (A) also makes
the image dramatically simpler to produce (see #1041 / #1042 synergy below).

### 3.5 Build pipeline — reuse what we have

We already build worker images with hand-listed crate Dockerfiles. Rather than
a bespoke installer, build an **imager-style OCI job**:

1. Assemble a rootfs dir: minimal userland + `containerd` + `youki` + CNI
   plugins + `iptables` + the static `rusternetes-kubelet` / `-kube-proxy`
   binaries + a first-boot supervisor unit.
2. `mksquashfs` → read-only rootfs image.
3. Assemble a bootable artifact (kernel + initramfs that loop-mounts the
   squashfs into RAM) with **systemd-boot** (UEFI) — GRUB for legacy BIOS.
4. Produce both an **ISO** (write to USB with `dd`) and a **raw disk image**
   (for direct-flash / SBC), from the same rootfs — Talos's imager does exactly
   this.

This can live as a new profile alongside the existing image Dockerfiles; it
does not need a separate build system.

### 3.6 Config delivery + join flow

Worker config is small: **api-server URL, cluster CA, and a bootstrap
credential.** Delivery options (support the first, allow the second):

- **(1) NoCloud-style config partition (recommended v1):** a small labelled
  partition / second FAT volume containing `worker-config.yaml` (endpoint + CA
  + bootstrap token or a pre-issued kubelet kubeconfig). Zero network
  dependency at first boot; dead simple to author and to burn onto the USB
  alongside the image.
- **(2) Kernel-cmdline config URL:** `rusternetes.config=https://…` fetched in
  the initramfs — good for PXE/fleet scenarios.

Join sequence:

1. First boot reads the config from `STATE`/config partition.
2. If only a **bootstrap token** is present, the kubelet uses it to submit a
   **CSR** and obtain its client cert (standard kubelet TLS bootstrap); if a
   full **kubeconfig** is present, use it directly. Rusternetes already grew
   **kubeconfig-server-based API-mode node registration** (commit `5ea1e662`,
   #1594) and **client-certificate mTLS in `ApiClient`** (#1578/#1585) — the
   worker join path builds directly on those.
3. Kubelet registers the `Node`, kube-proxy starts programming iptables, CNI is
   configured, pods schedule. **No control-plane bootstrap step on a worker** —
   same as Talos.

### 3.7 Security posture (Talos parity, keep it minimal)

- **No SSH, no login shell** by default; node identity is an mTLS client cert.
- Read-only rootfs; only `STATE` (identity) and `EPHEMERAL` (bulk) are
  writable, and both are re-creatable.
- Minimal package set — kernel, containerd, youki, CNI, iptables, the two
  Rusternetes binaries, a supervisor. Nothing else.

## 4. Phased roadmap

1. **P0 — static worker binaries.** Land musl static builds of
   `rusternetes-kubelet` + `-kube-proxy` (**#1041**). Hard prerequisite for a
   self-contained squashfs.
2. **P1 — bootable rootfs.** Imager-style OCI job → squashfs → ISO + raw image
   that boots to a shell-less node with containerd + youki + CNI up. No join
   yet.
3. **P2 — config + join.** NoCloud config partition → kubelet TLS bootstrap →
   node registers against an existing Rusternetes control plane and runs a pod.
   This is the MVP "worker USB".
4. **P3 — immutability + upgrades.** A/B or installer-image upgrade flow,
   Secure-Boot UKI, `STATE`/`EPHEMERAL` wipe-and-rebuild semantics.
5. **P4 — Talos-faithful isolation (optional).** Move kubelet/kube-proxy into a
   dedicated system containerd (§3.4-B).

## 5. Open questions / decisions for the maintainer

- **Base userland:** from-scratch Buildroot/Alpine (smallest, most work) vs a
  minimal existing immutable base (Flatcar-like). Recommend Alpine/Buildroot
  for footprint parity with the "k3s without melting your laptop" positioning.
- **Supervisor:** tiny custom init vs systemd. Custom init keeps the image
  tiny and shell-less; systemd is faster to stand up. Recommend custom/minimal.
- **Bootstrap credential:** ship a short-lived **bootstrap token** (needs a
  token-authenticated CSR-approve path server-side) vs a **pre-issued
  kubeconfig** (simpler, but per-node provisioning). Recommend token for fleet,
  kubeconfig for single-node demos.
- **Secure Boot:** target UKI now or later? (P3.)

## 6. Related Rusternetes work

- **#1041** musl + mimalloc static builds + arm64 — **prerequisite** for the
  self-contained rootfs (§3.4-A, §4-P0).
- **#1042** one-command install + pre-built release binaries — the release
  binaries this image consumes; same distribution story.
- Node-registration / mTLS plumbing the join path reuses: **#1594** (kubeconfig
  server API-mode registration), **#1578/#1585** (ApiClient mTLS).
- Lightweight-distro roadmap positioning ("k3s without melting your laptop").

## 7. Sources

- [Talos architecture (Sidero docs)](https://docs.siderolabs.com/talos/v1.9/learn-more/architecture/)
- [Talos control plane](https://www.talos.dev/v1.7/learn-more/control-plane/)
- [Talos boot assets](https://docs.siderolabs.com/talos/v1.9/platform-specific-installations/boot-assets)
- [Talos Image Factory](https://factory.talos.dev/) · [siderolabs/image-factory](https://github.com/siderolabs/image-factory)
- [Sidero Labs — Talos Linux](https://www.siderolabs.com/talos-linux)

[#1036]: https://github.com/indyjonesnl/rusternetes/issues/1036
[siderolabs/image-factory]: https://github.com/siderolabs/image-factory
