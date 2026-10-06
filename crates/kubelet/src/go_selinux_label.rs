//! Port of `label.InitLabels` and the `go-selinux` pieces it calls
//! (`ContainerLabels`, `NewContext`/`Context.Get`, `ReserveLabel`/`ReleaseLabel`,
//! `PrivContainerMountLabel`), as vendored at
//! `vendor/github.com/opencontainers/selinux/go-selinux/` in kubernetes
//! release-1.35. `pkg/volume/util/selinux.go:73` calls `label.InitLabels` to
//! turn a container's `SELinuxOptions` into a mount label.
//!
//! Deviation, deliberate: upstream keeps the MCS reservation set and the
//! `lxc_contexts` table in package globals. They live on [`Labeler`] here so
//! tests are isolated; [`Labeler::global`] is the process-wide instance that
//! mirrors upstream's globals. `uniqMcs` reads `crypto/rand`; this reads
//! `/dev/urandom` instead of adding a `rand` dependency.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Mutex, OnceLock};

/// `maxCategory` (`selinux.go:15`) == `DefaultCategoryRange`/`CategoryRange`.
const MAX_CATEGORY: u32 = 1024;
/// `contextFile` (`selinux_linux.go:29`).
const CONTEXT_FILE: &str = "/usr/share/containers/selinux/contexts";
/// `selinuxDir` / `selinuxConfig` / `selinuxTypeTag` (`selinux_linux.go:30-35`).
const SELINUX_DIR: &str = "/etc/selinux/";
const SELINUX_CONFIG: &str = "/etc/selinux/config";
const SELINUX_TYPE_TAG: &str = "SELINUXTYPE";

/// `ErrInvalidLabel` (`selinux.go:27`).
pub const ERR_INVALID_LABEL: &str = "invalid Label";

/// `label.validOptions` (`label_linux.go:12-19`) — `disable` is handled before
/// this lookup, as upstream.
fn valid_option(o: &str) -> bool {
    matches!(
        o,
        "disable" | "type" | "filetype" | "user" | "role" | "level"
    )
}

/// `selinux.Context` (`selinux.go:55`): a label broken into its four parts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Context(BTreeMap<String, String>);

impl Context {
    /// `newContext` (`selinux_linux.go:853-870`).
    pub fn new(label: &str) -> Result<Context, &'static str> {
        let mut c = Context::default();
        if !label.is_empty() {
            let con: Vec<&str> = label.splitn(4, ':').collect();
            if con.len() < 3 {
                return Err(ERR_INVALID_LABEL);
            }
            c.0.insert("user".into(), con[0].into());
            c.0.insert("role".into(), con[1].into());
            c.0.insert("type".into(), con[2].into());
            if con.len() > 3 {
                c.0.insert("level".into(), con[3].into());
            }
        }
        Ok(c)
    }

    /// Go map index: a missing key reads as `""`.
    pub fn field(&self, k: &str) -> &str {
        self.0.get(k).map(String::as_str).unwrap_or("")
    }

    pub fn set(&mut self, k: &str, v: &str) {
        self.0.insert(k.into(), v.into());
    }

    /// `Context.get` (`selinux_linux.go:845-851`).
    pub fn get(&self) -> String {
        let l = self.field("level");
        if !l.is_empty() {
            format!(
                "{}:{}:{}:{}",
                self.field("user"),
                self.field("role"),
                self.field("type"),
                l
            )
        } else {
            format!(
                "{}:{}:{}",
                self.field("user"),
                self.field("role"),
                self.field("type")
            )
        }
    }
}

/// `readConfig` (`selinux_linux.go:228-256`) over already-read file contents.
pub fn read_config(contents: &str, target: &str) -> String {
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with(';') || line.starts_with('#') {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        // Go compares `fields[0]` untrimmed.
        if k.as_bytes() == target.as_bytes() {
            return v.trim_matches('"').to_string();
        }
    }
    String::new()
}

/// The key/value table `loadLabels` builds from `lxc_contexts`
/// (`selinux_linux.go:1026-1058`).
#[derive(Debug, Clone, Default)]
pub struct LxcContexts {
    labels: BTreeMap<String, String>,
}

impl LxcContexts {
    /// Parse the file body exactly as the `loadLabels` scanner loop does.
    pub fn parse(contents: &str) -> LxcContexts {
        let mut labels = BTreeMap::new();
        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with(';') || line.starts_with('#') {
                continue;
            }
            let Some((k, v)) = line.split_once('=') else {
                continue;
            };
            labels.insert(k.trim().to_string(), v.trim().trim_matches('"').to_string());
        }
        LxcContexts { labels }
    }

    /// `label(key)` (`selinux_linux.go:1060-1065`).
    pub fn label(&self, key: &str) -> &str {
        self.labels.get(key).map(String::as_str).unwrap_or("")
    }
}

/// `openContextFile` (`selinux_linux.go:1019-1024`): the containers-selinux
/// override, else `<policyRoot>/contexts/lxc_contexts`.
fn load_lxc_contexts_from_host() -> Option<LxcContexts> {
    let body = std::fs::read_to_string(CONTEXT_FILE).ok().or_else(|| {
        let cfg = std::fs::read_to_string(SELINUX_CONFIG).unwrap_or_default();
        let root = format!("{}{}", SELINUX_DIR, read_config(&cfg, SELINUX_TYPE_TAG));
        std::fs::read_to_string(format!("{root}/contexts/lxc_contexts")).ok()
    })?;
    Some(LxcContexts::parse(&body))
}

/// Process-wide MCS reservation set (`selinuxState.mcsList`) plus the
/// `lxc_contexts` table and `privContainerMountLabel`.
pub struct Labeler {
    mcs_list: Mutex<HashSet<String>>,
    lxc: LxcContexts,
    priv_container_mount_label: String,
}

impl Labeler {
    /// The tail of `loadLabels` (`selinux_linux.go:1054-1057`): derive and
    /// reserve `privContainerMountLabel`.
    pub fn new(lxc: LxcContexts) -> Labeler {
        let mut con = Context::new(lxc.label("file")).unwrap_or_default();
        con.set(
            "level",
            &format!("s0:c{},c{}", MAX_CATEGORY - 2, MAX_CATEGORY - 1),
        );
        let l = Labeler {
            mcs_list: Mutex::new(HashSet::new()),
            priv_container_mount_label: con.get(),
            lxc,
        };
        l.reserve_label(&l.priv_container_mount_label);
        l
    }

    /// The process-wide instance (upstream's package globals, loaded once by
    /// `loadLabelsOnce`). Missing policy files yield an empty table, as
    /// upstream's `loadLabels` returning early on open error.
    pub fn global() -> &'static Labeler {
        static G: OnceLock<Labeler> = OnceLock::new();
        G.get_or_init(|| Labeler::new(load_lxc_contexts_from_host().unwrap_or_default()))
    }

    /// `mcsAdd` (`selinux_linux.go:935-946`): `Err` is `ErrMCSAlreadyExists`.
    fn mcs_add(&self, mcs: &str) -> Result<(), ()> {
        if mcs.is_empty() {
            return Ok(());
        }
        if self.mcs_list.lock().unwrap().insert(mcs.to_string()) {
            Ok(())
        } else {
            Err(())
        }
    }

    /// `mcsDelete` (`selinux_linux.go:948-956`).
    fn mcs_delete(&self, mcs: &str) {
        if !mcs.is_empty() {
            self.mcs_list.lock().unwrap().remove(mcs);
        }
    }

    pub fn is_reserved(&self, mcs: &str) -> bool {
        self.mcs_list.lock().unwrap().contains(mcs)
    }

    /// `reserveLabel` (`selinux_linux.go:879-886`).
    pub fn reserve_label(&self, label: &str) {
        if let Some(level) = label.splitn(4, ':').nth(3) {
            let _ = self.mcs_add(level);
        }
    }

    /// `releaseLabel` (`selinux_linux.go:987-995`). A no-op for a label that
    /// was never reserved.
    pub fn release_label(&self, label: &str) {
        if let Some(level) = label.splitn(4, ':').nth(3) {
            self.mcs_delete(level);
        }
    }

    /// `uniqMcs` (`selinux_linux.go:972-1000`) with the random source injected.
    fn uniq_mcs_with(&self, cat_range: u32, mut rnd: impl FnMut() -> u32) -> String {
        loop {
            let mut c1 = rnd() % cat_range;
            let mut c2 = rnd() % cat_range;
            if c1 == c2 {
                continue;
            } else if c1 > c2 {
                std::mem::swap(&mut c1, &mut c2);
            }
            let mcs = format!("s0:c{c1},c{c2}");
            if self.mcs_add(&mcs).is_err() {
                continue;
            }
            return mcs;
        }
    }

    /// `addMcs` (`selinux_linux.go:1111-1122`).
    fn add_mcs(
        &self,
        process_label: &str,
        file_label: &str,
        rnd: impl FnMut() -> u32,
    ) -> (String, String) {
        let mut scon = Context::new(process_label).unwrap_or_default();
        if !scon.field("level").is_empty() {
            let mcs = self.uniq_mcs_with(MAX_CATEGORY, rnd);
            scon.set("level", &mcs);
            let p = scon.get();
            let mut fcon = Context::new(file_label).unwrap_or_default();
            fcon.set("level", &mcs);
            return (p, fcon.get());
        }
        (process_label.to_string(), file_label.to_string())
    }

    /// `containerLabels` (`selinux_linux.go:1091-1109`); `enabled` is
    /// `getEnabled()`.
    fn container_labels_with(&self, enabled: bool, rnd: impl FnMut() -> u32) -> (String, String) {
        if !enabled {
            return (String::new(), String::new());
        }
        let process_label = self.lxc.label("process");
        let file_label = self.lxc.label("file");
        if process_label.is_empty() || file_label.is_empty() {
            return (String::new(), file_label.to_string());
        }
        self.add_mcs(process_label, file_label, rnd)
    }

    /// `label.InitLabels` (`label_linux.go:29-80`). Returns
    /// `(process_label, mount_label)`.
    pub fn init_labels(
        &self,
        enabled: bool,
        options: &[String],
    ) -> Result<(String, String), String> {
        self.init_labels_with(enabled, options, urandom_u32)
    }

    pub fn init_labels_with(
        &self,
        enabled: bool,
        options: &[String],
        rnd: impl FnMut() -> u32,
    ) -> Result<(String, String), String> {
        if !enabled {
            return Ok((String::new(), String::new()));
        }
        let (mut process_label, mut mount_label) = self.container_labels_with(enabled, rnd);
        if process_label.is_empty() {
            return Ok((process_label, mount_label));
        }
        // `defer { if retErr != nil { ReleaseLabel(mountLabel) } }`; note the
        // deferred closure sees the *original* `mountLabel` captured by name,
        // which is only reassigned on the success path below.
        let original_mount = mount_label.clone();
        let r = (|| {
            let mut pcon = Context::new(&process_label).map_err(|e| e.to_string())?;
            let mcs_level = pcon.field("level").to_string();
            let mut mcon = Context::new(&mount_label).map_err(|e| e.to_string())?;
            for opt in options {
                if opt == "disable" {
                    self.release_label(&mount_label);
                    return Ok((String::new(), self.priv_container_mount_label.clone()));
                }
                if !opt.contains(':') {
                    return Err(format!(
                        "bad label option {opt:?}, valid options 'disable' or \n'user, role, level, type, filetype' followed by ':' and a value"
                    ));
                }
                let (k, v) = opt.split_once(':').unwrap();
                if !valid_option(k) {
                    return Err(format!(
                        "bad label option {k:?}, valid options 'disable, user, role, level, type, filetype'"
                    ));
                }
                if k == "filetype" {
                    mcon.set("type", v);
                    continue;
                }
                pcon.set(k, v);
                if k == "level" || k == "user" {
                    mcon.set(k, v);
                }
            }
            if pcon.get() != process_label {
                if pcon.field("level") != mcs_level {
                    self.release_label(&process_label);
                }
                process_label = pcon.get();
                self.reserve_label(&process_label);
            }
            mount_label = mcon.get();
            Ok((process_label.clone(), mount_label.clone()))
        })();
        if r.is_err() {
            self.release_label(&original_mount);
        }
        r
    }
}

/// One `crypto/rand` read, as `binary.Read(rand.Reader, LittleEndian, &n)`.
fn urandom_u32() -> u32 {
    use std::io::Read;
    let mut b = [0u8; 4];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        let _ = f.read_exact(&mut b);
    }
    u32::from_le_bytes(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    const LXC: &str = "process = \"system_u:system_r:container_t:s0\"\n\
        content = \"system_u:object_r:virt_var_lib_t:s0\"\n\
        file = \"system_u:object_r:container_file_t:s0\"\n\
        ro_file=\"system_u:object_r:container_ro_file_t:s0\"\n\
        # comment\n\n; other\n";

    fn labeler() -> Labeler {
        Labeler::new(LxcContexts::parse(LXC))
    }

    fn seq(v: Vec<u32>) -> impl FnMut() -> u32 {
        let mut i = 0;
        move || {
            let x = v[i % v.len()];
            i += 1;
            x
        }
    }

    fn opts(o: &[&str]) -> Vec<String> {
        o.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn context_roundtrip_and_invalid() {
        let c = Context::new("system_u:object_r:container_file_t:s0:c1,c2").unwrap();
        assert_eq!(c.field("level"), "s0:c1,c2");
        assert_eq!(c.get(), "system_u:object_r:container_file_t:s0:c1,c2");
        assert_eq!(Context::new("a:b").unwrap_err(), ERR_INVALID_LABEL);
        assert_eq!(Context::new("a:b:c").unwrap().get(), "a:b:c");
    }

    #[test]
    fn lxc_contexts_parse_trims_and_unquotes() {
        let l = LxcContexts::parse(LXC);
        assert_eq!(l.label("file"), "system_u:object_r:container_file_t:s0");
        assert_eq!(
            l.label("ro_file"),
            "system_u:object_r:container_ro_file_t:s0"
        );
        assert_eq!(l.label("nope"), "");
    }

    #[test]
    fn read_config_picks_target() {
        let c = "# x\nSELINUX=enforcing\nSELINUXTYPE=\"targeted\"\n";
        assert_eq!(read_config(c, "SELINUXTYPE"), "targeted");
        assert_eq!(read_config(c, "SELINUX"), "enforcing");
        assert_eq!(read_config(c, "X"), "");
    }

    #[test]
    fn priv_mount_label_is_reserved_max_categories() {
        let l = labeler();
        assert_eq!(
            l.priv_container_mount_label,
            "system_u:object_r:container_file_t:s0:c1022,c1023"
        );
        assert!(l.is_reserved("s0:c1022,c1023"));
    }

    #[test]
    fn disabled_returns_empty() {
        let l = labeler();
        assert_eq!(
            l.init_labels_with(false, &opts(&["user:u"]), || 1).unwrap(),
            (String::new(), String::new())
        );
    }

    #[test]
    fn allocates_unique_mcs_and_skips_collisions() {
        let l = labeler();
        // c1==c2 retried; (5,3) swapped to c3,c5; second call collides then moves on.
        let (p, m) = l
            .init_labels_with(true, &[], seq(vec![4, 4, 5, 3]))
            .unwrap();
        assert_eq!(p, "system_u:system_r:container_t:s0:c3,c5");
        assert_eq!(m, "system_u:object_r:container_file_t:s0:c3,c5");
        assert!(l.is_reserved("s0:c3,c5"));
        let (p2, _) = l
            .init_labels_with(true, &[], seq(vec![3, 5, 7, 8]))
            .unwrap();
        assert_eq!(p2, "system_u:system_r:container_t:s0:c7,c8");
    }

    #[test]
    fn level_option_overrides_both_and_reserves_process_label() {
        let l = labeler();
        let (p, m) = l
            .init_labels_with(true, &opts(&["level:s0:c10,c20"]), seq(vec![1, 2]))
            .unwrap();
        assert_eq!(p, "system_u:system_r:container_t:s0:c10,c20");
        assert_eq!(m, "system_u:object_r:container_file_t:s0:c10,c20");
        // the random level was released, the explicit one reserved
        assert!(!l.is_reserved("s0:c1,c2"));
        assert!(l.is_reserved("s0:c10,c20"));
    }

    #[test]
    fn type_applies_to_process_only_and_filetype_to_mount_only() {
        let l = labeler();
        let (p, m) = l
            .init_labels_with(
                true,
                &opts(&["type:spc_t", "filetype:my_file_t", "user:u_u", "role:r_r"]),
                seq(vec![1, 2]),
            )
            .unwrap();
        assert_eq!(p, "u_u:r_r:spc_t:s0:c1,c2");
        // user is copied to the mount label, role is not.
        assert_eq!(m, "u_u:object_r:my_file_t:s0:c1,c2");
    }

    #[test]
    fn disable_returns_priv_mount_label_and_releases() {
        let l = labeler();
        let (p, m) = l
            .init_labels_with(true, &opts(&["disable"]), seq(vec![1, 2]))
            .unwrap();
        assert_eq!(p, "");
        assert_eq!(m, "system_u:object_r:container_file_t:s0:c1022,c1023");
        assert!(!l.is_reserved("s0:c1,c2"));
    }

    #[test]
    fn bad_option_errors_and_releases_mount_label() {
        let l = labeler();
        let e = l
            .init_labels_with(true, &opts(&["bogus:x"]), seq(vec![1, 2]))
            .unwrap_err();
        assert_eq!(
            e,
            "bad label option \"bogus\", valid options 'disable, user, role, level, type, filetype'"
        );
        assert!(!l.is_reserved("s0:c1,c2"));
        let e = l
            .init_labels_with(true, &opts(&["nocolon"]), seq(vec![1, 2]))
            .unwrap_err();
        assert!(e.starts_with("bad label option \"nocolon\""));
    }

    #[test]
    fn missing_policy_yields_empty_process_label() {
        let l = Labeler::new(LxcContexts::default());
        assert_eq!(
            l.init_labels_with(true, &opts(&["user:u"]), || 1).unwrap(),
            (String::new(), String::new())
        );
    }
}
