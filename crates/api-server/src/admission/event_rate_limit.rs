//! The `EventRateLimit` admission plugin: a token-bucket limit on the Event
//! creation/update rate, per server, namespace, user or source+object.
//!
//! Ported from `plugin/pkg/admission/eventratelimit/`:
//! - `admission.go` (`Register`, `Plugin`, `newEventRateLimit`, `Validate`),
//! - `limitenforcer.go` (`limitEnforcer`, `newLimitEnforcer`, `accept` and the
//!   four key funcs),
//! - `cache.go` (`singleCache`, `lruCache`),
//! - `config.go` (`LoadConfiguration`),
//! - `apis/eventratelimit/{types.go,validation/validation.go}` and
//!   `apis/eventratelimit/v1alpha1` (wire form of the configuration).
//!
//! The limiter is `flowcontrol.NewTokenBucketRateLimiterWithClock`
//! (`staging/src/k8s.io/client-go/util/flowcontrol/throttle.go`), backed here
//! by [`rusternetes_common::event_correlator::TokenBucket`] (a port of
//! `x/time/rate.Limiter.AllowN`), the same limiter the client-side event spam
//! filter uses; the LRU is [`LruExpireCache`] with an entry TTL that never
//! elapses, which is `k8s.io/utils/lru.Cache`.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use rusternetes_common::event_correlator::TokenBucket;
use rusternetes_common::lru_expire_cache::LruExpireCache;
use rusternetes_common::resources::Event;
use rusternetes_common::validation::field;
use rusternetes_common::Error;
use serde::{Deserialize, Serialize};

pub const PLUGIN_NAME: &str = "EventRateLimit";

/// `defaultCacheSize` (limitenforcer.go:32).
const DEFAULT_CACHE_SIZE: usize = 4096;

/// The group/version of the configuration (`apis/eventratelimit/v1alpha1/register.go`).
const CONFIG_API_VERSION: &str = "eventratelimit.admission.k8s.io/v1alpha1";

/// `LimitType` values (types.go:29-37).
pub const SERVER: &str = "Server";
pub const NAMESPACE: &str = "Namespace";
pub const USER: &str = "User";
pub const SOURCE_AND_OBJECT: &str = "SourceAndObject";

/// `eventratelimit.Limit` (types.go:50-79).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Limit {
    #[serde(rename = "type", default)]
    pub limit_type: String,
    #[serde(default)]
    pub qps: i32,
    #[serde(default)]
    pub burst: i32,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub cache_size: i32,
}

fn is_zero(v: &i32) -> bool {
    *v == 0
}

/// `eventratelimit.Configuration` (types.go:39-48).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Configuration {
    #[serde(default, skip_serializing)]
    pub api_version: String,
    #[serde(default, skip_serializing)]
    pub kind: String,
    #[serde(default)]
    pub limits: Vec<Limit>,
}

/// `LoadConfiguration` (config.go:46-65). No configuration is the defaulted
/// (empty) one; otherwise the document must be a `Configuration` of
/// `eventratelimit.admission.k8s.io/v1alpha1`.
pub fn load_configuration(config: Option<&str>) -> Result<Configuration, String> {
    let Some(text) = config else {
        return Ok(Configuration::default());
    };
    let cfg: Configuration = serde_yaml::from_str(text).map_err(|e| e.to_string())?;
    if cfg.kind.is_empty() {
        return Err("Object 'Kind' is missing".to_string());
    }
    if cfg.api_version != CONFIG_API_VERSION || cfg.kind != "Configuration" {
        return Err(format!(
            "no kind \"{}\" is registered for version \"{}\"",
            cfg.kind, cfg.api_version
        ));
    }
    Ok(cfg)
}

/// `ValidateConfiguration` (validation/validation.go:30-60).
pub fn validate_configuration(config: &Configuration) -> Vec<field::Error> {
    let mut errs = Vec::new();
    let limits_path = field::Path::new("limits");
    if config.limits.is_empty() {
        errs.push(field::Error::invalid(
            &limits_path,
            field::BadValue::marshal(&config.limits),
            "must not be empty",
        ));
    }
    for (i, limit) in config.limits.iter().enumerate() {
        let idx = limits_path.index(i);
        if ![SERVER, NAMESPACE, USER, SOURCE_AND_OBJECT].contains(&limit.limit_type.as_str()) {
            errs.push(field::Error::not_supported(
                &idx.child("type"),
                limit.limit_type.as_str(),
                &[SERVER, NAMESPACE, USER, SOURCE_AND_OBJECT],
            ));
        }
        if limit.burst <= 0 {
            errs.push(field::Error::invalid(
                &idx.child("burst"),
                limit.burst,
                "must be positive",
            ));
        }
        if limit.qps <= 0 {
            errs.push(field::Error::invalid(
                &idx.child("qps"),
                limit.qps,
                "must be positive",
            ));
        }
        if limit.limit_type != SERVER && limit.cache_size < 0 {
            errs.push(field::Error::invalid(
                &idx.child("cacheSize"),
                limit.cache_size,
                "must not be negative",
            ));
        }
    }
    errs
}

/// The clock of `flowcontrol.Clock`.
pub type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

/// `flowcontrol.RateLimiter` as `TryAccept` sees it.
pub type RateLimiter = Arc<Mutex<TokenBucket>>;

/// `cache` (cache.go:27-30): `singleCache` or `lruCache`.
pub enum Cache {
    /// `singleCache`: the one limiter of the `Server` limit.
    Single(RateLimiter),
    /// `lruCache`: a limiter per key, the least recently used one evicted.
    Lru {
        qps: f64,
        burst: usize,
        cache: LruExpireCache<RateLimiter>,
    },
}

/// Entries of `utils/lru.Cache` never expire.
const NEVER: Duration = Duration::from_secs(100 * 365 * 24 * 3600);

impl Cache {
    /// `get` (cache.go:37-39, 55-62).
    pub fn get(&self, key: &str) -> RateLimiter {
        match self {
            Cache::Single(limiter) => limiter.clone(),
            Cache::Lru { qps, burst, cache } => {
                let now = Instant::now();
                if let Some(found) = cache.get(key, now) {
                    return found;
                }
                let limiter = Arc::new(Mutex::new(TokenBucket::new(*qps, *burst)));
                cache.add(key.to_string(), limiter.clone(), NEVER, now);
                limiter
            }
        }
    }
}

/// What the plugin reads of `admission.Attributes`.
pub struct Attributes<'a> {
    pub group: &'a str,
    pub kind: &'a str,
    pub namespace: &'a str,
    pub user: &'a str,
    pub dry_run: bool,
    pub object: Option<&'a Event>,
}

/// `limitEnforcer` (limitenforcer.go:34-45).
struct LimitEnforcer {
    limit_type: String,
    cache: Cache,
    key_func: fn(&Attributes) -> String,
}

impl LimitEnforcer {
    /// `newLimitEnforcer` (limitenforcer.go:47-87).
    fn new(config: &Limit) -> Result<Self, String> {
        let qps = f64::from(config.qps as f32);
        let burst = config.burst as usize;
        if config.limit_type == SERVER {
            return Ok(Self {
                limit_type: config.limit_type.clone(),
                cache: Cache::Single(Arc::new(Mutex::new(TokenBucket::new(qps, burst)))),
                key_func: server_key,
            });
        }
        let cache_size = if config.cache_size == 0 {
            DEFAULT_CACHE_SIZE
        } else {
            config.cache_size as usize
        };
        let key_func: fn(&Attributes) -> String = match config.limit_type.as_str() {
            NAMESPACE => namespace_key,
            USER => user_key,
            SOURCE_AND_OBJECT => source_and_object_key,
            t => return Err(format!("unknown event rate limit type: {t}")),
        };
        Ok(Self {
            limit_type: config.limit_type.clone(),
            cache: Cache::Lru {
                qps,
                burst,
                cache: LruExpireCache::new(cache_size),
            },
            key_func,
        })
    }

    /// `accept` (limitenforcer.go:89-100).
    fn accept(&self, attr: &Attributes, now: DateTime<Utc>) -> Result<(), String> {
        let key = (self.key_func)(attr);
        let limiter = self.cache.get(&key);
        let allow = limiter
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .try_accept(now);
        if !allow {
            return Err(format!(
                "limit reached on type {} for key {key}",
                self.limit_type
            ));
        }
        Ok(())
    }
}

/// `getServerKey` (limitenforcer.go:102-104).
fn server_key(_: &Attributes) -> String {
    String::new()
}

/// `getNamespaceKey` (limitenforcer.go:106-108).
fn namespace_key(attr: &Attributes) -> String {
    attr.namespace.to_string()
}

/// `getUserKey` (limitenforcer.go:110-116).
fn user_key(attr: &Attributes) -> String {
    attr.user.to_string()
}

/// `getSourceAndObjectKey` (limitenforcer.go:118-140): the plain
/// concatenation, with no separator.
fn source_and_object_key(attr: &Attributes) -> String {
    let Some(event) = attr.object else {
        return String::new();
    };
    let o = &event.involved_object;
    [
        event.source.component.as_str(),
        event.source.host.as_deref().unwrap_or_default(),
        o.kind.as_deref().unwrap_or_default(),
        o.namespace.as_deref().unwrap_or_default(),
        o.name.as_deref().unwrap_or_default(),
        o.uid.as_deref().unwrap_or_default(),
        o.api_version.as_deref().unwrap_or_default(),
    ]
    .concat()
}

/// `Plugin` (admission.go:62-69).
pub struct EventRateLimit {
    enforcers: Vec<LimitEnforcer>,
    clock: Clock,
}

impl EventRateLimit {
    /// `newEventRateLimit` (admission.go:75-89).
    pub fn new(config: &Configuration, clock: Clock) -> Result<Self, String> {
        let enforcers = config
            .limits
            .iter()
            .map(LimitEnforcer::new)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { enforcers, clock })
    }

    /// The `Register` factory (admission.go:46-62): load, validate, build.
    pub fn from_config(config: Option<&str>) -> Result<Self, String> {
        let cfg = load_configuration(config)?;
        let errs = validate_configuration(&cfg);
        if !errs.is_empty() {
            return Err(super::plugins::aggregate_message(
                &errs.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
            ));
        }
        Self::new(&cfg, Arc::new(Utc::now))
    }

    /// `Validate` (admission.go:91-120).
    pub fn validate(&self, attr: &Attributes) -> Result<(), Error> {
        // Only the core Event kind (`api.Kind("Event")`).
        if !(attr.group.is_empty() && attr.kind == "Event") {
            return Ok(());
        }
        // Dry-run requests never reach etcd, so they are not rate limited.
        if attr.dry_run {
            return Ok(());
        }
        let now = (self.clock)();
        let errors: Vec<String> = self
            .enforcers
            .iter()
            .filter_map(|e| e.accept(attr, now).err())
            .collect();
        if errors.is_empty() {
            return Ok(());
        }
        // `apierrors.NewTooManyRequestsError` (errors.go:416-423) over the
        // aggregate's message, which drops duplicates (util/errors.go:70-95).
        let mut seen: Vec<&str> = Vec::new();
        for e in &errors {
            if !seen.contains(&e.as_str()) {
                seen.push(e);
            }
        }
        let message = if seen.len() == 1 {
            seen[0].to_string()
        } else {
            format!("[{}]", seen.join(", "))
        };
        Err(Error::TooManyRequests(format!(
            "Too many requests: {message}"
        )))
    }
}

static INSTALLED: OnceLock<EventRateLimit> = OnceLock::new();

/// Install the process-wide plugin, built once at startup.
pub fn install(plugin: EventRateLimit) {
    let _ = INSTALLED.set(plugin);
}

/// The installed plugin, if the server was started with one.
pub fn installed() -> Option<&'static EventRateLimit> {
    INSTALLED.get()
}
