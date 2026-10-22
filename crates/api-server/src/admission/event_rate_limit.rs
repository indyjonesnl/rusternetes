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
const SERVER: &str = "Server";
const NAMESPACE: &str = "Namespace";
const USER: &str = "User";
const SOURCE_AND_OBJECT: &str = "SourceAndObject";

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
type RateLimiter = Arc<Mutex<TokenBucket>>;

/// `cache` (cache.go:27-30): `singleCache` or `lruCache`.
enum Cache {
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
    fn get(&self, key: &str) -> RateLimiter {
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::event::EventSource;
    use rusternetes_common::resources::{Event, ObjectReference};

    /// `testingclock.FakeClock`.
    #[derive(Clone)]
    struct FakeClock(Arc<Mutex<DateTime<Utc>>>);
    impl FakeClock {
        fn new() -> Self {
            Self(Arc::new(Mutex::new(Utc::now())))
        }
        fn step(&self, secs: i64) {
            let mut t = self.0.lock().unwrap();
            *t += chrono::Duration::seconds(secs);
        }
        fn as_clock(&self) -> Clock {
            let me = self.0.clone();
            Arc::new(move || *me.lock().unwrap())
        }
    }

    fn event_with(f: impl FnOnce(&mut Event)) -> Event {
        let mut e = Event::new(
            "e".into(),
            "ns".into(),
            ObjectReference::default(),
            "r".into(),
            "m".into(),
            Default::default(),
        );
        e.source = EventSource::default();
        f(&mut e);
        e
    }

    #[derive(Clone)]
    struct Req {
        kind: &'static str,
        namespace: &'static str,
        username: &'static str,
        event: Option<Event>,
        delay: i64,
        accepted: bool,
        dry_run: bool,
    }

    fn event_req() -> Req {
        Req {
            kind: "Event",
            namespace: "",
            username: "",
            event: None,
            delay: 0,
            accepted: true,
            dry_run: false,
        }
    }
    fn non_event_req() -> Req {
        Req {
            kind: "NonEvent",
            ..event_req()
        }
    }
    impl Req {
        fn ns(mut self, n: &'static str) -> Self {
            self.namespace = n;
            self
        }
        fn user(mut self, u: &'static str) -> Self {
            self.username = u;
            self
        }
        fn component(self, c: &str) -> Self {
            let c = c.to_string();
            self.event(event_with(|e| e.source.component = c))
        }
        fn event(mut self, e: Event) -> Self {
            self.event = Some(e);
            self
        }
        fn dry(mut self) -> Self {
            self.dry_run = true;
            self
        }
        fn blocked(mut self) -> Self {
            self.accepted = false;
            self
        }
        fn delay(mut self, s: i64) -> Self {
            self.delay = s;
            self
        }
    }

    /// `createSourceAndObjectKeyInclusionRequests`.
    fn inclusion(f: impl Fn(&str) -> Event) -> Vec<Req> {
        vec![
            event_req().event(f("A")),
            event_req().event(f("A")).blocked(),
            event_req().event(f("B")),
        ]
    }

    #[derive(Default)]
    struct Case {
        name: &'static str,
        server: i32,
        namespace: (i32, i32),
        source_and_object: (i32, i32),
        user: (i32, i32),
        requests: Vec<Req>,
    }

    fn limit(t: &str, burst: i32, cache_size: i32) -> Limit {
        Limit {
            limit_type: t.into(),
            qps: 1,
            burst,
            cache_size,
        }
    }

    fn run(c: Case) {
        let clock = FakeClock::new();
        let mut cfg = Configuration::default();
        if c.server > 0 {
            cfg.limits.push(limit(SERVER, c.server, 0));
        }
        if c.namespace.0 > 0 {
            cfg.limits
                .push(limit(NAMESPACE, c.namespace.0, c.namespace.1));
        }
        if c.user.0 > 0 {
            cfg.limits.push(limit(USER, c.user.0, c.user.1));
        }
        if c.source_and_object.0 > 0 {
            cfg.limits.push(limit(
                SOURCE_AND_OBJECT,
                c.source_and_object.0,
                c.source_and_object.1,
            ));
        }
        let plugin = EventRateLimit::new(&cfg, clock.as_clock()).unwrap();
        for (i, rq) in c.requests.iter().enumerate() {
            clock.step(rq.delay);
            let attr = Attributes {
                group: "",
                kind: rq.kind,
                namespace: rq.namespace,
                user: rq.username,
                dry_run: rq.dry_run,
                object: rq.event.as_ref(),
            };
            let res = plugin.validate(&attr);
            assert_eq!(
                res.is_ok(),
                rq.accepted,
                "{}: request {i} should have been {}: {res:?}",
                c.name,
                if rq.accepted { "admitted" } else { "blocked" }
            );
            if let Err(e) = res {
                assert!(matches!(e, Error::TooManyRequests(_)), "{}: {e:?}", c.name);
            }
        }
    }

    /// `TestEventRateLimiting` (admission_test.go:131-).
    #[test]
    fn event_rate_limiting() {
        let n = |s| event_req().ns(s);
        let c = |s: &str| event_req().component(s);
        let cases = vec![
            Case {
                name: "event not blocked when tokens available",
                server: 3,
                requests: vec![event_req()],
                ..Default::default()
            },
            Case {
                name: "non-event not blocked",
                server: 3,
                requests: vec![non_event_req()],
                ..Default::default()
            },
            Case {
                name: "event blocked after tokens exhausted",
                server: 3,
                requests: vec![event_req(), event_req(), event_req(), event_req().blocked()],
                ..Default::default()
            },
            Case {
                name: "event not blocked by dry-run requests",
                server: 3,
                requests: vec![
                    event_req(),
                    event_req(),
                    event_req().dry(),
                    event_req().dry(),
                    event_req().dry(),
                    event_req().dry(),
                    event_req(),
                    event_req().blocked(),
                    event_req().dry(),
                ],
                ..Default::default()
            },
            Case {
                name: "non-event not blocked after tokens exhausted",
                server: 3,
                requests: vec![event_req(), event_req(), event_req(), non_event_req()],
                ..Default::default()
            },
            Case {
                name: "non-events should not count against limit",
                server: 3,
                requests: vec![event_req(), event_req(), non_event_req(), event_req()],
                ..Default::default()
            },
            Case {
                name: "event accepted after token refill",
                server: 3,
                requests: vec![
                    event_req(),
                    event_req(),
                    event_req(),
                    event_req().blocked(),
                    event_req().delay(1),
                ],
                ..Default::default()
            },
            Case {
                name: "event blocked by namespace limits",
                server: 100,
                namespace: (3, 10),
                requests: vec![n("A"), n("A"), n("A"), n("A").blocked()],
                ..Default::default()
            },
            Case {
                name: "event from other namespace not blocked",
                server: 100,
                namespace: (3, 10),
                requests: vec![n("A"), n("A"), n("A"), n("B")],
                ..Default::default()
            },
            Case {
                name: "events from other namespaces should not count against limit",
                server: 100,
                namespace: (3, 10),
                requests: vec![n("A"), n("A"), n("B"), n("A")],
                ..Default::default()
            },
            Case {
                name: "event accepted after namespace token refill",
                server: 100,
                namespace: (3, 10),
                requests: vec![n("A"), n("A"), n("A"), n("A").blocked(), n("A").delay(1)],
                ..Default::default()
            },
            Case {
                name: "event from other namespaces should not clear namespace limits",
                server: 100,
                namespace: (3, 10),
                requests: vec![n("A"), n("A"), n("A"), n("B"), n("A").blocked()],
                ..Default::default()
            },
            Case {
                name: "namespace limits from lru namespace should clear when cache size exceeded",
                server: 100,
                namespace: (3, 2),
                requests: vec![
                    n("A"),
                    n("A"),
                    n("B"),
                    n("B"),
                    n("B"),
                    n("A"),
                    n("B").blocked(),
                    n("A").blocked(),
                    // This should clear out namespace B from the lru cache
                    n("C"),
                    n("A").blocked(),
                    n("B"),
                ],
                ..Default::default()
            },
            Case {
                name: "event blocked by source+object limits",
                server: 100,
                source_and_object: (3, 10),
                requests: vec![c("A"), c("A"), c("A"), c("A").blocked()],
                ..Default::default()
            },
            Case {
                name: "event from other source+object not blocked",
                server: 100,
                source_and_object: (3, 10),
                requests: vec![c("A"), c("A"), c("A"), c("B")],
                ..Default::default()
            },
            Case {
                name: "events from other source+object should not count against limit",
                server: 100,
                source_and_object: (3, 10),
                requests: vec![c("A"), c("A"), c("B"), c("A")],
                ..Default::default()
            },
            Case {
                name: "event accepted after source+object token refill",
                server: 100,
                source_and_object: (3, 10),
                requests: vec![c("A"), c("A"), c("A"), c("A").blocked(), c("A").delay(1)],
                ..Default::default()
            },
            Case {
                name: "event from other source+object should not clear source+object limits",
                server: 100,
                source_and_object: (3, 10),
                requests: vec![c("A"), c("A"), c("A"), c("B"), c("A").blocked()],
                ..Default::default()
            },
            Case {
                name: "source+object limits from lru source+object clear when cache exceeded",
                server: 100,
                source_and_object: (3, 2),
                requests: vec![
                    c("A"),
                    c("A"),
                    c("B"),
                    c("B"),
                    c("B"),
                    c("A"),
                    c("B").blocked(),
                    c("A").blocked(),
                    // This should clear out component B from the lru cache
                    c("C"),
                    c("A").blocked(),
                    c("B"),
                ],
                ..Default::default()
            },
            Case {
                name: "source host should be included in source+object key",
                server: 100,
                source_and_object: (1, 10),
                requests: inclusion(|l| event_with(|e| e.source.host = Some(l.into()))),
                ..Default::default()
            },
            Case {
                name: "involved object kind should be included in source+object key",
                server: 100,
                source_and_object: (1, 10),
                requests: inclusion(|l| event_with(|e| e.involved_object.kind = Some(l.into()))),
                ..Default::default()
            },
            Case {
                name: "involved object namespace should be included in source+object key",
                server: 100,
                source_and_object: (1, 10),
                requests: inclusion(|l| {
                    event_with(|e| e.involved_object.namespace = Some(l.into()))
                }),
                ..Default::default()
            },
            Case {
                name: "involved object name should be included in source+object key",
                server: 100,
                source_and_object: (1, 10),
                requests: inclusion(|l| event_with(|e| e.involved_object.name = Some(l.into()))),
                ..Default::default()
            },
            Case {
                name: "involved object UID should be included in source+object key",
                server: 100,
                source_and_object: (1, 10),
                requests: inclusion(|l| event_with(|e| e.involved_object.uid = Some(l.into()))),
                ..Default::default()
            },
            Case {
                name: "involved object APIVersion should be included in source+object key",
                server: 100,
                source_and_object: (1, 10),
                requests: inclusion(|l| {
                    event_with(|e| e.involved_object.api_version = Some(l.into()))
                }),
                ..Default::default()
            },
            Case {
                name: "event blocked by user limits",
                user: (3, 10),
                requests: vec![
                    event_req().user("A"),
                    event_req().user("A"),
                    event_req().user("A"),
                    event_req().user("A").blocked(),
                ],
                ..Default::default()
            },
            // As upstream: no user limit is configured, so nothing blocks.
            Case {
                name: "event from other user not blocked",
                requests: vec![
                    event_req().user("A"),
                    event_req().user("A"),
                    event_req().user("A"),
                    event_req().user("B"),
                ],
                ..Default::default()
            },
            Case {
                name: "events from other user should not count against limit",
                requests: vec![
                    event_req().user("A"),
                    event_req().user("A"),
                    event_req().user("B"),
                    event_req().user("A"),
                ],
                ..Default::default()
            },
        ];
        for c in cases {
            run(c);
        }
    }

    /// An event of another group (events.k8s.io) is not the core `Event` kind.
    #[test]
    fn other_group_event_not_limited() {
        let plugin = EventRateLimit::new(
            &Configuration {
                limits: vec![limit(SERVER, 1, 0)],
                ..Default::default()
            },
            FakeClock::new().as_clock(),
        )
        .unwrap();
        for _ in 0..3 {
            let attr = Attributes {
                group: "events.k8s.io",
                kind: "Event",
                namespace: "",
                user: "",
                dry_run: false,
                object: None,
            };
            assert!(plugin.validate(&attr).is_ok());
        }
    }

    #[test]
    fn blocked_message_and_status() {
        let plugin = EventRateLimit::new(
            &Configuration {
                limits: vec![limit(SERVER, 1, 0), limit(NAMESPACE, 1, 0)],
                ..Default::default()
            },
            FakeClock::new().as_clock(),
        )
        .unwrap();
        let attr = Attributes {
            group: "",
            kind: "Event",
            namespace: "ns",
            user: "",
            dry_run: false,
            object: None,
        };
        plugin.validate(&attr).unwrap();
        let Err(Error::TooManyRequests(m)) = plugin.validate(&attr) else {
            panic!("expected 429");
        };
        assert_eq!(
            m,
            "Too many requests: [limit reached on type Server for key , limit reached on type Namespace for key ns]"
        );
    }

    /// `TestLRUCache` (cache_test.go:40-115).
    #[test]
    fn lru_cache_evicts_least_recently_used() {
        let cache = Cache::Lru {
            qps: 1.0,
            burst: 1,
            cache: LruExpireCache::new(2),
        };
        let a = cache.get("0");
        assert!(Arc::ptr_eq(&a, &cache.get("0")));
        let b = cache.get("1");
        assert!(Arc::ptr_eq(&b, &cache.get("1")));
        assert!(Arc::ptr_eq(&a, &cache.get("0")));
        let c = cache.get("2"); // evicts "1"
        assert!(Arc::ptr_eq(&c, &cache.get("2")));
        assert!(Arc::ptr_eq(&a, &cache.get("0")));
        assert!(!Arc::ptr_eq(&b, &cache.get("1"))); // re-created after eviction
    }

    /// `TestSingleCache` (cache_test.go:26-38).
    #[test]
    fn single_cache_ignores_key() {
        let l: RateLimiter = Arc::new(Mutex::new(TokenBucket::new(1.0, 1)));
        let cache = Cache::Single(l.clone());
        for k in ["", "key1", "key2"] {
            assert!(Arc::ptr_eq(&l, &cache.get(k)));
        }
    }

    /// `TestValidateConfiguration` (validation_test.go:25-).
    #[test]
    fn validate_configuration_cases() {
        let l = |t: &str, burst, qps, cache| Limit {
            limit_type: t.into(),
            qps,
            burst,
            cache_size: cache,
        };
        let cfg = |limits| Configuration {
            limits,
            ..Default::default()
        };
        let ok = [
            vec![l("Server", 5, 1, 0)],
            vec![l("Namespace", 10, 2, 100)],
            vec![l("User", 10, 2, 100)],
            vec![l("SourceAndObject", 5, 1, 1000)],
            vec![
                l("Server", 5, 1, 0),
                l("Namespace", 10, 2, 100),
                l("SourceAndObject", 25, 10, 1000),
            ],
        ];
        for limits in ok {
            assert!(validate_configuration(&cfg(limits)).is_empty());
        }
        let bad = [
            vec![],
            vec![l("", 25, 10, 1000)],
            vec![l("unknown-type", 25, 10, 1000)],
            vec![l("Server", 0, 1, 0)],
            vec![l("Server", 5, 0, 0)],
            vec![l("Namespace", 5, 1, -1)],
        ];
        for limits in bad {
            assert!(!validate_configuration(&cfg(limits)).is_empty());
        }
        // The cache size of a `Server` limit is not validated.
        assert!(validate_configuration(&cfg(vec![l("Server", 5, 1, -1)])).is_empty());
    }

    #[test]
    fn load_configuration_yaml() {
        let cfg = load_configuration(Some(
            "apiVersion: eventratelimit.admission.k8s.io/v1alpha1\nkind: Configuration\nlimits:\n- type: Namespace\n  qps: 50\n  burst: 100\n  cacheSize: 2000\n",
        ))
        .unwrap();
        assert_eq!(
            cfg.limits,
            vec![Limit {
                limit_type: NAMESPACE.into(),
                qps: 50,
                burst: 100,
                cache_size: 2000
            }]
        );
        assert!(load_configuration(Some("kind: Configuration\nlimits: []\n")).is_err());
        assert!(load_configuration(Some("limits: []\n")).is_err());
        // No configuration is the empty default, which validation rejects.
        assert!(EventRateLimit::from_config(None).is_err());
    }
}
