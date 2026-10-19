//! `k8s.io/apimachinery/pkg/util/cache.LRUExpireCache`: a bounded cache whose
//! entries expire after a per-entry TTL; at capacity the least recently used
//! entry is evicted (expired entries first). Shared by the api-server
//! authorization webhook and the kubelet delegating authn/authz.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct LruExpireCache<V: Clone> {
    cap: usize,
    inner: Mutex<CacheInner<V>>,
}

struct CacheInner<V> {
    tick: u64,
    entries: HashMap<String, (V, Instant, u64)>,
}

impl<V: Clone> LruExpireCache<V> {
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            inner: Mutex::new(CacheInner {
                tick: 0,
                entries: HashMap::new(),
            }),
        }
    }

    pub fn get(&self, key: &str, now: Instant) -> Option<V> {
        let mut c = self.inner.lock().unwrap();
        c.tick += 1;
        let tick = c.tick;
        match c.entries.get_mut(key) {
            Some((value, expires, used)) if *expires > now => {
                *used = tick;
                Some(value.clone())
            }
            Some(_) => {
                c.entries.remove(key);
                None
            }
            None => None,
        }
    }

    pub fn add(&self, key: String, value: V, ttl: Duration, now: Instant) {
        let mut c = self.inner.lock().unwrap();
        c.tick += 1;
        let tick = c.tick;
        if !c.entries.contains_key(&key) && c.entries.len() >= self.cap {
            // drop expired entries first, else the least recently used
            c.entries.retain(|_, (_, exp, _)| *exp > now);
            if c.entries.len() >= self.cap {
                if let Some(lru) = c
                    .entries
                    .iter()
                    .min_by_key(|(_, (_, _, used))| *used)
                    .map(|(k, _)| k.clone())
                {
                    c.entries.remove(&lru);
                }
            }
        }
        c.entries.insert(key, (value, now + ttl, tick));
    }
}
