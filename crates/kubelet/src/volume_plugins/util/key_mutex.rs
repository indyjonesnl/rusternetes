//! Port of `k8s.io/utils/keymutex` (`hashed.go`): `NewHashed(n)` (`hashed.go:30-38`).
//!
//! "A KeyMutex ... that serializes goroutines operating on the same key": a
//! fixed set of mutexes, the key hashed onto one of them. `NewHashed(n)` with
//! `n <= 0` uses `runtime.NumCPU()` (`hashed.go:31-33`). Two distinct keys may
//! share a mutex, as upstream's do.

use std::sync::{Mutex, MutexGuard};

pub struct KeyMutex {
    mutexes: Vec<Mutex<()>>,
}

impl KeyMutex {
    /// `NewHashed(0)`.
    pub fn new_hashed(n: usize) -> Self {
        let n = if n == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        } else {
            n
        };
        Self {
            mutexes: (0..n).map(|_| Mutex::new(())).collect(),
        }
    }

    /// `LockKey`; the guard is `UnlockKey` (Go's `defer UnlockKey`).
    pub fn lock_key(&self, id: &str) -> MutexGuard<'_, ()> {
        let idx = (fnv32a(id) % self.mutexes.len() as u32) as usize;
        // A poisoned lock only means another holder panicked; the key is
        // still exclusive.
        self.mutexes[idx].lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// `hashedKeyMutex.hash` (`hashed.go:54-58`): `fnv.New32a()`.
fn fnv32a(id: &str) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for b in id.as_bytes() {
        h ^= u32::from(*b);
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}
