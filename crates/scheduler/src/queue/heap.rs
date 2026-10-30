//! Keyed heap — port of `pkg/scheduler/backend/heap/heap.go` (`:30-244`).
//!
//! A producer/consumer queue implementing a heap keyed by `key_fn`, so an
//! item can be updated in place (`AddOrUpdate` → `heap.Fix`) or removed by
//! key (`Delete` → `heap.Remove`). Items in `items` are exactly the keys in
//! `queue` and vice versa. The metric recorder hook is not ported.

use std::collections::HashMap;

pub type KeyFn<T> = Box<dyn Fn(&T) -> String + Send + Sync>;
pub type LessFn<T> = Box<dyn Fn(&T, &T) -> bool + Send + Sync>;

struct HeapItem<T> {
    obj: T,
    /// The index of the object's key in `queue`.
    index: usize,
}

pub struct Heap<T> {
    items: HashMap<String, HeapItem<T>>,
    queue: Vec<String>,
    key_fn: KeyFn<T>,
    less_fn: LessFn<T>,
}

impl<T> Heap<T> {
    /// `New` (`heap.go:225-227`).
    pub fn new(key_fn: KeyFn<T>, less_fn: LessFn<T>) -> Self {
        Self {
            items: HashMap::new(),
            queue: Vec::new(),
            key_fn,
            less_fn,
        }
    }

    fn less(&self, i: usize, j: usize) -> bool {
        let (Some(a), Some(b)) = (
            self.items.get(&self.queue[i]),
            self.items.get(&self.queue[j]),
        ) else {
            return false;
        };
        (self.less_fn)(&a.obj, &b.obj)
    }

    fn swap(&mut self, i: usize, j: usize) {
        self.queue.swap(i, j);
        let ki = self.queue[i].clone();
        let kj = self.queue[j].clone();
        if let Some(it) = self.items.get_mut(&ki) {
            it.index = i;
        }
        if let Some(it) = self.items.get_mut(&kj) {
            it.index = j;
        }
    }

    // container/heap `up`.
    fn up(&mut self, mut j: usize) {
        while j > 0 {
            let i = (j - 1) / 2; // parent
            if i == j || !self.less(j, i) {
                break;
            }
            self.swap(i, j);
            j = i;
        }
    }

    // container/heap `down`; returns true if the element moved.
    fn down(&mut self, i0: usize, n: usize) -> bool {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n {
                break;
            }
            let mut j = j1; // left child
            let j2 = j1 + 1;
            if j2 < n && self.less(j2, j1) {
                j = j2; // right child
            }
            if !self.less(j, i) {
                break;
            }
            self.swap(i, j);
            i = j;
        }
        i > i0
    }

    // container/heap `Fix`.
    fn fix(&mut self, i: usize) {
        let n = self.queue.len();
        if !self.down(i, n) {
            self.up(i);
        }
    }

    // container/heap `Remove`.
    fn remove_at(&mut self, i: usize) -> T {
        let n = self.queue.len() - 1;
        if n != i {
            self.swap(i, n);
            if !self.down(i, n) {
                self.up(i);
            }
        }
        let key = self.queue.pop().expect("non-empty");
        self.items.remove(&key).expect("item present").obj
    }

    /// `AddOrUpdate` (`heap.go:144-155`): inserts an item, updating it in
    /// place if it already exists.
    pub fn add_or_update(&mut self, obj: T) {
        let key = (self.key_fn)(&obj);
        if let Some(item) = self.items.get_mut(&key) {
            item.obj = obj;
            let idx = item.index;
            self.fix(idx);
        } else {
            let n = self.queue.len();
            self.items.insert(key.clone(), HeapItem { obj, index: n });
            self.queue.push(key);
            self.up(n);
        }
    }

    /// `Delete` (`heap.go:158-168`). `Err` when the object is not found.
    pub fn delete(&mut self, obj: &T) -> Result<T, String> {
        let key = (self.key_fn)(obj);
        self.delete_by_key(&key)
    }

    pub fn delete_by_key(&mut self, key: &str) -> Result<T, String> {
        match self.items.get(key) {
            Some(item) => {
                let idx = item.index;
                Ok(self.remove_at(idx))
            }
            None => Err("object not found".to_string()),
        }
    }

    /// `Peek` (`heap.go:171-173`).
    pub fn peek(&self) -> Option<&T> {
        self.queue
            .first()
            .and_then(|k| self.items.get(k))
            .map(|i| &i.obj)
    }

    /// `Pop` (`heap.go:176-186`). `Err` when the heap is empty.
    pub fn pop(&mut self) -> Result<T, String> {
        if self.queue.is_empty() {
            return Err("heap is empty".to_string());
        }
        Ok(self.remove_at(0))
    }

    /// `Get` (`heap.go:189-192`).
    pub fn get(&self, obj: &T) -> Option<&T> {
        self.get_by_key(&(self.key_fn)(obj))
    }

    /// `GetByKey` (`heap.go:195-202`).
    pub fn get_by_key(&self, key: &str) -> Option<&T> {
        self.items.get(key).map(|i| &i.obj)
    }

    pub fn get_mut_by_key(&mut self, key: &str) -> Option<&mut T> {
        self.items.get_mut(key).map(|i| &mut i.obj)
    }

    /// `Has` (`heap.go:204-208`).
    pub fn has(&self, obj: &T) -> bool {
        self.items.contains_key(&(self.key_fn)(obj))
    }

    pub fn has_key(&self, key: &str) -> bool {
        self.items.contains_key(key)
    }

    /// `List` (`heap.go:211-217`).
    pub fn list(&self) -> Vec<&T> {
        self.items.values().map(|i| &i.obj).collect()
    }

    /// `Len` (`heap.go:220-222`).
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn key_of(&self, obj: &T) -> String {
        (self.key_fn)(obj)
    }
}
