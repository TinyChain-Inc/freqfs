//! Cache priority order: front insertion and one-position promotion per hit.

// Adapted from ds-ext/src/queue.rs (Apache-2.0); see this crate's LICENSE.

use std::collections::HashMap;
use std::mem;
use std::path::PathBuf;
use std::sync::Arc;

struct ItemState {
    prev: Option<Arc<PathBuf>>,
    next: Option<Arc<PathBuf>>,
}

struct Item<V> {
    key: Arc<PathBuf>,
    value: V,
    state: ItemState,
}

type Inner<V> = HashMap<Arc<PathBuf>, Item<V>>;

/// An iterator over the entries in a [`PriorityMap`]
pub(super) struct Iter<'a, V> {
    list: &'a Inner<V>,
    next: Option<&'a Item<V>>,
    last: Option<&'a Item<V>>,
    size: usize,
}

impl<'a, V> Iterator for Iter<'a, V> {
    type Item = (&'a PathBuf, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.next?;
        self.size -= 1;

        if self.size == 0 {
            self.next = None;
            self.last = None;
        } else {
            self.next = item
                .state
                .next
                .as_ref()
                .map(|key| self.list.get::<PathBuf>(key).expect("next"));
        }

        Some((&item.key, &item.value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size, Some(self.size))
    }
}

impl<'a, V> DoubleEndedIterator for Iter<'a, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let item = self.last?;
        self.size -= 1;

        if self.size == 0 {
            self.next = None;
            self.last = None;
        } else {
            self.last = item
                .state
                .prev
                .as_ref()
                .map(|key| self.list.get::<PathBuf>(key).expect("previous"));
        }

        Some((&item.key, &item.value))
    }
}

/// Cache-owned paths and values, ordered independently of their keys.
pub(super) struct PriorityMap<V> {
    list: Inner<V>,
    head: Option<Arc<PathBuf>>,
    tail: Option<Arc<PathBuf>>,
}

impl<V> PriorityMap<V> {
    /// Construct a new [`PriorityMap`].
    pub(super) fn new() -> Self {
        Self {
            list: HashMap::new(),
            head: None,
            tail: None,
        }
    }

    /// If `key` is present, increase its priority by one and return `true`.
    pub(super) fn bump(&mut self, key: &PathBuf) -> bool {
        let Some(item) = self.list.get(key) else {
            return false;
        };
        let Some(prev_key) = item.state.prev.clone() else {
            return true;
        };

        // A predecessor is distinct from the promoted entry. Move their links,
        // then reconnect the outer neighbors after releasing the mutable borrows.
        let (item_key, before, after) = {
            let [Some(item), Some(prev)] = self.list.get_disjoint_mut([key, &*prev_key]) else {
                unreachable!("linked predecessor");
            };
            item.state.prev = prev.state.prev.take();
            prev.state.next = item.state.next.take();
            item.state.next = Some(prev_key.clone());
            prev.state.prev = Some(item.key.clone());

            (
                item.key.clone(),
                item.state.prev.clone(),
                prev.state.next.clone(),
            )
        };

        if let Some(before) = before {
            self.list
                .get_mut::<PathBuf>(&before)
                .expect("previous")
                .state
                .next = Some(item_key);
        } else {
            self.head = Some(item_key);
        }

        if let Some(after) = after {
            self.list
                .get_mut::<PathBuf>(&after)
                .expect("next")
                .state
                .prev = Some(prev_key);
        } else {
            self.tail = Some(prev_key);
        }

        true
    }

    /// Insert a new `value` at `key` and return the previous value, if any.
    pub(super) fn insert(&mut self, key: PathBuf, value: V) -> Option<V> {
        let old_value = self.remove(&key);

        let key = Arc::new(key);
        let mut next = Some(key.clone());
        mem::swap(&mut self.head, &mut next);

        if let Some(prev_key) = &next {
            let prev = &mut self.list.get_mut::<PathBuf>(prev_key).expect("prev").state;
            prev.prev = Some(key.clone());
        } else {
            debug_assert!(self.tail.is_none());
            self.tail = Some(key.clone());
        }

        let item = Item {
            key: key.clone(),
            value,
            state: ItemState { prev: None, next },
        };

        assert!(self.list.insert(key, item).is_none());

        old_value
    }

    /// Construct an iterator over the entries in this [`PriorityMap`].
    pub(super) fn iter(&self) -> Iter<'_, V> {
        Iter {
            list: &self.list,
            next: self
                .head
                .as_ref()
                .map(|key| self.list.get::<PathBuf>(key).expect("head")),
            last: self
                .tail
                .as_ref()
                .map(|key| self.list.get::<PathBuf>(key).expect("tail")),
            size: self.list.len(),
        }
    }

    /// Remove a path without changing the relative order of surviving entries.
    pub(super) fn remove(&mut self, key: &PathBuf) -> Option<V> {
        let item = self.list.remove(key)?;
        let Item { value, state, .. } = item;

        if let Some(prev) = &state.prev {
            self.list
                .get_mut::<PathBuf>(prev)
                .expect("previous")
                .state
                .next = state.next.clone();
        } else {
            self.head = state.next.clone();
        }

        if let Some(next) = &state.next {
            self.list.get_mut::<PathBuf>(next).expect("next").state.prev = state.prev;
        } else {
            self.tail = state.prev;
        }

        Some(value)
    }
}

#[cfg(test)]
#[path = "../../tests/unit/priority.rs"]
mod tests;
