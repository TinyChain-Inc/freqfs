use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::PriorityMap;

fn path(id: u32) -> PathBuf {
    PathBuf::from(format!("cache-entry-{id}"))
}

fn validate<V>(map: &PriorityMap<V>) {
    if map.list.is_empty() {
        assert!(map.head.is_none());
        assert!(map.tail.is_none());
    } else {
        let head = map.head.as_ref().expect("head");
        let tail = map.tail.as_ref().expect("tail");
        assert!(map.list.get::<PathBuf>(head).unwrap().state.prev.is_none());
        assert!(map.list.get::<PathBuf>(tail).unwrap().state.next.is_none());
    }

    let mut previous = None;
    let mut next = map.head.as_deref();
    let mut visited = 0;
    while let Some(key) = next {
        let item = map.list.get(key).expect("linked item");
        assert_eq!(item.state.prev.as_deref(), previous);
        assert_ne!(item.state.prev.as_deref(), Some(key));
        assert_ne!(item.state.next.as_deref(), Some(key));
        previous = Some(&*item.key);
        next = item.state.next.as_deref();
        visited += 1;
        assert!(visited <= map.list.len(), "cycle in priority order");
    }
    assert_eq!(visited, map.list.len());
    assert_eq!(previous, map.tail.as_deref());
}

fn assert_contents(map: &PriorityMap<u32>, expected: &[(PathBuf, u32)]) {
    validate(map);
    assert_eq!(
        map.iter().size_hint(),
        (expected.len(), Some(expected.len()))
    );
    assert_eq!(
        map.iter()
            .map(|(key, value)| (key.clone(), *value))
            .collect::<Vec<_>>(),
        expected
    );
    assert_eq!(
        map.iter()
            .rev()
            .map(|(key, value)| (key.clone(), *value))
            .collect::<Vec<_>>(),
        expected.iter().rev().cloned().collect::<Vec<_>>()
    );
}

#[test]
fn test_order() {
    let mut map = PriorityMap::new();
    for id in 0..10 {
        assert!(map.insert(path(id), id).is_none());
        validate(&map);
    }
    assert_contents(
        &map,
        &(0..10).rev().map(|id| (path(id), id)).collect::<Vec<_>>(),
    );
    assert_eq!(map.insert(path(4), 40), Some(4));
    let expected = std::iter::once((path(4), 40))
        .chain((0..10).rev().filter(|id| *id != 4).map(|id| (path(id), id)))
        .collect::<Vec<_>>();
    assert_contents(&map, &expected);
    assert_eq!(map.remove(&path(99)), None);
    assert!(!map.bump(&path(99)));
    assert_contents(&map, &expected);
}

#[test]
fn test_access() {
    let mut map = PriorityMap::new();
    let mut rng = StdRng::seed_from_u64(0x_51a3_2b7d);
    for _ in 1..100_000 {
        let id = rng.random_range(0..1000);
        map.insert(path(id), id);
        validate(&map);
        assert_eq!(map.iter().count(), map.list.len());
        while !map.list.is_empty() {
            map.bump(&path(rng.random_range(0..map.list.len() as u32)));
            validate(&map);
            let key = map.iter().next().unwrap().0.clone();
            assert!(map.remove(&key).is_some());
            validate(&map);
            if let Some((key, _)) = map.iter().next_back() {
                let key = key.clone();
                assert!(map.remove(&key).is_some());
                validate(&map);
            }
        }
        assert_contents(&map, &[]);
    }
}

#[test]
fn test_random_ops_invariants() {
    let mut rng = StdRng::seed_from_u64(0x_51a3_2b7d);
    let mut map = PriorityMap::new();
    let mut expected: Vec<(PathBuf, u32)> = Vec::new();
    for _ in 0..10_000 {
        let key = path(rng.random_range(0..50));
        let position = expected.iter().position(|(k, _)| *k == key);
        match rng.random_range(0..4) {
            0 => {
                let value = rng.random();
                let old = position.map(|index| expected.remove(index).1);
                assert_eq!(map.insert(key.clone(), value), old);
                expected.insert(0, (key, value));
            }
            1 => {
                assert_eq!(
                    map.remove(&key),
                    position.map(|index| expected.remove(index).1)
                );
            }
            2 => {
                assert_eq!(map.bump(&key), position.is_some());
                if let Some(index) = position.filter(|index| *index > 0) {
                    expected.swap(index, index - 1);
                }
            }
            _ => {
                if let Some((key, _)) = expected.last() {
                    assert!(map.remove(key).is_some());
                    expected.pop();
                }
            }
        }
        assert_contents(&map, &expected);
    }
}

#[test]
fn test_bump_head_tail_middle() {
    let mut map = PriorityMap::new();
    for id in 1..=3 {
        map.insert(path(id), id);
    }
    assert!(map.bump(&path(3)));
    assert_contents(&map, &[(path(3), 3), (path(2), 2), (path(1), 1)]);
    assert!(map.bump(&path(1)));
    assert_contents(&map, &[(path(3), 3), (path(1), 1), (path(2), 2)]);
    assert!(map.bump(&path(1)));
    assert_contents(&map, &[(path(1), 1), (path(3), 3), (path(2), 2)]);
}

#[test]
fn removal_preserves_order() {
    for len in 0..=8 {
        for removed in 0..=len {
            let mut map = PriorityMap::new();
            for id in 0..len {
                map.insert(path(id), id);
            }
            assert_eq!(
                map.remove(&path(removed)),
                (removed < len).then_some(removed)
            );
            let expected = (0..len)
                .rev()
                .filter(|id| *id != removed)
                .map(|id| (path(id), id))
                .collect::<Vec<_>>();
            assert_contents(&map, &expected);
            while let Some((key, value)) = map.iter().next_back() {
                let key = key.clone();
                let value = *value;
                assert_eq!(map.remove(&key), Some(value));
                validate(&map);
            }
            assert_contents(&map, &[]);
        }
    }
}

#[test]
fn borrowed_cursors_do_not_retain_keys() {
    for len in 0..=8 {
        let mut map = PriorityMap::new();
        for id in 0..len {
            map.insert(path(id), id);
        }
        let counts = map
            .list
            .keys()
            .map(|key| ((**key).clone(), Arc::strong_count(key)))
            .collect::<HashMap<_, _>>();
        let mut expected = (0..len).rev().collect::<VecDeque<_>>();
        let mut iter = map.iter();
        for step in 0..=len {
            for key in map.list.keys() {
                assert_eq!(Arc::strong_count(key), counts[&**key]);
            }
            assert_eq!(iter.size_hint(), (expected.len(), Some(expected.len())));
            let actual = if step % 2 == 0 {
                iter.next()
            } else {
                iter.next_back()
            };
            let wanted = if step % 2 == 0 {
                expected.pop_front()
            } else {
                expected.pop_back()
            };
            assert_eq!(
                actual.map(|(key, value)| (key.clone(), *value)),
                wanted.map(|id| (path(id), id))
            );
        }
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next_back(), None);
        assert_eq!(iter.size_hint(), (0, Some(0)));
    }
}

#[test]
fn nonclone_payloads_release_owners() {
    struct Payload(Arc<()>);

    fn assert_send<T: Send>(_: &T) {}

    let mut map = PriorityMap::new();
    let original = Arc::new(());
    let original_released = Arc::downgrade(&original);
    assert!(map.insert(path(1), Payload(original)).is_none());
    assert!(map.bump(&path(1)));
    assert_send(&map);
    assert_send(&map.iter());
    assert_eq!(Arc::strong_count(&map.iter().next().unwrap().1 .0), 1);

    let replacement = Arc::new(());
    let replacement_released = Arc::downgrade(&replacement);
    let old = map
        .insert(path(1), Payload(replacement))
        .expect("old payload");
    assert_eq!(Arc::strong_count(&old.0), 1);
    assert!(original_released.upgrade().is_some());
    drop(old);
    assert!(original_released.upgrade().is_none());

    let removed = map.remove(&path(1)).expect("replacement payload");
    assert!(replacement_released.upgrade().is_some());
    drop(removed);
    assert!(replacement_released.upgrade().is_none());
    assert!(map.iter().next().is_none());

    let final_owner = Arc::new(());
    let final_released = Arc::downgrade(&final_owner);
    map.insert(path(2), Payload(final_owner));
    drop(map);
    assert!(final_released.upgrade().is_none());
}
