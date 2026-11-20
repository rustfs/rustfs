use super::*;
use std::cell::{Cell, RefCell};
use std::collections::HashSet;

#[derive(Default)]
struct Registry(RefCell<HashSet<String>>, Cell<bool>);

impl MetadataRegistry for Registry {
    fn load(&self, bytes: &[u8]) -> Result<String, NixlError> {
        let name = std::str::from_utf8(bytes)
            .expect("test metadata")
            .split(':')
            .next()
            .expect("agent name");
        assert!(self.0.borrow_mut().insert(name.to_owned()));
        Ok(name.to_owned())
    }
    fn unload(&self, name: &str) -> Result<(), NixlError> {
        if self.1.replace(false) {
            return Err(NixlError::Token("simulated backend failure".to_owned()));
        }
        assert!(self.0.borrow_mut().remove(name));
        Ok(())
    }
}

#[test]
fn pressure_and_expiry_skip_live_transfers_and_unload_native_metadata() {
    let now = Instant::now();
    let backend = Registry::default();
    let mut cache = RemoteCache::new(2);
    let active = cache.acquire(&backend, "a", Some(b"a"), now).expect("pin a");
    drop(cache.acquire(&backend, "b", Some(b"b"), now).expect("cache b"));
    drop(cache.acquire(&backend, "c", Some(b"c"), now).expect("evict b"));
    assert!(!backend.0.borrow().contains("b"));
    assert!(cache.acquire(&backend, "b", None, now).is_err());
    let later = Instant::now() + IDLE_TTL;
    cache.sweep(&backend, later);
    assert_eq!(*backend.0.borrow(), HashSet::from(["a".to_owned()]));
    assert!(cache.acquire(&backend, "a", Some(b"a:new"), later).is_err());
    drop(active);
    drop(
        cache
            .acquire(&backend, "a", Some(b"a:new"), later)
            .expect("replace idle generation"),
    );
    assert!(cache.acquire(&backend, "a", None, later).is_ok());
    cache.sweep(&backend, later + IDLE_TTL);
    assert!(backend.0.borrow().is_empty());
}

#[test]
fn a_failed_transfer_requires_fresh_metadata_after_its_siblings_finish() {
    let now = Instant::now();
    let backend = Registry::default();
    let mut cache = RemoteCache::new(2);
    let active = cache.acquire(&backend, "a", Some(b"a"), now).expect("in flight");
    cache.mark_stale("a");
    assert!(cache.acquire(&backend, "a", None, now).is_err());
    assert!(cache.acquire(&backend, "a", Some(b"a"), now).is_err());
    drop(active);
    drop(cache.acquire(&backend, "a", Some(b"a"), now).expect("reconnect"));
    assert!(cache.acquire(&backend, "wrong", Some(b"another"), now).is_err());
    assert!(!backend.0.borrow().contains("another"));
}

#[test]
fn failed_eviction_cannot_reuse_a_partially_invalidated_registration() {
    let now = Instant::now();
    let backend = Registry::default();
    let mut cache = RemoteCache::new(1);
    drop(cache.acquire(&backend, "a", Some(b"a"), now).expect("initial registration"));
    backend.1.set(true);
    assert!(cache.acquire(&backend, "b", Some(b"b"), now).is_err());
    assert!(cache.acquire(&backend, "a", None, now).is_err());
    drop(cache.acquire(&backend, "a", Some(b"a"), now).expect("reload peer"));
}

#[test]
fn idle_lifetime_starts_after_the_last_native_request_finishes() {
    let backend = Registry::default();
    let mut cache = RemoteCache::new(2);
    let start = Instant::now() - IDLE_TTL - SWEEP_INTERVAL;
    let first = cache.acquire(&backend, "a", Some(b"a"), start).unwrap();
    let last = cache.acquire(&backend, "a", None, start).unwrap();
    drop(first);
    cache.sweep(&backend, Instant::now());
    assert!(backend.0.borrow().contains("a"));
    drop(last);
    cache.sweep(&backend, Instant::now() + SWEEP_INTERVAL);
    assert!(cache.acquire(&backend, "a", None, Instant::now()).is_ok());
    cache.sweep(&backend, Instant::now() + IDLE_TTL);
    assert!(backend.0.borrow().is_empty());
}
