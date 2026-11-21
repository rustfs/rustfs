use super::*;
use std::cell::{Cell, RefCell};
use std::collections::HashSet;

#[derive(Default)]
struct Registry {
    peers: RefCell<HashSet<String>>,
    fail_next_load: Cell<bool>,
    fail_next_unload: Cell<bool>,
}

impl MetadataRegistry for Registry {
    fn load(&self, bytes: &[u8]) -> Result<String, NixlError> {
        if self.fail_next_load.replace(false) {
            return Err(NixlError::Token("simulated load failure".to_owned()));
        }
        let name = std::str::from_utf8(bytes)
            .expect("test metadata")
            .split(':')
            .next()
            .expect("agent name");
        self.peers.borrow_mut().insert(name.to_owned());
        Ok(name.to_owned())
    }
    fn unload(&self, name: &str) -> Result<(), NixlError> {
        if self.fail_next_unload.replace(false) {
            return Err(NixlError::Token("simulated backend failure".to_owned()));
        }
        assert!(self.peers.borrow_mut().remove(name));
        Ok(())
    }
}

#[test]
fn pressure_and_expiry_skip_live_transfers_and_unload_native_metadata() {
    let backend = Registry::default();
    let mut cache = RemoteCache::new(2);
    let active = cache.acquire(&backend, "a", Some(b"a")).expect("pin a");
    drop(cache.acquire(&backend, "b", Some(b"b")).expect("cache b"));
    drop(cache.acquire(&backend, "c", Some(b"c")).expect("evict b"));
    assert!(!backend.peers.borrow().contains("b"));
    assert!(cache.acquire(&backend, "b", None).is_err());
    let later = Instant::now() + IDLE_TTL;
    cache.sweep(&backend, later);
    assert_eq!(*backend.peers.borrow(), HashSet::from(["a".to_owned()]));
    assert!(cache.acquire(&backend, "a", Some(b"a:new")).is_err());
    drop(active);
    drop(cache.acquire(&backend, "a", Some(b"a:new")).expect("replace idle generation"));
    assert!(cache.acquire(&backend, "a", None).is_ok());
    cache.sweep(&backend, later + IDLE_TTL);
    assert!(backend.peers.borrow().is_empty());
}

#[test]
fn a_failed_transfer_requires_fresh_metadata_after_its_siblings_finish() {
    let backend = Registry::default();
    let mut cache = RemoteCache::new(2);
    let active = cache.acquire(&backend, "a", Some(b"a")).expect("in flight");
    cache.mark_stale("a");
    assert!(cache.acquire(&backend, "a", None).is_err());
    assert!(cache.acquire(&backend, "a", Some(b"a")).is_err());
    drop(active);
    drop(cache.acquire(&backend, "a", Some(b"a")).expect("reconnect"));
    assert!(cache.acquire(&backend, "wrong", Some(b"another")).is_err());
    assert!(!backend.peers.borrow().contains("another"));
}

#[test]
fn failed_eviction_cannot_reuse_a_partially_invalidated_registration() {
    let backend = Registry::default();
    let mut cache = RemoteCache::new(1);
    drop(cache.acquire(&backend, "a", Some(b"a")).expect("initial registration"));
    backend.fail_next_unload.set(true);
    drop(cache.acquire(&backend, "b", Some(b"b")).expect("new registration is usable"));
    assert!(cache.acquire(&backend, "b", None).is_ok());
    assert!(cache.acquire(&backend, "a", None).is_err());
    drop(cache.acquire(&backend, "a", Some(b"a")).expect("reload peer"));
}

#[test]
fn idle_lifetime_starts_after_the_last_native_request_finishes() {
    let backend = Registry::default();
    let mut cache = RemoteCache::new(2);
    let first = cache.acquire(&backend, "a", Some(b"a")).unwrap();
    let last = cache.acquire(&backend, "a", None).unwrap();
    // The last idle timestamp predates both transfers.
    last.0.set(Instant::now() - IDLE_TTL - SWEEP_INTERVAL);
    drop(first);
    cache.sweep(&backend, Instant::now());
    assert!(backend.peers.borrow().contains("a"));
    drop(last);
    cache.sweep(&backend, Instant::now() + SWEEP_INTERVAL);
    assert!(cache.acquire(&backend, "a", None).is_ok());
    cache.sweep(&backend, Instant::now() + IDLE_TTL);
    assert!(backend.peers.borrow().is_empty());
}

#[test]
fn unsuccessful_registration_preserves_idle_peers_at_capacity() {
    let backend = Registry::default();
    let mut cache = RemoteCache::new(1);
    drop(cache.acquire(&backend, "a", Some(b"a")).expect("cache a"));

    backend.fail_next_load.set(true);
    assert!(cache.acquire(&backend, "b", Some(b"b")).is_err());
    assert!(cache.acquire(&backend, "a", None).is_ok());
    assert!(cache.acquire(&backend, "b", Some(b"c")).is_err());
    assert!(cache.acquire(&backend, "a", None).is_ok());
    assert_eq!(*backend.peers.borrow(), HashSet::from(["a".to_owned()]));
}

#[test]
fn unexpected_loaded_name_requires_a_refresh_after_its_transfers_finish() {
    let backend = Registry::default();
    let mut cache = RemoteCache::new(2);
    let active = cache.acquire(&backend, "a", Some(b"a")).expect("pin a");

    assert!(cache.acquire(&backend, "b", Some(b"a:new")).is_err());
    assert!(backend.peers.borrow().contains("a"));
    assert!(cache.acquire(&backend, "a", None).is_err());
    drop(active);
    assert!(cache.acquire(&backend, "a", Some(b"a")).is_ok());
}
