//! Native peer registrations owned by one NIXL completion thread.

use super::{Agent, NIXL_METADATA_HEADER, NixlError, metadata_hash, nixl_err};
use std::cell::Cell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

const IDLE_TTL: Duration = Duration::from_secs(300);
pub(super) const SWEEP_INTERVAL: Duration = Duration::from_secs(60);

pub(super) trait MetadataRegistry {
    fn load(&self, bytes: &[u8]) -> Result<String, NixlError>;
    fn unload(&self, name: &str) -> Result<(), NixlError>;
}

impl MetadataRegistry for Agent {
    fn load(&self, bytes: &[u8]) -> Result<String, NixlError> {
        self.load_remote_md(bytes).map_err(nixl_err("load remote metadata"))
    }

    fn unload(&self, name: &str) -> Result<(), NixlError> {
        self.invalidate_remote_md(name)
            .map_err(nixl_err("invalidate remote metadata"))
    }
}

struct Peer {
    hash: Option<u64>,
    idle_since: Rc<Cell<Instant>>,
}

impl Peer {
    fn idle(&self) -> bool {
        Rc::strong_count(&self.idle_since) == 1
    }

    fn acquire(&self) -> RemoteLease {
        RemoteLease(Rc::clone(&self.idle_since))
    }
}

/// The completion thread drops this lease after the native request handle.
pub(super) struct RemoteLease(Rc<Cell<Instant>>);

impl Drop for RemoteLease {
    fn drop(&mut self) {
        // The cache and this final request are the two remaining owners.
        if Rc::strong_count(&self.0) == 2 {
            self.0.set(Instant::now());
        }
    }
}

pub(super) struct RemoteCache {
    peers: HashMap<String, Peer>,
    capacity: usize,
}

impl RemoteCache {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            peers: HashMap::new(),
            capacity,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    pub(super) fn acquire(
        &mut self,
        registry: &impl MetadataRegistry,
        name: &str,
        metadata: Option<&[u8]>,
    ) -> Result<RemoteLease, NixlError> {
        let hash = metadata.map(metadata_hash);
        if let Some(peer) = self.peers.get(name) {
            if peer.hash.is_some() && (hash.is_none() || hash == peer.hash) {
                return Ok(peer.acquire());
            }
            if !peer.idle() {
                return Err(NixlError::Token("peer metadata changed while transfers are active".to_owned()));
            }
            self.evict(registry, name)?;
        }
        let metadata = metadata.ok_or_else(|| NixlError::Token(format!("agent {name:?} needs {NIXL_METADATA_HEADER}")))?;
        // Retry outstanding cleanup before loading another registration.
        self.trim(registry, |peer| peer.hash.is_none())?;
        let loaded = registry.load(metadata)?;
        if loaded != name {
            if let Some(peer) = self.peers.get_mut(&loaded) {
                // Native loads can merge descriptors into an existing registration.
                peer.hash = None;
            } else if let Err(error) = registry.unload(&loaded) {
                self.peers.insert(
                    loaded,
                    Peer {
                        hash: None,
                        idle_since: Rc::new(Cell::new(Instant::now())),
                    },
                );
                return Err(error);
            }
            return Err(NixlError::Token(format!("metadata belongs to agent {loaded:?}, request names {name:?}")));
        }
        let peer = Peer {
            hash,
            idle_since: Rc::new(Cell::new(Instant::now())),
        };
        let lease = peer.acquire();
        self.peers.insert(loaded, peer);
        // Only a successful registration displaces an idle peer. Failed cleanup
        // leaves the victim stale for the next admission or periodic sweep.
        let _ = self.trim(registry, |_| true);
        Ok(lease)
    }

    fn trim(&mut self, registry: &impl MetadataRegistry, eligible: impl Fn(&Peer) -> bool) -> Result<(), NixlError> {
        if self.peers.len() > self.capacity
            && let Some((oldest, _)) = self
                .peers
                .iter()
                .filter(|(_, peer)| peer.idle() && eligible(peer))
                .min_by_key(|(_, peer)| peer.idle_since.get())
        {
            let oldest = oldest.clone();
            self.evict(registry, &oldest)?;
        }
        Ok(())
    }

    fn evict(&mut self, registry: &impl MetadataRegistry, name: &str) -> Result<(), NixlError> {
        // A backend error can occur after native cleanup has partly succeeded.
        // Never reuse a registration once invalidation has started.
        self.mark_stale(name);
        registry.unload(name)?;
        self.peers.remove(name);
        Ok(())
    }

    pub(super) fn mark_stale(&mut self, name: &str) {
        if let Some(peer) = self.peers.get_mut(name) {
            peer.hash = None;
        }
    }

    pub(super) fn sweep(&mut self, registry: &impl MetadataRegistry, now: Instant) {
        self.peers.retain(|name, peer| {
            if !peer.idle() || (peer.hash.is_some() && now.duration_since(peer.idle_since.get()) < IDLE_TTL) {
                return true;
            }
            peer.hash = None;
            registry.unload(name).is_err()
        });
    }
}

#[cfg(test)]
mod tests;
