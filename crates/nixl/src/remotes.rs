//! Native peer registrations owned by one NIXL completion thread.

use super::{Agent, NIXL_METADATA_HEADER, NixlError, metadata_hash, nixl_err};
use std::collections::HashMap;
use std::sync::Arc;
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
    last_used: Instant,
    // Each outstanding native request pins this registration until its request
    // handle has been dropped, including work whose caller has timed out.
    lease: Arc<()>,
}

impl Peer {
    fn idle(&self) -> bool {
        Arc::strong_count(&self.lease) == 1
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
        now: Instant,
    ) -> Result<Arc<()>, NixlError> {
        let hash = metadata.map(metadata_hash);
        if let Some(peer) = self.peers.get_mut(name) {
            if peer.hash.is_some() && (hash.is_none() || hash == peer.hash) {
                peer.last_used = now;
                return Ok(peer.lease.clone());
            }
            if !peer.idle() {
                return Err(NixlError::Token("peer metadata changed while transfers are active".to_owned()));
            }
            self.evict(registry, name)?;
        }
        let metadata = metadata.ok_or_else(|| NixlError::Token(format!("agent {name:?} needs {NIXL_METADATA_HEADER}")))?;
        if self.peers.len() >= self.capacity
            && let Some((oldest, _)) = self
                .peers
                .iter()
                .filter(|(_, peer)| peer.idle())
                .min_by_key(|(_, peer)| peer.last_used)
        {
            let oldest = oldest.clone();
            self.evict(registry, &oldest)?;
        }
        let loaded = registry.load(metadata)?;
        let lease = Arc::new(());
        if loaded != name {
            if !self.peers.contains_key(&loaded)
                && let Err(error) = registry.unload(&loaded)
            {
                self.peers.insert(
                    loaded,
                    Peer {
                        hash: None,
                        last_used: now,
                        lease,
                    },
                );
                return Err(error);
            }
            return Err(NixlError::Token(format!("metadata belongs to agent {loaded:?}, request names {name:?}")));
        }
        self.peers.insert(
            loaded,
            Peer {
                hash,
                last_used: now,
                lease: lease.clone(),
            },
        );
        Ok(lease)
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
            if !peer.idle() || (peer.hash.is_some() && now.duration_since(peer.last_used) < IDLE_TTL) {
                return true;
            }
            peer.hash = None;
            registry.unload(name).is_err()
        });
    }
}

#[cfg(test)]
mod tests;
