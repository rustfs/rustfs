# Local Descriptor Cache Invalidation

**Use this when:** changing local descriptor cache keys, adding an open mode, or modifying descriptor invalidation after replacement, heal, rename or deletion.
**Source of truth:** `crates/ecstore/src/disk/local.rs` (`FdKey`, `FdCache`, `invalidate_cached_fd`) and `crates/ecstore/src/disk/fd_cache_tests.rs`.

## Key and inode ownership

A cache key includes volume, path and open mode. Buffered and direct-mode
descriptors must never be exchanged. Production currently populates buffered
entries only; testing a `direct: true` key does not enable direct caching or
prove that its file was opened with O_DIRECT.

Entries retain an `Arc<File>` and the length snapshot of that inode. Removing
a cache entry prevents future cache hits but does not revoke descriptors
already borrowed by readers. Those readers may finish on the old inode while
later opens observe a replacement inode. Invalidation is not snapshot isolation.

## Invalidation scope

Exact invalidation advances the generation once and invalidates both open-mode
keys for that volume/path. It does not invalidate descendants, adjacent path
names or the same path in another volume. The key's strings are reused; no
per-read invalidation predicate is added. There is one extra exact-key lookup
when no direct-mode entry exists, and no measured speedup is claimed.

Prefix invalidation covers the named component and its descendants, not a
shared text prefix such as `a/bc` for `a/b`. Volume invalidation covers both
modes within that volume, and clear covers the whole cache.

The generation changes before invalidation. Miss-path insertion checks its
captured generation both before and after insertion, so a pre-invalidation
open cannot repopulate the cache after an intervening generation change.
Current-generation refill is still allowed.

## Cancellation and integration boundary

The two exact-key invalidations are awaited sequentially, not atomically. The
normal-completion contract covers both keys; cancellation between those awaits
does not guarantee that both removals completed. Callers must preserve their
mutation/invalidation lifecycle rather than assuming this method fixes every
commit-to-cleanup cancellation window.

Adding a direct cache still requires separate permission-freshness, alignment,
capacity, replacement-race and native O_DIRECT validation. This contract does
not change TTL, authorization checks, production cache population or durability.
