// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Deterministic crash-point injection for erasure-coded commit sequences.
//!
//! Each [`CrashPoint`] names one instant inside a multi-step commit at which a
//! `#[cfg(test)]` scenario can simulate a hard power loss: the commit stops dead
//! at the armed step with **no** cleanup, leaving the on-disk state exactly as
//! the preceding steps left it. The test then reopens the disk (or rebuilds the
//! erasure set) and asserts the raw state is coherent — the object reads back as
//! either the whole old version or the whole new version, never a torn mix — and
//! that any staged leftovers are reclaimable and the operation is safely
//! retryable. This is the crash-consistency counterpart to the graceful
//! `should_fail_*` failpoints, which exercise in-process rollback rather than an
//! abrupt loss.
//!
//! Unlike the graceful failpoints, a crash point runs no rollback: it models the
//! process disappearing, so the assertion is purely over the bytes left on disk.
//!
//! Test-only: the arming registry and the real [`should_crash_at`] are
//! `#[cfg(test)]`. In every other build `should_crash_at` is a const-`false`
//! `#[inline(always)]` no-op, so the injection points on the real commit paths
//! (`rename_data`, `write_all_meta`, `complete_multipart_upload`) compile away to
//! nothing and add no branch to production writes. The [`CrashPoint`] variants
//! are still constructed at those call sites in every build, so no variant is
//! dead code.
//!
//! Coverage plan: rustfs/backlog#935 / rustfs/backlog#896 (rename_data),
//! rustfs/backlog#864 (fault-interrupted overwrite/delete must not mutate a
//! committed object), rustfs/backlog#878 (old-or-new-never-mixed).

/// A named instant inside an erasure-coded commit sequence at which a test can
/// simulate a hard power loss.
///
/// All points share one keyed registry (each arm is matched on `(point, key)`),
/// so a scenario arms exactly the window under test and every other commit path
/// runs untouched.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CrashPoint {
    /// `rename_data`: after the data dir is renamed into its destination but
    /// before the old-metadata rollback backup is written. Pre-commit — xl.meta
    /// is untouched, so a crash here must leave the object readable as the old
    /// version (or absent when there was none).
    RenameAfterDataRename,
    /// `rename_data`: after the rollback backup is durable, immediately before
    /// the xl.meta commit rename that makes the new version visible. Still
    /// pre-commit, so a crash here must also leave the old version readable.
    RenameAfterBackupBeforeMetaCommit,
    /// `rename_data`: after the xl.meta commit rename has made the new version
    /// visible, before the durability fsync and with **no** rollback. The commit
    /// rename already landed, so a crash here must leave the object readable as
    /// the new version (rustfs/backlog#878, new-version side).
    RenameAfterMetaCommit,
    /// `write_all_meta` (the atomic temp+rename behind `update_metadata` and
    /// `write_metadata`): after the replacement xl.meta is staged in the tmp
    /// bucket but before the rename that publishes it. Pre-commit — the
    /// destination xl.meta is untouched, so a crash here must leave the object's
    /// metadata byte-for-byte the old version (rustfs/backlog#864); the staged
    /// tmp file is a harmless orphan swept by tmp-bucket GC.
    MetaWriteAfterTmpBeforeRename,
    /// `complete_multipart_upload`: after the upload is fully staged and locked
    /// but before the authoritative `rename_data` commit. Pre-commit — no disk
    /// has moved staged data, so a crash here must leave any prior committed
    /// version byte-for-byte intact (rustfs/backlog#864) and the upload fully
    /// retryable.
    MultipartBeforeCommitRename,
    /// `complete_multipart_upload`: after the `rename_data` commit succeeds but
    /// before the stale `part.N.meta` cleanup. Post-commit — the new version is
    /// durably committed and visible, so a crash here must leave the object
    /// readable as the new version; the un-reclaimed staging parts are swept by
    /// a retried completion or upload GC (rustfs/backlog#946).
    MultipartAfterCommitBeforePartsCleanup,
}

#[cfg(test)]
pub(crate) use armed::{arm, disarm, should_crash_at};

/// Production no-op: the compiler inlines this to `false`, so armed crash points
/// exist only in `#[cfg(test)]` builds and never add a branch to real commits.
#[cfg(not(test))]
#[inline(always)]
pub(crate) fn should_crash_at(_point: CrashPoint, _key: &str) -> bool {
    false
}

#[cfg(test)]
mod armed {
    use super::CrashPoint;
    use std::sync::{Mutex, MutexGuard};

    // A keyed multiset of pending arms, not a single slot: the rename_data /
    // xl.meta scenarios serialize via `durability_mode_override` while the
    // multipart scenarios use `#[serial]`, so the two groups can overlap under
    // `cargo test`'s shared-process threads. Keying every arm on a unique object
    // path lets concurrent scenarios coexist without clobbering each other; a
    // match on `(point, key)` is unambiguous. `#[serial]` still gives the
    // multipart tests their own serialization for unrelated global state.
    static ARMED: Mutex<Vec<(CrashPoint, String)>> = Mutex::new(Vec::new());

    fn guard() -> MutexGuard<'static, Vec<(CrashPoint, String)>> {
        // A poisoned lock only means a prior test panicked mid-scenario; recover
        // the registry rather than cascade the panic into unrelated tests.
        ARMED.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Arm a one-shot crash: the next operation that reaches `point` while
    /// committing `key` stops there. Consumed on the first match so it never
    /// leaks into an unrelated commit. Arming the same `(point, key)` twice is
    /// idempotent.
    pub(crate) fn arm(point: CrashPoint, key: &str) {
        let mut armed = guard();
        if !armed.iter().any(|(p, k)| *p == point && k == key) {
            armed.push((point, key.to_string()));
        }
    }

    /// Clear the pending arm for exactly this `(point, key)`. A scenario retries
    /// the same key after the crash, so teardown disarms in case the injection
    /// was somehow never reached and would otherwise fire inside the retry.
    pub(crate) fn disarm(point: CrashPoint, key: &str) {
        guard().retain(|(p, k)| !(*p == point && k == key));
    }

    /// Returns `true` (consuming the arm) iff a crash is armed for exactly this
    /// `point` and `key`.
    pub(crate) fn should_crash_at(point: CrashPoint, key: &str) -> bool {
        let mut armed = guard();
        if let Some(idx) = armed.iter().position(|(p, k)| *p == point && k == key) {
            armed.swap_remove(idx);
            true
        } else {
            false
        }
    }
}
