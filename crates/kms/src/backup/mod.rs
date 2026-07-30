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

//! Backup/restore contract types for KMS state.
//!
//! This module is contract-only: it defines the versioned backup manifest,
//! the per-backend responsibility matrix, typed failure modes, and the
//! restore dry-run report. Nothing here is wired into handlers or backends;
//! backup export, restore orchestration, and the admin API build on these
//! types in follow-up changes.
//!
//! # Bundle model
//!
//! A backup bundle is a set of AEAD-encrypted artifacts described by a single
//! [`BackupManifest`]. All state in a bundle belongs to one snapshot
//! generation — there is no partially consistent bundle. The bundle is
//! protected by a backup KEK that is deliberately outside the business KMS
//! trust hierarchy, and the manifest is sealed with a completeness marker and
//! a final digest; a bundle that never reached its marker is permanently
//! non-restorable.
//!
//! # Restore ordering
//!
//! Restore implementations must follow this order: re-establish the external
//! trust root first (Vault/HSM native restore where one exists), then
//! material and version records into staging, then metadata and
//! configuration, then verification, and only then an explicit atomic
//! cutover. A dry-run ([`RestoreDryRunReport`]) performs zero writes.
//!
//! # Deliberately unfrozen
//!
//! Fields whose shape depends on contracts still in flight are reserved
//! rather than guessed (see [`ReservedSlot`]): the per-key version inventory
//! (backlog#1565) and capability discovery (backlog#1571). Alias and policy
//! artifacts are reserved names for features that do not exist yet. Reserved
//! slots reject data in format version 1 and become real types in a later
//! format version.

mod capability;
mod dry_run;
mod error;
mod manifest;

pub use capability::{AtRestProtection, BackupBackendKind, BackupResponsibility};
pub use dry_run::{
    ExternalDependencyMismatch, RestoreBlocker, RestoreBlockerCode, RestoreConflict, RestoreConflictKind, RestoreDryRunReport,
};
pub use error::BackupError;
pub use manifest::{
    AeadAlgorithm, ArtifactDescriptor, ArtifactKind, BackupKekDescriptor, BackupManifest, CompletenessState, ContentDigest,
    DigestAlgorithm, LocalKdfDescriptor, LocalKeyDerivation, ReservedSlot,
};
