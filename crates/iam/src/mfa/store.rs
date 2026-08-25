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

//! Persistence for MFA records.
//!
//! Records live in the same object store and under the same at-rest encryption
//! as IAM identities, one object per identity:
//!
//! ```text
//! .rustfs.sys/config/mfa/<access-key>/totp.json
//! ```
//!
//! # Why `config/mfa/` and not `config/iam/mfa/`
//!
//! The IAM cache loader walks the whole of `config/iam/` on startup and buckets
//! every key it finds by its first path segment. A new prefix under there would
//! be swept into that walk for no benefit, so MFA records sit in a sibling
//! prefix. They still go through the IAM at-rest crypto, which is the part worth
//! sharing.
//!
//! # Concurrency
//!
//! Every mutation is a read-modify-write, and two of them racing would lose an
//! update — including, in the worst case, a replay high-water mark or a spent
//! recovery code. Rather than holding a distributed lock across the round trip,
//! writes carry the ETag they read as an `If-Match` precondition and retry when
//! the store reports a conflict. That is the same optimistic scheme the IAM
//! lazy-rewrite path already uses, and it degrades to a retry rather than to a
//! held lock that a crashed node would have to time out.
//!
//! # No caching
//!
//! Records are read from the store on every verification. A cache would need
//! cluster-wide invalidation to keep the replay mark and the lockout counter
//! honest, and getting that wrong reopens exactly the holes this module exists
//! to close. Verifications are rare enough that the read is not worth
//! optimising.

use super::record::MfaRecord;
use crate::error::{Error, Result, is_err_config_not_found};
use crate::storage_api::object_store::HTTPPreconditions;
use crate::store::object::{decrypt_iam_blob, encrypt_iam_blob};
use crate::{
    IAM_CONFIG_ROOT_PREFIX, IamStorageError, IamStore, delete_iam_config, keyring, read_iam_config_with_metadata,
    save_iam_config_with_opts,
};
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::warn;

type IamObjectOptions = <IamStore as crate::storage_api::object_store::ObjectOperations>::ObjectOptions;

/// Attempts before a contended write gives up.
///
/// A conflict means another request for the *same identity* wrote first, which
/// is rare; more than a few in a row means something is wrong rather than busy.
const MAX_WRITE_ATTEMPTS: usize = 5;

/// Path of the record for `access_key`.
fn record_path(access_key: &str) -> String {
    format!("{IAM_CONFIG_ROOT_PREFIX}/mfa/{access_key}/totp.json")
}

/// Whether the server can protect a TOTP secret at rest.
///
/// Enrollment is refused when this is false. A TOTP secret is credential-
/// equivalent — anyone holding it can mint valid codes forever — so writing one
/// in plaintext would let the second factor be lifted straight off a disk,
/// leaving the user with a false sense of protection. IAM identities tolerate a
/// missing master key for backward compatibility with existing deployments;
/// a new feature has no such history to honour.
pub fn at_rest_protection_available() -> bool {
    keyring::encrypt_key().is_some()
}

/// Operator-facing explanation for a refused enrollment.
pub const ENROLLMENT_UNAVAILABLE_REASON: &str =
    "two-factor authentication requires RUSTFS_IAM_MASTER_KEY to be configured so the shared secret can be encrypted at rest";

/// A record plus the ETag it was read at, for a subsequent conditional write.
#[derive(Debug)]
pub struct LoadedRecord {
    pub record: MfaRecord,
    /// `None` when the record does not exist yet.
    etag: Option<String>,
}

impl LoadedRecord {
    /// Whether this identity has ever had a record written.
    pub const fn is_new(&self) -> bool {
        self.etag.is_none()
    }
}

/// Load the record for `access_key`, or a fresh one if none exists.
///
/// An absent record is not an error: it is the state every identity starts in.
pub async fn load(api: Arc<IamStore>, access_key: &str, now: OffsetDateTime) -> Result<LoadedRecord> {
    let path = record_path(access_key);

    match read_iam_config_with_metadata(api, &path, &IamObjectOptions::default()).await {
        Ok((data, info)) => {
            let plain = decrypt_iam_blob(&data)?;
            let record: MfaRecord = serde_json::from_slice(&plain).map_err(|err| {
                // A record that cannot be parsed must not silently become "no
                // second factor": that would turn corruption into a bypass.
                warn!(
                    path = %path,
                    error = %err,
                    "MFA record is unreadable; refusing to treat it as absent"
                );
                Error::other(format!("the stored MFA record for this identity is unreadable: {err}"))
            })?;
            record.validate_version().map_err(|err| Error::other(err.to_string()))?;

            Ok(LoadedRecord { record, etag: info.etag })
        }
        Err(err) if is_err_config_not_found(&err.clone().into()) => Ok(LoadedRecord {
            record: MfaRecord::new(access_key, now),
            etag: None,
        }),
        Err(err) => Err(err.into()),
    }
}

/// Read-only view of whether `access_key` has an active second factor.
///
/// Returns `false` for an identity with no record, and propagates a read
/// failure rather than defaulting: the login path calls this to decide whether
/// to demand a second factor, and a store outage must not be an automatic
/// bypass.
pub async fn is_enabled(api: Arc<IamStore>, access_key: &str, now: OffsetDateTime) -> Result<bool> {
    Ok(load(api, access_key, now).await?.record.is_enabled())
}

/// Apply `mutate` to the identity's record and persist the result.
///
/// Retries on a lost race, re-reading so the mutation always applies to current
/// state. `mutate` may therefore run more than once and must not have side
/// effects of its own.
///
/// Generic over the closure's error so a caller can return its own typed
/// failure — a rejected code, say — without encoding it into a string and
/// decoding it on the way out. Storage failures arrive through `From<Error>`.
pub async fn update<T, E, F>(
    api: Arc<IamStore>,
    access_key: &str,
    now: OffsetDateTime,
    mut mutate: F,
) -> std::result::Result<T, E>
where
    F: FnMut(&mut MfaRecord) -> std::result::Result<T, E>,
    E: From<Error>,
{
    let path = record_path(access_key);

    for attempt in 1..=MAX_WRITE_ATTEMPTS {
        let loaded = load(api.clone(), access_key, now).await.map_err(E::from)?;
        let mut record = loaded.record;
        let outcome = mutate(&mut record)?;

        let plain = serde_json::to_vec(&record).map_err(|err| E::from(Error::other(err.to_string())))?;
        let encrypted = encrypt_iam_blob(&plain).map_err(E::from)?;

        let mut opts = IamObjectOptions {
            max_parity: true,
            ..Default::default()
        };
        // A new record must not overwrite one another request created between
        // our read and our write, so an absent ETag becomes `If-None-Match: *`
        // rather than an unconditional put.
        opts.http_preconditions = Some(match loaded.etag {
            Some(etag) => HTTPPreconditions {
                if_match: Some(etag),
                ..Default::default()
            },
            None => HTTPPreconditions {
                if_none_match: Some("*".to_string()),
                ..Default::default()
            },
        });

        match save_iam_config_with_opts(api.clone(), &path, encrypted, &opts).await {
            Ok(()) => return Ok(outcome),
            Err(IamStorageError::PreconditionFailed) => {
                warn!(
                    path = %path,
                    attempt,
                    "MFA record write lost a race; retrying against current state"
                );
            }
            Err(err) => return Err(E::from(err.into())),
        }
    }

    Err(E::from(Error::other(
        "the MFA record for this identity is being modified concurrently; retry the request",
    )))
}

/// Remove the record entirely.
///
/// Used by the administrative reset, where leaving a disabled-but-present
/// record would only preserve a stale lockout counter.
pub async fn delete(api: Arc<IamStore>, access_key: &str) -> Result<()> {
    let path = record_path(access_key);
    match delete_iam_config(api, &path).await {
        Ok(()) => Ok(()),
        // Already absent is the desired end state.
        Err(err) if is_err_config_not_found(&err.clone().into()) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_paths_sit_beside_the_iam_tree_not_inside_it() {
        // Inside `config/iam/` the IAM cache loader would sweep these into its
        // startup walk; this pins them out of it.
        let path = record_path("sinan");

        assert_eq!(path, "config/mfa/sinan/totp.json");
        assert!(!path.starts_with("config/iam/"));
    }

    #[test]
    fn record_paths_are_scoped_per_identity() {
        assert_ne!(record_path("sinan"), record_path("someone-else"));
        assert!(record_path("sinan").contains("/sinan/"));
    }

    #[test]
    fn the_unavailable_reason_names_the_variable_an_operator_must_set() {
        // The message is the whole remediation path for a blocked enrollment.
        assert!(ENROLLMENT_UNAVAILABLE_REASON.contains("RUSTFS_IAM_MASTER_KEY"));
    }

    #[test]
    fn a_loaded_record_reports_whether_it_is_new() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp");

        let fresh = LoadedRecord {
            record: MfaRecord::new("sinan", now),
            etag: None,
        };
        assert!(fresh.is_new());

        let existing = LoadedRecord {
            record: MfaRecord::new("sinan", now),
            etag: Some("etag".to_string()),
        };
        assert!(!existing.is_new());
    }
}
