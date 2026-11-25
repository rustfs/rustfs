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

#[cfg(test)]
mod tests;

use super::{TargetClient, bound_replication_check_error};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use std::collections::{BTreeMap, BTreeSet, HashSet};

// A fresh probe creates at most four distinct versions. Allow for ambiguous writes and
// small target page sizes, but never traverse an unbounded response from a remote target.
// This bounds both tracked IDs and pages even if a target returns a single entry per page.
const PROBE_CLEANUP_LIMIT: usize = 64;
// S3 object keys and version IDs are each limited to 1,024 bytes.
const PROBE_CLEANUP_MAX_TOKEN_BYTES: usize = 1024;

struct ProbeCleanupError {
    detail: String,
    reconcilable: bool,
}

fn probe_cleanup_error<E: ProvideErrorMetadata>(operation: &str, err: &SdkError<E>) -> ProbeCleanupError {
    let status = err.raw_response().map(|response| response.status().as_u16());
    let code = err.code();
    // Keep remote messages, endpoint URLs, and arbitrary remote codes out of admin responses.
    let safe_code = code.filter(|code| {
        matches!(
            *code,
            "AccessDenied"
                | "InvalidAccessKeyId"
                | "SignatureDoesNotMatch"
                | "InvalidToken"
                | "ExpiredToken"
                | "InvalidRequest"
                | "NoSuchBucket"
                | "NoSuchKey"
                | "NoSuchVersion"
                | "RequestTimeout"
                | "SlowDown"
                | "ServiceUnavailable"
                | "InternalError"
                | "InternalServerError"
        )
    });
    let category = match err {
        SdkError::ConstructionFailure(_) => "request construction failure",
        SdkError::TimeoutError(_) => "timeout",
        SdkError::DispatchFailure(error) if error.is_timeout() => "timeout",
        SdkError::DispatchFailure(_) => "transport failure",
        SdkError::ResponseError(_) => "invalid response",
        _ => "remote request failed",
    };
    let detail = match (status, safe_code) {
        (Some(status), Some(code)) => format!("{operation}: HTTP {status} {code}"),
        (Some(status), None) => format!("{operation}: HTTP {status} ({category})"),
        (None, Some(code)) => format!("{operation}: {code}"),
        (None, None) => format!("{operation}: {category}"),
    };
    let reconcilable = match (code, status) {
        (_, Some(401 | 403)) => false,
        (Some("RequestTimeout" | "SlowDown" | "ServiceUnavailable" | "InternalError" | "InternalServerError"), _) => true,
        (Some(_), _) => false,
        (None, Some(408 | 429 | 500 | 502 | 503 | 504)) => true,
        // An unreadable success response can follow a committed DELETE.
        (None, Some(200..=299)) => true,
        (None, None) => match err {
            SdkError::TimeoutError(_) | SdkError::ResponseError(_) => true,
            SdkError::DispatchFailure(error) => !error.is_user(),
            _ => false,
        },
        _ => false,
    };
    ProbeCleanupError { detail, reconcilable }
}

#[derive(Default)]
struct ProbeVersionCleanup {
    attempts: u8,
    error: Option<ProbeCleanupError>,
}

impl ProbeVersionCleanup {
    fn can_delete(&self) -> bool {
        self.attempts < 2 && self.error.as_ref().is_none_or(|error| error.reconcilable)
    }

    async fn delete(&mut self, client: &TargetClient, bucket: &str, key: &str, version_id: &str) {
        self.attempts += 1;
        self.error = match client
            .client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .version_id(version_id)
            .send()
            .await
        {
            // A strict target may reject the repeat of the VersionDelete phase with NoSuchVersion.
            Ok(_) => None,
            Err(err)
                if matches!(err.code(), Some("NoSuchKey" | "NoSuchVersion"))
                    && err.raw_response().is_some_and(|response| response.status().as_u16() == 404) =>
            {
                None
            }
            Err(err) => Some(probe_cleanup_error("delete probe object version", &err)),
        };
    }
}

async fn list_replication_probe_versions(client: &TargetClient, bucket: &str, key: &str) -> Result<BTreeSet<String>, String> {
    let mut ids = BTreeSet::new();
    let mut cursor: (Option<String>, Option<String>) = (None, None);
    let mut seen_cursors = HashSet::new();
    for _ in 0..PROBE_CLEANUP_LIMIT {
        let output = client
            .client
            .list_object_versions()
            .bucket(bucket)
            .prefix(key)
            .max_keys(PROBE_CLEANUP_LIMIT as i32)
            .set_key_marker(cursor.0.clone())
            .set_version_id_marker(cursor.1.clone())
            .send()
            .await
            .map_err(|err| probe_cleanup_error("list probe object versions", &err).detail)?;
        if output.name().is_some_and(|name| name != bucket)
            || output.prefix().is_some_and(|prefix| prefix != key)
            || output
                .key_marker()
                .is_some_and(|marker| marker != cursor.0.as_deref().unwrap_or_default())
            || output
                .version_id_marker()
                .is_some_and(|marker| marker != cursor.1.as_deref().unwrap_or_default())
        {
            return Err("target returned a different probe cleanup listing than requested".to_string());
        }
        let entry_count = output.versions().len() + output.delete_markers().len();
        if entry_count > PROBE_CLEANUP_LIMIT {
            return Err("target exceeded the probe cleanup page entry limit".to_string());
        }
        let versions = output.versions().iter().map(|version| (version.key(), version.version_id()));
        let markers = output
            .delete_markers()
            .iter()
            .map(|marker| (marker.key(), marker.version_id()));
        for (entry_key, id) in versions.chain(markers) {
            if entry_key.ok_or("target listed a probe cleanup entry without a key")? == key {
                let id = id
                    .filter(|value| !value.is_empty() && value.len() <= PROBE_CLEANUP_MAX_TOKEN_BYTES)
                    .ok_or("target listed a probe entry without a valid version ID")?;
                ids.insert(id.to_owned());
                if ids.len() > PROBE_CLEANUP_LIMIT {
                    return Err("target exceeded the probe cleanup version limit".to_string());
                }
            }
        }

        let next_key = output.next_key_marker().filter(|value| !value.is_empty());
        let next_version = output.next_version_id_marker().filter(|value| !value.is_empty());
        let has_cursor = next_key.is_some() || next_version.is_some();
        match output.is_truncated() {
            Some(false) if !has_cursor => return Ok(ids),
            // Some compatible targets omit IsTruncated on a short terminal page. Require
            // their bucket identity so an empty or unrelated response cannot certify absence.
            None if !has_cursor && entry_count < PROBE_CLEANUP_LIMIT && output.name() == Some(bucket) => return Ok(ids),
            Some(true) => {}
            _ => return Err("target returned inconsistent probe cleanup pagination status".to_string()),
        }
        let next_key = next_key
            .filter(|value| value.len() <= PROBE_CLEANUP_MAX_TOKEN_BYTES)
            .ok_or("target returned an invalid cleanup pagination marker")?;
        if next_version.is_some_and(|value| value.len() > PROBE_CLEANUP_MAX_TOKEN_BYTES) {
            return Err("target returned an invalid cleanup pagination marker".to_string());
        }
        cursor = (Some(next_key.to_owned()), next_version.map(str::to_owned));
        if !seen_cursors.insert(cursor.clone()) {
            return Err("target repeated a cleanup pagination marker pair".to_string());
        }
    }
    Err("target exceeded the probe cleanup page limit".to_string())
}

pub(super) async fn cleanup_replication_probe<'a>(
    target_client: &TargetClient,
    target_bucket: &str,
    probe_key: &str,
    known_version_ids: impl IntoIterator<Item = Option<&'a str>>,
) -> Result<(), String> {
    let mut versions = BTreeMap::<String, ProbeVersionCleanup>::new();
    for id in known_version_ids.into_iter().flatten() {
        // Only address valid target-reported IDs; enumeration recovers unknown versions.
        if id.is_empty() || id.len() > PROBE_CLEANUP_MAX_TOKEN_BYTES {
            continue;
        }
        let version = versions.entry(id.to_owned()).or_default();
        if version.attempts == 0 {
            version.delete(target_client, target_bucket, probe_key, id).await;
        }
    }

    // List before mutating a page's cursor. A version first discovered by listing can
    // need its initial delete, one retry, and a final read to establish the outcome.
    let mut verification_error = None;
    for pass in 0..=2 {
        let remaining = match list_replication_probe_versions(target_client, target_bucket, probe_key).await {
            Ok(ids) => ids,
            Err(error) => {
                verification_error = Some(error);
                break;
            }
        };
        // Absence resolves an ambiguous commit, but cannot erase an explicit denial.
        for (id, version) in &mut versions {
            if !remaining.contains(id) && version.error.as_ref().is_some_and(|err| err.reconcilable) {
                version.error = None;
            }
        }
        if remaining.is_empty() {
            break;
        }
        let new_ids = remaining.iter().filter(|id| !versions.contains_key(*id)).count();
        if versions.len() + new_ids > PROBE_CLEANUP_LIMIT {
            verification_error = Some("target exceeded the probe cleanup version limit".to_string());
            break;
        }
        let mut attempted = false;
        if pass < 2 {
            for id in &remaining {
                let version = versions.entry(id.clone()).or_default();
                if version.can_delete() {
                    version.delete(target_client, target_bucket, probe_key, id).await;
                    attempted = true;
                }
            }
        }
        if !attempted {
            verification_error = Some(format!("target still lists {} probe object versions or delete markers", remaining.len()));
            break;
        }
    }
    let errors: Vec<_> = verification_error
        .into_iter()
        .chain(
            versions
                .into_values()
                .filter_map(|version| version.error.map(|error| error.detail)),
        )
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(bound_replication_check_error(errors.join("; ")))
    }
}
