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

//! Bounded thread-state profile collection.
//!
//! Linux native collection reads only the state byte from this process's
//! `/proc/self/task/*/stat` records. Thread names, identifiers, stacks, paths,
//! addresses, and raw procfs bytes cannot enter the exported result. Tokio
//! runtime state remains explicitly unsupported because Dial9 does not expose
//! the contract's RUNNABLE, WAITING, BLOCKED, and UNKNOWN counts.

#[cfg(target_os = "linux")]
use std::fs::{self, File};
#[cfg(target_os = "linux")]
use std::io::{ErrorKind, Read as _};
#[cfg(target_os = "linux")]
use std::time::Instant;
use tokio_util::sync::CancellationToken;

#[cfg(target_os = "linux")]
use super::profile_cpu::{CollectorLease, ProfileData, ThreadProfileData, ThreadState, ThreadStateCount};
use super::profile_cpu::{
    ProfileCaptureRequest, ProfileError, ProfileReasonCode, ProfileResult, ProfileTool, SignedProfileExport, ThreadProfileScope,
    check_cancel, encode_signed_profile_export, unix_now,
};
use crate::connect::DeviceIdentity;

#[cfg(target_os = "linux")]
const PROC_TASK_DIRECTORY: &str = "/proc/self/task";
#[cfg(target_os = "linux")]
const MAX_NATIVE_THREADS: usize = 4_096;
#[cfg(target_os = "linux")]
const MAX_PROC_STAT_BYTES: u64 = 4_096;

pub fn capture_thread_profile(
    request: &ProfileCaptureRequest,
    scope: ThreadProfileScope,
    cancel: &CancellationToken,
) -> Result<ProfileResult, ProfileError> {
    request.validate(ProfileTool::Threads, unix_now()?)?;
    check_cancel(cancel)?;

    if scope == ThreadProfileScope::TokioRuntime {
        return Ok(ProfileResult::unsupported(
            request,
            ProfileTool::Threads,
            ProfileReasonCode::UnsupportedTool,
        ));
    }

    #[cfg(not(target_os = "linux"))]
    return Ok(ProfileResult::unsupported(
        request,
        ProfileTool::Threads,
        ProfileReasonCode::UnsupportedPlatform,
    ));

    #[cfg(target_os = "linux")]
    {
        let _lease = CollectorLease::acquire()?;
        let started = Instant::now();
        let deadline = started.checked_add(request.duration).ok_or(ProfileError::LimitExceeded)?;
        let data = collect_native_thread_states(cancel, deadline)?;
        check_cancel(cancel)?;
        Ok(ProfileResult::succeeded(
            request,
            ProfileTool::Threads,
            started.elapsed(),
            ProfileData::Threads(data),
        ))
    }
}

pub async fn export_thread_profile(
    request: &ProfileCaptureRequest,
    scope: ThreadProfileScope,
    key: &DeviceIdentity,
    cancel: &CancellationToken,
) -> Result<SignedProfileExport, ProfileError> {
    let owned_request = request.clone();
    let owned_cancel = cancel.clone();
    let result = tokio::task::spawn_blocking(move || capture_thread_profile(&owned_request, scope, &owned_cancel))
        .await
        .map_err(|_| ProfileError::CollectionFailed)??;
    encode_signed_profile_export(request, &result, key, cancel)
}

#[cfg(target_os = "linux")]
fn collect_native_thread_states(cancel: &CancellationToken, deadline: Instant) -> Result<ThreadProfileData, ProfileError> {
    let entries = fs::read_dir(PROC_TASK_DIRECTORY).map_err(|_| ProfileError::SourceUnavailable)?;
    let mut counts = [0_u64; 4];
    let mut visited = 0_usize;

    for entry in entries {
        check_cancel(cancel)?;
        if Instant::now() >= deadline {
            return Err(ProfileError::TimedOut);
        }
        let entry = entry.map_err(|_| ProfileError::SourceUnavailable)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(ProfileError::SourceUnavailable);
        };
        if name.is_empty() || !name.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(ProfileError::SourceUnavailable);
        }
        visited = visited.checked_add(1).ok_or(ProfileError::LimitExceeded)?;
        if visited > MAX_NATIVE_THREADS {
            return Err(ProfileError::LimitExceeded);
        }

        let state = match read_proc_stat_state(&entry.path().join("stat"))? {
            Some(state) => state,
            None => continue,
        };
        let index = match state {
            ThreadState::Runnable => 0,
            ThreadState::Waiting => 1,
            ThreadState::Blocked => 2,
            ThreadState::Unknown => 3,
        };
        counts[index] = counts[index].checked_add(1).ok_or(ProfileError::LimitExceeded)?;
    }

    if counts.iter().all(|count| *count == 0) {
        return Err(ProfileError::SourceUnavailable);
    }

    Ok(ThreadProfileData::native(vec![
        ThreadStateCount::new(ThreadState::Runnable, counts[0]),
        ThreadStateCount::new(ThreadState::Waiting, counts[1]),
        ThreadStateCount::new(ThreadState::Blocked, counts[2]),
        ThreadStateCount::new(ThreadState::Unknown, counts[3]),
    ]))
}

#[cfg(target_os = "linux")]
fn read_proc_stat_state(path: &std::path::Path) -> Result<Option<ThreadState>, ProfileError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(ProfileError::SourceUnavailable),
    };
    let mut bytes = Vec::with_capacity(256);
    file.take(MAX_PROC_STAT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ProfileError::SourceUnavailable)?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_PROC_STAT_BYTES {
        return Err(ProfileError::SourceUnavailable);
    }
    parse_proc_stat_state(&bytes).map(Some)
}

#[cfg(target_os = "linux")]
fn parse_proc_stat_state(stat: &[u8]) -> Result<ThreadState, ProfileError> {
    let closing = stat
        .iter()
        .rposition(|byte| *byte == b')')
        .ok_or(ProfileError::SourceUnavailable)?;
    let suffix = stat.get(closing + 1..).ok_or(ProfileError::SourceUnavailable)?;
    let state = match suffix {
        [b' ', state, b' ', ..] => *state,
        _ => return Err(ProfileError::SourceUnavailable),
    };
    Ok(match state {
        b'R' => ThreadState::Runnable,
        b'D' => ThreadState::Blocked,
        b'S' | b'I' | b'T' | b't' | b'W' => ThreadState::Waiting,
        _ => ThreadState::Unknown,
    })
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn proc_stat_parser_uses_only_the_kernel_state_byte() {
        for (raw, expected) in [
            (b"123 (worker secret path) R 1 2".as_slice(), ThreadState::Runnable),
            (b"123 (worker) S 1 2", ThreadState::Waiting),
            (b"123 (worker) D 1 2", ThreadState::Blocked),
            (b"123 (worker) Z 1 2", ThreadState::Unknown),
        ] {
            assert_eq!(parse_proc_stat_state(raw).expect("valid proc stat"), expected);
        }
        assert!(matches!(parse_proc_stat_state(b"123 malformed"), Err(ProfileError::SourceUnavailable)));
    }
}
