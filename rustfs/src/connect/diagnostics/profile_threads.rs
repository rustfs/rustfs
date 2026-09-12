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

//! Explicit thread/runtime profile capability result.
//!
//! Dial9 currently exposes session and disk-buffer state, not bounded counts
//! for RUNNABLE, WAITING, BLOCKED, and UNKNOWN threads. Native thread state
//! would require a separate reviewed platform adapter. Neither source is
//! relabelled as the contract's thread data.

use tokio_util::sync::CancellationToken;

use super::profile_cpu::{
    ProfileCaptureRequest, ProfileError, ProfileReasonCode, ProfileResult, ProfileTool, ThreadProfileScope, check_cancel,
    encode_signed_profile_export, unix_now,
};
use crate::connect::DeviceIdentity;

use super::profile_cpu::SignedProfileExport;

pub fn capture_thread_profile(
    request: &ProfileCaptureRequest,
    _scope: ThreadProfileScope,
    cancel: &CancellationToken,
) -> Result<ProfileResult, ProfileError> {
    request.validate(ProfileTool::Threads, unix_now()?)?;
    check_cancel(cancel)?;
    Ok(ProfileResult::unsupported(
        request,
        ProfileTool::Threads,
        ProfileReasonCode::UnsupportedTool,
    ))
}

pub fn export_thread_profile(
    request: &ProfileCaptureRequest,
    scope: ThreadProfileScope,
    key: &DeviceIdentity,
    cancel: &CancellationToken,
) -> Result<SignedProfileExport, ProfileError> {
    let result = capture_thread_profile(request, scope, cancel)?;
    encode_signed_profile_export(request, &result, key, cancel)
}
