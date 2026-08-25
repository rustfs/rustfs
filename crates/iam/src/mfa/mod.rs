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

//! Two-factor authentication for RustFS identities.
//!
//! All of the logic lives here rather than in the admin handlers, so the
//! console and the `rc` CLI drive the same state machine through the same API
//! instead of each reimplementing enrollment and verification.

pub mod challenge;
pub mod qr;
pub mod record;
pub mod recovery;
pub mod service;
pub mod store;
pub mod totp;

pub use challenge::{CHALLENGE_TTL_SECONDS, ChallengeError};
pub use qr::{QrError, RenderedQr};
pub use record::{MfaRecord, MfaRecordError, MfaVerification, MfaVerifyError};
pub use recovery::{ConsumeOutcome, GeneratedRecoveryCodes, StoredRecoveryCode};
pub use service::{MFA_ISSUER, MfaServiceError};
pub use totp::{TOTP_ALGORITHM, TOTP_DIGITS, TOTP_PERIOD_SECONDS, TotpError, TotpSecret, looks_like_totp_code};
