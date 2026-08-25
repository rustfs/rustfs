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

//! Air-gapped Connect enrolment and support-bundle production.
//!
//! An air-gapped cluster cannot perform the registration exchange, so an
//! operator carries a signed challenge in and a signed response out. The device
//! half of that exchange lives here: verifying the challenge against a root
//! whose fingerprint is compiled into this binary, minting the key being
//! enrolled, and signing the response.
//!
//! The same enrolled key signs a deterministic bundle of the stopped runtime's
//! persisted inventory and bounded host diagnostics. Nothing here opens a
//! socket; upload is a separate, operator-controlled step.
//!
//! The trust model, the signing convention, and every rejection reason are
//! frozen by `protocol/agent/v1/fixtures/{offline-enrollment,bundle}/` and by
//! `docs/adr/0009-offline-signing.md` on the Connect side.

pub mod bundle_writer;
pub mod collectors;
pub mod enrollment;
pub mod key_store;
#[cfg(target_os = "linux")]
mod manifest;
pub mod manifest_entry;
pub mod redaction;

pub use bundle_writer::{BundleContext, BundleError, BundleReceipt, write_offline_bundle};
pub use collectors::{CollectorError, OfflineCollector, OfflineDiagnostics, collect_offline_diagnostics};
pub use enrollment::{EnrollmentError, OfflineEnrollment, VerifiedChallenge};
pub use key_store::OfflineKeyStore;
pub use manifest_entry::ManifestEntry;
pub use redaction::{RedactionError, RedactionResult, RedactionSource, redact_json};
