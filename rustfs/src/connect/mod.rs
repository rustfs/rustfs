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

//! RustFS Connect device identity.
//!
//! A cluster device proves possession of its own key when it exchanges a
//! one-time registration token for a durable certificate. This module owns the
//! device-side half of that exchange: the P-256 key, the PKCS#10 certificate
//! request built from it, and the proof-of-possession signature over the
//! canonical transcript frozen by
//! `protocol/agent/v1/registration-proof.md`.
//!
//! Enrolled deployments may start the optional outbound heartbeat runtime.
//! An unconfigured server starts no Connect task, generates no key, and holds
//! no Connect identity.

pub mod client;
pub mod config;
pub mod credential_store;
pub mod heartbeat;
pub mod identity;
pub mod identity_store;
pub mod offline;
pub mod registration;
pub mod runtime;

pub use client::{ClientError, ConnectClient, ConnectConfig};
pub use config::{HeartbeatConfig, HeartbeatConfigError, HeartbeatSchedule};
pub use credential_store::{CredentialStore, DeviceCredential};
pub use heartbeat::{CoarseNodeSummary, HeartbeatError, HeartbeatStatus};
pub use identity::{DeviceIdentity, IdentityError, RegistrationProof, RegistrationTranscript};
pub use identity_store::{IdentityStore, StoreError};
pub use offline::{EnrollmentError, OfflineEnrollment, OfflineKeyStore, VerifiedChallenge};
pub use registration::{RegistrationToken, TokenError};
pub use runtime::{HeartbeatRuntime, spawn_heartbeat_runtime};
