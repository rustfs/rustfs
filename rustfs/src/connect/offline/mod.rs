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

//! Offline enrolment: joining a Connect tenant without a network.
//!
//! An air-gapped cluster cannot perform the registration exchange, so an
//! operator carries a signed challenge in and a signed response out. The device
//! half of that exchange lives here: verifying the challenge against a root
//! whose fingerprint is compiled into this binary, minting the key being
//! enrolled, and signing the response.
//!
//! Nothing here opens a socket. That is the point of the surface, and it is
//! asserted rather than assumed: the enrolment path takes bytes and returns
//! bytes.
//!
//! The trust model, the signing convention, and every rejection reason are
//! frozen by `protocol/agent/v1/fixtures/offline-enrollment/` and by
//! `docs/adr/0009-offline-signing.md` on the Connect side.

pub mod enrollment;
pub mod key_store;

pub use enrollment::{EnrollmentError, OfflineEnrollment, VerifiedChallenge};
pub use key_store::OfflineKeyStore;
