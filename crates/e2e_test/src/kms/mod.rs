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

//! KMS (Key Management Service) End-to-End Tests
//!
//! This module contains comprehensive end-to-end tests for RustFS KMS functionality,
//! including tests for both Local and Vault backends.

// KMS-specific common utilities
#[cfg(test)]
pub mod common;

#[cfg(test)]
mod kms_local_test;

#[cfg(test)]
mod kms_vault_test;

#[cfg(test)]
mod kms_comprehensive_test;

#[cfg(test)]
mod multipart_encryption_test;

#[cfg(test)]
mod kms_edge_cases_test;

#[cfg(test)]
mod kms_fault_recovery_test;

#[cfg(test)]
mod test_runner;

#[cfg(test)]
mod bucket_default_encryption_test;
