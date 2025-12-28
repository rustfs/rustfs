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

mod reliant;

// Common utilities for all E2E tests
#[cfg(test)]
pub mod common;

#[cfg(test)]
mod version_id_regression_test;

// Data usage regression tests
#[cfg(test)]
mod data_usage_test;

// KMS-specific test modules
#[cfg(test)]
mod kms;

// Special characters in path test modules
#[cfg(test)]
mod special_chars_test;

// Content-Encoding header preservation test
#[cfg(test)]
mod content_encoding_test;

// Policy variables tests
#[cfg(test)]
mod policy;
