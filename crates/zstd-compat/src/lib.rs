// Copyright 2026 RustFS Team
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

//! Internal version-constraint adapter for RustFS's locked zstd consumers.
//!
//! Every codec API and low-level type comes from the fixed registry implementation.

// RUSTFS_COMPAT_TODO(zstd-013-fixed-backend): Bridge dependencies requiring zstd 0.13. Remove after every consumer accepts the fixed zstd line.
pub use zstd_fixed::*;
