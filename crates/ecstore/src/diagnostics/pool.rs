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

//! BytesPool metric label constants.
//!
//! These constants are used when recording pool acquisition and return
//! metrics to avoid string allocations and ensure label consistency.

/// BytesPool tier labels
pub const POOL_TIER_SMALL: &str = "small";
pub const POOL_TIER_MEDIUM: &str = "medium";
pub const POOL_TIER_LARGE: &str = "large";
pub const POOL_TIER_XLARGE: &str = "xlarge";

/// BytesPool outcome labels
pub const POOL_OUTCOME_HIT: &str = "hit";
pub const POOL_OUTCOME_MISS: &str = "miss";
pub const POOL_OUTCOME_RECYCLED: &str = "recycled";
pub const POOL_OUTCOME_DROPPED: &str = "dropped";
