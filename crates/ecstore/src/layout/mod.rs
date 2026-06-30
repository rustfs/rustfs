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

// #730: set-layout contracts are staged while ECStore ownership boundaries shrink.
#![allow(dead_code)]

//! Static ECStore layout boundaries.
//!
//! This module owns read-only layout descriptors used to keep static set
//! topology separate from runtime `Sets`/`SetDisks` orchestration before any
//! file moves happen.

pub(crate) mod disks_layout;
pub(crate) mod endpoint;
pub(crate) mod endpoints;
pub(crate) mod format;
pub(crate) mod pool_space;
pub(crate) mod set_heal;
pub(crate) mod set_layout;
