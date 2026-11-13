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

//! The authorization shapes one migration batch owns (rustfs/backlog#2765), one file per batch so
//! parallel batches edit disjoint files. Shapes several batches share stay in `storage/access.rs`.

pub(crate) mod bucket_access;
pub(crate) mod bucket_configs;
pub(crate) mod bucket_core;
pub(crate) mod bucket_replication_lifecycle;
pub(crate) mod listing_multipart;
pub(crate) mod object_commit;
pub(crate) mod object_meta;
pub(crate) mod object_misc;
pub(crate) mod object_read;
pub(crate) mod object_write;
