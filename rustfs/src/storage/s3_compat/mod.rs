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

//! The legacy s3s edge: conversions from s3s request types into the app
//! layer's own types (rustfs/backlog#2734), and between the s3s DTOs and the
//! `rustfs-gateway-types` shapes the engine crates hold (rustfs/backlog#2745),
//! one module per configuration family; every such conversion is total and
//! lossless. Everything here goes away with the s3s edge itself (task T4.1).

mod envelope;
pub(crate) mod replication;
