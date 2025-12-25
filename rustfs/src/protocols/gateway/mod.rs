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

//! Gateway module for protocol implementations
//!
//! This module provides the gateway layer that translates protocol-specific operations
//! into S3 actions, ensuring all protocols follow the same S3 semantics.
//!
//! MINIO CONSTRAINT: All protocol operations MUST be translated to S3 actions
//! and MUST go through the same authorization and error handling paths.

pub mod action;
pub mod adapter;
pub mod authorize;
pub mod error;
pub mod restrictions;