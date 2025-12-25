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

//! Session management for protocol implementations
//!
//! This module provides session management for all protocol implementations.
//! It ensures that all protocols use the same session management and context.
//!
//! MINIO CONSTRAINT: Session management MUST be protocol-agnostic and
//! MUST NOT bypass S3 policy enforcement.

pub mod context;
pub mod principal;