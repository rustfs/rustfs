//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//! RustFS Audit System
//!
//! This crate provides a comprehensive audit logging system with multi-target fan-out capabilities,
//! configuration management, and hot reload functionality. It is modeled after the notify system
//! but specifically designed for audit logging requirements.

pub mod entity;
pub mod error;
pub mod factory;
pub mod global;
pub mod observability;
pub mod registry;
pub mod system;

pub use entity::{ApiDetails, AuditEntry, ObjectVersion};
pub use error::{AuditError, AuditResult};
pub use global::*;
pub use observability::{AuditMetrics, AuditMetricsReport, PerformanceValidation};
pub use registry::AuditRegistry;
pub use system::AuditSystem;
