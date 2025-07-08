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

//! Core coordination and lifecycle management for the AHM system

pub mod coordinator;
pub mod scheduler;
pub mod lifecycle;

pub use coordinator::{Coordinator, CoordinatorConfig};
pub use scheduler::{Scheduler, SchedulerConfig, Task, TaskPriority};
pub use lifecycle::{LifecycleManager, LifecycleConfig};

/// Status of the core coordination system
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    /// System is initializing
    Initializing,
    /// System is running normally
    Running,
    /// System is degraded but operational
    Degraded,
    /// System is shutting down
    Stopping,
    /// System has stopped
    Stopped,
    /// System encountered an error
    Error(String),
} 