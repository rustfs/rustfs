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

pub mod audit;
pub mod bucket;
pub mod bucket_replication;
pub mod cluster_config;
pub mod cluster_erasure_set;
pub mod cluster_health;
pub mod cluster_iam;
pub mod cluster_notification;
pub mod cluster_usage;
pub mod entry;
pub mod ilm;
pub mod logger_webhook;
pub mod replication;
pub mod request;
pub mod scanner;
pub mod system_cpu;
pub mod system_drive;
pub mod system_memory;
pub mod system_network;
pub mod system_process;

pub use entry::descriptor::MetricDescriptor;
pub use entry::metric_name::MetricName;
pub use entry::metric_type::MetricType;
pub use entry::namespace::MetricNamespace;
pub use entry::subsystem::MetricSubsystem;
pub use entry::subsystem::subsystems;
pub use entry::{new_counter_md, new_gauge_md, new_histogram_md};
