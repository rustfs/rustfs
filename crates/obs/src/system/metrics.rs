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

pub const PROCESS_CPU_USAGE: &str = "process.cpu.usage";
pub const PROCESS_CPU_UTILIZATION: &str = "process.cpu.utilization";
pub const PROCESS_MEMORY_USAGE: &str = "process.memory.usage";
pub const PROCESS_MEMORY_VIRTUAL: &str = "process.memory.virtual";
pub const PROCESS_DISK_IO: &str = "process.disk.io";
pub const PROCESS_NETWORK_IO: &str = "process.network.io";
pub const PROCESS_NETWORK_IO_PER_INTERFACE: &str = "process.network.io.per_interface";
pub const PROCESS_STATUS: &str = "process.status";
#[cfg(feature = "gpu")]
pub const PROCESS_GPU_MEMORY_USAGE: &str = "process.gpu.memory.usage";
pub const DIRECTION: opentelemetry::Key = opentelemetry::Key::from_static_str("direction");
pub const STATUS: opentelemetry::Key = opentelemetry::Key::from_static_str("status");
pub const INTERFACE: opentelemetry::Key = opentelemetry::Key::from_static_str("interface");

/// `Metrics` struct holds the OpenTelemetry metrics for process monitoring.
/// It contains various metrics such as CPU usage, memory usage,
/// disk I/O, network I/O, and process status.
///
/// The `Metrics` struct is designed to be used with OpenTelemetry's
/// metrics API to record and export these metrics.
///
/// The `new` method initializes the metrics using the provided
/// `opentelemetry::metrics::Meter`.
pub struct Metrics {
    pub cpu_usage: opentelemetry::metrics::Gauge<f64>,
    pub cpu_utilization: opentelemetry::metrics::Gauge<f64>,
    pub memory_usage: opentelemetry::metrics::Gauge<i64>,
    pub memory_virtual: opentelemetry::metrics::Gauge<i64>,
    pub disk_io: opentelemetry::metrics::Gauge<i64>,
    pub network_io: opentelemetry::metrics::Gauge<i64>,
    pub network_io_per_interface: opentelemetry::metrics::Gauge<i64>,
    pub process_status: opentelemetry::metrics::Gauge<i64>,
    #[cfg(feature = "gpu")]
    pub gpu_memory_usage: opentelemetry::metrics::Gauge<u64>,
}

impl Metrics {
    pub fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        let cpu_usage = meter
            .f64_gauge(PROCESS_CPU_USAGE)
            .with_description("The percentage of CPU in use.")
            .with_unit("percent")
            .build();
        let cpu_utilization = meter
            .f64_gauge(PROCESS_CPU_UTILIZATION)
            .with_description("The amount of CPU in use.")
            .with_unit("percent")
            .build();
        let memory_usage = meter
            .i64_gauge(PROCESS_MEMORY_USAGE)
            .with_description("The amount of physical memory in use.")
            .with_unit("byte")
            .build();
        let memory_virtual = meter
            .i64_gauge(PROCESS_MEMORY_VIRTUAL)
            .with_description("The amount of committed virtual memory.")
            .with_unit("byte")
            .build();
        let disk_io = meter
            .i64_gauge(PROCESS_DISK_IO)
            .with_description("Disk bytes transferred.")
            .with_unit("byte")
            .build();
        let network_io = meter
            .i64_gauge(PROCESS_NETWORK_IO)
            .with_description("Network bytes transferred.")
            .with_unit("byte")
            .build();
        let network_io_per_interface = meter
            .i64_gauge(PROCESS_NETWORK_IO_PER_INTERFACE)
            .with_description("Network bytes transferred (per interface).")
            .with_unit("byte")
            .build();

        let process_status = meter
            .i64_gauge(PROCESS_STATUS)
            .with_description("Process status (0: Running, 1: Sleeping, 2: Zombie, etc.)")
            .build();

        #[cfg(feature = "gpu")]
        let gpu_memory_usage = meter
            .u64_gauge(PROCESS_GPU_MEMORY_USAGE)
            .with_description("The amount of physical GPU memory in use.")
            .with_unit("byte")
            .build();

        Metrics {
            cpu_usage,
            cpu_utilization,
            memory_usage,
            memory_virtual,
            disk_io,
            network_io,
            network_io_per_interface,
            process_status,
            #[cfg(feature = "gpu")]
            gpu_memory_usage,
        }
    }
}
