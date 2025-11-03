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

use crate::GlobalError;
use crate::system::attributes::ProcessAttributes;
use crate::system::metrics::{DIRECTION, INTERFACE, Metrics, STATUS};
use opentelemetry::KeyValue;
use std::time::SystemTime;
use sysinfo::{Networks, Pid, ProcessStatus, System};
use tokio::time::{Duration, sleep};

/// Collector is responsible for collecting system metrics and attributes.
/// It uses the sysinfo crate to gather information about the system and processes.
/// It also uses OpenTelemetry to record metrics.
pub struct Collector {
    metrics: Metrics,
    attributes: ProcessAttributes,
    #[cfg(feature = "gpu")]
    gpu_collector: crate::system::gpu::GpuCollector,
    pid: Pid,
    system: System,
    networks: Networks,
    core_count: usize,
    interval_ms: u64,
}

impl Collector {
    pub fn new(pid: Pid, meter: opentelemetry::metrics::Meter, interval_ms: u64) -> Result<Self, GlobalError> {
        let mut system = System::new();
        let attributes = ProcessAttributes::new(pid, &mut system)?;
        let core_count = System::physical_core_count().ok_or(GlobalError::CoreCountError)?;
        let metrics = Metrics::new(&meter);
        #[cfg(feature = "gpu")]
        let gpu_collector = crate::system::gpu::GpuCollector::new(pid)?;
        let networks = Networks::new_with_refreshed_list();

        Ok(Collector {
            metrics,
            attributes,
            #[cfg(feature = "gpu")]
            gpu_collector,
            pid,
            system,
            networks,
            core_count,
            interval_ms,
        })
    }

    pub async fn run(&mut self) -> Result<(), GlobalError> {
        loop {
            self.collect()?;
            tracing::debug!("Collected metrics for PID: {} ,time: {:?}", self.pid, SystemTime::now());
            sleep(Duration::from_millis(self.interval_ms)).await;
        }
    }

    fn collect(&mut self) -> Result<(), GlobalError> {
        self.system
            .refresh_processes(sysinfo::ProcessesToUpdate::Some(&[self.pid]), false);

        // refresh the network interface list and statistics
        self.networks.refresh(false);

        let process = self
            .system
            .process(self.pid)
            .ok_or_else(|| GlobalError::ProcessNotFound(self.pid.as_u32()))?;

        // CPU metrics
        let cpu_usage = process.cpu_usage();
        self.metrics.cpu_usage.record(cpu_usage as f64, &[]);
        self.metrics
            .cpu_utilization
            .record((cpu_usage / self.core_count as f32) as f64, &self.attributes.attributes);

        // Memory metrics
        self.metrics
            .memory_usage
            .record(process.memory() as i64, &self.attributes.attributes);
        self.metrics
            .memory_virtual
            .record(process.virtual_memory() as i64, &self.attributes.attributes);

        // Disk I/O metrics
        let disk_io = process.disk_usage();
        self.metrics.disk_io.record(
            disk_io.read_bytes as i64,
            &[&self.attributes.attributes[..], &[KeyValue::new(DIRECTION, "read")]].concat(),
        );
        self.metrics.disk_io.record(
            disk_io.written_bytes as i64,
            &[&self.attributes.attributes[..], &[KeyValue::new(DIRECTION, "write")]].concat(),
        );

        // Network I/O indicators (corresponding to /system/network/internode)
        let mut total_received: i64 = 0;
        let mut total_transmitted: i64 = 0;

        // statistics by interface
        for (interface_name, data) in self.networks.iter() {
            total_received += data.total_received() as i64;
            total_transmitted += data.total_transmitted() as i64;

            let received = data.received() as i64;
            let transmitted = data.transmitted() as i64;
            self.metrics.network_io_per_interface.record(
                received,
                &[
                    &self.attributes.attributes[..],
                    &[
                        KeyValue::new(INTERFACE, interface_name.to_string()),
                        KeyValue::new(DIRECTION, "received"),
                    ],
                ]
                .concat(),
            );
            self.metrics.network_io_per_interface.record(
                transmitted,
                &[
                    &self.attributes.attributes[..],
                    &[
                        KeyValue::new(INTERFACE, interface_name.to_string()),
                        KeyValue::new(DIRECTION, "transmitted"),
                    ],
                ]
                .concat(),
            );
        }
        // global statistics
        self.metrics.network_io.record(
            total_received,
            &[&self.attributes.attributes[..], &[KeyValue::new(DIRECTION, "received")]].concat(),
        );
        self.metrics.network_io.record(
            total_transmitted,
            &[&self.attributes.attributes[..], &[KeyValue::new(DIRECTION, "transmitted")]].concat(),
        );

        // Process status indicator (corresponding to /system/process)
        let status_value = match process.status() {
            ProcessStatus::Run => 0,
            ProcessStatus::Sleep => 1,
            ProcessStatus::Zombie => 2,
            _ => 3, // other status
        };
        self.metrics.process_status.record(
            status_value,
            &[
                &self.attributes.attributes[..],
                &[KeyValue::new(STATUS, format!("{:?}", process.status()))],
            ]
            .concat(),
        );

        // GPU Metrics (Optional) Non-MacOS
        #[cfg(feature = "gpu")]
        self.gpu_collector.collect(&self.metrics, &self.attributes)?;

        Ok(())
    }
}
