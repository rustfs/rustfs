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

use crate::{GlobalError, is_observability_enabled};
use opentelemetry::global::meter;

mod attributes;
mod collector;
#[cfg(feature = "gpu")]
mod gpu;
mod metrics;

pub struct SystemObserver {}

impl SystemObserver {
    /// Initialize the indicator collector for the current process
    /// This function will create a new `Collector` instance and start collecting metrics.
    /// It will run indefinitely until the process is terminated.
    pub async fn init_process_observer() -> Result<(), GlobalError> {
        if is_observability_enabled() {
            let meter = meter("system");
            return SystemObserver::init_process_observer_for_pid(meter, 30000).await;
        }
        Ok(())
    }

    /// Initialize the metric collector for the specified PID process
    /// This function will create a new `Collector` instance and start collecting metrics.
    /// It will run indefinitely until the process is terminated.
    pub async fn init_process_observer_for_pid(meter: opentelemetry::metrics::Meter, pid: u32) -> Result<(), GlobalError> {
        let pid = sysinfo::Pid::from_u32(pid);
        let mut collector = collector::Collector::new(pid, meter, 30000)?;
        collector.run().await
    }
}
