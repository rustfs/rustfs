use crate::GlobalError;

pub(crate) mod attributes;
mod collector;
pub(crate) mod gpu;
pub(crate) mod metrics;

pub struct SystemObserver {}

impl SystemObserver {
    /// Initialize the indicator collector for the current process
    /// This function will create a new `Collector` instance and start collecting metrics.
    /// It will run indefinitely until the process is terminated.
    pub async fn init_process_observer(meter: opentelemetry::metrics::Meter) -> Result<(), GlobalError> {
        let pid = sysinfo::get_current_pid().map_err(|e| GlobalError::PidError(e.to_string()))?;
        let mut collector = collector::Collector::new(pid, meter, 30000)?;
        collector.run().await
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
