#[cfg(feature = "gpu")]
use crate::GlobalError;
#[cfg(feature = "gpu")]
use crate::system::attributes::ProcessAttributes;
#[cfg(feature = "gpu")]
use crate::system::metrics::Metrics;
#[cfg(feature = "gpu")]
use nvml_wrapper::Nvml;
#[cfg(feature = "gpu")]
use nvml_wrapper::enums::device::UsedGpuMemory;
#[cfg(feature = "gpu")]
use sysinfo::Pid;
#[cfg(feature = "gpu")]
use tracing::warn;

/// `GpuCollector` is responsible for collecting GPU memory usage metrics.
#[cfg(feature = "gpu")]
pub struct GpuCollector {
    nvml: Nvml,
    pid: Pid,
}

#[cfg(feature = "gpu")]
impl GpuCollector {
    pub fn new(pid: Pid) -> Result<Self, GlobalError> {
        let nvml = Nvml::init().map_err(|e| GlobalError::GpuInitError(e.to_string()))?;
        Ok(GpuCollector { nvml, pid })
    }

    pub fn collect(&self, metrics: &Metrics, attributes: &ProcessAttributes) -> Result<(), GlobalError> {
        if let Ok(device) = self.nvml.device_by_index(0) {
            if let Ok(gpu_stats) = device.running_compute_processes() {
                for stat in gpu_stats.iter() {
                    if stat.pid == self.pid.as_u32() {
                        let memory_used = match stat.used_gpu_memory {
                            UsedGpuMemory::Used(bytes) => bytes,
                            UsedGpuMemory::Unavailable => 0,
                        };
                        metrics.gpu_memory_usage.record(memory_used, &attributes.attributes);
                        return Ok(());
                    }
                }
            } else {
                warn!("Could not get GPU stats, recording 0 for GPU memory usage");
            }
        } else {
            return Err(GlobalError::GpuDeviceError("No GPU device found".to_string()));
        }
        metrics.gpu_memory_usage.record(0, &attributes.attributes);
        Ok(())
    }
}

#[cfg(not(feature = "gpu"))]
pub struct GpuCollector;

#[cfg(not(feature = "gpu"))]
impl GpuCollector {
    pub fn new(_pid: sysinfo::Pid) -> Result<Self, crate::GlobalError> {
        Ok(GpuCollector)
    }

    pub fn collect(
        &self,
        _metrics: &crate::system::metrics::Metrics,
        _attributes: &crate::system::attributes::ProcessAttributes,
    ) -> Result<(), crate::GlobalError> {
        Ok(())
    }
}
