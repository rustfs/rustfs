//! # Metrics
//! This file is part of the RustFS project
//! Current metrics observed are:
//! - CPU
//! - Memory
//! - Disk
//! - Network
//!
//! # Getting started
//!
//! ```
//! use opentelemetry::global;
//! use rustfs_obs::init_system_metrics;
//!
//! #[tokio::main]
//! async fn main() {
//!     let meter = global::meter("rustfs-system-meter");
//!     let result = init_system_metrics(meter);
//! }
//! ```
//!

use crate::GlobalError;
#[cfg(feature = "gpu")]
use nvml_wrapper::enums::device::UsedGpuMemory;
#[cfg(feature = "gpu")]
use nvml_wrapper::Nvml;
use opentelemetry::metrics::Meter;
use opentelemetry::Key;
use opentelemetry::KeyValue;
use std::time::Duration;
use sysinfo::{get_current_pid, System};
use tokio::time::sleep;
use tracing::warn;

const PROCESS_PID: Key = Key::from_static_str("process.pid");
const PROCESS_EXECUTABLE_NAME: Key = Key::from_static_str("process.executable.name");
const PROCESS_EXECUTABLE_PATH: Key = Key::from_static_str("process.executable.path");
const PROCESS_COMMAND: Key = Key::from_static_str("process.command");
const PROCESS_CPU_USAGE: &str = "process.cpu.usage";
const PROCESS_CPU_UTILIZATION: &str = "process.cpu.utilization";
const PROCESS_MEMORY_USAGE: &str = "process.memory.usage";
const PROCESS_MEMORY_VIRTUAL: &str = "process.memory.virtual";
const PROCESS_DISK_IO: &str = "process.disk.io";
const DIRECTION: Key = Key::from_static_str("direction");
const PROCESS_GPU_MEMORY_USAGE: &str = "process.gpu.memory.usage";

// add static variables that delay initialize nvml
#[cfg(feature = "gpu")]
static NVML_INSTANCE: OnceCell<Arc<Mutex<Option<Result<Nvml, nvml_wrapper::error::NvmlError>>>>> = OnceCell::const_new();

// get or initialize an nvml instance
#[cfg(feature = "gpu")]
async fn get_or_init_nvml() -> &'static Arc<Mutex<Option<Result<Nvml, nvml_wrapper::error::NvmlError>>>> {
    NVML_INSTANCE
        .get_or_init(|| async { Arc::new(Mutex::new(Some(Nvml::init()))) })
        .await
}

/// Record asynchronously information about the current process.
/// This function is useful for monitoring the current process.
///
/// # Arguments
/// * `meter` - The OpenTelemetry meter to use.
///
/// # Returns
/// * `Ok(())` if successful
/// * `Err(GlobalError)` if an error occurs
///
/// # Example
/// ```
/// use opentelemetry::global;
/// use rustfs_obs::init_system_metrics;
///
/// #[tokio::main]
/// async fn main() {
///     let meter = global::meter("rustfs-system-meter");
///     let result = init_system_metrics(meter);
/// }
/// ```
pub async fn init_system_metrics(meter: Meter) -> Result<(), GlobalError> {
    let pid = get_current_pid().map_err(|err| GlobalError::MetricsError(err.to_string()))?;
    register_system_metrics(meter, pid).await
}

/// Record asynchronously information about a specific process by its PID.
/// This function is useful for monitoring processes other than the current one.
///
/// # Arguments
/// * `meter` - The OpenTelemetry meter to use.
/// * `pid` - The PID of the process to monitor.
///
/// # Returns
/// * `Ok(())` if successful
/// * `Err(GlobalError)` if an error occurs
///
/// # Example
/// ```
/// use opentelemetry::global;
/// use rustfs_obs::init_system_metrics_for_pid;
///
/// #[tokio::main]
/// async fn main() {
///     let meter = global::meter("rustfs-system-meter");
///     // replace with the actual PID
///     let pid = 1234;
///     let result = init_system_metrics_for_pid(meter, pid).await;
/// }
/// ```
///
pub async fn init_system_metrics_for_pid(meter: Meter, pid: u32) -> Result<(), GlobalError> {
    let pid = sysinfo::Pid::from_u32(pid);
    register_system_metrics(meter, pid).await
}

/// Register system metrics for the current process.
/// This function is useful for monitoring the current process.
///
/// # Arguments
/// * `meter` - The OpenTelemetry meter to use.
/// * `pid` - The PID of the process to monitor.
///
/// # Returns
/// * `Ok(())` if successful
/// * `Err(GlobalError)` if an error occurs
///
async fn register_system_metrics(meter: Meter, pid: sysinfo::Pid) -> Result<(), GlobalError> {
    // cache core counts to avoid repeated calculations
    let core_count = System::physical_core_count()
        .ok_or_else(|| GlobalError::SystemMetricsError("Could not get physical core count".to_string()))?;
    let core_count_f32 = core_count as f32;

    // create metric meter
    let (
        process_cpu_utilization,
        process_cpu_usage,
        process_memory_usage,
        process_memory_virtual,
        process_disk_io,
        process_gpu_memory_usage,
    ) = create_metrics(&meter);

    // initialize system object
    let mut sys = System::new_all();
    sys.refresh_all();

    // Prepare public properties to avoid repeated construction in loops
    let common_attributes = prepare_common_attributes(&sys, pid)?;

    // get the metric export interval
    let interval = get_export_interval();

    // Use asynchronous tasks to process CPU, memory, and disk metrics to avoid blocking the main asynchronous tasks
    let cpu_mem_task = tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(interval)).await;
            if let Err(e) = update_process_metrics(
                &mut sys,
                pid,
                &process_cpu_usage,
                &process_cpu_utilization,
                &process_memory_usage,
                &process_memory_virtual,
                &process_disk_io,
                &common_attributes,
                core_count_f32,
            ) {
                warn!("Failed to update process metrics: {}", e);
            }
        }
    });

    // Use another asynchronous task to handle GPU metrics
    #[cfg(feature = "gpu")]
    let gpu_task = tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(interval)).await;

            // delayed initialization nvml
            let nvml_arc = get_or_init_nvml().await;
            let nvml_option = nvml_arc.lock().unwrap();

            if let Err(e) = update_gpu_metrics(&nvml, pid, &process_gpu_memory_usage, &common_attributes) {
                warn!("Failed to update GPU metrics: {}", e);
            }
        }
    });

    // record empty values when non gpu function
    #[cfg(not(feature = "gpu"))]
    let gpu_task = tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(interval)).await;
            process_gpu_memory_usage.record(0, &common_attributes);
        }
    });

    // Wait for the two tasks to complete (actually they will run forever)
    let _ = tokio::join!(cpu_mem_task, gpu_task);

    Ok(())
}

fn create_metrics(meter: &Meter) -> (F64Gauge, F64Gauge, I64Gauge, I64Gauge, I64Gauge, U64Gauge) {
    let process_cpu_utilization = meter
        .f64_gauge(PROCESS_CPU_USAGE)
        .with_description("The percentage of CPU in use.")
        .with_unit("percent")
        .build();

    let process_cpu_usage = meter
        .f64_gauge(PROCESS_CPU_UTILIZATION)
        .with_description("The amount of CPU in use.")
        .with_unit("percent")
        .build();

    let process_memory_usage = meter
        .i64_gauge(PROCESS_MEMORY_USAGE)
        .with_description("The amount of physical memory in use.")
        .with_unit("byte")
        .build();

    let process_memory_virtual = meter
        .i64_gauge(PROCESS_MEMORY_VIRTUAL)
        .with_description("The amount of committed virtual memory.")
        .with_unit("byte")
        .build();

    let process_disk_io = meter
        .i64_gauge(PROCESS_DISK_IO)
        .with_description("Disk bytes transferred.")
        .with_unit("byte")
        .build();

    let process_gpu_memory_usage = meter
        .u64_gauge(PROCESS_GPU_MEMORY_USAGE)
        .with_description("The amount of physical GPU memory in use.")
        .with_unit("byte")
        .build();

    (
        process_cpu_utilization,
        process_cpu_usage,
        process_memory_usage,
        process_memory_virtual,
        process_disk_io,
        process_gpu_memory_usage,
    )
}

fn prepare_common_attributes(sys: &System, pid: sysinfo::Pid) -> Result<[KeyValue; 4], GlobalError> {
    let process = sys
        .process(pid)
        .ok_or_else(|| GlobalError::SystemMetricsError(format!("Process with PID {} not found", pid.as_u32())))?;

    // optimize string operations and reduce allocation
    let cmd = process.cmd().iter().filter_map(|s| s.to_str()).collect::<Vec<_>>().join(" ");

    let executable_path = process
        .exe()
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_default();

    let name = process.name().to_os_string().into_string().unwrap_or_default();

    Ok([
        KeyValue::new(PROCESS_PID, pid.as_u32() as i64),
        KeyValue::new(PROCESS_EXECUTABLE_NAME, name),
        KeyValue::new(PROCESS_EXECUTABLE_PATH, executable_path),
        KeyValue::new(PROCESS_COMMAND, cmd),
    ])
}

fn get_export_interval() -> u64 {
    std::env::var("OTEL_METRIC_EXPORT_INTERVAL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30000)
}

fn update_process_metrics(
    sys: &mut System,
    pid: sysinfo::Pid,
    process_cpu_usage: &F64Gauge,
    process_cpu_utilization: &F64Gauge,
    process_memory_usage: &I64Gauge,
    process_memory_virtual: &I64Gauge,
    process_disk_io: &I64Gauge,
    common_attributes: &[KeyValue; 4],
    core_count: f32,
) -> Result<(), GlobalError> {
    // Only refresh the data of the required process to reduce system call overhead
    sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);

    let process = match sys.process(pid) {
        Some(p) => p,
        None => {
            return Err(GlobalError::SystemMetricsError(format!(
                "Process with PID {} no longer exists",
                pid.as_u32()
            )))
        }
    };

    // collect data in batches and record it again
    let cpu_usage = process.cpu_usage();
    process_cpu_usage.record(cpu_usage.into(), &[]);
    process_cpu_utilization.record((cpu_usage / core_count).into(), &common_attributes);

    // safe type conversion
    let memory = process.memory();
    let virtual_memory = process.virtual_memory();

    // Avoid multiple error checks and use .map_err to handle errors in chain
    let memory_i64 =
        i64::try_from(memory).map_err(|_| GlobalError::ConversionError("Failed to convert memory usage to i64".to_string()))?;

    let virtual_memory_i64 = i64::try_from(virtual_memory)
        .map_err(|_| GlobalError::ConversionError("Failed to convert virtual memory to i64".to_string()))?;

    process_memory_usage.record(memory_i64, common_attributes);
    process_memory_virtual.record(virtual_memory_i64, common_attributes);

    // process disk io metrics
    let disk_io = process.disk_usage();

    // batch conversion to reduce duplicate code
    let read_bytes_i64 = i64::try_from(disk_io.read_bytes)
        .map_err(|_| GlobalError::ConversionError("Failed to convert read bytes to i64".to_string()))?;

    let written_bytes_i64 = i64::try_from(disk_io.written_bytes)
        .map_err(|_| GlobalError::ConversionError("Failed to convert written bytes to i64".to_string()))?;

    // Optimize attribute array stitching to reduce heap allocation
    let mut read_attributes = [KeyValue::new(DIRECTION, "read")];
    let read_attrs = [common_attributes, &read_attributes].concat();

    let mut write_attributes = [KeyValue::new(DIRECTION, "write")];
    let write_attrs = [common_attributes, &write_attributes].concat();

    process_disk_io.record(read_bytes_i64, &read_attrs);
    process_disk_io.record(written_bytes_i64, &write_attrs);

    Ok(())
}

// GPU metric update function, conditional compilation based on feature flags
#[cfg(feature = "gpu")]
fn update_gpu_metrics(
    nvml: &Result<Nvml, nvml_wrapper::error::NvmlError>,
    pid: sysinfo::Pid,
    process_gpu_memory_usage: &U64Gauge,
    common_attributes: &[KeyValue; 4],
) -> Result<(), GlobalError> {
    match nvml {
        Ok(nvml) => {
            if let Ok(device) = nvml.device_by_index(0) {
                if let Ok(gpu_stats) = device.running_compute_processes() {
                    for stat in gpu_stats.iter() {
                        if stat.pid == pid.as_u32() {
                            let memory_used = match stat.used_gpu_memory {
                                UsedGpuMemory::Used(bytes) => bytes,
                                UsedGpuMemory::Unavailable => 0,
                            };

                            process_gpu_memory_usage.record(memory_used, common_attributes);
                            return Ok(());
                        }
                    }
                }
            }
            // If no GPU usage record of the process is found, the record is 0
            process_gpu_memory_usage.record(0, common_attributes);
            Ok(())
        }
        Err(e) => {
            warn!("Could not get NVML, recording 0 for GPU memory usage: {}", e);
            process_gpu_memory_usage.record(0, common_attributes);
            Ok(())
        }
    }
}

#[cfg(not(feature = "gpu"))]
fn update_gpu_metrics(
    _: &(), // blank placeholder parameters
    _: sysinfo::Pid,
    process_gpu_memory_usage: &U64Gauge,
    common_attributes: &[KeyValue; 4],
) -> Result<(), GlobalError> {
    // always logged when non gpu function 0
    process_gpu_memory_usage.record(0, common_attributes);
    Ok(())
}
