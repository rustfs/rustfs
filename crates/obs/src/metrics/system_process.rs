/// process related metric descriptors
use crate::metrics::{new_counter_md, new_gauge_md, subsystems, MetricDescriptor, MetricName};


lazy_static::lazy_static! {
    pub static ref PROCESS_LOCKS_READ_TOTAL_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ProcessLocksReadTotal,
            "Number of current READ locks on this peer",
            &[],
            subsystems::SYSTEM_PROCESS
        );

    pub static ref PROCESS_LOCKS_WRITE_TOTAL_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ProcessLocksWriteTotal,
            "Number of current WRITE locks on this peer",
            &[],
            subsystems::SYSTEM_PROCESS
        );

    pub static ref PROCESS_CPU_TOTAL_SECONDS_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ProcessCPUTotalSeconds,
            "Total user and system CPU time spent in seconds",
            &[],
            subsystems::SYSTEM_PROCESS
        );

    pub static ref PROCESS_GO_ROUTINE_TOTAL_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ProcessGoRoutineTotal,
            "Total number of go routines running",
            &[],
            subsystems::SYSTEM_PROCESS
        );

    pub static ref PROCESS_IO_RCHAR_BYTES_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ProcessIORCharBytes,
            "Total bytes read by the process from the underlying storage system including cache, /proc/[pid]/io rchar",
            &[],
            subsystems::SYSTEM_PROCESS
        );

    pub static ref PROCESS_IO_READ_BYTES_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ProcessIOReadBytes,
            "Total bytes read by the process from the underlying storage system, /proc/[pid]/io read_bytes",
            &[],
            subsystems::SYSTEM_PROCESS
        );

    pub static ref PROCESS_IO_WCHAR_BYTES_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ProcessIOWCharBytes,
            "Total bytes written by the process to the underlying storage system including page cache, /proc/[pid]/io wchar",
            &[],
            subsystems::SYSTEM_PROCESS
        );

    pub static ref PROCESS_IO_WRITE_BYTES_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ProcessIOWriteBytes,
            "Total bytes written by the process to the underlying storage system, /proc/[pid]/io write_bytes",
            &[],
            subsystems::SYSTEM_PROCESS
        );

    pub static ref PROCESS_START_TIME_SECONDS_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ProcessStartTimeSeconds,
            "Start time for RustFS process in seconds since Unix epoc",
            &[],
            subsystems::SYSTEM_PROCESS
        );

    pub static ref PROCESS_UPTIME_SECONDS_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ProcessUptimeSeconds,
            "Uptime for RustFS process in seconds",
            &[],
            subsystems::SYSTEM_PROCESS
        );

    pub static ref PROCESS_FILE_DESCRIPTOR_LIMIT_TOTAL_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ProcessFileDescriptorLimitTotal,
            "Limit on total number of open file descriptors for the RustFS Server process",
            &[],
            subsystems::SYSTEM_PROCESS
        );

    pub static ref PROCESS_FILE_DESCRIPTOR_OPEN_TOTAL_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ProcessFileDescriptorOpenTotal,
            "Total number of open file descriptors by the RustFS Server process",
            &[],
            subsystems::SYSTEM_PROCESS
        );

    pub static ref PROCESS_SYSCALL_READ_TOTAL_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ProcessSyscallReadTotal,
            "Total read SysCalls to the kernel. /proc/[pid]/io syscr",
            &[],
            subsystems::SYSTEM_PROCESS
        );

    pub static ref PROCESS_SYSCALL_WRITE_TOTAL_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ProcessSyscallWriteTotal,
            "Total write SysCalls to the kernel. /proc/[pid]/io syscw",
            &[],
            subsystems::SYSTEM_PROCESS
        );

    pub static ref PROCESS_RESIDENT_MEMORY_BYTES_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ProcessResidentMemoryBytes,
            "Resident memory size in bytes",
            &[],
            subsystems::SYSTEM_PROCESS
        );

    pub static ref PROCESS_VIRTUAL_MEMORY_BYTES_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ProcessVirtualMemoryBytes,
            "Virtual memory size in bytes",
            &[],
            subsystems::SYSTEM_PROCESS
        );

    pub static ref PROCESS_VIRTUAL_MEMORY_MAX_BYTES_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ProcessVirtualMemoryMaxBytes,
            "Maximum virtual memory size in bytes",
            &[],
            subsystems::SYSTEM_PROCESS
        );
}
