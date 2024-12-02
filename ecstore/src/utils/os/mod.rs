#[cfg(target_os = "linux")]
mod linux;
#[cfg(all(unix, not(target_os = "linux")))]
mod unix;
#[cfg(target_os = "windows")]
mod windows;

#[cfg(target_os = "linux")]
pub use linux::{get_drive_stats, get_info, same_disk};
// pub use linux::same_disk;

#[cfg(all(unix, not(target_os = "linux")))]
pub use unix::{get_drive_stats, get_info, same_disk};
#[cfg(target_os = "windows")]
pub use windows::{get_drive_stats, get_info, same_disk};

#[derive(Debug, Default)]
pub struct IOStats {
    pub read_ios: u64,
    pub read_merges: u64,
    pub read_sectors: u64,
    pub read_ticks: u64,
    pub write_ios: u64,
    pub write_merges: u64,
    pub write_sectors: u64,
    pub write_ticks: u64,
    pub current_ios: u64,
    pub total_ticks: u64,
    pub req_ticks: u64,
    pub discard_ios: u64,
    pub discard_merges: u64,
    pub discard_sectors: u64,
    pub discard_ticks: u64,
    pub flush_ios: u64,
    pub flush_ticks: u64,
}
