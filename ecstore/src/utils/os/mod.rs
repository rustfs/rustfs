#[cfg(target_os = "linux")]
mod linux;
#[cfg(all(unix, not(target_os = "linux")))]
mod unix;
#[cfg(target_os = "windows")]
mod windows;

#[cfg(target_os = "linux")]
pub use linux::get_info;
pub use linux::same_disk;
#[cfg(all(unix, not(target_os = "linux")))]
pub use unix::get_info;
#[cfg(target_os = "windows")]
pub use windows::get_info;