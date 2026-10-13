/// A signal sent to the timer.
///
/// Either schedules another wake-up, or asks
/// the timer thread to terminate.
#[derive(Debug, Clone, Copy)]
pub enum TimerSignal {
    // Thread termination was requested.
    Terminate,
    // When to take the next snapshot using the `Backend`.
    NextSnapshot(u64),
}

impl std::fmt::Display for TimerSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Terminate => write!(f, "Terminate"),
            Self::NextSnapshot(when) => write!(f, "NextSnapshot({when})"),
        }
    }
}

// Possibly: ios, netbsd, openbsd, freebsd
#[cfg(target_os = "macos")]
pub mod kqueue;

#[cfg(target_os = "macos")]
pub use kqueue::Timer;

// Possibly: android
#[cfg(target_os = "linux")]
pub mod epoll;
#[cfg(target_os = "linux")]
pub use epoll::Timer;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub mod sleep;
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub use sleep::Timer;
