use std::{fmt, time::Duration};

use crate::{error::Result, PyroscopeError};

/// Error Wrapper for libc return. Only check for errors.
pub fn check_err<T: Ord + Default>(num: T) -> Result<T> {
    if num < T::default() {
        return Err(PyroscopeError::from(std::io::Error::last_os_error()));
    }
    Ok(num)
}

#[cfg(test)]
mod check_err_tests {
    use crate::utils::check_err;

    #[test]
    fn check_err_success() {
        assert_eq!(check_err(1).unwrap(), 1)
    }

    #[test]
    fn check_err_error() {
        assert!(check_err(-1).is_err())
    }
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct ThreadId {
    pthread: libc::pthread_t,
}

// SAFETY: pthread_t is an opaque thread identifier used as a handle,
// never dereferenced. On musl it's *mut c_void, on glibc it's c_ulong.
unsafe impl Send for ThreadId {}
unsafe impl Sync for ThreadId {}

impl From<libc::pthread_t> for ThreadId {
    fn from(value: libc::pthread_t) -> Self {
        Self { pthread: value }
    }
}
impl ThreadId {
    pub fn pthread_self() -> Self {
        Self {
            pthread: unsafe { libc::pthread_self() },
        }
    }
}

impl fmt::Display for ThreadId {
    #[cfg(target_env = "musl")]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", { self.pthread as libc::uintptr_t })
    }
    #[cfg(not(target_env = "musl"))]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", { self.pthread })
    }
}

/// Return the current time in seconds.
pub fn get_current_time_secs() -> Result<u64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs())
}

#[cfg(test)]
mod get_current_time_secs_tests {
    use crate::utils::get_current_time_secs;

    #[test]
    fn get_current_time_secs_success() {
        assert!(get_current_time_secs().is_ok())
    }
}

/// A representation of a time range. The time range is represented by a start
/// time, an end time, a current time and remaining time in seconds. The
/// remaining time is the duration in seconds until the end time.
#[derive(Debug, PartialEq)]
pub struct TimeRange {
    pub from: u64,
    pub until: u64,
    pub current: u64,
    pub rem: u64,
}

/// Return a range of timestamps in the form [start, end).
/// The range is inclusive of start and exclusive of end.
///
/// `interval` is the bucket size: timestamps are aligned down to a whole-second
/// multiple of it and the returned window is one interval wide.
pub fn get_time_range(timestamp: u64, interval: Duration) -> Result<TimeRange> {
    // if timestamp is 0, then get the current time
    if timestamp == 0 {
        return get_time_range(get_current_time_secs()?, interval);
    }

    // Bucketing works in whole seconds; clamp to a 1s minimum to avoid a
    // divide-by-zero below.
    let interval = interval.as_secs().max(1);

    Ok(TimeRange {
        from: timestamp / interval * interval,
        until: timestamp / interval * interval + interval,
        current: timestamp,
        rem: interval - (timestamp % interval),
    })
}

#[cfg(test)]
mod get_time_range_tests {
    use crate::utils::{get_time_range, TimeRange};
    use std::time::Duration;

    #[test]
    fn get_time_range_verify() {
        assert_eq!(
            get_time_range(1644194479, Duration::from_secs(10)).unwrap(),
            TimeRange {
                from: 1644194470,
                until: 1644194480,
                current: 1644194479,
                rem: 1,
            }
        );
        assert_eq!(
            get_time_range(1644194470, Duration::from_secs(10)).unwrap(),
            TimeRange {
                from: 1644194470,
                until: 1644194480,
                current: 1644194470,
                rem: 10,
            }
        );
        assert_eq!(
            get_time_range(1644194476, Duration::from_secs(10)).unwrap(),
            TimeRange {
                from: 1644194470,
                until: 1644194480,
                current: 1644194476,
                rem: 4,
            }
        );
    }
}

pub(crate) fn rustc_version() -> Option<&'static str> {
    option_env!("PYROSCOPE__RUSTC_VERSION")
}

#[cfg(test)]
mod rustc_version_tests {

    #[test]
    fn get_time_range_verify() {
        let version = super::rustc_version().expect("build.rs provided COMPILER_VERSION");
        // println!("{}", version); // "rustc 1.96.0 (ac68faa20 2026-05-25)"
        let fields: Vec<_> = version.split(" ").collect();
        let compiler = fields[0];
        let version = fields[1];
        assert_eq!("rustc", compiler);
        assert_eq!(3, version.split(".").count());
    }
}
