use tracing::{debug, error, info};

/// Log an info message
///
/// # Arguments
///     msg: &str - The message to log
///     
/// # Example
/// ```
/// use rustfs_obs::log_info;
///
/// log_info("This is an info message");
/// ```
pub fn log_info(msg: &str) {
    info!("{}", msg);
}

/// Log an error message
///
/// # Arguments
///    msg: &str - The message to log
///
/// # Example
/// ```
/// use rustfs_obs::log_error;
///
/// log_error("This is an error message");
/// ```
pub fn log_error(msg: &str) {
    error!("{}", msg);
}

/// Log a debug message
///
/// # Arguments
///   msg: &str - The message to log
///
/// # Example
/// ```
/// use rustfs_obs::log_debug;
///
/// log_debug("This is a debug message");
/// ```
pub fn log_debug(msg: &str) {
    debug!("{}", msg);
}
