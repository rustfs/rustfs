use crate::config;
use common::error::{Error, Result};
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tracing::error;
use tracing::info;

pub fn license_check() -> Result<()> {
    let inval_license = config::get_config().license.as_ref().map(|license| {
        if license.is_empty() {
            error!("License is empty");
            return Err(Error::from_string("Incorrect license, please contact RustFS.".to_string()));
        }
        let token = appauth::token::parse_license(license)?;
        if token.expired < SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() {
            error!("License expired");
            return Err(Error::from_string("Incorrect license, please contact RustFS.".to_string()));
        }

        info!("License is valid ! expired at {}", token.expired);
        Ok(())
    });

    if inval_license.is_none() || inval_license.is_some_and(|v| v.is_err()) {
        return Err(Error::from_string("Incorrect license, please contact RustFS.".to_string()));
    }

    Ok(())
}
