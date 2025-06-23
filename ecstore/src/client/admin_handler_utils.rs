use http::status::StatusCode;
use std::fmt::{self, Display, Formatter};

#[derive(Default, thiserror::Error, Debug, PartialEq)]
pub struct AdminError {
    pub code: &'static str,
    pub message: &'static str,
    pub status_code: StatusCode,
}

impl Display for AdminError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl AdminError {
    pub fn new(code: &'static str, message: &'static str, status_code: StatusCode) -> Self {
        Self {
            code,
            message,
            status_code,
        }
    }

    pub fn msg(message: &'static str) -> Self {
        Self {
            code: "InternalError",
            message,
            status_code: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
