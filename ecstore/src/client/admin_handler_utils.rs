use http::status::StatusCode;
use std::fmt::{self, Display, Formatter};

#[derive(Default, thiserror::Error, Debug, Clone, PartialEq)]
pub struct AdminError {
    pub code: String,
    pub message: String,
    pub status_code: StatusCode,
}

impl Display for AdminError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl AdminError {
    pub fn new(code: &str, message: &str, status_code: StatusCode) -> Self {
        Self {
            code: code.to_string(),
            message: message.to_string(),
            status_code,
        }
    }

    pub fn msg(message: &str) -> Self {
        Self {
            code: "InternalError".to_string(),
            message: message.to_string(),
            status_code: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
