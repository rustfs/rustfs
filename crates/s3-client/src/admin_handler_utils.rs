// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
