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

use std::collections::HashMap;
use time::OffsetDateTime;

pub fn resolve_aws_variables(pattern: &str, aws_variables: &HashMap<String, String>) -> String {
    let mut result = pattern.to_string();

    for (key, value) in aws_variables {
        let var_placeholder = format!("${{{}}}", key);
        result = result.replace(&var_placeholder, value);
    }

    result
}

pub fn create_aws_variables_map(username: &str, userid: &str, principal_type: &str) -> HashMap<String, String> {
    let mut vars = HashMap::new();

    vars.insert("aws:username".to_string(), username.to_string());
    vars.insert("aws:userid".to_string(), userid.to_string());
    vars.insert("aws:PrincipalType".to_string(), principal_type.to_string());
    vars.insert("aws:SecureTransport".to_string(), "false".to_string());

    let now = OffsetDateTime::now_utc();
    vars.insert("aws:CurrentTime".to_string(),
                now.format(&time::format_description::well_known::Rfc3339).unwrap_or_else(|_| now.to_string()));
    vars.insert("aws:EpochTime".to_string(), now.unix_timestamp().to_string());

    vars
}