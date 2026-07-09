#![allow(unused_imports)]
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
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default)]
pub struct TierCreds {
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "secretKey")]
    pub secret_key: String,

    #[serde(rename = "awsRole")]
    pub aws_role: bool,
    #[serde(rename = "awsRoleWebIdentityTokenFile")]
    pub aws_role_web_identity_token_file: String,
    #[serde(rename = "awsRoleArn")]
    pub aws_role_arn: String,

    //azsp: ServicePrincipalAuth,

    //#[serde(rename = "credsJson")]
    pub creds_json: Vec<u8>,
}
