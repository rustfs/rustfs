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

use jsonwebtoken::{Algorithm, DecodingKey, TokenData, Validation};

use crate::Error;
use crate::jwt::Claims;

pub fn decode(token: &str, token_secret: &[u8]) -> Result<TokenData<Claims>, Error> {
    Ok(jsonwebtoken::decode(
        token,
        &DecodingKey::from_secret(token_secret),
        &Validation::new(Algorithm::HS512),
    )?)
}
