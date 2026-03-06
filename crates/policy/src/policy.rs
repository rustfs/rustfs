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

pub mod action;
mod doc;
mod effect;
mod function;
mod id;
pub mod opa;
#[allow(clippy::module_inception)]
mod policy;
mod principal;
pub mod resource;
pub mod statement;
pub(crate) mod utils;
pub mod variables;

pub use action::ActionSet;
pub use doc::PolicyDoc;
pub use effect::Effect;
pub use function::Functions;
pub use id::ID;
pub use policy::*;
pub use principal::Principal;
pub use resource::ResourceSet;
pub use statement::Statement;

#[derive(thiserror::Error, Debug)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub enum Error {
    #[error("invalid Version '{0}'")]
    InvalidVersion(String),

    #[error("invalid Effect '{0}'")]
    InvalidEffect(String),

    #[error("both 'Action' and 'NotAction' are empty")]
    NonAction,

    #[error("'Action' and 'NotAction' cannot both be specified in the same statement")]
    BothActionAndNotAction,

    #[error("'Resource' is empty")]
    NonResource,

    #[error("'Resource' and 'NotResource' cannot both be specified in the same statement")]
    BothResourceAndNotResource,

    #[error("invalid key name: '{0}'")]
    InvalidKeyName(String),

    #[error("invalid key: '{0}'")]
    InvalidKey(String),

    #[error("invalid action: '{0}'")]
    InvalidAction(String),

    #[error("invalid resource, type: '{0}', pattern: '{1}'")]
    InvalidResource(String, String),
}
