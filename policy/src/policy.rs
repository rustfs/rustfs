pub mod action;
mod doc;
mod effect;
mod function;
mod id;
#[allow(clippy::module_inception)]
mod policy;
mod principal;
pub mod resource;
pub mod statement;
pub(crate) mod utils;

pub use action::ActionSet;
pub use doc::PolicyDoc;

pub use effect::Effect;
pub use function::Functions;
pub use id::ID;
pub use policy::*;
pub use principal::Principal;
pub use resource::ResourceSet;
pub use statement::Statement;

pub const EMBEDDED_POLICY_TYPE: &str = "embedded-policy";
pub const INHERITED_POLICY_TYPE: &str = "inherited-policy";

#[derive(thiserror::Error, Debug)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub enum Error {
    #[error("invalid Version '{0}'")]
    InvalidVersion(String),

    #[error("invalid Effect '{0}'")]
    InvalidEffect(String),

    #[error("both 'Action' and 'NotAction' are empty")]
    NonAction,

    #[error("'Resource' is empty")]
    NonResource,

    #[error("invalid key name: '{0}'")]
    InvalidKeyName(String),

    #[error("invalid key: '{0}'")]
    InvalidKey(String),

    #[error("invalid action: '{0}'")]
    InvalidAction(String),

    #[error("invalid resource, type: '{0}', pattern: '{1}'")]
    InvalidResource(String, String),
}
