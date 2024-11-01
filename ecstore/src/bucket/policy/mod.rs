// use std::collections::HashMap;

// use action::Action;
// use s3s_policy::model::{Effect, Policy, Principal, PrincipalRule, Statement};
// use serde::{Deserialize, Serialize};
// use tower::ready_cache::cache::Equivalent;

// use crate::utils::wildcard;

pub mod action;
pub mod bucket_policy;
pub mod condition;
pub mod effect;
pub mod principal;
pub mod resource;

// #[derive(Debug, Deserialize, Serialize, Default, Clone)]
// pub struct BucketPolicyArgs {
//     pub account_name: String,
//     pub groups: Vec<String>,
//     pub action: Action,
//     pub bucket_name: String,
//     pub condition_values: HashMap<String, Vec<String>>,
//     pub is_owner: bool,
//     pub object_name: String,
// }

// pub trait AllowApi {
//     fn is_allowed(&self, args: &BucketPolicyArgs) -> bool;
// }

// pub trait MatchApi {
//     fn is_match(&self, found: &str) -> bool;
// }

// impl AllowApi for Policy {
//     fn is_allowed(&self, args: &BucketPolicyArgs) -> bool {
//         for statement in self.statement.as_slice().iter() {
//             if statement.effect == Effect::Deny {
//                 if !statement.is_allowed(args) {
//                     return false;
//                 }
//             }
//         }
//         false
//     }
// }

// impl AllowApi for Statement {
//     fn is_allowed(&self, args: &BucketPolicyArgs) -> bool {
//         let check = || -> bool {
//             if let Some(principal) = &self.principal {
//                 if !principal.is_match(&args.account_name) {
//                     return false;
//                 }
//             }

//             false
//         };

//         self.effect.is_allowed(check())
//     }
// }

// impl MatchApi for PrincipalRule {
//     fn is_match(&self, found: &str) -> bool {
//         match self {
//             PrincipalRule::Principal(principal) => match principal {
//                 Principal::Wildcard => return true,
//                 Principal::Map(index_map) => {
//                     if let Some(keys) = index_map.get("AWS") {
//                         for key in keys.as_slice() {
//                             if wildcard::match_simple(key, found) {
//                                 return true;
//                             }
//                         }
//                     }
//                     return false;
//                 }
//             },
//             PrincipalRule::NotPrincipal(principal) => match principal {
//                 Principal::Wildcard => return true,
//                 Principal::Map(index_map) => todo!(),
//             },
//         }

//         false
//     }
// }

// trait EffectApi {
//     fn is_allowed(&self, b: bool) -> bool;
// }

// impl EffectApi for Effect {
//     fn is_allowed(&self, b: bool) -> bool {
//         if self == &Effect::Allow {
//             b
//         } else {
//             !b
//         }
//     }
// }
