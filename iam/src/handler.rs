// use std::{borrow::Cow, collections::HashMap};

// use log::{info, warn};

// use crate::{
//     arn::ARN,
//     auth::UserIdentity,
//     cache::CacheInner,
//     policy::{utils::get_values_from_claims, Args, Policy},
//     store::Store,
//     Error,
// };

// pub(crate) struct Handler<'m, T> {
//     cache: CacheInner,
//     api: &'m T,
//     roles: &'m HashMap<ARN, Vec<String>>,
// }

// impl<'m, T> Handler<'m, T> {
//     pub fn new(cache: CacheInner, api: &'m T, roles: &'m HashMap<ARN, Vec<String>>) -> Self {
//         Self { cache, api, roles }
//     }
// }

// impl<'m, T> Handler<'m, T>
// where
//     T: Store,
// {
//     #[inline]
//     fn get_user<'a>(&self, user_name: &'a str) -> Option<&UserIdentity> {
//         self.cache
//             .users
//             .get(user_name)
//             .or_else(|| self.cache.sts_accounts.get(user_name))
//     }

//     async fn get_policy(&self, name: &str, _groups: &[String]) -> crate::Result<Vec<String>> {
//         if name.is_empty() {
//             return Err(Error::InvalidArgument);
//         }

//         todo!()
//         // self.api.policy_db_get(name, groups)
//     }

//     /// 如果是临时用户，返回Ok(Some(partent_name)))
//     /// 如果不是临时用户，返回Ok(None)
//     fn is_temp_user<'a>(&self, user_name: &'a str) -> crate::Result<Option<&str>> {
//         let user = self
//             .get_user(user_name)
//             .ok_or_else(|| Error::NoSuchUser(user_name.to_owned()))?;

//         if user.credentials.is_temp() {
//             Ok(Some(&user.credentials.parent_user))
//         } else {
//             Ok(None)
//         }
//     }

//     /// 如果是临时用户，返回Ok(Some(partent_name)))
//     /// 如果不是临时用户，返回Ok(None)
//     fn is_service_account<'a>(&self, user_name: &'a str) -> crate::Result<Option<&str>> {
//         let user = self
//             .get_user(user_name)
//             .ok_or_else(|| Error::NoSuchUser(user_name.to_owned()))?;

//         if user.credentials.is_service_account() {
//             Ok(Some(&user.credentials.parent_user))
//         } else {
//             Ok(None)
//         }
//     }

//     // todo
//     pub fn is_allowed_sts(&self, args: &Args, parent: &str) -> bool {
//         warn!("unimplement is_allowed_sts");
//         false
//     }

//     // todo
//     pub async fn is_allowed_service_account<'a>(&self, args: &Args<'a>, parent: &str) -> bool {
//         let Some(p) = args.claims.get(parent) else {
//             return false;
//         };

//         if let Some(parent_in_chaim) = p.as_str() {
//             if parent_in_chaim != parent {
//                 return false;
//             }
//         } else {
//             return false;
//         }

//         let is_owner_derived = parent == "rustfsadmin"; // todo ,使用全局变量
//         let role_arn = args.get_role_arn();
//         let mut svc_policies = None;

//         if is_owner_derived {
//         } else if let Some(x) = role_arn {
//             let Ok(arn) = x.parse::<ARN>() else {
//                 info!("error parsing role ARN {x}");
//                 return false;
//             };

//             svc_policies = self.roles.get(&arn).map(|x| Cow::from(x));
//         } else {
//             let Ok(mut p) = self.get_policy(parent, &args.groups[..]).await else { return false };
//             if p.is_empty() {
//                 // todo iamPolicyClaimNameOpenID
//                 let (p1, _) = get_values_from_claims(&args.claims, "");
//                 p = p1;
//             }
//             svc_policies = Some(Cow::Owned(p));
//         }

//         if is_owner_derived && svc_policies.as_ref().map(|x| x.as_ref().len()).unwrap_or_default() == 0 {
//             return false;
//         }

//         false
//     }

//     pub async fn get_combined_policy(&self, _policies: &[String]) -> Policy {
//         todo!()
//     }

//     pub async fn is_allowed<'a>(&self, args: Args<'a>) -> bool {
//         if args.is_owner {
//             return true;
//         }

//         match self.is_temp_user(&args.account) {
//             Ok(Some(parent)) => return self.is_allowed_sts(&args, parent),
//             Err(_) => return false,
//             _ => {}
//         }

//         match self.is_service_account(&args.account) {
//             Ok(Some(parent)) => return self.is_allowed_service_account(&args, parent).await,
//             Err(_) => return false,
//             _ => {}
//         }

//         let Ok(policies) = self.get_policy(&args.account, &args.groups).await else { return false };

//         if policies.is_empty() {
//             return false;
//         }

//         let policy = self.get_combined_policy(&policies[..]).await;
//         policy.is_allowed(&args)
//     }
// }
