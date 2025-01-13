use serde::Serialize;
use time::OffsetDateTime;

// #[derive(Deserialize, Default)]
// #[serde(rename_all = "camelCase", default)]
// pub struct AddServiceAccountReq {
//     pub access_key: String,
//     pub secret_key: String,

//     pub policy: Option<Vec<u8>>,
//     pub target_user: Option<String>,
//     pub name: String,
//     pub description: String,
//     #[serde(with = "time::serde::rfc3339::option")]
//     #[serde(skip_serializing_if = "Option::is_none")]
//     pub expiration: Option<OffsetDateTime>,
// }

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Credentials<'a> {
    pub access_key: &'a str,
    pub secret_key: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_token: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "time::serde::rfc3339::option")]
    pub expiration: Option<OffsetDateTime>,
}

#[derive(Serialize)]
pub struct AddServiceAccountResp<'a> {
    pub credentials: Credentials<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InfoServiceAccountResp {
    pub parent_user: String,
    pub account_status: String,
    pub implied_policy: bool,
    pub policy: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub description: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "time::serde::rfc3339::option")]
    pub expiration: Option<OffsetDateTime>,
}
