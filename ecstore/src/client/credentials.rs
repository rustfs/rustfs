#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use std::fmt::{Display, Formatter};

use time::OffsetDateTime;

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub enum SignatureType {
    #[default]
    SignatureDefault,
    SignatureV4,
    SignatureV2,
    SignatureV4Streaming,
    SignatureAnonymous,
}

#[derive(Debug, Clone, Default)]
pub struct Credentials<P: Provider + Default> {
    creds: Value,
    force_refresh: bool,
    provider: P,
}

impl<P: Provider + Default> Credentials<P> {
    pub fn new(provider: P) -> Self {
        Self {
            provider,
            force_refresh: true,
            ..Default::default()
        }
    }

    pub fn get(&mut self) -> Result<Value, std::io::Error> {
        self.get_with_context(None)
    }

    pub fn get_with_context(&mut self, mut cc: Option<CredContext>) -> Result<Value, std::io::Error> {
        if self.is_expired() {
            let creds = self.provider.retrieve_with_cred_context(cc.expect("err"));
            self.creds = creds;
            self.force_refresh = false;
        }

        Ok(self.creds.clone())
    }

    fn expire(&mut self) {
        self.force_refresh = true;
    }

    pub fn is_expired(&self) -> bool {
        self.force_refresh || self.provider.is_expired()
    }
}

#[derive(Debug, Clone)]
pub struct Value {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub expiration: OffsetDateTime,
    pub signer_type: SignatureType,
}

impl Default for Value {
    fn default() -> Self {
        Self {
            access_key_id: "".to_string(),
            secret_access_key: "".to_string(),
            session_token: "".to_string(),
            expiration: OffsetDateTime::now_utc(),
            signer_type: SignatureType::SignatureDefault,
        }
    }
}

pub struct CredContext {
    //pub client: SendRequest,
    pub endpoint: String,
}

pub trait Provider {
    fn retrieve(&self) -> Value;
    fn retrieve_with_cred_context(&self, _: CredContext) -> Value;
    fn is_expired(&self) -> bool;
}

#[derive(Debug, Clone, Default)]
pub struct Static(pub Value);

impl Provider for Static {
    fn retrieve(&self) -> Value {
        if self.0.access_key_id == "" || self.0.secret_access_key == "" {
            return Value {
                signer_type: SignatureType::SignatureAnonymous,
                ..Default::default()
            };
        }
        self.0.clone()
    }

    fn retrieve_with_cred_context(&self, _: CredContext) -> Value {
        self.retrieve()
    }

    fn is_expired(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Default)]
pub struct STSError {
    pub r#type: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, thiserror::Error)]
pub struct ErrorResponse {
    pub sts_error: STSError,
    pub request_id: String,
}

impl Display for ErrorResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error())
    }
}

impl ErrorResponse {
    fn error(&self) -> String {
        if self.sts_error.message == "" {
            return format!("Error response code {}.", self.sts_error.code);
        }
        return self.sts_error.message.clone();
    }
}

pub fn xml_decoder<T>(body: &[u8]) -> Result<T, std::io::Error> {
    todo!();
}

pub fn xml_decode_and_body<T>(body_reader: &[u8]) -> Result<(Vec<u8>, T), std::io::Error> {
    todo!();
}
