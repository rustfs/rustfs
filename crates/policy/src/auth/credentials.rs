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

use crate::error::Error as IamError;
use crate::error::{Error, Result};
use crate::policy::{INHERITED_POLICY_TYPE, Policy, Validator, iam_policy_claim_name_sa};
use crate::utils;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use time::OffsetDateTime;
use time::macros::offset;
use regex::Regex;
use std::sync::OnceLock;

const ACCESS_KEY_MIN_LEN: usize = 3;
const ACCESS_KEY_MAX_LEN: usize = 20;
const SECRET_KEY_MIN_LEN: usize = 8;
const SECRET_KEY_MAX_LEN: usize = 40;

pub const ACCOUNT_ON: &str = "on";
pub const ACCOUNT_OFF: &str = "off";

// Access key validation regexes using OnceLock
static ACCESS_KEY_CHARSET_REGEX: OnceLock<Regex> = OnceLock::new();
static SECRET_KEY_CHARSET_REGEX: OnceLock<Regex> = OnceLock::new();

// Pattern validation regexes
static RESERVED_CHARS_REGEX: OnceLock<Regex> = OnceLock::new();
static SEQUENTIAL_CHARS_REGEX: OnceLock<Regex> = OnceLock::new();
static SEQUENTIAL_CHARS_4_REGEX: OnceLock<Regex> = OnceLock::new();

// Weak pattern regexes
static WEAK_ACCESS_PATTERNS_REGEX: OnceLock<Regex> = OnceLock::new();
static WEAK_SECRET_PATTERNS_REGEX: OnceLock<Regex> = OnceLock::new();

// Complexity validation regexes
static UPPERCASE_REGEX: OnceLock<Regex> = OnceLock::new();
static LOWERCASE_REGEX: OnceLock<Regex> = OnceLock::new();
static DIGIT_REGEX: OnceLock<Regex> = OnceLock::new();
static SPECIAL_REGEX: OnceLock<Regex> = OnceLock::new();

// Helper functions to get compiled regexes
fn get_access_key_charset_regex() -> &'static Regex {
    ACCESS_KEY_CHARSET_REGEX.get_or_init(|| Regex::new(r"^[A-Za-z0-9]+$").unwrap())
}

fn get_secret_key_charset_regex() -> &'static Regex {
    SECRET_KEY_CHARSET_REGEX.get_or_init(|| Regex::new(r"^[A-Za-z0-9+/=]+$").unwrap())
}

fn get_reserved_chars_regex() -> &'static Regex {
    RESERVED_CHARS_REGEX.get_or_init(|| Regex::new(r"[=,]").unwrap())
}

fn get_sequential_chars_regex() -> &'static Regex {
    SEQUENTIAL_CHARS_REGEX.get_or_init(|| Regex::new(r"(abc|bcd|cde|def|efg|fgh|ghi|hij|ijk|jkl|klm|lmn|mno|nop|opq|pqr|qrs|rst|stu|tuv|uvw|vwx|wxy|xyz|012|123|234|345|456|567|678|789)").unwrap())
}

fn get_sequential_chars_4_regex() -> &'static Regex {
    SEQUENTIAL_CHARS_4_REGEX.get_or_init(|| Regex::new(r"(abcd|bcde|cdef|defg|efgh|fghi|ghij|hijk|ijkl|jklm|klmn|lmno|mnop|nopq|opqr|pqrs|qrst|rstu|stuv|tuvw|uvwx|vwxy|wxyz|0123|1234|2345|3456|4567|5678|6789)").unwrap())
}

fn get_weak_access_patterns_regex() -> &'static Regex {
    WEAK_ACCESS_PATTERNS_REGEX.get_or_init(|| Regex::new(r"(?i)(admin|test|demo|user|guest|root|default|123|abc|password|pass|key|secret)").unwrap())
}

fn get_weak_secret_patterns_regex() -> &'static Regex {
    WEAK_SECRET_PATTERNS_REGEX.get_or_init(|| Regex::new(r"(?i)(password|12345678|abcdefgh|qwertyui|asdfghjk|zxcvbnm|admin123|password1|secret123|default|changeme|welcome|letmein)").unwrap())
}

fn get_uppercase_regex() -> &'static Regex {
    UPPERCASE_REGEX.get_or_init(|| Regex::new(r"[A-Z]").unwrap())
}

fn get_lowercase_regex() -> &'static Regex {
    LOWERCASE_REGEX.get_or_init(|| Regex::new(r"[a-z]").unwrap())
}

fn get_digit_regex() -> &'static Regex {
    DIGIT_REGEX.get_or_init(|| Regex::new(r"[0-9]").unwrap())
}

fn get_special_regex() -> &'static Regex {
    SPECIAL_REGEX.get_or_init(|| Regex::new(r"[+/=]").unwrap())
}

// Check for repeated characters using manual iteration (since regex doesn't support backreferences)
fn has_repeated_chars_manual(s: &str, min_repeat_len: usize) -> bool {
    if s.len() < min_repeat_len {
        return false;
    }
    
    let chars: Vec<char> = s.chars().collect();
    let mut repeat_count = 1;
    
    for i in 1..chars.len() {
        if chars[i] == chars[i-1] {
            repeat_count += 1;
            if repeat_count >= min_repeat_len {
                return true;
            }
        } else {
            repeat_count = 1;
        }
    }
    
    false
}

// ContainsReservedChars - returns whether the input string contains reserved characters.
pub fn contains_reserved_chars(s: &str) -> bool {
    get_reserved_chars_regex().is_match(s)
}

// Validate character set for access key
pub fn is_access_key_charset_valid(access_key: &str) -> bool {
    get_access_key_charset_regex().is_match(access_key)
}

// Validate character set for secret key
pub fn is_secret_key_charset_valid(secret_key: &str) -> bool {
    get_secret_key_charset_regex().is_match(secret_key)
}

// Comprehensive access key validation
pub fn is_access_key_valid(access_key: &str) -> bool {
    // Check length bounds
    if access_key.len() < ACCESS_KEY_MIN_LEN || access_key.len() > ACCESS_KEY_MAX_LEN {
        return false;
    }
    
    // Check for empty or whitespace-only
    if access_key.trim().is_empty() {
        return false;
    }
    
    // Check for reserved characters
    if contains_reserved_chars(access_key) {
        return false;
    }
    
    // Check character set
    if !is_access_key_charset_valid(access_key) {
        return false;
    }
    
    // Check for common weak patterns
    if is_weak_access_key(access_key) {
        return false;
    }
    
    true
}

// Comprehensive secret key validation
pub fn is_secret_key_valid(secret_key: &str) -> bool {
    // Check length bounds
    if secret_key.len() < SECRET_KEY_MIN_LEN || secret_key.len() > SECRET_KEY_MAX_LEN {
        return false;
    }
    
    // Check for empty or whitespace-only
    if secret_key.trim().is_empty() {
        return false;
    }
    
    // Check for reserved characters in inappropriate context
    if contains_reserved_chars(secret_key) {
        return false;
    }
    
    // Check character set
    if !is_secret_key_charset_valid(secret_key) {
        return false;
    }
    
    // Check for minimum complexity requirements
    if !meets_secret_key_complexity(secret_key) {
        return false;
    }
    
    // Check for common weak patterns
    if is_weak_secret_key(secret_key) {
        return false;
    }
    
    true
}

// Check for weak access key patterns
fn is_weak_access_key(access_key: &str) -> bool {
    // Check for sequential characters (3+ in a row)
    if get_sequential_chars_regex().is_match(&access_key.to_lowercase()) {
        return true;
    }
    
    // Check for repeated characters (3+ in a row)
    if has_repeated_chars_manual(access_key, 3) {
        return true;
    }
    
    // Check for common weak patterns
    get_weak_access_patterns_regex().is_match(access_key)
}

// Check for weak secret key patterns
fn is_weak_secret_key(secret_key: &str) -> bool {
    // Check for sequential characters (4+ in a row)
    if get_sequential_chars_4_regex().is_match(&secret_key.to_lowercase()) {
        return true;
    }
    
    // Check for repeated characters (4+ in a row)
    if has_repeated_chars_manual(secret_key, 4) {
        return true;
    }
    
    // Check for common weak patterns
    get_weak_secret_patterns_regex().is_match(secret_key)
}

// Check secret key complexity requirements
fn meets_secret_key_complexity(secret_key: &str) -> bool {
    let has_upper = get_uppercase_regex().is_match(secret_key);
    let has_lower = get_lowercase_regex().is_match(secret_key);
    let has_digit = get_digit_regex().is_match(secret_key);
    let has_special = get_special_regex().is_match(secret_key);
    
    // Require at least 2 of the 4 character types for keys >= 12 chars
    let complexity_count = [has_upper, has_lower, has_digit, has_special]
        .iter()
        .filter(|&&x| x)
        .count();
    
    if secret_key.len() >= 12 {
        complexity_count >= 2
    } else {
        complexity_count >= 1
    }
}

// Enhanced credential validation
pub fn validate_credentials(access_key: &str, secret_key: &str) -> Result<()> {
    // Validate access key
    if !is_access_key_valid(access_key) {
        return Err(IamError::InvalidAccessKeyLength);
    }
    
    // Validate secret key
    if !is_secret_key_valid(secret_key) {
        return Err(IamError::InvalidSecretKeyLength);
    }
    
    // Check for credential reuse patterns
    if access_key.to_lowercase() == secret_key.to_lowercase() {
        return Err(Error::other("Access key and secret key cannot be identical"));
    }
    
    // Check if secret key contains access key
    if secret_key.to_lowercase().contains(&access_key.to_lowercase()) {
        return Err(Error::other("Secret key cannot contain access key"));
    }
    
    Ok(())
}

// #[cfg_attr(test, derive(PartialEq, Eq, Debug))]
// struct CredentialHeader {
//     access_key: String,
//     scop: CredentialHeaderScope,
// }

// #[cfg_attr(test, derive(PartialEq, Eq, Debug))]
// struct CredentialHeaderScope {
//     date: Date,
//     region: String,
//     service: ServiceType,
//     request: String,
// }

// impl TryFrom<&str> for CredentialHeader {
//     type Error = Error;
//     fn try_from(value: &str) -> Result<Self, Self::Error> {
//         let mut elem = value.trim().splitn(2, '=');
//         let (Some(h), Some(cred_elems)) = (elem.next(), elem.next()) else {
//             return Err(IamError::ErrCredMalformed));
//         };

//         if h != "Credential" {
//             return Err(IamError::ErrCredMalformed));
//         }

//         let mut cred_elems = cred_elems.trim().rsplitn(5, '/');

//         let Some(request) = cred_elems.next() else {
//             return Err(IamError::ErrCredMalformed));
//         };

//         let Some(service) = cred_elems.next() else {
//             return Err(IamError::ErrCredMalformed));
//         };

//         let Some(region) = cred_elems.next() else {
//             return Err(IamError::ErrCredMalformed));
//         };

//         let Some(date) = cred_elems.next() else {
//             return Err(IamError::ErrCredMalformed));
//         };

//         let Some(ak) = cred_elems.next() else {
//             return Err(IamError::ErrCredMalformed));
//         };

//         if ak.len() < 3 {
//             return Err(IamError::ErrCredMalformed));
//         }

//         if request != "aws4_request" {
//             return Err(IamError::ErrCredMalformed));
//         }

//         Ok(CredentialHeader {
//             access_key: ak.to_owned(),
//             scop: CredentialHeaderScope {
//                 date: {
//                     const FORMATTER: LazyCell<Vec<BorrowedFormatItem<'static>>> =
//                         LazyCell::new(|| time::format_description::parse("[year][month][day]").unwrap());

//                     Date::parse(date, &FORMATTER).map_err(|_| IamError::ErrCredMalformed))?
//                 },
//                 region: region.to_owned(),
//                 service: service.try_into()?,
//                 request: request.to_owned(),
//             },
//         })
//     }
// }

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: String,
    pub expiration: Option<OffsetDateTime>,
    pub status: String,
    pub parent_user: String,
    pub groups: Option<Vec<String>>,
    pub claims: Option<HashMap<String, Value>>,
    pub name: Option<String>,
    pub description: Option<String>,
}

impl Credentials {
    // pub fn new(elem: &str) -> Result<Self> {
    //     let header: CredentialHeader = elem.try_into()?;
    //     Self::check_key_value(header)
    // }

    // pub fn check_key_value(_header: CredentialHeader) -> Result<Self> {
    //     todo!()
    // }

    pub fn is_expired(&self) -> bool {
        if self.expiration.is_none() {
            return false;
        }

        self.expiration
            .as_ref()
            .map(|e| time::OffsetDateTime::now_utc() > *e)
            .unwrap_or(false)
    }

    pub fn is_temp(&self) -> bool {
        !self.session_token.is_empty() && !self.is_expired()
    }

    pub fn is_service_account(&self) -> bool {
        const IAM_POLICY_CLAIM_NAME_SA: &str = "sa-policy";
        self.claims
            .as_ref()
            .map(|x| x.get(IAM_POLICY_CLAIM_NAME_SA).is_some_and(|_| !self.parent_user.is_empty()))
            .unwrap_or_default()
    }

    pub fn is_implied_policy(&self) -> bool {
        if self.is_service_account() {
            return self
                .claims
                .as_ref()
                .map(|x| x.get(&iam_policy_claim_name_sa()).is_some_and(|v| v == INHERITED_POLICY_TYPE))
                .unwrap_or_default();
        }

        false
    }

    pub fn is_valid(&self) -> bool {
        if self.status == "off" {
            return false;
        }

        // Use comprehensive validation instead of simple length checks
        if !is_access_key_valid(&self.access_key) || !is_secret_key_valid(&self.secret_key) {
            return false;
        }
        
        // Additional validation for credential relationship
        if validate_credentials(&self.access_key, &self.secret_key).is_err() {
            return false;
        }

        !self.is_expired()
    }

    pub fn is_owner(&self) -> bool {
        false
    }
}

pub fn generate_credentials() -> Result<(String, String)> {
    let ak = utils::gen_access_key(20)?;
    let sk = utils::gen_secret_key(40)?;
    Ok((ak, sk))
}

pub fn get_new_credentials_with_metadata(claims: &HashMap<String, Value>, token_secret: &str) -> Result<Credentials> {
    let (ak, sk) = generate_credentials()?;

    create_new_credentials_with_metadata(&ak, &sk, claims, token_secret)
}

pub fn create_new_credentials_with_metadata(
    ak: &str,
    sk: &str,
    claims: &HashMap<String, Value>,
    token_secret: &str,
) -> Result<Credentials> {
    // Use comprehensive validation instead of simple length checks
    if !is_access_key_valid(ak) {
        return Err(IamError::InvalidAccessKeyLength);
    }

    if !is_secret_key_valid(sk) {
        return Err(IamError::InvalidSecretKeyLength);
    }
    
    // Validate credential relationship
    validate_credentials(ak, sk)?;

    if token_secret.is_empty() {
        return Ok(Credentials {
            access_key: ak.to_owned(),
            secret_key: sk.to_owned(),
            status: ACCOUNT_OFF.to_owned(),
            ..Default::default()
        });
    }

    let expiration = {
        if let Some(v) = claims.get("exp") {
            if let Some(expiry) = v.as_i64() {
                Some(OffsetDateTime::from_unix_timestamp(expiry)?.to_offset(offset!(+8)))
            } else {
                None
            }
        } else {
            None
        }
    };

    let token = utils::generate_jwt(&claims, token_secret)?;

    Ok(Credentials {
        access_key: ak.to_owned(),
        secret_key: sk.to_owned(),
        session_token: token,
        status: ACCOUNT_ON.to_owned(),
        expiration,
        ..Default::default()
    })
}

pub fn jwt_sign<T: Serialize>(claims: &T, token_secret: &str) -> Result<String> {
    let token = utils::generate_jwt(claims, token_secret)?;
    Ok(token)
}

#[derive(Default)]
pub struct CredentialsBuilder {
    session_policy: Option<Policy>,
    access_key: String,
    secret_key: String,
    name: Option<String>,
    description: Option<String>,
    expiration: Option<OffsetDateTime>,
    allow_site_replicator_account: bool,
    claims: Option<serde_json::Value>,
    parent_user: String,
    groups: Option<Vec<String>>,
}

impl CredentialsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn session_policy(mut self, policy: Option<Policy>) -> Self {
        self.session_policy = policy;
        self
    }

    pub fn access_key(mut self, access_key: String) -> Self {
        self.access_key = access_key;
        self
    }

    pub fn secret_key(mut self, secret_key: String) -> Self {
        self.secret_key = secret_key;
        self
    }

    pub fn name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn description(mut self, description: String) -> Self {
        self.description = Some(description);
        self
    }

    pub fn expiration(mut self, expiration: Option<OffsetDateTime>) -> Self {
        self.expiration = expiration;
        self
    }

    pub fn allow_site_replicator_account(mut self, allow_site_replicator_account: bool) -> Self {
        self.allow_site_replicator_account = allow_site_replicator_account;
        self
    }

    pub fn claims(mut self, claims: serde_json::Value) -> Self {
        self.claims = Some(claims);
        self
    }

    pub fn parent_user(mut self, parent_user: String) -> Self {
        self.parent_user = parent_user;
        self
    }

    pub fn groups(mut self, groups: Vec<String>) -> Self {
        self.groups = Some(groups);
        self
    }

    pub fn try_build(self) -> Result<Credentials> {
        self.try_into()
    }
}

impl TryFrom<CredentialsBuilder> for Credentials {
    type Error = Error;
    fn try_from(mut value: CredentialsBuilder) -> std::result::Result<Self, Self::Error> {
        if value.parent_user.is_empty() {
            return Err(IamError::InvalidArgument);
        }

        if (value.access_key.is_empty() && !value.secret_key.is_empty())
            || (!value.access_key.is_empty() && value.secret_key.is_empty())
        {
            return Err(Error::other("Either ak or sk is empty"));
        }

        if value.parent_user == value.access_key.as_str() {
            return Err(IamError::InvalidArgument);
        }

        if value.access_key == "site-replicator-0" && !value.allow_site_replicator_account {
            return Err(IamError::InvalidArgument);
        }
        
        // Validate provided credentials if not empty
        if !value.access_key.is_empty() && !value.secret_key.is_empty() {
            validate_credentials(&value.access_key, &value.secret_key)?;
        }

        let mut claim = serde_json::json!({
            "parent": value.parent_user
        });

        if let Some(p) = value.session_policy {
            p.is_valid()?;
            let policy_buf = serde_json::to_vec(&p).map_err(|_| IamError::InvalidArgument)?;
            if policy_buf.len() > 4096 {
                return Err(Error::other("session policy is too large"));
            }
            claim["sessionPolicy"] = serde_json::json!(base64_simd::STANDARD.encode_to_string(&policy_buf));
            claim["sa-policy"] = serde_json::json!("embedded-policy");
        } else {
            claim["sa-policy"] = serde_json::json!("inherited-policy");
        }

        if let Some(Value::Object(obj)) = value.claims {
            for (key, value) in obj {
                if claim.get(&key).is_some() {
                    continue;
                }
                claim[key] = value;
            }
        }

        if value.access_key.is_empty() {
            value.access_key = utils::gen_access_key(20)?;
        }

        if value.secret_key.is_empty() {
            value.secret_key = utils::gen_secret_key(40)?;  // Fix: was assigning to access_key
        }
        
        // Validate generated credentials
        validate_credentials(&value.access_key, &value.secret_key)?;

        claim["accessKey"] = json!(&value.access_key);

        let mut cred = Credentials {
            status: "on".into(),
            parent_user: value.parent_user,
            groups: value.groups,
            name: value.name,
            description: value.description,
            ..Default::default()
        };

        if !value.secret_key.is_empty() {
            let session_token = rustfs_crypto::jwt_encode(value.access_key.as_bytes(), &claim)
                .map_err(|_| Error::other("session policy is too large"))?;
            cred.session_token = session_token;
            // cred.expiration = Some(
            //     OffsetDateTime::from_unix_timestamp(
            //         claim
            //             .get("exp")
            //             .and_then(|x| x.as_i64())
            //             .ok_or(Error::StringError("invalid exp".into()))?,
            //     )
            //     .map_err(|_| Error::StringError("invalie timestamp".into()))?,
            // );
        } else {
            // cred.expiration =
            // Some(OffsetDateTime::from_unix_timestamp(0).map_err(|_| Error::StringError("invalie timestamp".into()))?);
        }

        cred.expiration = value.expiration;
        cred.access_key = value.access_key;
        cred.secret_key = value.secret_key;

        Ok(cred)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_access_key_validation() {
        // Valid access keys
        assert!(is_access_key_valid("abc123"));
        assert!(is_access_key_valid("MyAccessKey12345"));
        assert!(is_access_key_valid("A1B2C3"));
        
        // Invalid - too short
        assert!(!is_access_key_valid("ab"));
        
        // Invalid - too long
        assert!(!is_access_key_valid("a".repeat(21).as_str()));
        
        // Invalid - contains reserved characters
        assert!(!is_access_key_valid("abc=123"));
        assert!(!is_access_key_valid("abc,123"));
        
        // Invalid - weak patterns
        assert!(!is_access_key_valid("admin"));
        assert!(!is_access_key_valid("test123"));
        assert!(!is_access_key_valid("abcd"));
        assert!(!is_access_key_valid("1234"));
        
        // Invalid - empty or whitespace
        assert!(!is_access_key_valid(""));
        assert!(!is_access_key_valid("   "));
    }

    #[test]
    fn test_secret_key_validation() {
        // Valid secret keys
        assert!(is_secret_key_valid("MySecret123"));
        assert!(is_secret_key_valid("Abc12345Def"));
        assert!(is_secret_key_valid("ComplexSecret123+"));
        
        // Invalid - too short
        assert!(!is_secret_key_valid("1234567"));
        
        // Invalid - too long
        assert!(!is_secret_key_valid("a".repeat(41).as_str()));
        
        // Invalid - weak patterns
        assert!(!is_secret_key_valid("password"));
        assert!(!is_secret_key_valid("12345678"));
        assert!(!is_secret_key_valid("abcdefgh"));
        assert!(!is_secret_key_valid("aaaaaaaa"));
        
        // Invalid - contains reserved characters
        assert!(!is_secret_key_valid("secret=123"));
        assert!(!is_secret_key_valid("secret,123"));
        
        // Invalid - empty or whitespace
        assert!(!is_secret_key_valid(""));
        assert!(!is_secret_key_valid("   "));
    }

    #[test]
    fn test_credential_relationship_validation() {
        // Valid - different values
        assert!(validate_credentials("access123", "secretkey456").is_ok());
        
        // Invalid - identical
        assert!(validate_credentials("same123", "same123").is_err());
        
        // Invalid - secret contains access key
        assert!(validate_credentials("abc", "secretabc123").is_err());
        assert!(validate_credentials("test", "mytestpassword").is_err());
    }

    #[test]
    fn test_repeated_chars_detection() {
        assert!(has_repeated_chars_manual("aaa", 3));
        assert!(has_repeated_chars_manual("111", 3));
        assert!(has_repeated_chars_manual("aaabbb", 3));
        assert!(!has_repeated_chars_manual("aba", 3));
        assert!(!has_repeated_chars_manual("aa", 3));
    }

    #[test]
    fn test_sequential_chars_detection() {
        // Using regex-based detection
        assert!(get_sequential_chars_regex().is_match("abc"));
        assert!(get_sequential_chars_regex().is_match("123"));
        assert!(get_sequential_chars_regex().is_match("defgh"));
        assert!(!get_sequential_chars_regex().is_match("ace"));
        
        // Test 4+ character sequences
        assert!(get_sequential_chars_4_regex().is_match("abcd"));
        assert!(get_sequential_chars_4_regex().is_match("1234"));
        assert!(!get_sequential_chars_4_regex().is_match("abc"));
    }

    #[test]
    fn test_secret_key_complexity() {
        // Complex enough - mixed case and digits
        assert!(meets_secret_key_complexity("MySecret123"));
        
        // Complex enough - uppercase and special chars
        assert!(meets_secret_key_complexity("SECRET+KEY"));
        
        // Not complex enough - only lowercase
        assert!(!meets_secret_key_complexity("mysecretkey"));
        
        // Short key with some complexity is ok
        assert!(meets_secret_key_complexity("MyKey1"));
    }

    #[test]
    fn test_charset_validation() {
        // Valid access key charset
        assert!(is_access_key_charset_valid("ABCabc123"));
        assert!(!is_access_key_charset_valid("abc@123"));
        assert!(!is_access_key_charset_valid("abc#123"));
        
        // Valid secret key charset
        assert!(is_secret_key_charset_valid("ABCabc123+/="));
        assert!(!is_secret_key_charset_valid("abc@123"));
        assert!(!is_secret_key_charset_valid("abc#123"));
    }

    #[test]
    fn test_credentials_is_valid() {
        // Valid credentials
        let valid_creds = Credentials {
            access_key: "ValidKey123".to_string(),
            secret_key: "ValidSecret456".to_string(),
            status: "on".to_string(),
            ..Default::default()
        };
        assert!(valid_creds.is_valid());
        
        // Invalid - status off
        let invalid_status = Credentials {
            access_key: "ValidKey123".to_string(),
            secret_key: "ValidSecret456".to_string(),
            status: "off".to_string(),
            ..Default::default()
        };
        assert!(!invalid_status.is_valid());
        
        // Invalid - weak access key
        let weak_access = Credentials {
            access_key: "admin".to_string(),
            secret_key: "ValidSecret456".to_string(),
            status: "on".to_string(),
            ..Default::default()
        };
        assert!(!weak_access.is_valid());
        
        // Invalid - identical keys
        let identical_keys = Credentials {
            access_key: "SameKey123".to_string(),
            secret_key: "SameKey123".to_string(),
            status: "on".to_string(),
            ..Default::default()
        };
        assert!(!identical_keys.is_valid());
    }
}
