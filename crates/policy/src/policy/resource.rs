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

use crate::error::{Error, Result};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, Error as DeError, Visitor},
};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    ops::Deref,
};

use super::{Error as IamError, Validator, function::key_name::KeyName, utils::path, variables::PolicyVariableResolver};

#[derive(Clone, Default, Debug)]
pub struct ResourceSet(pub Vec<Resource>);

impl Serialize for ResourceSet {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;

        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for resource in &self.0 {
            seq.serialize_element(resource)?;
        }
        seq.end()
    }
}

impl ResourceSet {
    /// Returns true if the resource set is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub async fn is_match(&self, resource: &str, conditions: &HashMap<String, Vec<String>>) -> bool {
        self.is_match_with_resolver(resource, conditions, None).await
    }

    pub async fn is_match_with_resolver(
        &self,
        resource: &str,
        conditions: &HashMap<String, Vec<String>>,
        resolver: Option<&dyn PolicyVariableResolver>,
    ) -> bool {
        for re in self.0.iter() {
            if re.is_match_with_resolver(resource, conditions, resolver).await {
                return true;
            }
        }

        false
    }

    pub async fn match_resource(&self, resource: &str) -> bool {
        for re in self.0.iter() {
            if re.match_resource(resource).await {
                return true;
            }
        }

        false
    }
}

impl Deref for ResourceSet {
    type Target = [Resource];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Validator for ResourceSet {
    type Error = Error;
    fn is_valid(&self) -> Result<()> {
        for resource in self.0.iter() {
            resource.is_valid()?;
        }

        Ok(())
    }
}

impl PartialEq for ResourceSet {
    fn eq(&self, other: &Self) -> bool {
        self.0.iter().all(|x| other.0.contains(x)) && other.0.iter().all(|x| self.0.contains(x))
    }
}

impl<'de> Deserialize<'de> for ResourceSet {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ResourceOrVecVisitor;

        impl<'de> Visitor<'de> for ResourceOrVecVisitor {
            type Value = ResourceSet;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string or an array of strings")
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                let resource = Resource::try_from(value).map_err(|e| E::custom(format!("invalid resource: {}", e)))?;
                Ok(ResourceSet(vec![resource]))
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
                A::Error: DeError,
            {
                let mut resources = ResourceSet::default();
                let mut seen = HashSet::new();
                while let Some(value) = seq.next_element::<String>()? {
                    match Resource::try_from(value.as_str()) {
                        Ok(resource) => {
                            if seen.insert(resource.clone()) {
                                resources.0.push(resource);
                            }
                        }
                        Err(e) => {
                            return Err(A::Error::custom(format!("invalid resource: {}", e)));
                        }
                    }
                }
                Ok(resources)
            }
        }

        deserializer.deserialize_any(ResourceOrVecVisitor)
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub enum Resource {
    All,
    S3(String),
    Kms(String),
}

enum ResourceToken {
    Literal(u8),
    Star,
    Any,
    Values(Vec<String>),
}

fn resource_tokens_match(pattern: &[ResourceToken], name: &str, wildcard: bool) -> bool {
    let name = name.as_bytes();
    if pattern.iter().all(|token| !matches!(token, ResourceToken::Values(_))) {
        let (mut p, mut n) = (0, 0);
        let mut star = None;
        while n < name.len() {
            match pattern.get(p) {
                Some(ResourceToken::Literal(byte)) if *byte == name[n] => {
                    p += 1;
                    n += 1;
                }
                Some(ResourceToken::Any) if wildcard => {
                    p += 1;
                    n += 1;
                }
                Some(ResourceToken::Star) if wildcard => {
                    star = Some((p, n));
                    p += 1;
                }
                Some(ResourceToken::Star) if name[n] == b'*' => {
                    p += 1;
                    n += 1;
                }
                Some(ResourceToken::Any) if name[n] == b'?' => {
                    p += 1;
                    n += 1;
                }
                _ => {
                    let Some((star_pattern, star_name)) = star else { return false };
                    n = star_name + 1;
                    p = star_pattern + 1;
                    star = Some((star_pattern, n));
                }
            }
        }
        return wildcard && pattern[p..].iter().all(|token| matches!(token, ResourceToken::Star)) || p == pattern.len();
    }
    let mut reachable = vec![false; name.len() + 1];
    let mut next = vec![false; name.len() + 1];
    reachable[0] = true;
    for token in pattern {
        next.fill(false);
        match token {
            ResourceToken::Star if wildcard => {
                if let Some(first) = reachable.iter().position(|&matched| matched) {
                    next[first..].fill(true);
                }
            }
            ResourceToken::Any if wildcard => {
                next[1..].copy_from_slice(&reachable[..name.len()]);
            }
            ResourceToken::Literal(byte) => {
                for (pos, &matched) in reachable[..name.len()].iter().enumerate() {
                    next[pos + 1] = matched && name[pos] == *byte;
                }
            }
            ResourceToken::Values(values) => {
                for (pos, &matched) in reachable.iter().enumerate() {
                    if matched {
                        for value in values {
                            if name[pos..].starts_with(value.as_bytes()) {
                                next[pos + value.len()] = true;
                            }
                        }
                    }
                }
            }
            ResourceToken::Star | ResourceToken::Any => {
                let byte = if matches!(token, ResourceToken::Star) { b'*' } else { b'?' };
                for (pos, &matched) in reachable[..name.len()].iter().enumerate() {
                    next[pos + 1] = matched && name[pos] == byte;
                }
            }
        }
        if !next.iter().any(|&matched| matched) {
            return false;
        }
        std::mem::swap(&mut reachable, &mut next);
    }
    reachable[name.len()]
}

/// Compile policy operators before interpolation so identity values remain literal data.
async fn resolve_resource_tokens(
    pattern: &str,
    conditions: &HashMap<String, Vec<String>>,
    resolver: Option<&dyn PolicyVariableResolver>,
    depth: usize,
) -> Option<Vec<ResourceToken>> {
    if depth >= 10 {
        return None;
    }
    let mut tokens = Vec::new();
    let bytes = pattern.as_bytes();
    let mut pos = 0;
    while pos < bytes.len() {
        if bytes[pos..].starts_with(b"${") {
            let start = pos + 2;
            let mut end = start;
            let mut depth = 1;
            while end < bytes.len() {
                match bytes[end] {
                    b'{' => depth += 1,
                    b'}' => depth -= 1,
                    _ => {}
                }
                if depth == 0 {
                    break;
                }
                end += 1;
            }
            if depth != 0 {
                return None;
            }
            let variable = &pattern[start..end];
            let mut values = Vec::new();
            if variable.contains("${") {
                // Historical nested syntax substitutes the inner expression as
                // the value itself, rather than looking it up again. Compile it
                // recursively without expanding its Cartesian product or treating
                // resulting wildcard characters as policy operators.
                let nested = Box::pin(resolve_resource_tokens(variable, conditions, resolver, depth + 1)).await?;
                tokens.extend(nested.into_iter().map(|token| match token {
                    ResourceToken::Star => ResourceToken::Literal(b'*'),
                    ResourceToken::Any => ResourceToken::Literal(b'?'),
                    other => other,
                }));
                pos = end + 1;
                continue;
            } else {
                let name = variable;
                let resolved = if let Some(resolver) = resolver {
                    resolver.resolve_multiple(name).await
                } else {
                    None
                };
                if let Some(resolved) = resolved {
                    values.extend(resolved);
                } else if let Some(key) = KeyName::COMMON_KEYS
                    .iter()
                    .find(|key| key.var_name() == format!("${{{name}}}"))
                    && let Some(resolved) = conditions.get(key.name())
                {
                    values.extend(resolved.iter().cloned());
                }
            }
            // Missing variables invalidate this resource rather than matching the
            // placeholder's literal spelling, including for anonymous callers.
            values.retain(|value| !value.is_empty());
            if values.is_empty() {
                return None;
            }
            // Keep alternatives separate; expanding their Cartesian product can
            // exhaust memory or silently suppress an explicit Deny.
            tokens.push(ResourceToken::Values(values));
            pos = end + 1;
        } else {
            let token = match bytes[pos] {
                b'*' => ResourceToken::Star,
                b'?' => ResourceToken::Any,
                byte => ResourceToken::Literal(byte),
            };
            tokens.push(token);
            pos += 1;
        }
    }
    Some(tokens)
}

impl Resource {
    pub const S3_PREFIX: &'static str = "arn:aws:s3:::";
    /// KMS ARNs use the same empty-account form as [`Self::S3_PREFIX`]; the suffix is
    /// `key/<key_id>` (wildcards allowed in the id), `alias/<name>`, or a bare `*`.
    pub const KMS_PREFIX: &'static str = "arn:aws:kms:::";
    /// Resource-type segment for key ids; request-side KMS resource strings are
    /// `key/<key_id>` so they line up with these patterns.
    pub const KMS_KEY_SEGMENT: &'static str = "key/";
    /// Resource-type segment reserved for key aliases. Alias patterns parse and
    /// validate, but requests are always evaluated against `key/<key_id>` strings,
    /// so an alias pattern matches nothing until alias resolution lands.
    pub const KMS_ALIAS_SEGMENT: &'static str = "alias/";

    pub fn is_kms(&self) -> bool {
        matches!(self, Resource::Kms(_))
    }

    pub async fn is_match(&self, resource: &str, conditions: &HashMap<String, Vec<String>>) -> bool {
        self.is_match_with_resolver(resource, conditions, None).await
    }

    pub async fn is_match_with_resolver(
        &self,
        resource: &str,
        conditions: &HashMap<String, Vec<String>>,
        resolver: Option<&dyn PolicyVariableResolver>,
    ) -> bool {
        let pattern = match self {
            Resource::All => return true,
            Resource::S3(s) => s.to_owned(),
            Resource::Kms(s) => s.to_owned(),
        };

        if let Some(pattern) = resolve_resource_tokens(&pattern, conditions, resolver, 0).await {
            let cp = path::clean(resource);
            if cp != "." && resource_tokens_match(&pattern, &cp, false) {
                return true;
            }

            // Keep directory objects distinct from their bare key for prefix
            // matching, while retaining path cleaning and exact-match compatibility.
            let wildcard_resource = if matches!(self, Resource::S3(_)) && resource.ends_with('/') && cp.contains('/') {
                format!("{cp}/")
            } else {
                cp.clone()
            };
            if cp != "." && resource_tokens_match(&pattern, &wildcard_resource, true) {
                return true;
            }
        }

        false
    }

    pub async fn match_resource(&self, resource: &str) -> bool {
        self.is_match(resource, &HashMap::new()).await
    }
}

impl TryFrom<&str> for Resource {
    type Error = Error;
    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        let resource = if value == "*" {
            Resource::All
        } else if let Some(suffix) = value.strip_prefix(Self::S3_PREFIX) {
            Resource::S3(suffix.into())
        } else if let Some(suffix) = value.strip_prefix(Self::KMS_PREFIX) {
            Resource::Kms(suffix.into())
        } else {
            return Err(IamError::InvalidResource("unknown".into(), value.into()).into());
        };

        resource.is_valid()?;
        Ok(resource)
    }
}

impl Validator for Resource {
    type Error = Error;
    fn is_valid(&self) -> std::result::Result<(), Error> {
        match self {
            Self::All => {}
            Self::S3(pattern) => {
                if pattern.is_empty() || pattern.starts_with('/') {
                    return Err(IamError::InvalidResource("s3".into(), pattern.into()).into());
                }
            }
            Self::Kms(pattern) => {
                // A bare `*` matches every key resource; anything else must carry a
                // resource-type segment. Key ids never contain separators (the KMS
                // backends reject them), while alias names may nest ("alias/aws/s3").
                let well_formed = pattern == "*"
                    || pattern
                        .strip_prefix(Self::KMS_KEY_SEGMENT)
                        .is_some_and(|id| !id.is_empty() && !id.contains('/') && !id.contains('\\'))
                    || pattern
                        .strip_prefix(Self::KMS_ALIAS_SEGMENT)
                        .is_some_and(|name| !name.is_empty() && !name.contains('\\'));
                if !well_formed {
                    return Err(IamError::InvalidResource("kms".into(), pattern.into()).into());
                }
            }
        }
        Ok(())
    }
}

impl Serialize for Resource {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Resource::All => serializer.serialize_str("*"),
            Resource::S3(s) => serializer.serialize_str(&format!("{}{}", Self::S3_PREFIX, s)),
            Resource::Kms(s) => serializer.serialize_str(&format!("{}{}", Self::KMS_PREFIX, s)),
        }
    }
}

impl<'de> Deserialize<'de> for Resource {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Resource::try_from(value.as_str()).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use crate::policy::resource::Resource;
    use std::collections::HashMap;
    use test_case::test_case;

    #[test]
    fn large_resource_sets_deduplicate_without_changing_first_seen_order() {
        let unique = (0..40_000)
            .map(|index| format!("arn:aws:s3:::bucket/key-{index}"))
            .collect::<Vec<_>>();
        let mut repeated = unique.clone();
        repeated.extend(unique.iter().rev().cloned());
        let resources: super::ResourceSet = serde_json::from_slice(&serde_json::to_vec(&repeated).expect("encode resources"))
            .expect("large valid resource list parses");
        assert_eq!(resources.len(), unique.len());
        assert_eq!(serde_json::to_value(resources).expect("serialize resources"), serde_json::json!(unique));
        assert!(serde_json::from_str::<super::ResourceSet>(r#"["arn:aws:s3:::bucket/key","invalid"]"#).is_err());
    }

    #[tokio::test]
    async fn resource_variables_are_literal_and_missing_values_do_not_match() {
        use super::KeyName;
        use crate::policy::variables::{VariableContext, VariableResolver};
        let resource = Resource::S3("bucket/${aws:username}/*".to_owned());
        let conditions = HashMap::from([(
            KeyName::COMMON_KEYS
                .iter()
                .find(|key| key.var_name() == "${aws:username}")
                .expect("username common key")
                .name()
                .to_owned(),
            vec!["team*?".to_owned()],
        )]);
        assert!(resource.is_match("bucket/team*?/file", &conditions).await);
        assert!(!resource.is_match("bucket/team-other/file", &conditions).await);
        assert!(!resource.is_match("bucket/${aws:username}/file", &HashMap::new()).await);
        let prefix_resource = Resource::S3("bucket/${aws:username}*".to_owned());
        let empty_conditions = HashMap::from([(
            KeyName::COMMON_KEYS
                .iter()
                .find(|key| key.var_name() == "${aws:username}")
                .expect("username common key")
                .name()
                .to_owned(),
            vec![String::new()],
        )]);
        assert!(!prefix_resource.is_match("bucket/private", &empty_conditions).await);
        let empty_resolver = VariableResolver::new(VariableContext {
            username: Some(String::new()),
            ..Default::default()
        });
        assert!(
            !prefix_resource
                .is_match_with_resolver("bucket/private", &HashMap::new(), Some(&empty_resolver))
                .await
        );
        let resolver = VariableResolver::new(VariableContext {
            username: Some("team*?".to_owned()),
            ..Default::default()
        });
        assert!(
            resource
                .is_match_with_resolver("bucket/team*?/file", &HashMap::new(), Some(&resolver))
                .await
        );
        assert!(
            !resource
                .is_match_with_resolver("bucket/team-other/file", &HashMap::new(), Some(&resolver))
                .await
        );
    }

    #[tokio::test]
    async fn resource_variable_alternatives_do_not_disappear_when_product_exceeds_limit() {
        use super::KeyName;
        let key = KeyName::COMMON_KEYS
            .iter()
            .find(|key| key.var_name() == "${aws:username}")
            .expect("username common key")
            .name()
            .to_owned();
        let values = (0..33).map(|index| format!("user-{index}")).collect();
        let conditions = HashMap::from([(key, values)]);
        let resource = Resource::S3("bucket/${aws:username}/${aws:username}/*".to_owned());
        assert!(resource.is_match("bucket/user-32/user-31/object", &conditions).await);
        assert!(!resource.is_match("bucket/user-32/not-listed/object", &conditions).await);
    }

    #[tokio::test]
    async fn nested_resource_variables_preserve_compatibility_without_glob_injection() {
        use crate::policy::variables::{VariableContext, VariableResolver};
        let resolver = VariableResolver::new(VariableContext {
            claims: Some(HashMap::from([("sub".to_owned(), serde_json::json!("user*"))])),
            ..Default::default()
        });
        let resource = Resource::S3("bucket/${${aws:PrincipalType}-${aws:userid}}/*".to_owned());
        assert!(
            resource
                .is_match_with_resolver("bucket/User-user*/file", &HashMap::new(), Some(&resolver))
                .await
        );
        assert!(
            !resource
                .is_match_with_resolver("bucket/User-user-other/file", &HashMap::new(), Some(&resolver))
                .await
        );
    }

    #[tokio::test]
    async fn directory_exact_match_preserves_literal_wildcard_compatibility() {
        let resource = Resource::S3("bucket/name?".to_owned());
        assert!(resource.is_match("bucket/name?/", &HashMap::new()).await);
    }

    #[test]
    fn authored_wildcard_matcher_agrees_with_existing_matcher() {
        use super::{ResourceToken, resource_tokens_match};
        fn strings(alphabet: &[u8], max_len: usize) -> Vec<String> {
            let mut results = vec![String::new()];
            for _ in 0..max_len {
                let current = results.clone();
                for prefix in current {
                    for &byte in alphabet {
                        results.push(format!("{prefix}{}", char::from(byte)));
                    }
                }
            }
            results.sort();
            results.dedup();
            results
        }
        for pattern in strings(b"ab*?", 4) {
            let tokens: Vec<_> = pattern
                .bytes()
                .map(|byte| match byte {
                    b'*' => ResourceToken::Star,
                    b'?' => ResourceToken::Any,
                    literal => ResourceToken::Literal(literal),
                })
                .collect();
            for name in strings(b"ab", 4) {
                assert_eq!(
                    resource_tokens_match(&tokens, &name, true),
                    crate::policy::utils::wildcard::is_match(&pattern, &name),
                    "pattern={pattern:?} name={name:?}"
                );
            }
        }
    }

    #[test_case("*", "mybucket" => true; "all buckets")]
    #[test_case("*", "mybucket/myobject" => true; "all objects")]
    #[test_case("*", "key/mykey" => true; "all keys")]
    #[test_case("*", "" => true; "unscoped action")]
    #[test_case("arn:aws:s3:::*","mybucket" => true; "1")]
    #[test_case("arn:aws:s3:::*","mybucket/myobject" => true; "2")]
    #[test_case("arn:aws:s3:::mybucket*","mybucket" => true; "3")]
    #[test_case("arn:aws:s3:::mybucket*","mybucket/myobject" => true; "4")]
    #[test_case("arn:aws:s3:::*/*","mybucket/myobject"=> true; "5")]
    #[test_case("arn:aws:s3:::mybucket/*","mybucket/myobject" => true; "6")]
    #[test_case("arn:aws:s3:::mybucket*/myobject","mybucket/myobject" => true; "7")]
    #[test_case("arn:aws:s3:::mybucket*/myobject","mybucket100/myobject" => true; "8")]
    #[test_case("arn:aws:s3:::mybucket?0/2010/photos/*","mybucket20/2010/photos/1.jpg" => true; "9")]
    #[test_case("arn:aws:s3:::mybucket","mybucket" => true; "10")]
    #[test_case("arn:aws:s3:::mybucket?0","mybucket30" => true; "11")]
    #[test_case("arn:aws:s3:::*/*","mybucket" => false; "12")]
    #[test_case("arn:aws:s3:::mybucket/*","mybucket10/myobject" => false; "13")]
    #[test_case("arn:aws:s3:::mybucket?0/2010/photos/*","mybucket0/2010/photos/1.jpg" => false; "14")]
    #[test_case("arn:aws:s3:::mybucket","mybucket/myobject" => false; "15")]
    #[test_case("arn:aws:s3:::attacker-bucket/*","attacker-bucket/../victim-bucket/evil.txt" => false; "16")]
    #[test_case("arn:aws:s3:::attacker-bucket/*","attacker-bucket/safe/../../victim-bucket/evil.txt" => false; "17")]
    #[test_case("arn:aws:s3:::bucket/secret/*","bucket/secret/" => true; "directory prefix")]
    #[test_case("arn:aws:s3:::bucket/secret","bucket/secret/" => true; "directory exact compatibility")]
    #[test_case("arn:aws:s3:::bucket/*","bucket/" => false; "bucket is not a directory object")]
    #[test_case("arn:aws:s3:::attacker-bucket/*","attacker-bucket/../victim-bucket/" => false; "directory traversal")]
    #[test_case("arn:aws:kms:::key/mykey","key/mykey" => true; "kms exact key")]
    #[test_case("arn:aws:kms:::key/mykey","key/otherkey" => false; "kms wrong key")]
    #[test_case("arn:aws:kms:::key/*","key/mykey" => true; "kms key wildcard")]
    #[test_case("arn:aws:kms:::key/app-*","key/app-primary" => true; "kms key prefix wildcard")]
    #[test_case("arn:aws:kms:::key/app-*","key/backup-primary" => false; "kms key prefix mismatch")]
    #[test_case("arn:aws:kms:::key/mykey?","key/mykey1" => true; "kms key question mark")]
    #[test_case("arn:aws:kms:::*","key/mykey" => true; "kms bare star")]
    #[test_case("arn:aws:kms:::key/mykey","key/mykey/../otherkey" => false; "kms traversal cleaned")]
    #[test_case("arn:aws:kms:::alias/myalias","key/myalias" => false; "kms alias never matches key")]
    #[test_case("arn:aws:kms:::alias/*","key/mykey" => false; "kms alias wildcard never matches key")]
    #[test_case("arn:aws:kms:::key/mykey","mykey" => false; "kms bare id lacks key segment")]
    fn test_resource_is_match(resource: &str, object: &str) -> bool {
        let resource: Resource = resource.try_into().unwrap();

        pollster::block_on(resource.is_match(object, &HashMap::new()))
    }

    #[test_case("arn:aws:s3:::mybucket/${jwt:preferred_username}/*", "mybucket/alice/photo.jpg" => true; "jwt variable")]
    #[test_case("arn:aws:s3:::mybucket/${jwt:preferred_username}/*", "mybucket/bob/photo.jpg" => false; "jwt variable other user")]
    #[test_case("arn:aws:s3:::mybucket/${ldap:username}/*", "mybucket/alice-ldap/photo.jpg" => true; "ldap variable")]
    fn test_resource_is_match_substitutes_condition_variables(resource: &str, object: &str) -> bool {
        let resource: Resource = resource.try_into().unwrap();
        let conditions = HashMap::from([
            ("preferred_username".to_string(), vec!["alice".to_string()]),
            ("username".to_string(), vec!["alice-ldap".to_string()]),
        ]);

        pollster::block_on(resource.is_match(object, &conditions))
    }

    #[test]
    fn test_wildcard_resource_serialization_round_trip() {
        use crate::policy::resource::ResourceSet;

        let resource: Resource = serde_json::from_str(r#""*""#).expect("wildcard resource should parse");
        assert_eq!(serde_json::to_string(&resource).expect("resource should serialize"), r#""*""#);

        for json in [r#""*""#, r#"["*"]"#, r#"["*","*"]"#] {
            let set: ResourceSet = serde_json::from_str(json).expect("wildcard resource set should parse");
            assert_eq!(set.len(), 1, "duplicate wildcard resources should be removed");
            let serialized = serde_json::to_string(&set).expect("resource set should serialize");
            assert_eq!(serialized, r#"["*"]"#);
            let round_trip: ResourceSet = serde_json::from_str(&serialized).expect("resource set should parse again");
            assert_eq!(round_trip, set);
        }
    }

    #[test_case(""; "empty resource")]
    #[test_case("**"; "multiple wildcards")]
    #[test_case("*/*"; "unqualified path")]
    #[test_case(" *"; "leading whitespace")]
    #[test_case("arn:aws:unknown:::*"; "unknown service")]
    fn test_invalid_resource_is_rejected(resource: &str) {
        assert!(Resource::try_from(resource).is_err(), "invalid resource must not become a wildcard");
    }

    #[test_case("arn:aws:kms:::key/mykey" => true; "key id parses")]
    #[test_case("arn:aws:kms:::key/*" => true; "key wildcard parses")]
    #[test_case("arn:aws:kms:::key/app-key.v2" => true; "key id with dot parses")]
    #[test_case("arn:aws:kms:::alias/myalias" => true; "alias parses")]
    #[test_case("arn:aws:kms:::alias/aws/s3" => true; "nested alias parses")]
    #[test_case("arn:aws:kms:::*" => true; "bare star parses")]
    #[test_case("arn:aws:kms:::" => false; "empty suffix rejected")]
    #[test_case("arn:aws:kms:::key/" => false; "empty key id rejected")]
    #[test_case("arn:aws:kms:::alias/" => false; "empty alias name rejected")]
    #[test_case("arn:aws:kms:::mykey" => false; "missing resource type segment rejected")]
    #[test_case("arn:aws:kms:::key/a/b" => false; "separator in key id rejected")]
    #[test_case("arn:aws:kms:::key/a\\b" => false; "backslash in key id rejected")]
    #[test_case("arn:aws:kms:us-east-1:123456789012:key/mykey" => false; "region and account form rejected")]
    fn test_kms_resource_parse(resource: &str) -> bool {
        Resource::try_from(resource).is_ok()
    }

    #[test]
    fn test_kms_resource_serialization_round_trip() {
        for raw in [
            "arn:aws:kms:::key/mykey",
            "arn:aws:kms:::key/*",
            "arn:aws:kms:::alias/myalias",
            "arn:aws:kms:::*",
        ] {
            let resource = Resource::try_from(raw).expect("KMS resource should parse");
            assert!(resource.is_kms());

            let json = serde_json::to_string(&resource).expect("KMS resource should serialize");
            assert_eq!(json, format!("\"{raw}\""), "serialization must write back the full ARN");

            let round_trip: Resource = serde_json::from_str(&json).expect("serialized KMS resource should deserialize");
            assert_eq!(round_trip, resource);
        }
    }

    #[test]
    fn test_kms_resource_set_serialization_round_trip() {
        use crate::policy::resource::ResourceSet;

        let set: ResourceSet =
            serde_json::from_str(r#"["arn:aws:kms:::key/app-*","arn:aws:kms:::alias/myalias"]"#).expect("set should parse");
        assert_eq!(set.len(), 2);

        let json = serde_json::to_string(&set).expect("set should serialize");
        assert_eq!(json, r#"["arn:aws:kms:::key/app-*","arn:aws:kms:::alias/myalias"]"#);

        let round_trip: ResourceSet = serde_json::from_str(&json).expect("serialized set should deserialize");
        assert_eq!(round_trip, set);
    }
}
