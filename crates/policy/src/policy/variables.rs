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

use lru::LruCache;
use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use time::OffsetDateTime;

/// Context information for variable resolution
#[derive(Debug, Clone)]
pub struct VariableContext {
    pub is_https: bool,
    pub source_ip: Option<String>,
    pub account_id: Option<String>,
    pub region: Option<String>,
    pub username: Option<String>,
    pub claims: Option<HashMap<String, Value>>,
    pub conditions: HashMap<String, Vec<String>>,
    pub custom_variables: HashMap<String, String>,
}

impl VariableContext {
    pub fn new() -> Self {
        Self {
            is_https: false,
            source_ip: None,
            account_id: None,
            region: None,
            username: None,
            claims: None,
            conditions: HashMap::new(),
            custom_variables: HashMap::new(),
        }
    }
}

impl Default for VariableContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Variable resolution cache
struct CachedVariable {
    value: String,
    timestamp: Instant,
    is_dynamic: bool,
}

pub struct VariableResolverCache {
    /// LRU cache storing resolved results
    cache: LruCache<String, CachedVariable>,
    /// Cache expiration time
    ttl: Duration,
}

impl VariableResolverCache {
    pub fn new(capacity: usize, ttl_seconds: u64) -> Self {
        Self {
            cache: LruCache::new(usize::from(NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(100).unwrap()))),
            ttl: Duration::from_secs(ttl_seconds),
        }
    }

    pub fn get(&mut self, key: &str) -> Option<String> {
        if let Some(cached) = self.cache.get(key) {
            // Check if expired
            if !cached.is_dynamic && cached.timestamp.elapsed() < self.ttl {
                return Some(cached.value.clone());
            }
        }
        None
    }

    pub fn put(&mut self, key: String, value: String, is_dynamic: bool) {
        let cached = CachedVariable {
            value,
            timestamp: Instant::now(),
            is_dynamic,
        };
        self.cache.put(key, cached);
    }

    pub fn clear(&mut self) {
        self.cache.clear();
    }
}

/// Cached dynamic AWS variable resolver
pub struct CachedAwsVariableResolver {
    inner: VariableResolver,
    cache: RefCell<VariableResolverCache>,
}

impl CachedAwsVariableResolver {
    pub fn new(context: VariableContext) -> Self {
        Self {
            inner: VariableResolver::new(context),
            cache: RefCell::new(VariableResolverCache::new(100, 300)), // 100 entries, 5 minutes expiration
        }
    }
}

impl PolicyVariableResolver for CachedAwsVariableResolver {
    fn resolve(&self, variable_name: &str) -> Option<String> {
        if self.is_dynamic(variable_name) {
            return self.inner.resolve(variable_name);
        }

        if let Some(cached) = self.cache.borrow_mut().get(variable_name) {
            return Some(cached);
        }

        let value = self.inner.resolve(variable_name)?;

        self.cache.borrow_mut().put(variable_name.to_string(), value.clone(), false);

        Some(value)
    }

    fn resolve_multiple(&self, variable_name: &str) -> Option<Vec<String>> {
        if self.is_dynamic(variable_name) {
            return self.inner.resolve_multiple(variable_name);
        }

        self.inner.resolve_multiple(variable_name)
    }

    fn is_dynamic(&self, variable_name: &str) -> bool {
        self.inner.is_dynamic(variable_name)
    }
}

/// Policy variable resolver trait
pub trait PolicyVariableResolver {
    fn resolve(&self, variable_name: &str) -> Option<String>;
    fn resolve_multiple(&self, variable_name: &str) -> Option<Vec<String>> {
        self.resolve(variable_name).map(|s| vec![s])
    }
    fn is_dynamic(&self, variable_name: &str) -> bool;
}

/// AWS variable resolver
pub struct VariableResolver {
    context: VariableContext,
}

impl VariableResolver {
    pub fn new(context: VariableContext) -> Self {
        Self { context }
    }

    fn get_claim_as_strings(&self, claim_name: &str) -> Option<Vec<String>> {
        self.context
            .claims
            .as_ref()
            .and_then(|claims| claims.get(claim_name))
            .and_then(|value| match value {
                Value::String(s) => Some(vec![s.clone()]),
                Value::Array(arr) => Some(
                    arr.iter()
                        .filter_map(|item| match item {
                            Value::String(s) => Some(s.clone()),
                            Value::Number(n) => Some(n.to_string()),
                            Value::Bool(b) => Some(b.to_string()),
                            _ => None,
                        })
                        .collect(),
                ),
                Value::Number(n) => Some(vec![n.to_string()]),
                Value::Bool(b) => Some(vec![b.to_string()]),
                _ => None,
            })
    }

    fn resolve_username(&self) -> Option<String> {
        self.context.username.clone()
    }

    fn resolve_userid(&self) -> Option<String> {
        // Check claims for sub or parent
        if let Some(claims) = &self.context.claims {
            if let Some(sub) = claims.get("sub").and_then(|v| v.as_str()) {
                return Some(sub.to_string());
            }

            if let Some(parent) = claims.get("parent").and_then(|v| v.as_str()) {
                return Some(parent.to_string());
            }
        }

        None
    }

    fn resolve_principal_type(&self) -> String {
        if let Some(claims) = &self.context.claims {
            if claims.contains_key("roleArn") {
                return "AssumedRole".to_string();
            }

            if claims.contains_key("parent") && claims.contains_key("sa-policy") {
                return "ServiceAccount".to_string();
            }
        }

        "User".to_string()
    }

    fn resolve_secure_transport(&self) -> String {
        if self.context.is_https { "true" } else { "false" }.to_string()
    }

    fn resolve_current_time(&self) -> String {
        let now = OffsetDateTime::now_utc();
        now.format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| now.to_string())
    }

    fn resolve_epoch_time(&self) -> String {
        OffsetDateTime::now_utc().unix_timestamp().to_string()
    }

    fn resolve_account_id(&self) -> Option<String> {
        self.context.account_id.clone()
    }

    fn resolve_region(&self) -> Option<String> {
        self.context.region.clone()
    }

    fn resolve_source_ip(&self) -> Option<String> {
        self.context.source_ip.clone()
    }

    fn resolve_custom_variable(&self, variable_name: &str) -> Option<String> {
        let custom_key = variable_name.strip_prefix("custom:")?;
        self.context.custom_variables.get(custom_key).cloned()
    }
}

impl PolicyVariableResolver for VariableResolver {
    fn resolve(&self, variable_name: &str) -> Option<String> {
        match variable_name {
            "aws:username" => self.resolve_username(),
            "aws:userid" => self.resolve_userid(),
            "aws:PrincipalType" => Some(self.resolve_principal_type()),
            "aws:SecureTransport" => Some(self.resolve_secure_transport()),
            "aws:CurrentTime" => Some(self.resolve_current_time()),
            "aws:EpochTime" => Some(self.resolve_epoch_time()),
            "aws:AccountId" => self.resolve_account_id(),
            "aws:Region" => self.resolve_region(),
            "aws:SourceIp" => self.resolve_source_ip(),
            _ => {
                // Handle custom:* variables
                if variable_name.starts_with("custom:") {
                    self.resolve_custom_variable(variable_name)
                } else {
                    None
                }
            }
        }
    }

    fn resolve_multiple(&self, variable_name: &str) -> Option<Vec<String>> {
        match variable_name {
            "aws:username" => {
                // Check context.username
                if let Some(ref username) = self.context.username {
                    Some(vec![username.clone()])
                } else {
                    None
                }
            }
            "aws:userid" => {
                // Check claims for sub or parent
                self.get_claim_as_strings("sub")
                    .or_else(|| self.get_claim_as_strings("parent"))
            }
            _ => self.resolve(variable_name).map(|s| vec![s]),
        }
    }

    fn is_dynamic(&self, variable_name: &str) -> bool {
        matches!(variable_name, "aws:CurrentTime" | "aws:EpochTime")
    }
}

/// Dynamically resolve AWS variables
pub fn resolve_aws_variables(pattern: &str, resolver: &dyn PolicyVariableResolver) -> Vec<String> {
    let mut results = vec![pattern.to_string()];

    let mut changed = true;
    let max_iterations = 10; // Prevent infinite loops
    let mut iteration = 0;

    while changed && iteration < max_iterations {
        changed = false;
        iteration += 1;

        let mut new_results = Vec::new();
        for result in &results {
            let resolved = resolve_single_pass(result, resolver);
            if resolved.len() > 1 || (resolved.len() == 1 && &resolved[0] != result) {
                changed = true;
            }
            new_results.extend(resolved);
        }

        // Remove duplicates while preserving order
        results.clear();
        let mut seen = std::collections::HashSet::new();
        for result in new_results {
            if seen.insert(result.clone()) {
                results.push(result);
            }
        }
    }

    results
}

/// Single pass resolution of variables in a string
fn resolve_single_pass(pattern: &str, resolver: &dyn PolicyVariableResolver) -> Vec<String> {
    // Find all ${...} format variables
    let mut results = vec![pattern.to_string()];

    // Process each result string
    let mut i = 0;
    while i < results.len() {
        let mut start = 0;
        let mut modified = false;

        // Find variables in current string
        while let Some(pos) = results[i][start..].find("${") {
            let actual_pos = start + pos;

            // Find the matching closing brace, taking into account nested braces
            let mut brace_count = 1;
            let mut end_pos = actual_pos + 2; // Start after "${"

            while end_pos < results[i].len() && brace_count > 0 {
                match results[i].chars().nth(end_pos).unwrap() {
                    '{' => brace_count += 1,
                    '}' => brace_count -= 1,
                    _ => {}
                }
                if brace_count > 0 {
                    end_pos += 1;
                }
            }

            if brace_count == 0 {
                let var_name = &results[i][actual_pos + 2..end_pos];

                // Check if this is a nested variable (contains ${...} inside)
                if var_name.contains("${") {
                    // For nested variables like ${${a}-${b}}, we need to resolve the inner variables first
                    // Then use the resolved result as a new variable to resolve
                    let resolved_inner = resolve_aws_variables(var_name, resolver);
                    let mut new_results = Vec::new();

                    for resolved_var_name in resolved_inner {
                        let prefix = &results[i][..actual_pos];
                        let suffix = &results[i][end_pos + 1..];
                        new_results.push(format!("{prefix}{resolved_var_name}{suffix}"));
                    }

                    if !new_results.is_empty() {
                        // Update result set
                        results.splice(i..i + 1, new_results);
                        modified = true;
                        break;
                    } else {
                        // If we couldn't resolve the nested variable, keep the original
                        start = end_pos + 1;
                    }
                } else {
                    // Regular variable resolution
                    if let Some(values) = resolver.resolve_multiple(var_name) {
                        if !values.is_empty() {
                            // If there are multiple values, create a new result for each value
                            let mut new_results = Vec::new();
                            let prefix = &results[i][..actual_pos];
                            let suffix = &results[i][end_pos + 1..];

                            for value in values {
                                new_results.push(format!("{prefix}{value}{suffix}"));
                            }

                            results.splice(i..i + 1, new_results);
                            modified = true;
                            break;
                        } else {
                            // Variable resolved to empty, just remove the variable placeholder
                            let mut new_results = Vec::new();
                            let prefix = &results[i][..actual_pos];
                            let suffix = &results[i][end_pos + 1..];
                            new_results.push(format!("{prefix}{suffix}"));

                            results.splice(i..i + 1, new_results);
                            modified = true;
                            break;
                        }
                    } else {
                        // Variable not found, skip
                        start = end_pos + 1;
                    }
                }
            } else {
                // No matching closing brace found, break loop
                break;
            }
        }

        if !modified {
            i += 1;
        }
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::collections::HashMap;

    #[test]
    fn test_resolve_aws_variables_with_username() {
        let mut context = VariableContext::new();
        context.username = Some("testuser".to_string());

        let resolver = VariableResolver::new(context);

        let result = resolve_aws_variables("${aws:username}-bucket", &resolver);
        assert_eq!(result, vec!["testuser-bucket".to_string()]);
    }

    #[test]
    fn test_resolve_aws_variables_with_userid() {
        let mut claims = HashMap::new();
        claims.insert("sub".to_string(), Value::String("AIDACKCEVSQ6C2EXAMPLE".to_string()));

        let mut context = VariableContext::new();
        context.claims = Some(claims);

        let resolver = VariableResolver::new(context);

        let result = resolve_aws_variables("${aws:userid}-bucket", &resolver);
        assert_eq!(result, vec!["AIDACKCEVSQ6C2EXAMPLE-bucket".to_string()]);
    }

    #[test]
    fn test_resolve_aws_variables_with_multiple_variables() {
        let mut claims = HashMap::new();
        claims.insert("sub".to_string(), Value::String("AIDACKCEVSQ6C2EXAMPLE".to_string()));

        let mut context = VariableContext::new();
        context.claims = Some(claims);
        context.username = Some("testuser".to_string());

        let resolver = VariableResolver::new(context);

        let result = resolve_aws_variables("${aws:username}-${aws:userid}-bucket", &resolver);
        assert_eq!(result, vec!["testuser-AIDACKCEVSQ6C2EXAMPLE-bucket".to_string()]);
    }

    #[test]
    fn test_resolve_aws_variables_no_variables() {
        let context = VariableContext::new();
        let resolver = VariableResolver::new(context);

        let result = resolve_aws_variables("test-bucket", &resolver);
        assert_eq!(result, vec!["test-bucket".to_string()]);
    }
}
