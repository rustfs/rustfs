// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Optional, fixed-cardinality attribution of scanner size-call attempts on stored paths.

use std::sync::{Arc, OnceLock};

use serde::Deserialize;
use sha2::{Digest, Sha256};

const ENV_PREFIX_ATTRIBUTION: &str = "RUSTFS_SCANNER_PREFIX_ATTRIBUTION";
const MAX_CONFIG_BYTES: usize = 4096;
const MAX_PREFIX_BYTES: usize = 1024;
const VISITS: &str = "rustfs_scanner_object_metadata_prefix_visits_total";
const SUCCESSES: &str = "rustfs_scanner_object_metadata_prefix_successes_total";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    bucket: String,
    hot_prefix: String,
    cold_prefix: String,
}

pub(super) struct Scope {
    bucket: String,
    hot_prefix: String,
    cold_prefix: String,
    id: String,
}

impl Scope {
    #[cfg(test)]
    pub(super) fn id(&self) -> &str {
        &self.id
    }

    fn parse(raw: &str) -> Option<Self> {
        if raw.len() > MAX_CONFIG_BYTES {
            return None;
        }
        let input: Input = serde_json::from_str(raw).ok()?;
        if input.bucket.is_empty()
            || input.bucket.len() > 63
            || input.bucket.contains('/')
            || input.bucket.chars().any(char::is_control)
            || crate::is_reserved_or_invalid_bucket(&input.bucket, false)
            || !valid_prefix(&input.hot_prefix)
            || !valid_prefix(&input.cold_prefix)
            || input.hot_prefix.starts_with(&input.cold_prefix)
            || input.cold_prefix.starts_with(&input.hot_prefix)
        {
            return None;
        }
        // Length prefixes prevent ambiguous identities. The configuration is
        // never used as a metric label, only this stable digest is exported.
        let mut hash = Sha256::new();
        for value in [
            "scanner-prefix-attribution-v2",
            &input.bucket,
            &input.hot_prefix,
            &input.cold_prefix,
        ] {
            hash.update(u32::try_from(value.len()).ok()?.to_be_bytes());
            hash.update(value.as_bytes());
        }
        let mut id = String::with_capacity(64);
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for byte in hash.finalize() {
            id.push(HEX[(byte >> 4) as usize] as char);
            id.push(HEX[(byte & 15) as usize] as char);
        }
        Some(Self {
            bucket: input.bucket,
            hot_prefix: input.hot_prefix,
            cold_prefix: input.cold_prefix,
            id,
        })
    }
}

fn valid_prefix(value: &str) -> bool {
    if value.is_empty()
        || value.len() > MAX_PREFIX_BYTES
        || !value.ends_with('/')
        || value.starts_with('/')
        || value.chars().any(char::is_control)
    {
        return false;
    }
    value[..value.len() - 1]
        .split('/')
        .all(|part| !part.is_empty() && part != "." && part != "..")
}

#[cfg(test)]
tokio::task_local! {
    pub(super) static TEST_SCOPE: Option<Arc<Scope>>;
}

fn scope() -> Option<Arc<Scope>> {
    #[cfg(test)]
    if let Ok(scope) = TEST_SCOPE.try_with(Clone::clone) {
        return scope;
    }
    static SCOPE: OnceLock<Option<Arc<Scope>>> = OnceLock::new();
    SCOPE
        .get_or_init(|| {
            std::env::var(ENV_PREFIX_ATTRIBUTION)
                .ok()
                .and_then(|raw| Scope::parse(&raw))
                .map(Arc::new)
        })
        .clone()
}

pub(super) struct VisitAttribution {
    scope: Arc<Scope>,
    hot: metrics::Counter,
    cold: metrics::Counter,
    other: metrics::Counter,
    ambiguous: metrics::Counter,
    hot_success: metrics::Counter,
    cold_success: metrics::Counter,
    other_success: metrics::Counter,
    ambiguous_success: metrics::Counter,
}

impl VisitAttribution {
    fn new(scope: Arc<Scope>, bucket: &str, disk: &str) -> Option<Self> {
        if scope.bucket != bucket {
            return None;
        }
        let counter = |name: &'static str, class: &'static str| metrics::counter!(name, "disk" => disk.to_string(), "class" => class, "scope" => scope.id.clone());
        let hot = counter(VISITS, "hot");
        let cold = counter(VISITS, "cold");
        let other = counter(VISITS, "other");
        let ambiguous = counter(VISITS, "ambiguous");
        let hot_success = counter(SUCCESSES, "hot");
        let cold_success = counter(SUCCESSES, "cold");
        let other_success = counter(SUCCESSES, "other");
        let ambiguous_success = counter(SUCCESSES, "ambiguous");
        // Make an observed zero distinguishable from a missing series.
        for counter in [
            &hot,
            &cold,
            &other,
            &ambiguous,
            &hot_success,
            &cold_success,
            &other_success,
            &ambiguous_success,
        ] {
            counter.increment(0);
        }
        Some(Self {
            scope,
            hot,
            cold,
            other,
            ambiguous,
            hot_success,
            cold_success,
            other_success,
            ambiguous_success,
        })
    }

    fn record_inner(&self, bucket: &str, object_path: &str, success: bool) {
        if bucket != self.scope.bucket {
            return;
        }
        // An encoded directory marker and a literal suffix can share the
        // same storage path. Never guess which original key produced it.
        let counter = if object_path
            .split(|ch| ch == '/' || (cfg!(windows) && ch == '\\'))
            .any(|component| rustfs_utils::path::has_suffix(component, rustfs_utils::path::GLOBAL_DIR_SUFFIX))
        {
            if success { &self.ambiguous_success } else { &self.ambiguous }
        } else if object_path.starts_with(&self.scope.hot_prefix) {
            if success { &self.hot_success } else { &self.hot }
        } else if object_path.starts_with(&self.scope.cold_prefix) {
            if success { &self.cold_success } else { &self.cold }
        } else if success {
            &self.other_success
        } else {
            &self.other
        };
        counter.increment(1);
    }

    pub(super) fn record(&self, bucket: &str, object_path: &str) {
        self.record_inner(bucket, object_path, false);
    }

    pub(super) fn record_success(&self, bucket: &str, object_path: &str) {
        self.record_inner(bucket, object_path, true);
    }
}

pub(super) fn configured_for(bucket: &str, disk: &str) -> Option<VisitAttribution> {
    VisitAttribution::new(scope()?, bucket, disk)
}

#[cfg(test)]
pub(super) fn test_scope(raw: &str) -> Option<Arc<Scope>> {
    Scope::parse(raw).map(Arc::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes_require_disjoint_canonical_nonempty_prefixes() {
        let valid = r#"{"bucket":"bucket","hot_prefix":"hot/","cold_prefix":"cold/"}"#;
        let scope = Scope::parse(valid).expect("valid scope");
        assert_eq!(scope.id, "48a1cfe7ac0b0e2e0e1a1df5285bf0b1ff2261c9fd17eb9e9acfb53ea1d540ed");
        assert_eq!(scope.id, Scope::parse(valid).expect("same scope").id);
        let changed = r#"{"bucket":"bucket","hot_prefix":"other/","cold_prefix":"cold/"}"#;
        assert_ne!(scope.id, Scope::parse(changed).expect("different scope").id);
        for invalid in [
            r#"{"bucket":"bucket","hot_prefix":"hot","cold_prefix":"cold/"}"#,
            r#"{"bucket":"bucket","hot_prefix":"hot/","cold_prefix":"hot/deep/"}"#,
            r#"{"bucket":"bucket","hot_prefix":"/hot/","cold_prefix":"cold/"}"#,
            r#"{"bucket":"bucket","hot_prefix":"hot//nested/","cold_prefix":"cold/"}"#,
            r#"{"bucket":"bucket","hot_prefix":"../hot/","cold_prefix":"cold/"}"#,
            r#"{"bucket":"bucket","hot_prefix":"hot/","cold_prefix":"cold/","extra":0}"#,
            r#"{"bucket":"bucket","hot_prefix":"hot/","hot_prefix":"other/","cold_prefix":"cold/"}"#,
        ] {
            assert!(Scope::parse(invalid).is_none(), "invalid scope accepted");
        }
        assert!(Scope::parse(&"x".repeat(MAX_CONFIG_BYTES + 1)).is_none());
    }
}
