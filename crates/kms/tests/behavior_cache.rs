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

//! Black-box behavior: the KMS metadata cache.
//!
//! The cache exists to keep `describe_key` off the backend on hot paths. Two
//! properties matter far more than its hit rate:
//!
//! * **It must never outlive the truth it caches.** Every state mutation has to
//!   drop the entry, or the state gate in front of encryption would consult a
//!   stale `Enabled` snapshot and let a disabled key keep minting data keys.
//! * **It must never extend to data keys.** `lib.rs` allows caching stable
//!   master-key metadata and forbids caching generated DEKs, because a DEK is
//!   bound to one object's encryption context. This file asserts the boundary
//!   holds with the cache both enabled and disabled.

mod common;

use common::{TestKms, assert_invalid_operation, ctx};
use rustfs_kms::{
    CancelKeyDeletionRequest, DeleteKeyRequest, DescribeKeyRequest, GenerateDataKeyRequest, KeySpec, KeyState, KmsManager,
};

async fn describe_state(kms: &KmsManager, key_id: &str) -> KeyState {
    kms.describe_key(DescribeKeyRequest {
        key_id: key_id.to_string(),
    })
    .await
    .expect("describe should succeed")
    .key_metadata
    .key_state
}

async fn entry_count(kms: &KmsManager) -> u64 {
    kms.cache_stats().await.expect("cache is enabled").0
}

#[tokio::test]
async fn cache_reporting_follows_the_enable_flag() {
    let enabled = TestKms::local().await;
    let manager = enabled.kms().await;
    assert!(manager.cache_stats().await.is_some(), "a cache-enabled service must report statistics");

    let disabled = TestKms::local_with(|config| config.enable_cache = false).await;
    let disabled_manager = disabled.kms().await;
    assert!(
        disabled_manager.cache_stats().await.is_none(),
        "a cache-disabled service must report no statistics at all"
    );

    // Clearing a cache that does not exist is a no-op, not an error.
    disabled_manager
        .clear_cache()
        .await
        .expect("clear_cache must succeed even when caching is off");
}

#[tokio::test]
async fn cache_population_and_clearing_are_observable() {
    let kms = TestKms::local().await;
    let manager = kms.kms().await;

    assert_eq!(entry_count(&manager).await, 0, "a fresh service caches nothing");

    // Creating a key populates the cache eagerly.
    kms.create_key("cached-a").await;
    assert_eq!(entry_count(&manager).await, 1, "create_key caches the new key's metadata");

    kms.create_key("cached-b").await;
    assert_eq!(entry_count(&manager).await, 2);

    // Repeated describes of a cached key add no entries.
    for _ in 0..5 {
        assert_eq!(describe_state(&manager, "cached-a").await, KeyState::Enabled);
    }
    assert_eq!(entry_count(&manager).await, 2, "repeat describes must not grow the cache");

    // A failed describe must not create a negative-cache entry.
    assert!(
        manager
            .describe_key(DescribeKeyRequest {
                key_id: "never-existed".to_string(),
            })
            .await
            .is_err()
    );
    assert_eq!(entry_count(&manager).await, 2, "a missing key must not be cached");

    manager.clear_cache().await.expect("clear should succeed");
    assert_eq!(entry_count(&manager).await, 0, "clear_cache must empty the cache");

    // Clearing does not lose data: the backend is still the source of truth.
    assert_eq!(describe_state(&manager, "cached-a").await, KeyState::Enabled);
    assert_eq!(entry_count(&manager).await, 1, "a describe after clearing repopulates");
}

/// A stale cache entry would silently defeat the state gate, so every mutation
/// path is checked: the gate must see the post-mutation state, and the cached
/// entry must be gone rather than merely overwritten later.
#[tokio::test]
async fn every_state_mutation_invalidates_the_cached_entry() {
    let kms = TestKms::local().await;
    let manager = kms.kms().await;
    let key_id = kms.create_key("invalidated").await;
    let context = ctx(&[("bucket", "cache-behavior")]);

    let generate = || GenerateDataKeyRequest {
        key_id: key_id.clone(),
        key_spec: KeySpec::Aes256,
        encryption_context: context.clone(),
    };

    // Warm the cache so a missing invalidation would be observable.
    assert_eq!(describe_state(&manager, &key_id).await, KeyState::Enabled);
    manager
        .generate_data_key(generate())
        .await
        .expect("Enabled permits generation");

    // Invalidation is asserted behaviourally, not by counting entries: moka's
    // `entry_count` is eventually consistent, so a count is not a reliable
    // observable. What must hold is that the next read sees backend truth and
    // the state gate acts on it.
    manager.disable_key(&key_id).await.expect("disable");
    assert_eq!(
        describe_state(&manager, &key_id).await,
        KeyState::Disabled,
        "the read after a disable must not be served from the pre-mutation snapshot"
    );
    assert_invalid_operation(manager.generate_data_key(generate()).await, "is disabled");

    manager.enable_key(&key_id).await.expect("enable");
    assert_eq!(
        describe_state(&manager, &key_id).await,
        KeyState::Enabled,
        "the read after an enable must not be served from the Disabled snapshot"
    );
    manager
        .generate_data_key(generate())
        .await
        .expect("re-enabling must restore generation");

    manager
        .delete_key(DeleteKeyRequest {
            key_id: key_id.clone(),
            pending_window_in_days: Some(7),
            force_immediate: None,
        })
        .await
        .expect("schedule deletion");
    assert_eq!(
        describe_state(&manager, &key_id).await,
        KeyState::PendingDeletion,
        "the read after a scheduled deletion must not be served from the Enabled snapshot"
    );
    assert_invalid_operation(manager.generate_data_key(generate()).await, "pending deletion");

    manager
        .cancel_key_deletion(CancelKeyDeletionRequest { key_id: key_id.clone() })
        .await
        .expect("cancel deletion");
    assert_eq!(
        describe_state(&manager, &key_id).await,
        KeyState::Enabled,
        "the cache must not resurrect the PendingDeletion snapshot after a cancel"
    );
    manager
        .generate_data_key(generate())
        .await
        .expect("a cancelled key must generate again");
}

#[tokio::test]
async fn a_destroyed_key_cannot_be_served_from_cache() {
    let kms = TestKms::local().await;
    let manager = kms.kms().await;
    let key_id = kms.create_key("destroyed").await;

    // Warm the cache, then destroy the key outright.
    assert_eq!(describe_state(&manager, &key_id).await, KeyState::Enabled);
    manager
        .delete_key(DeleteKeyRequest {
            key_id: key_id.clone(),
            pending_window_in_days: None,
            force_immediate: Some(true),
        })
        .await
        .expect("forced deletion");

    assert!(
        manager
            .describe_key(DescribeKeyRequest { key_id: key_id.clone() })
            .await
            .is_err(),
        "a destroyed key must not be served from the cache"
    );
    assert!(
        manager
            .generate_data_key(GenerateDataKeyRequest {
                key_id: key_id.clone(),
                key_spec: KeySpec::Aes256,
                encryption_context: ctx(&[("bucket", "cache-behavior")]),
            })
            .await
            .is_err(),
        "a destroyed key must not keep minting data keys through a cached snapshot"
    );
}

/// The invariant `lib.rs` states outright: metadata may be cached, data keys
/// may not. Asserted with the cache both on and off so a caching change cannot
/// quietly extend to DEKs.
#[tokio::test]
async fn caching_never_extends_to_data_keys() {
    for enable_cache in [true, false] {
        let kms = TestKms::local_with(|config| config.enable_cache = enable_cache).await;
        let manager = kms.kms().await;
        let key_id = kms.create_key("dek-freshness").await;
        let context = ctx(&[("bucket", "cache-behavior"), ("object", "same.bin")]);

        // Warm the metadata cache first: if DEK generation ever consulted it,
        // this is where a reused key would come from.
        assert_eq!(describe_state(&manager, &key_id).await, KeyState::Enabled);

        let mut seen_plaintext = Vec::new();
        let mut seen_ciphertext = Vec::new();
        for _ in 0..16 {
            let dek = manager
                .generate_data_key(GenerateDataKeyRequest {
                    key_id: key_id.clone(),
                    key_spec: KeySpec::Aes256,
                    encryption_context: context.clone(),
                })
                .await
                .expect("generate should succeed");
            assert!(
                !seen_plaintext.contains(&dek.plaintext_key),
                "cache={enable_cache}: a data key was reused across calls with identical inputs"
            );
            assert!(
                !seen_ciphertext.contains(&dek.ciphertext_blob),
                "cache={enable_cache}: a wrapped data key was reused across calls with identical inputs"
            );
            seen_plaintext.push(dek.plaintext_key);
            seen_ciphertext.push(dek.ciphertext_blob);
        }
    }
}

#[tokio::test]
async fn disabling_the_cache_changes_no_observable_behavior() {
    // Same script under both settings: results must be identical apart from
    // the statistics, so the cache is a pure performance concern.
    for enable_cache in [true, false] {
        let kms = TestKms::local_with(|config| config.enable_cache = enable_cache).await;
        let manager = kms.kms().await;
        let key_id = kms.create_key("parity").await;

        assert_eq!(describe_state(&manager, &key_id).await, KeyState::Enabled, "cache={enable_cache}");
        manager.disable_key(&key_id).await.expect("disable");
        assert_eq!(describe_state(&manager, &key_id).await, KeyState::Disabled, "cache={enable_cache}");
        manager.enable_key(&key_id).await.expect("enable");
        assert_eq!(describe_state(&manager, &key_id).await, KeyState::Enabled, "cache={enable_cache}");

        assert!(
            manager
                .describe_key(DescribeKeyRequest {
                    key_id: "absent".to_string(),
                })
                .await
                .is_err(),
            "cache={enable_cache}: an unknown key is an error either way"
        );
    }
}

/// What `cache_stats` actually returns: an entry count and nothing else.
///
/// The tuple is `(entry_count, 0)` — the second element is a hard-coded zero
/// because moka exposes no miss counter unless statistics are enabled. This is
/// asserted as the real contract so callers do not compute a hit rate from it.
/// The doc comment on `KmsCache::stats` still claims "(hit count, miss count)"
/// and is wrong; that is a documentation fix, not a behavioural one — nothing
/// at runtime depends on the second element.
#[tokio::test]
async fn cache_stats_returns_an_entry_count_and_no_hit_or_miss_data() {
    let kms = TestKms::local().await;
    let manager = kms.kms().await;
    kms.create_key("stats-a").await;
    kms.create_key("stats-b").await;

    // Generate plenty of traffic that would move a real hit/miss counter.
    for _ in 0..10 {
        assert_eq!(describe_state(&manager, "stats-a").await, KeyState::Enabled);
        assert!(
            manager
                .describe_key(DescribeKeyRequest {
                    key_id: "absent".to_string(),
                })
                .await
                .is_err()
        );
    }

    let (first, second) = manager.cache_stats().await.expect("cache is enabled");
    assert_eq!(first, 2, "the first element is the number of cached entries, not hits");
    assert_eq!(second, 0, "the second element is always zero: no hit or miss data is collected");
}
