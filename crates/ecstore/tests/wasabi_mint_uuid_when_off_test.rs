// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Separate binary: `LazyLock` must read `false` on first use (UUID mint).

use rustfs_ecstore::mint_new_object_version_id;
use rustfs_filemeta::S3VersionId;
use temp_env::with_var;

#[test]
fn mint_is_uuid_when_mode_off() {
    with_var("RUSTFS_WASABI_VERSION_IDS", Some("false"), || {
        let id = mint_new_object_version_id();
        assert!(matches!(id, S3VersionId::Uuid(_)), "expected UUID mint when Wasabi mode off, got {id:?}");
    });
}
