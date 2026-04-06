// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Separate binary: `LazyLock` for Wasabi mode must read `true` on first use.

use rustfs_ecstore::mint_new_object_version_id;
use rustfs_filemeta::is_strict_wasabi_create_version_id;
use temp_env::with_var;

#[test]
fn mint_matches_strict_wasabi_shape_when_mode_on() {
    with_var("RUSTFS_WASABI_VERSION_IDS", Some("true"), || {
        let id = mint_new_object_version_id();
        let s = id.to_string();
        assert_eq!(s.len(), 32, "{s}");
        assert!(is_strict_wasabi_create_version_id(&s), "minted id should pass strict check: {s}");
    });
}
