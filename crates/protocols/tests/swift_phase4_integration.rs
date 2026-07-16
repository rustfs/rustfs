// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Integration tests for Swift API Phase 4 features
//!
//! These tests validate the integration between different Swift API modules,
//! ensuring they work together correctly.

#[cfg(feature = "swift")]
mod swift_integration {
    use rustfs_protocols::swift::*;
    use std::collections::HashMap;

    #[test]
    fn test_phase4_modules_compile() {
        // This test ensures all Phase 4 modules are properly integrated
        // Actual integration test would require full runtime with storage
    }

    #[test]
    fn test_symlink_with_expiration_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("x-object-symlink-target".to_string(), "original.txt".to_string());
        metadata.insert("x-delete-at".to_string(), "1740000000".to_string());

        // Both features should coexist in metadata
        assert!(symlink::is_symlink(&metadata));
        let target = symlink::get_symlink_target(&metadata).unwrap();
        assert!(target.is_some());

        let delete_at = metadata.get("x-delete-at").unwrap();
        let parsed = expiration::parse_delete_at(delete_at).unwrap();
        assert_eq!(parsed, 1740000000);
    }
}
