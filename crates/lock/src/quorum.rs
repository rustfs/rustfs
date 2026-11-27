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

//! Namespace-lock quorum sizes.
//!
//! Exclusive locks use a strict majority of the lock clients in the set.
//! Shared locks use `client_count - client_count / 2`. Callers pass the
//! client domain they actually vote with (one client per distinct node).

/// Shared / read-lock quorum for `client_count` lock clients.
pub fn shared_lock_quorum(client_count: usize) -> usize {
    if client_count <= 1 {
        1
    } else {
        client_count - (client_count / 2)
    }
}

/// Exclusive / write-lock quorum for `client_count` lock clients.
pub fn exclusive_lock_quorum(client_count: usize) -> usize {
    if client_count > 1 { (client_count / 2) + 1 } else { 1 }
}

#[cfg(test)]
mod tests {
    use super::{exclusive_lock_quorum, shared_lock_quorum};

    #[test]
    fn official_dst_node_counts_match_lock_quorum() {
        assert_eq!(exclusive_lock_quorum(4), 3);
        assert_eq!(shared_lock_quorum(4), 2);
        assert_eq!(exclusive_lock_quorum(3), 2);
        assert_eq!(shared_lock_quorum(3), 2);
        assert_eq!(exclusive_lock_quorum(1), 1);
        assert_eq!(shared_lock_quorum(1), 1);
    }
}
