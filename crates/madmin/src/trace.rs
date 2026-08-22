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

use serde::{Deserialize, Serialize};

/// Bitflag helper for service trace categories.
///
/// Each variant occupies a single bit so that a `TraceType` value can represent
/// an arbitrary combination of categories via bitwise OR.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct TraceType(u64);

impl TraceType {
    pub const OS: TraceType = TraceType(1 << 0);
    pub const STORAGE: TraceType = TraceType(1 << 1);
    pub const S3: TraceType = TraceType(1 << 2);
    pub const INTERNAL: TraceType = TraceType(1 << 3);
    pub const SCANNER: TraceType = TraceType(1 << 4);
    pub const DECOMMISSION: TraceType = TraceType(1 << 5);
    pub const HEALING: TraceType = TraceType(1 << 6);
    pub const BATCH_REPLICATION: TraceType = TraceType(1 << 7);
    pub const BATCH_KEY_ROTATION: TraceType = TraceType(1 << 8);
    pub const BATCH_EXPIRE: TraceType = TraceType(1 << 9);
    pub const REBALANCE: TraceType = TraceType(1 << 10);
    pub const REPLICATION_RESYNC: TraceType = TraceType(1 << 11);
    pub const BOOTSTRAP: TraceType = TraceType(1 << 12);
    pub const FTP: TraceType = TraceType(1 << 13);
    pub const ILM: TraceType = TraceType(1 << 14);

    /// All trace categories combined. Must be updated when adding new variants.
    pub const ALL: TraceType = TraceType((1 << 15) - 1);

    pub fn new(t: u64) -> Self {
        Self(t)
    }

    pub fn contains(&self, x: &TraceType) -> bool {
        (self.0 & x.0) == x.0
    }

    pub fn overlaps(&self, x: &TraceType) -> bool {
        (self.0 & x.0) != 0
    }

    pub fn single_type(&self) -> bool {
        self.0.count_ones() == 1
    }

    pub fn merge(&mut self, other: &TraceType) {
        self.0 |= other.0
    }

    pub fn set_if(&mut self, b: bool, other: &TraceType) {
        if b {
            self.0 |= other.0
        }
    }

    pub fn mask(&self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_type_contains_and_overlaps() {
        let mut combined = TraceType::default();
        combined.merge(&TraceType::S3);
        combined.merge(&TraceType::HEALING);

        assert!(combined.contains(&TraceType::S3));
        assert!(combined.contains(&TraceType::HEALING));
        assert!(!combined.contains(&TraceType::SCANNER));
        assert!(combined.overlaps(&TraceType::S3));
        assert!(combined.overlaps(&TraceType::HEALING));
        assert!(!combined.overlaps(&TraceType::SCANNER));
    }

    #[test]
    fn trace_type_set_if() {
        let mut tt = TraceType::default();
        tt.set_if(true, &TraceType::OS);
        tt.set_if(false, &TraceType::S3);
        assert!(tt.contains(&TraceType::OS));
        assert!(!tt.contains(&TraceType::S3));
    }

    #[test]
    fn trace_type_single_type() {
        assert!(TraceType::S3.single_type());
        let mut combined = TraceType::S3;
        combined.merge(&TraceType::HEALING);
        assert!(!combined.single_type());
    }
}
