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

//! Dumps every seed-rule anchor as TSV (`rule_id\tanchor`) for the CI guard
//! `scripts/check_log_analyzer_rules.sh` (rustfs/backlog#1289). Anchors are
//! guaranteed tab/newline-free by `RuleSet::new` validation.

fn main() {
    for rule in rustfs_log_analyzer::seed_rule_set().rules() {
        for anchor in &rule.anchors {
            println!("{}\t{}", rule.id, anchor);
        }
    }
}
