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

//! Process-level rules.

use super::super::model::{Matcher, Rule, Severity::*};
use super::{base, strings};

pub(super) fn rules() -> Vec<Rule> {
    vec![Rule {
        evidence_fields: strings(["panic_location", "panic_thread"]),
        // Structural matcher — no message anchor to guard.
        ..base(
            "process-panic",
            P1Unavailable,
            "process",
            "进程发生 panic",
            Matcher::IsPanic,
            "进程发生 panic(可能随后重启/触发锁中毒)。",
            "将 panic_location 与 panic_full 上报研发;检查是否伴随 rwlock-poisoned 与重启痕迹(时间线断档)。",
        )
    }]
}
