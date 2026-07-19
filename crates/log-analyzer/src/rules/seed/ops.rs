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

//! Decommission / rebalance operational rules.

use super::super::model::{Matcher, Rule, Severity::*};
use super::{all, any, base, contains, prefix, strings};
use crate::model::LogLevel;

pub(super) fn rules() -> Vec<Rule> {
    vec![
        Rule {
            anchors: strings(["decommission_object err"]),
            min_count: 3,
            ..base(
                "decom-object-failed",
                P2Degraded,
                "ops",
                "下线迁移部分对象失败",
                any([
                    contains("decommission_object err"),
                    contains("get_object_reader err"),
                    contains("decommission_entry failed"),
                ]),
                "下线迁移部分对象失败(会重试;持续失败需关注)。",
                "看失败对象是否集中(坏对象/坏盘)。",
            )
        },
        Rule {
            anchors: strings(["Rebalance worker"]),
            ..base(
                "rebalance-worker-error",
                P2Degraded,
                "ops",
                "再平衡 worker 失败",
                all([prefix("Rebalance worker"), Matcher::MinLevel(LogLevel::Error)]),
                "再平衡 worker 失败。",
                "看 samples 内层错误;结合盘/网络 finding。",
            )
        },
        Rule {
            anchors: strings(["source and destination pool are the same"]),
            ..base(
                "datamove-same-pool",
                P1Unavailable,
                "ops",
                "数据迁移源目标同 pool",
                contains("source and destination pool are the same"),
                "数据迁移源目标同 pool(配置错误)。",
                "核对 decommission/rebalance 目标参数。",
            )
        },
        Rule {
            anchors: strings(["Decommission already running"]),
            ..base(
                "ops-state-conflict",
                P4Info,
                "ops",
                "运维操作状态机冲突",
                any([
                    contains("Decommission already running"),
                    contains("Decommission not started"),
                    contains("Rebalance already running"),
                ]),
                "运维操作状态机冲突(重复触发,通常无害)。",
                "确认是否有并行运维脚本。",
            )
        },
    ]
}
