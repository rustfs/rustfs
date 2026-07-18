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

//! Capacity rules.

use super::super::model::{Rule, Severity::*};
use super::{any, base, contains, prefix, strings};

pub(super) fn rules() -> Vec<Rule> {
    vec![
        Rule {
            evidence_fields: strings(["disk"]),
            anchors: strings(["Disk full"]),
            ..base(
                "disk-full",
                P1Unavailable,
                "capacity",
                "盘写满",
                any([
                    contains("Disk full"),
                    contains("drive path full"),
                    contains("No space left on device"),
                ]),
                "盘写满。",
                "清理/扩容;检查 ILM 是否按预期转储;结合 heal-orphan 泄漏 finding。",
            )
        },
        Rule {
            anchors: strings(["Storage reached its minimum free drive threshold"]),
            ..base(
                "min-free-threshold",
                P1Unavailable,
                "capacity",
                "触及最小空闲阈值,拒绝新写",
                contains("Storage reached its minimum free drive threshold"),
                "触及最小空闲阈值,拒绝新写(保护机制,先于物理满盘)。",
                "扩容或清理;这解释「df 有空间但写入报满」。",
            )
        },
        Rule {
            anchors: strings(["Storage resources are insufficient for the"]),
            ..base(
                "insufficient-storage",
                P1Unavailable,
                "capacity",
                "存储资源不足",
                prefix("Storage resources are insufficient for the"),
                "存储资源不足(读/写)。",
                "同 disk-full 排查。",
            )
        },
        Rule {
            evidence_fields: strings(["current", "limit", "operation"]),
            anchors: strings(["Bucket quota exceeded"]),
            ..base(
                "bucket-quota-exceeded",
                P3ClientSide,
                "capacity",
                "bucket 配额超限",
                prefix("Bucket quota exceeded"),
                "bucket 配额超限(管理面设置,非集群故障)。",
                "调整配额或清理该桶。",
            )
        },
        Rule {
            evidence_fields: strings(["required", "target_free"]),
            anchors: strings(["insufficient target pool capacity"]),
            ..base(
                "decom-capacity-insufficient",
                P1Unavailable,
                "capacity",
                "pool 下线目标容量不足",
                contains("insufficient target pool capacity"),
                "pool 下线迁移目标容量不足,decommission 无法开始。",
                "先扩容目标 pool。",
            )
        },
    ]
}
