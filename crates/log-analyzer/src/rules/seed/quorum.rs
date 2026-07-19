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

//! Read/write quorum rules.

use super::super::model::{Rule, Severity::*};
use super::{base, contains, prefix, strings};

pub(super) fn rules() -> Vec<Rule> {
    vec![
        Rule {
            evidence_fields: strings(["bucket", "object"]),
            anchors: strings(["Namespace lock quorum unavailable"]),
            ..base(
                "nslock-quorum",
                P1Unavailable,
                "quorum",
                "命名空间锁仲裁不可用",
                prefix("Namespace lock quorum unavailable"),
                "命名空间锁拿不到仲裁,写路径被阻断(节点半数以上不可达的典型症状)。",
                "检查节点间连通与存活节点数是否过半。",
            )
        },
        Rule {
            anchors: strings(["below write quorum"]),
            implies_root_cause: strings(["disk-marked-faulty", "remote-peer-faulty", "peer-disks-offline", "disk-full"]),
            ..base(
                "below-write-quorum",
                P1Unavailable,
                "quorum",
                "在线盘数低于写仲裁",
                contains("below write quorum"),
                "在线盘快照低于写仲裁。",
                "同 ec-write-quorum。",
            )
        },
        Rule {
            anchors: strings(["reduce_write_quorum_errs"]),
            ..base(
                "bucket-op-quorum",
                P1Unavailable,
                "quorum",
                "bucket 级操作写仲裁失败",
                contains("reduce_write_quorum_errs"),
                "bucket 级操作(建桶/heal/list)跨 pool 写仲裁失败。",
                "检查各 pool 节点在线状态。",
            )
        },
        Rule {
            anchors: strings(["object_quorum_from_meta"]),
            ..base(
                "quorum-meta-derive",
                P1Unavailable,
                "quorum",
                "从元数据推导仲裁参数失败",
                contains("object_quorum_from_meta"),
                "从对象元数据推导仲裁参数失败,元数据可能损坏。",
                "对样本对象做 xl.meta 检查(参见 docs/operations/tier-ilm-debugging.md 的 xl.meta 工具)。",
            )
        },
    ]
}
