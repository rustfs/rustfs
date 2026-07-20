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

//! Erasure-coding / bitrot / data-integrity rules.

use super::super::model::{Rule, Severity::*};
use super::{any, base, contains, prefix, strings};

const QUORUM_ROOT_CAUSES: [&str; 4] = ["disk-marked-faulty", "remote-peer-faulty", "peer-disks-offline", "disk-full"];

pub(super) fn rules() -> Vec<Rule> {
    vec![
        Rule {
            evidence_fields: strings(["bucket", "object", "disk"]),
            anchors: strings(["bitrot hash mismatch", "bitrot shard file size mismatch"]),
            ..base(
                "bitrot-detected",
                P1Unavailable,
                "erasure",
                "检测到分片位腐烂(bitrot)",
                any([
                    contains("bitrot hash mismatch"),
                    contains("bitrot checksum verification failed"),
                    contains("bitrot shard file size mismatch"),
                ]),
                "分片位腐烂(校验和/长度不符),读到损坏数据。",
                "触发 heal;若集中于单盘,按坏盘处理。",
            )
        },
        Rule {
            anchors: strings(["short shard read: got"]),
            ..base(
                "short-shard-read",
                P2Degraded,
                "erasure",
                "分片读取长度不足",
                contains("short shard read: got"),
                "分片读取长度不足(截断/损坏)。",
                "结合 bitrot 与盘健康 finding 定位坏盘。",
            )
        },
        Rule {
            evidence_fields: strings(["bucket", "object"]),
            anchors: strings(["cannot reconstruct with available shards"]),
            ..base(
                "heal-cannot-reconstruct",
                P0DataRisk,
                "erasure",
                "对象无法用现存分片重建",
                contains("cannot reconstruct with available shards"),
                "存活分片少于数据分片,对象不可重建,存在数据丢失风险。",
                "立即停止换盘/清盘类操作,清点离线盘并尽量恢复上线,再评估受影响对象清单。",
            )
        },
        Rule {
            evidence_fields: strings(["required", "achieved", "failed", "offline-disks", "dominant-error"]),
            anchors: strings(["erasure write quorum"]),
            implies_root_cause: strings(QUORUM_ROOT_CAUSES),
            ..base(
                "ec-write-quorum",
                P1Unavailable,
                "erasure",
                "写仲裁不足,写入被拒",
                contains("erasure write quorum"),
                "在线盘数低于写仲裁阈值,写入被拒。",
                "此为级联症状,排查同时段 disk/peer 类 P2 finding 定位根因盘或节点。",
            )
        },
        Rule {
            evidence_fields: strings(["bucket", "object"]),
            anchors: strings(["erasure read quorum", "reduce_read_quorum_errs"]),
            implies_root_cause: strings(QUORUM_ROOT_CAUSES),
            ..base(
                "ec-read-quorum",
                P1Unavailable,
                "erasure",
                "读仲裁不足,读取失败",
                any([contains("erasure read quorum"), prefix("reduce_read_quorum_errs:")]),
                "读仲裁不足,对象读取失败。",
                "同 ec-write-quorum,先恢复离线盘/节点。",
            )
        },
        Rule {
            evidence_fields: strings(["bucket", "object"]),
            anchors: strings(["part missing or corrupt"]),
            ..base(
                "file-corrupted",
                P1Unavailable,
                "erasure",
                "对象数据或 xl.meta 损坏",
                any([
                    contains("part missing or corrupt"),
                    contains("file is corrupted"),
                    contains("File is corrupted"),
                    contains("outdated XL meta"),
                ]),
                "对象数据或 xl.meta 损坏/过期。",
                "对受影响对象触发 heal;批量出现时按 bitrot/坏盘路径排查。",
            )
        },
    ]
}
