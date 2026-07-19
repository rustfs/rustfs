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

//! Distributed-lock rules.

use super::super::model::{Rule, Severity::*};
use super::{any, base, contains, prefix, strings};

pub(super) fn rules() -> Vec<Rule> {
    vec![
        Rule {
            evidence_fields: strings(["resource"]),
            anchors: strings(["Lock acquisition timeout for resource"]),
            min_count: 3,
            ..base(
                "lock-acquire-timeout",
                P2Degraded,
                "lock",
                "锁获取超时",
                prefix("Lock acquisition timeout for resource"),
                "锁获取超时(热点对象/慢盘/死锁的症状)。",
                "看 resource 集中度:集中单对象=热点;广泛分布=盘慢或节点失联。",
            )
        },
        Rule {
            evidence_fields: strings(["required", "achieved", "available"]),
            anchors: strings(["Insufficient nodes for quorum", "Quorum not reached"]),
            ..base(
                "lock-quorum-nodes",
                P1Unavailable,
                "lock",
                "锁子系统节点数不足仲裁",
                any([prefix("Insufficient nodes for quorum:"), prefix("Quorum not reached:")]),
                "锁子系统节点数不足仲裁。",
                "恢复失联节点;确认部署节点数为奇数且过半存活。",
            )
        },
        Rule {
            anchors: strings(["Not the lock owner"]),
            ..base(
                "lock-owner-mismatch",
                P2Degraded,
                "lock",
                "锁 owner 不匹配",
                prefix("Not the lock owner"),
                "释放/续约了非自己持有的锁,锁状态错乱(通常伴随超时后重试)。",
                "结合 lock-acquire-timeout 判断;孤立出现可忽略。",
            )
        },
        Rule {
            anchors: strings(["distributed unlock failed on client"]),
            ..base(
                "dist-unlock-failed",
                P2Degraded,
                "lock",
                "分布式加/解锁在部分节点失败",
                any([
                    contains("distributed unlock failed on client"),
                    contains("Failed to acquire lock on client"),
                ]),
                "分布式加/解锁在部分节点失败(peer 不可达)。",
                "检查该节点连通性。",
            )
        },
        Rule {
            anchors: strings(["Atomic state inconsistency during exclusive lock release"]),
            ..base(
                "lock-state-inconsistent",
                P2Degraded,
                "lock",
                "锁内部原子状态不一致",
                contains("Atomic state inconsistency during exclusive lock release"),
                "锁内部原子状态不一致(并发缺陷级信号)。",
                "收集完整日志上报研发,附 samples。",
            )
        },
        Rule {
            anchors: strings(["poisoned, recovering"]),
            implies_root_cause: strings(["process-panic"]),
            ..base(
                "rwlock-poisoned",
                P1Unavailable,
                "lock",
                "RwLock 中毒(进程曾发生 panic 的间接证据)",
                contains("poisoned, recovering"),
                "某线程曾持锁 panic 导致 RwLock 中毒——这是进程发生过 panic 的间接证据。",
                "在同批日志中查 process-panic finding(stderr 段);无 panic 块时说明 stderr 未被采集,建议客户补采。",
            )
        },
    ]
}
