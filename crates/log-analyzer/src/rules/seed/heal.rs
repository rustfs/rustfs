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

//! Self-heal rules.

use super::super::model::{Rule, Severity::*};
use super::{any, base, contains, prefix, strings};

pub(super) fn rules() -> Vec<Rule> {
    vec![
        Rule {
            evidence_fields: strings(["bucket", "object"]),
            anchors: strings(["has no data_dir, cannot heal object data"]),
            ..base(
                "heal-no-datadir",
                P0DataRisk,
                "heal",
                "元数据缺 data_dir,对象数据无法自愈",
                contains("has no data_dir, cannot heal object data"),
                "最新元数据缺 data_dir,对象数据无法自愈,存在数据不可恢复风险。",
                "保留现场,收集对象 versionId 与 xl.meta 上报。",
            )
        },
        Rule {
            anchors: strings(["all drives had write errors, unable to heal"]),
            ..base(
                "heal-all-writes-failed",
                P0DataRisk,
                "heal",
                "heal 结果无法写入任何盘",
                contains("all drives had write errors, unable to heal"),
                "heal 结果无法写入任何盘(全盘故障/满盘)。",
                "先解决容量/盘故障 finding,再重跑 heal。",
            )
        },
        Rule {
            anchors: strings(["all healed data rename attempts failed"]),
            ..base(
                "heal-rename-failed",
                P1Unavailable,
                "heal",
                "heal 数据落位失败",
                contains("all healed data rename attempts failed"),
                "heal 数据落位(rename)全部失败。",
                "检查盘写权限与空间。",
            )
        },
        Rule {
            anchors: strings(["failed to regenerate recoverable xl.meta"]),
            ..base(
                "heal-xlmeta-regen-failed",
                P1Unavailable,
                "heal",
                "可恢复 xl.meta 重建失败",
                contains("failed to regenerate recoverable xl.meta"),
                "可恢复 xl.meta 重建失败。",
                "收集对象路径上报;检查该盘可写性。",
            )
        },
        Rule {
            anchors: strings(["create_bitrot_writer"]),
            min_count: 3,
            ..base(
                "heal-writer-create-failed",
                P2Degraded,
                "heal",
                "heal 写入器创建失败",
                contains("create_bitrot_writer"),
                "heal 写入器创建失败并跳过部分盘。",
                "检查对应盘状态。",
            )
        },
        Rule {
            anchors: strings(["orphan data-dir reclaim failed"]),
            ..base(
                "heal-orphan-reclaim-failed",
                P3ClientSide,
                "heal",
                "孤儿数据目录清理失败",
                any([
                    contains("orphan data-dir reclaim failed"),
                    contains("Heal remote data-dir cleanup failed"),
                ]),
                "孤儿数据目录清理失败(空间泄漏隐患,非紧急)。",
                "观察容量;持续出现时排查对应盘。",
            )
        },
        Rule {
            anchors: strings(["Heal task execution failed", "Heal manager is not running"]),
            ..base(
                "heal-task-failure",
                P2Degraded,
                "heal",
                "heal 任务调度/执行失败",
                any([
                    prefix("Heal task timeout"),
                    prefix("Heal task execution failed"),
                    contains("Heal manager is not running"),
                ]),
                "heal 任务调度/执行层故障。",
                "检查 heal 后台服务状态与资源压力。",
            )
        },
    ]
}
