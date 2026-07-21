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

//! Disk health rules.

use super::super::model::{Matcher, Rule, Severity::*};
use super::{all, any, base, contains, field, strings};

pub(super) fn rules() -> Vec<Rule> {
    vec![
        Rule {
            evidence_fields: strings(["disk", "endpoint"]),
            anchors: strings(["Disk health check marked disk faulty"]),
            ..base(
                "disk-marked-faulty",
                P2Degraded,
                "disk",
                "磁盘被标记为 faulty",
                any([
                    field("reason", "faulty_disk"),
                    contains("Disk health check marked disk faulty"),
                ]),
                "某盘连续健康检查失败被标记 faulty,后续请求短路避开。",
                "检查该盘 SMART/内核日志(dmesg)与挂载状态;确认是否伴随 quorum 类 finding(级联)。",
            )
        },
        Rule {
            evidence_fields: strings(["disk"]),
            anchors: strings(["rejected operation because disk is marked faulty"]),
            ..base(
                "disk-faulty-rejected",
                P2Degraded,
                "disk",
                "请求被 faulty 盘保护性拒绝",
                any([
                    field("reason", "disk_marked_faulty"),
                    contains("rejected operation because disk is marked faulty"),
                ]),
                "请求命中已标记 faulty 的盘被拒(保护性短路)。",
                "与 disk-marked-faulty 同源,处理根因盘。",
            )
        },
        Rule {
            evidence_fields: strings(["peer", "host"]),
            anchors: strings(["Remote peer marked faulty", "Remote peer health check failed"]),
            ..base(
                "remote-peer-faulty",
                P2Degraded,
                "disk",
                "远端节点被标记 faulty",
                any([
                    contains("Remote peer marked faulty"),
                    contains("Remote peer health check failed"),
                ]),
                "远端节点网络不可达/健康检查失败,被标记 faulty。",
                "检查节点间网络连通与目标节点进程存活。",
            )
        },
        Rule {
            anchors: strings(["reporting peer disks offline after consecutive storage_info failures"]),
            ..base(
                "peer-disks-offline",
                P2Degraded,
                "disk",
                "peer 磁盘被整体判定离线",
                contains("reporting peer disks offline after consecutive storage_info failures"),
                "对某 peer 连续 storage_info 失败,判定其磁盘整体离线。",
                "检查该 peer 节点存活与 RPC 端口可达。",
            )
        },
        Rule {
            anchors: strings(["drive is faulty"]),
            ..base(
                "drive-faulty-error",
                P2Degraded,
                "disk",
                "存储路径出现盘级 faulty 错误",
                any([
                    contains("Faulty disk"),
                    contains("drive is faulty"),
                    contains("Faulty remote disk"),
                    contains("remote drive is faulty"),
                ]),
                "盘级 faulty 错误出现在存储路径错误链。",
                "定位具体盘(结合 samples 的 fields),检查硬件。",
            )
        },
        Rule {
            evidence_fields: strings(["bucket", "path", "state", "timeout_ms", "drive"]),
            anchors: strings(["Metacache listing quorum failed", "Metacache reader peek timed out"]),
            min_count: 2,
            ..base(
                "metacache-listing-timeout",
                P2Degraded,
                "disk",
                "metacache listing 多盘等待超时",
                any([
                    all([contains("Metacache listing quorum failed"), field("state", "quorum_failed")]),
                    all([contains("Metacache reader peek timed out"), field("state", "peek_timed_out")]),
                ]),
                "对象 listing/scanner 期间 metacache reader 等待超时并可能导致 listing quorum failure;常见于宽前缀、高延迟盘或慢远端 reader。",
                "先确认是否存在宽前缀/慢盘/高延迟远端 reader;再参考 docs/operations/drive-timeout-tuning.md 调整 RUSTFS_DRIVE_WALKDIR_STALL_TIMEOUT_SECS、RUSTFS_DRIVE_WALKDIR_PEEK_TIMEOUT_SECS 或 RUSTFS_DRIVE_TIMEOUT_PROFILE=high_latency。",
            )
        },
        Rule {
            anchors: strings(["Unformatted disk"]),
            ..base(
                "unformatted-disk",
                P2Degraded,
                "disk",
                "存在未格式化的盘",
                contains("Unformatted disk"),
                "盘未格式化(新盘/format.json 缺失)。",
                "确认是否新换盘待 heal;检查该盘挂载点是否指向了空目录。",
            )
        },
        Rule {
            anchors: strings(["disk access denied"]),
            ..base(
                "disk-access-denied",
                P1Unavailable,
                "disk",
                "盘目录权限不可访问",
                all([
                    Matcher::MinLevel(crate::model::LogLevel::Warn),
                    any([contains("disk access denied"), contains("Disk access denied")]),
                ]),
                "盘目录权限不可访问,该盘等效离线。",
                "检查数据目录属主/权限与 SELinux/AppArmor。",
            )
        },
        Rule {
            anchors: strings(["inconsistent drive found"]),
            ..base(
                "inconsistent-drive",
                P2Degraded,
                "disk",
                "盘上格式与集群预期不一致",
                contains("inconsistent drive found"),
                "盘上格式/成员信息与集群预期不一致(错插盘/复用旧盘)。",
                "核对该盘 format.json 与集群拓扑。",
            )
        },
        Rule {
            anchors: strings(["too many open files"]),
            ..base(
                "fd-exhausted",
                P1Unavailable,
                "disk",
                "文件描述符耗尽",
                contains("too many open files"),
                "文件描述符耗尽,读写随机失败。",
                "提高 `ulimit -n`(systemd 需配 LimitNOFILE),排查 fd 泄漏。",
            )
        },
        Rule {
            anchors: strings(["does not support O_DIRECT"]),
            ..base(
                "odirect-unsupported",
                P1Unavailable,
                "disk",
                "后端文件系统不支持 O_DIRECT",
                contains("does not support O_DIRECT"),
                "后端文件系统不支持 O_DIRECT(常见:tmpfs/部分网络盘)。",
                "更换数据目录所在文件系统。",
            )
        },
        Rule {
            anchors: strings(["Rename across devices not allowed"]),
            ..base(
                "rename-across-devices",
                P1Unavailable,
                "disk",
                "数据目录跨设备,原子 rename 不可用",
                contains("Rename across devices not allowed"),
                "数据目录跨设备(挂载布局错误),原子 rename 不可用。",
                "确保每个盘路径整体位于单一挂载点。",
            )
        },
    ]
}
