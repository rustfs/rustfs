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

//! Inter-node network / RPC rules.

use super::super::model::{Rule, Severity::*};
use super::{all, any, base, contains, field, prefix, strings};

pub(super) fn rules() -> Vec<Rule> {
    vec![
        Rule {
            evidence_fields: strings(["peer"]),
            anchors: strings(["Remote peer operation timeout after"]),
            ..base(
                "peer-rpc-timeout",
                P2Degraded,
                "network",
                "节点间 RPC 超时",
                prefix("Remote peer operation timeout after"),
                "节点间 RPC 超时。",
                "检查网络延迟/丢包与目标节点负载。",
            )
        },
        Rule {
            anchors: strings(["server_info timed out after retry"]),
            ..base(
                "peer-probe-timeout",
                P2Degraded,
                "network",
                "peer 探活重试后仍超时",
                contains("server_info timed out after retry"),
                "peer 探活重试后仍超时。",
                "检查网络延迟/丢包与目标节点负载。",
            )
        },
        Rule {
            anchors: strings(["peer request failed"]),
            ..base(
                "internode-signature-mismatch",
                P1Unavailable,
                "network",
                "节点间 RPC 鉴权失败",
                all([contains("peer request failed"), contains("SignatureDoesNotMatch")]),
                "节点间 RPC 鉴权失败——各节点 RUSTFS_ACCESS_KEY/SECRET_KEY 不一致,或节点间时钟偏移过大。",
                "核对各节点凭证环境变量一致;检查 NTP 同步。",
            )
        },
        Rule {
            anchors: strings(["RPC auth secret resolution failed"]),
            ..base(
                "rpc-secret-resolution",
                P1Unavailable,
                "network",
                "内部 RPC 密钥解析失败",
                prefix("RPC auth secret resolution failed"),
                "内部 RPC 密钥解析失败,节点间调用将全部失败。",
                "检查凭证配置来源(env/文件)完整性。",
            )
        },
        Rule {
            evidence_fields: strings(["peer", "host"]),
            anchors: strings(["peer_connection_marked_offline"]),
            ..base(
                "peer-connection-offline",
                P2Degraded,
                "network",
                "peer 连接被标记离线",
                any([
                    contains("peer_connection_marked_offline"),
                    field("event", "peer_connection_marked_offline"),
                ]),
                "peer 连接被判定离线。",
                "检查目标节点与网络。",
            )
        },
        Rule {
            anchors: strings(["Expected number of all hosts"]),
            ..base(
                "topology-mismatch",
                P1Unavailable,
                "network",
                "集群成员拓扑不一致",
                prefix("Expected number of all hosts"),
                "集群成员拓扑与配置不一致(各节点 volumes 参数不一致/DNS 漂移)。",
                "逐节点比对启动参数中的 endpoint 列表。",
            )
        },
    ]
}
