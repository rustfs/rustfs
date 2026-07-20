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

//! IAM / auth / signature rules — mostly client-side (P3) with burst
//! thresholds so isolated client mistakes don't clutter the report.

use super::super::model::{Matcher, Rule, Severity::*};
use super::{all, any, base, contains, prefix, strings};
use crate::model::LogLevel;

pub(super) fn rules() -> Vec<Rule> {
    vec![
        Rule {
            anchors: strings(["SignatureDoesNotMatch"]),
            min_count: 10,
            ..base(
                "client-signature-mismatch",
                P3ClientSide,
                "iam",
                "客户端签名不匹配",
                any([
                    contains("SignatureDoesNotMatch"),
                    contains("request signature we calculated does not match"),
                ]),
                "客户端签名不匹配:SK 错误、客户端与服务端时钟偏移过大、或代理改写了 Host/Authorization 头。",
                "核对凭证;检查 NTP;若走反代,对照 docs/operations/reverse-proxy.md 检查头透传。",
            )
        },
        Rule {
            anchors: strings(["Access Key Id you provided does not exist"]),
            min_count: 5,
            ..base(
                "unknown-access-key",
                P3ClientSide,
                "iam",
                "Access Key 不存在",
                contains("Access Key Id you provided does not exist"),
                "AK 不存在(凭证配错/已删除/打到错误集群)。",
                "核对客户端配置与目标端点。",
            )
        },
        Rule {
            evidence_fields: strings(["access_key"]),
            anchors: strings(["authenticate_request: authentication failed"]),
            min_count: 5,
            ..base(
                "admin-auth-failed",
                P3ClientSide,
                "iam",
                "管理接口鉴权失败",
                prefix("authenticate_request: authentication failed"),
                "管理接口鉴权失败。",
                "核对 console/admin 凭证;高频出现需警惕爆破(看来源 IP)。",
            )
        },
        Rule {
            anchors: strings(["action not allowed"]),
            min_count: 50,
            ..base(
                "access-denied-burst",
                P3ClientSide,
                "iam",
                "大量授权拒绝",
                all([
                    Matcher::MinLevel(LogLevel::Warn),
                    any([contains("Access Denied"), contains("action not allowed")]),
                ]),
                "大量授权拒绝(策略不匹配或客户端行为异常)。",
                "抽样 samples 核对 bucket 策略与用户策略。",
            )
        },
        Rule {
            anchors: strings(["invalid access key length"]),
            ..base(
                "credential-format-invalid",
                P3ClientSide,
                "iam",
                "凭证格式非法",
                any([
                    contains("invalid access key length"),
                    contains("invalid secret key length"),
                    contains("malformed credential"),
                ]),
                "凭证格式非法(常见:环境变量带引号/空格/换行)。",
                "检查凭证注入方式。",
            )
        },
        Rule {
            anchors: strings(["Invalid Keystone token"]),
            ..base(
                "keystone-auth-failed",
                P3ClientSide,
                "iam",
                "Keystone 集成认证失败",
                any([prefix("Invalid Keystone token"), contains("Keystone authentication requires")]),
                "Keystone 集成认证失败。",
                "检查 Keystone 服务与 token 有效期。",
            )
        },
        Rule {
            anchors: strings(["AssumeRole get policy failed"]),
            ..base(
                "assume-role-failed",
                P3ClientSide,
                "iam",
                "STS AssumeRole 取策略失败",
                prefix("AssumeRole get policy failed"),
                "STS AssumeRole 取策略失败。",
                "核对角色策略配置。",
            )
        },
    ]
}
