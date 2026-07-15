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

//! Startup / configuration / TLS rules.

use super::super::model::{Rule, Severity::*};
use super::{all, any, base, contains, field, prefix, strings};

pub(super) fn rules() -> Vec<Rule> {
    vec![
        Rule {
            evidence_fields: strings(["host"]),
            anchors: strings(["Create pool endpoints host"]),
            ..base(
                "endpoint-resolve-failed",
                P1Unavailable,
                "startup",
                "启动期端点主机名解析失败",
                all([contains("Create pool endpoints host"), contains("not found")]),
                "启动期端点主机名解析失败(DNS 未就绪/拼写错误/hosts 缺失)。",
                "容器/K8s 场景确认 DNS 就绪顺序;核对 volumes 参数拼写。",
            )
        },
        Rule {
            anchors: strings(["Console and endpoint should use different ports"]),
            ..base(
                "listen-config-invalid",
                P1Unavailable,
                "startup",
                "监听地址/端口配置错误",
                any([
                    contains("Invalid endpoint port"),
                    contains("Invalid console port"),
                    contains("Console and endpoint should use different ports"),
                    contains("Failed to parse with address args"),
                ]),
                "监听地址/端口配置错误。",
                "核对 --address/--console-address。",
            )
        },
        Rule {
            anchors: strings([
                "persisted server config cannot be decoded",
                "server config is corrupt after heal",
            ]),
            ..base(
                "server-config-corrupt",
                P1Unavailable,
                "startup",
                "持久化服务配置损坏或解密失败",
                any([
                    contains("persisted server config cannot be decoded"),
                    prefix("server config corrupt:"),
                    contains("server config is corrupt after heal"),
                ]),
                "持久化服务配置损坏或解密失败;若提示 fallback 被禁用则启动会失败。",
                "按日志指引评估 RUSTFS_CONFIG_RECOVER_ON_CORRUPTION;排查 KMS/密钥变更历史。",
            )
        },
        Rule {
            anchors: strings(["unable to load the certificate for"]),
            ..base(
                "tls-cert-load-failed",
                P1Unavailable,
                "startup",
                "TLS 证书加载失败",
                any([
                    prefix("unable to load the certificate for"),
                    prefix("unable to load root directory certificate"),
                ]),
                "TLS 证书加载失败(路径/权限/格式)。",
                "用 `rustfs tls inspect --path <dir>` 现场检查证书目录。",
            )
        },
        Rule {
            anchors: strings(["client_cert and client_key must be specified as a pair"]),
            ..base(
                "tls-config-invalid",
                P2Degraded,
                "startup",
                "TLS 目标配置组合非法",
                any([
                    contains("client_cert and client_key must be specified as a pair"),
                    contains("skipTlsVerify and caCertPem cannot be enabled together"),
                    contains("caCertPem requires an HTTPS remote target"),
                ]),
                "TLS 目标配置组合非法。",
                "按报错修正配置对。",
            )
        },
        Rule {
            anchors: strings(["Global server configuration not loaded"]),
            ..base(
                "subsystem-init-order",
                P1Unavailable,
                "startup",
                "全局配置未加载导致子系统初始化失败",
                any([
                    contains("Global server configuration not loaded"),
                    contains("Global server config not loaded"),
                ]),
                "全局配置未加载导致子系统初始化失败(启动顺序/更早的配置错误)。",
                "向前查同批日志更早的 startup 类 finding。",
            )
        },
        Rule {
            anchors: strings(["[FATAL] "]),
            ..base(
                "startup-fatal",
                P1Unavailable,
                "startup",
                "进程启动期致命错误",
                prefix("[FATAL]"),
                "进程启动期致命错误(observability/预检失败),进程未能起来。",
                "按 message 内层错误处理;这是「服务起不来」最直接的证据。",
            )
        },
        Rule {
            evidence_fields: strings(["error"]),
            anchors: strings(["server_runtime_failed"]),
            ..base(
                "runtime-failed",
                P1Unavailable,
                "startup",
                "服务运行时整体退出",
                any([field("event", "server_runtime_failed"), contains("Server runtime failed")]),
                "服务运行时整体退出。",
                "看 error 字段与临近 finding。",
            )
        },
    ]
}
