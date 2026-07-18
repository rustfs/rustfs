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

//! Scanner rules. The scanner logs under `target: "rustfs::scanner*"` with
//! structured fields, so the burst rule keys on the target prefix.

use super::super::model::{Matcher, Rule, Severity::*};
use super::{all, base, contains, prefix, strings};
use crate::model::LogLevel;

pub(super) fn rules() -> Vec<Rule> {
    vec![
        Rule {
            anchors: strings(["Scanner stopped with partial data usage cache"]),
            ..base(
                "scanner-partial-cache",
                P3ClientSide,
                "scanner",
                "扫描中断,容量统计不完整",
                contains("Scanner stopped with partial data usage cache"),
                "扫描中断,容量统计不完整(显示值可能偏低,非数据问题)。",
                "确认 scanner 是否被频繁重启打断。",
            )
        },
        Rule {
            anchors: strings(["invalid scanner config value for"]),
            ..base(
                "scanner-config-invalid",
                P3ClientSide,
                "scanner",
                "scanner 运行时配置非法",
                prefix("invalid scanner config value for"),
                "scanner 运行时配置非法,回落默认值。",
                "修正对应环境变量(见 docs/operations/scanner-runtime-controls.md)。",
            )
        },
        Rule {
            min_count: 10,
            ..base(
                "scanner-error-burst",
                P4Info,
                "scanner",
                "scanner 持续报错",
                all([
                    Matcher::TargetPrefix("rustfs::scanner".to_string()),
                    Matcher::MinLevel(LogLevel::Error),
                ]),
                "scanner 持续报错(聚合信号,具体原因看 samples)。",
                "结合盘健康 finding 判断是否坏盘引起。",
            )
        },
    ]
}
