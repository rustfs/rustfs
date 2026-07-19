#!/usr/bin/env bash
# Copyright 2024 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Guard (rustfs/backlog#1289): every rule anchor in rustfs-log-analyzer's
# seed rule library must exist verbatim in the rustfs source tree. If this
# fails you either changed a log message (update the matching rule in
# crates/log-analyzer/src/rules/seed/) or added a rule whose anchor does not
# match real code.
set -euo pipefail
cd "$(dirname "$0")/.."

dump=$(cargo run --quiet -p rustfs-log-analyzer --bin la-dump-anchors)

fail=0
while IFS=$'\t' read -r rule_id anchor; do
    [ -z "${anchor:-}" ] && continue
    if ! grep -rqF --include='*.rs' \
         --exclude-dir=target --exclude-dir=log-analyzer \
         -- "$anchor" crates/ rustfs/; then
        echo "MISSING anchor for rule '${rule_id}': ${anchor}" >&2
        fail=1
    fi
done <<<"$dump"

if [ "$fail" -ne 0 ]; then
    echo "check_log_analyzer_rules: some rule anchors no longer exist in source." >&2
    echo "Fix the rule(s) in crates/log-analyzer/src/rules/seed/ to match current log text." >&2
    exit 1
fi
echo "check_log_analyzer_rules: OK"
