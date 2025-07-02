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

use api::QueryResult;
use api::query::analyzer::Analyzer;
use api::query::session::SessionCtx;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::analyzer::Analyzer as DFAnalyzer;

pub struct DefaultAnalyzer {
    inner: DFAnalyzer,
}

impl DefaultAnalyzer {
    pub fn new() -> Self {
        let analyzer = DFAnalyzer::default();
        // we can add analyzer rule at here

        Self { inner: analyzer }
    }
}

impl Default for DefaultAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl Analyzer for DefaultAnalyzer {
    fn analyze(&self, plan: &LogicalPlan, session: &SessionCtx) -> QueryResult<LogicalPlan> {
        let plan = self
            .inner
            .execute_and_check(plan.to_owned(), session.inner().config_options(), |_, _| {})?;
        Ok(plan)
    }
}
