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

use std::sync::Arc;

use async_trait::async_trait;

use crate::QueryResult;

use super::{
    Query,
    execution::{Output, QueryStateMachine},
    logical_planner::Plan,
    session::QueryAdmission,
};

pub type DispatchedQuery = (Query, Output);

#[async_trait]
pub trait QueryDispatcher: Send + Sync {
    // fn create_query_id(&self) -> QueryId;

    // fn query_info(&self, id: &QueryId);

    async fn execute_query(&self, query: &Query) -> QueryResult<Output>;

    fn try_reserve_query(&self) -> QueryResult<QueryAdmission> {
        Ok(QueryAdmission::unmanaged())
    }

    async fn execute_query_admitted(&self, query: &Query, _admission: QueryAdmission) -> QueryResult<Output> {
        self.execute_query(query).await
    }

    async fn dispatch_query(&self, query: &Query) -> QueryResult<DispatchedQuery> {
        let execution_query = query.for_execution();
        let output = self.execute_query(&execution_query).await?;
        Ok((execution_query, output))
    }

    async fn dispatch_query_admitted(&self, query: &Query, admission: QueryAdmission) -> QueryResult<DispatchedQuery> {
        let execution_query = query.for_execution();
        let output = self.execute_query_admitted(&execution_query, admission).await?;
        Ok((execution_query, output))
    }

    async fn build_logical_plan(&self, query_state_machine: Arc<QueryStateMachine>) -> QueryResult<Option<Plan>>;

    async fn execute_logical_plan(&self, logical_plan: Plan, query_state_machine: Arc<QueryStateMachine>) -> QueryResult<Output>;

    async fn build_query_state_machine(&self, query: Query) -> QueryResult<Arc<QueryStateMachine>>;

    // fn running_query_infos(&self) -> Vec<QueryInfo>;

    // fn running_query_status(&self) -> Vec<QueryStatus>;

    // fn cancel_query(&self, id: &QueryId);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::test_query;
    use parking_lot::Mutex;

    #[derive(Default)]
    struct DefaultDispatchDispatcher {
        executed_metrics: Mutex<Vec<Arc<crate::SelectInputMetrics>>>,
    }

    #[async_trait]
    impl QueryDispatcher for DefaultDispatchDispatcher {
        async fn execute_query(&self, query: &Query) -> QueryResult<Output> {
            self.executed_metrics.lock().push(Arc::clone(query.input_metrics()));
            Ok(Output::Nil(()))
        }

        async fn build_logical_plan(&self, _query_state_machine: Arc<QueryStateMachine>) -> QueryResult<Option<Plan>> {
            unreachable!("default dispatch test does not plan queries")
        }

        async fn execute_logical_plan(
            &self,
            _logical_plan: Plan,
            _query_state_machine: Arc<QueryStateMachine>,
        ) -> QueryResult<Output> {
            unreachable!("default dispatch test does not execute plans")
        }

        async fn build_query_state_machine(&self, _query: Query) -> QueryResult<Arc<QueryStateMachine>> {
            unreachable!("default dispatch test does not build state machines")
        }
    }

    #[derive(Default)]
    struct DistinctAdmittedDispatcher {
        plain_metrics: Mutex<Vec<Arc<crate::SelectInputMetrics>>>,
        admitted_metrics: Mutex<Vec<Arc<crate::SelectInputMetrics>>>,
        fail_plain: bool,
        fail_admitted: bool,
    }

    #[async_trait]
    impl QueryDispatcher for DistinctAdmittedDispatcher {
        async fn execute_query(&self, query: &Query) -> QueryResult<Output> {
            self.plain_metrics.lock().push(Arc::clone(query.input_metrics()));
            if self.fail_plain {
                Err(crate::QueryError::Cancel)
            } else {
                Ok(Output::Nil(()))
            }
        }

        async fn execute_query_admitted(&self, query: &Query, _admission: QueryAdmission) -> QueryResult<Output> {
            self.admitted_metrics.lock().push(Arc::clone(query.input_metrics()));
            if self.fail_admitted {
                Err(crate::QueryError::Cancel)
            } else {
                Ok(Output::Nil(()))
            }
        }

        async fn build_logical_plan(&self, _query_state_machine: Arc<QueryStateMachine>) -> QueryResult<Option<Plan>> {
            unreachable!("dispatch routing test does not plan queries")
        }

        async fn execute_logical_plan(
            &self,
            _logical_plan: Plan,
            _query_state_machine: Arc<QueryStateMachine>,
        ) -> QueryResult<Output> {
            unreachable!("dispatch routing test does not execute plans")
        }

        async fn build_query_state_machine(&self, _query: Query) -> QueryResult<Arc<QueryStateMachine>> {
            unreachable!("dispatch routing test does not build state machines")
        }
    }

    #[tokio::test]
    async fn plain_dispatch_propagates_override_errors() {
        let dispatcher = DistinctAdmittedDispatcher {
            fail_plain: true,
            ..Default::default()
        };

        let error = match dispatcher.dispatch_query(&test_query()).await {
            Err(error) => error,
            Ok(_) => panic!("plain override error should propagate"),
        };
        assert!(matches!(error, crate::QueryError::Cancel));
        assert_eq!(dispatcher.plain_metrics.lock().len(), 1);
        assert!(dispatcher.admitted_metrics.lock().is_empty());
    }

    #[tokio::test]
    async fn default_dispatch_methods_use_distinct_execution_metrics() {
        let dispatcher = DefaultDispatchDispatcher::default();
        let query = test_query();

        let (first, _) = dispatcher
            .dispatch_query(&query)
            .await
            .expect("first dispatch should execute");
        let (second, _) = dispatcher
            .dispatch_query(&query)
            .await
            .expect("second dispatch should execute");
        let (admitted, _) = dispatcher
            .dispatch_query_admitted(&query, QueryAdmission::unmanaged())
            .await
            .expect("admitted dispatch should execute");
        let executed_metrics = dispatcher.executed_metrics.lock();

        assert!(!Arc::ptr_eq(first.input_metrics(), second.input_metrics()));
        assert!(!Arc::ptr_eq(first.input_metrics(), admitted.input_metrics()));
        assert!(Arc::ptr_eq(first.input_metrics(), &executed_metrics[0]));
        assert!(Arc::ptr_eq(second.input_metrics(), &executed_metrics[1]));
        assert!(Arc::ptr_eq(admitted.input_metrics(), &executed_metrics[2]));
    }

    #[tokio::test]
    async fn admitted_dispatch_uses_the_admitted_override_and_propagates_errors() {
        let dispatcher = DistinctAdmittedDispatcher::default();
        let query = test_query();

        let (dispatched, _) = dispatcher
            .dispatch_query_admitted(&query, QueryAdmission::unmanaged())
            .await
            .expect("admitted dispatch should execute through its override");
        assert!(dispatcher.plain_metrics.lock().is_empty());
        {
            let admitted_metrics = dispatcher.admitted_metrics.lock();
            assert_eq!(admitted_metrics.len(), 1);
            assert!(Arc::ptr_eq(dispatched.input_metrics(), &admitted_metrics[0]));
        }

        let failing = DistinctAdmittedDispatcher {
            fail_admitted: true,
            ..Default::default()
        };
        let error = match failing.dispatch_query_admitted(&query, QueryAdmission::unmanaged()).await {
            Err(error) => error,
            Ok(_) => panic!("admitted override error should propagate"),
        };
        assert!(matches!(error, crate::QueryError::Cancel));
        assert!(failing.plain_metrics.lock().is_empty());
        assert_eq!(failing.admitted_metrics.lock().len(), 1);
    }
}
