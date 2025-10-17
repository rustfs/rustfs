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

#[cfg(test)]
mod tests {
    use crate::query::execution::{DONE, Output, QueryExecution, QueryState, QueryType, RUNNING};
    use crate::{QueryError, QueryResult};
    use async_trait::async_trait;

    #[test]
    fn test_query_type_display() {
        assert_eq!(format!("{}", QueryType::Batch), "batch");
        assert_eq!(format!("{}", QueryType::Stream), "stream");
    }

    #[test]
    fn test_query_type_equality() {
        assert_eq!(QueryType::Batch, QueryType::Batch);
        assert_ne!(QueryType::Batch, QueryType::Stream);
        assert_eq!(QueryType::Stream, QueryType::Stream);
    }

    #[tokio::test]
    async fn test_output_nil_methods() {
        let output = Output::Nil(());

        let result = output.chunk_result().await;
        assert!(result.is_ok(), "Output::Nil result should be Ok");

        let output2 = Output::Nil(());
        let rows = output2.num_rows().await;
        assert_eq!(rows, 0, "Output::Nil should have 0 rows");

        let output3 = Output::Nil(());
        let affected = output3.affected_rows().await;
        assert_eq!(affected, 0, "Output::Nil should have 0 affected rows");
    }

    #[test]
    fn test_query_state_as_ref() {
        let accepting = QueryState::ACCEPTING;
        assert_eq!(accepting.as_ref(), "ACCEPTING");

        let running = QueryState::RUNNING(RUNNING::ANALYZING);
        assert_eq!(running.as_ref(), "ANALYZING");

        let done = QueryState::DONE(DONE::FINISHED);
        assert_eq!(done.as_ref(), "FINISHED");
    }

    #[test]
    fn test_running_state_as_ref() {
        assert_eq!(RUNNING::DISPATCHING.as_ref(), "DISPATCHING");
        assert_eq!(RUNNING::ANALYZING.as_ref(), "ANALYZING");
        assert_eq!(RUNNING::OPTIMIZING.as_ref(), "OPTIMIZING");
        assert_eq!(RUNNING::SCHEDULING.as_ref(), "SCHEDULING");
    }

    #[test]
    fn test_done_state_as_ref() {
        assert_eq!(DONE::FINISHED.as_ref(), "FINISHED");
        assert_eq!(DONE::FAILED.as_ref(), "FAILED");
        assert_eq!(DONE::CANCELLED.as_ref(), "CANCELLED");
    }

    // Mock implementation for testing
    struct MockQueryExecution {
        should_succeed: bool,
        should_cancel: bool,
    }

    #[async_trait]
    impl QueryExecution for MockQueryExecution {
        async fn start(&self) -> QueryResult<Output> {
            if self.should_cancel {
                return Err(QueryError::Cancel);
            }

            if self.should_succeed {
                Ok(Output::Nil(()))
            } else {
                Err(QueryError::NotImplemented {
                    err: "Mock execution failed".to_string(),
                })
            }
        }

        fn cancel(&self) -> QueryResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_mock_query_execution_success() {
        let execution = MockQueryExecution {
            should_succeed: true,
            should_cancel: false,
        };

        let result = execution.start().await;
        assert!(result.is_ok(), "Mock execution should succeed");

        if let Ok(Output::Nil(_)) = result {
            // Expected result
        } else {
            panic!("Expected Output::Nil");
        }
    }

    #[tokio::test]
    async fn test_mock_query_execution_failure() {
        let execution = MockQueryExecution {
            should_succeed: false,
            should_cancel: false,
        };

        let result = execution.start().await;
        assert!(result.is_err(), "Mock execution should fail");

        if let Err(QueryError::NotImplemented { .. }) = result {
            // Expected error
        } else {
            panic!("Expected NotImplemented error");
        }
    }

    #[tokio::test]
    async fn test_mock_query_execution_cancel() {
        let execution = MockQueryExecution {
            should_succeed: false,
            should_cancel: true,
        };

        let result = execution.start().await;
        assert!(result.is_err(), "Cancelled execution should fail");

        if let Err(QueryError::Cancel) = result {
            // Expected cancellation error
        } else {
            panic!("Expected Cancel error");
        }

        let cancel_result = execution.cancel();
        assert!(cancel_result.is_ok(), "Cancel should succeed");
    }

    #[test]
    fn test_query_execution_default_type() {
        let execution = MockQueryExecution {
            should_succeed: true,
            should_cancel: false,
        };

        assert_eq!(execution.query_type(), QueryType::Batch);
    }
}
