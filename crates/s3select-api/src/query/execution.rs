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

use std::fmt::Display;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{Stream, StreamExt, TryStreamExt};
use parking_lot::RwLock;
use tracing::debug;

use crate::{QueryError, QueryResult};

use super::Query;
use super::logical_planner::Plan;
use super::session::SessionCtx;

pub struct PhaseTimer {
    phase_name: &'static str,
    start_time: Instant,
}

impl PhaseTimer {
    pub fn new(phase_name: &'static str) -> Self {
        Self {
            phase_name,
            start_time: Instant::now(),
        }
    }
}

impl Drop for PhaseTimer {
    fn drop(&mut self) {
        let duration = self.start_time.elapsed();

        if !std::thread::panicking() {
            metrics::histogram!(
                "rustfs_s3select_phase_duration_seconds",
                "phase" => self.phase_name
            )
            .record(duration.as_secs_f64());
        }

        debug!("Phase '{}' took {:?}", self.phase_name, duration);
    }
}

pub type QueryExecutionRef = Arc<dyn QueryExecution>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    Batch,
    Stream,
}

impl Display for QueryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Batch => write!(f, "batch"),
            Self::Stream => write!(f, "stream"),
        }
    }
}

#[async_trait]
pub trait QueryExecution: Send + Sync {
    fn query_type(&self) -> QueryType {
        QueryType::Batch
    }
    // Start
    async fn start(&self) -> QueryResult<Output>;
    // Stop
    fn cancel(&self) -> QueryResult<()>;
}

pub enum Output {
    StreamData(SendableRecordBatchStream),
    Nil(()),
}

impl Output {
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::StreamData(stream) => stream.schema(),
            Self::Nil(_) => Arc::new(Schema::empty()),
        }
    }

    pub async fn chunk_result(self) -> QueryResult<Vec<RecordBatch>> {
        match self {
            Self::Nil(_) => Ok(vec![]),
            Self::StreamData(stream) => {
                let schema = stream.schema();
                let mut res: Vec<RecordBatch> = stream.try_collect::<Vec<RecordBatch>>().await?;
                if res.is_empty() {
                    res.push(RecordBatch::new_empty(schema));
                }
                Ok(res)
            }
        }
    }

    pub async fn num_rows(self) -> usize {
        match self.chunk_result().await {
            Ok(rb) => rb.iter().map(|e| e.num_rows()).sum(),
            Err(_) => 0,
        }
    }

    /// Returns the number of records affected by the query operation  
    ///  
    /// If it is a select statement, returns the number of rows in the result set  
    ///  
    /// -1 means unknown  
    ///  
    /// panic! when StreamData's number of records greater than i64::Max  
    pub async fn affected_rows(self) -> i64 {
        self.num_rows().await as i64
    }
}

impl Stream for Output {
    type Item = Result<RecordBatch, QueryError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this {
            Output::StreamData(stream) => stream.poll_next_unpin(cx).map_err(|e| e.into()),
            Output::Nil(_) => Poll::Ready(None),
        }
    }
}

#[async_trait]
pub trait QueryExecutionFactory {
    async fn create_query_execution(
        &self,
        plan: Plan,
        query_state_machine: QueryStateMachineRef,
    ) -> QueryResult<QueryExecutionRef>;
}

pub type QueryStateMachineRef = Arc<QueryStateMachine>;

pub struct QueryStateMachine {
    pub session: SessionCtx,
    pub query: Query,

    state: RwLock<QueryState>,
    start: Instant,
}

impl QueryStateMachine {
    fn record_phase_timestamp(&self, phase: &'static str, event: &'static str) {
        let elapsed = self.start.elapsed();
        metrics::histogram!(
            "rustfs_s3select_phase_timestamp_seconds",
            "phase" => phase,
            "event" => event
        )
        .record(elapsed.as_secs_f64());
    }
    #[must_use]
    pub fn time_phase(&self, phase_name: &'static str) -> PhaseTimer {
        PhaseTimer::new(phase_name)
    }

    pub fn begin(query: Query, session: SessionCtx) -> Self {
        Self {
            session,
            query,
            state: RwLock::new(QueryState::ACCEPTING),
            start: Instant::now(),
        }
    }

    pub fn begin_analyze(&self) {
        self.record_phase_timestamp("analyze", "start");
        self.translate_to(QueryState::RUNNING(RUNNING::ANALYZING));
    }

    pub fn end_analyze(&self) {
        self.record_phase_timestamp("analyze", "end");
    }

    pub fn begin_optimize(&self) {
        self.record_phase_timestamp("optimize", "start");
        self.translate_to(QueryState::RUNNING(RUNNING::OPTIMIZING));
    }

    pub fn end_optimize(&self) {
        self.record_phase_timestamp("optimize", "end");
    }

    pub fn begin_schedule(&self) {
        self.record_phase_timestamp("schedule", "start");
        self.translate_to(QueryState::RUNNING(RUNNING::SCHEDULING));
    }

    pub fn end_schedule(&self) {
        self.record_phase_timestamp("schedule", "end");
    }

    pub fn finish(&self) {
        // TODO
        self.translate_to(QueryState::DONE(DONE::FINISHED));
    }

    pub fn cancel(&self) {
        // TODO
        self.translate_to(QueryState::DONE(DONE::CANCELLED));
    }

    pub fn fail(&self) {
        // TODO
        self.translate_to(QueryState::DONE(DONE::FAILED));
    }

    pub fn state(&self) -> QueryState {
        self.state.read().clone()
    }

    pub fn duration(&self) -> Duration {
        self.start.elapsed()
    }

    fn translate_to(&self, state: QueryState) {
        *self.state.write() = state;
    }
}

#[derive(Debug, Clone)]
pub enum QueryState {
    ACCEPTING,
    RUNNING(RUNNING),
    DONE(DONE),
}

impl AsRef<str> for QueryState {
    fn as_ref(&self) -> &str {
        match self {
            QueryState::ACCEPTING => "ACCEPTING",
            QueryState::RUNNING(e) => e.as_ref(),
            QueryState::DONE(e) => e.as_ref(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum RUNNING {
    DISPATCHING,
    ANALYZING,
    OPTIMIZING,
    SCHEDULING,
}

impl AsRef<str> for RUNNING {
    fn as_ref(&self) -> &str {
        match self {
            Self::DISPATCHING => "DISPATCHING",
            Self::ANALYZING => "ANALYZING",
            Self::OPTIMIZING => "OPTIMIZING",
            Self::SCHEDULING => "SCHEDULING",
        }
    }
}

#[derive(Debug, Clone)]
pub enum DONE {
    FINISHED,
    FAILED,
    CANCELLED,
}

impl AsRef<str> for DONE {
    fn as_ref(&self) -> &str {
        match self {
            Self::FINISHED => "FINISHED",
            Self::FAILED => "FAILED",
            Self::CANCELLED => "CANCELLED",
        }
    }
}
