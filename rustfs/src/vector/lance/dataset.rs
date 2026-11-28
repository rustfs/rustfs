// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    num::NonZeroUsize,
    sync::Arc,
};

use futures::TryStreamExt;
use lance::{
    Dataset,
    dataset::{
        ProjectionRequest,
        write::{
            CommitBuilder, DeleteBuilder,
            merge_insert::{MergeInsertBuilder, MergeInsertWriteMode, WhenMatched, WhenNotMatched},
        },
    },
    deps::{
        arrow_array::{
            Array, ArrayRef, FixedSizeListArray, Float32Array, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray,
            UInt64Array,
        },
        arrow_schema::{ArrowError, DataType, Field, Schema as ArrowSchema},
        datafusion::{
            logical_expr::Expr,
            prelude::{col, lit},
        },
    },
};
use lance_file::version::LanceFileVersion;
use lance_table::io::commit::CommitHandler;
use serde_json::{Map, Value};

use crate::vector::{
    filter::ValidatedFilter,
    model::{VectorDistanceMetric, VectorInputError, validate_metadata_key_count, validate_vector},
};

use super::{LanceManifestError, VectorIndexManifestSpec, filter::filter_expr};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VectorRecord {
    pub key: String,
    pub data: Vec<f32>,
    pub metadata: Option<Map<String, Value>>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ProjectedVector {
    pub key: String,
    pub data: Option<Vec<f32>>,
    pub metadata: Option<Map<String, Value>>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct VectorProjection {
    pub data: bool,
    pub metadata: bool,
}

impl VectorProjection {
    fn columns(self) -> Vec<&'static str> {
        let mut columns = vec!["key"];
        if self.data {
            columns.push("vector");
        }
        if self.metadata {
            columns.push("metadata");
        }
        columns
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct QueryMatch {
    pub key: String,
    pub row_id: u64,
    pub distance_f64: f64,
}

struct RankedQueryMatch(QueryMatch);

impl Ord for RankedQueryMatch {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .distance_f64
            .total_cmp(&other.0.distance_f64)
            .then_with(|| self.0.key.as_bytes().cmp(other.0.key.as_bytes()))
            .then_with(|| self.0.row_id.cmp(&other.0.row_id))
    }
}

impl PartialOrd for RankedQueryMatch {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for RankedQueryMatch {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for RankedQueryMatch {}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DatasetError {
    #[error("vector input is invalid")]
    InvalidVector(#[from] VectorInputError),
    #[error("Lance Dataset operation failed")]
    Engine(#[from] lance_core::Error),
    #[error("Lance Dataset Arrow value is invalid")]
    Arrow(#[from] ArrowError),
    #[error("Lance Dataset metadata is invalid JSON")]
    Json(#[from] serde_json::Error),
    #[error("Lance Dataset binding failed")]
    Manifest(#[from] LanceManifestError),
    #[error("Lance Dataset contains an invalid {column} value")]
    InvalidColumn { column: &'static str },
    #[error("Lance Dataset row offset exceeds the engine limit")]
    InvalidOffset,
    #[error("Lance Dataset query limit exceeds 10000")]
    InvalidQueryLimit,
    #[error(transparent)]
    Budget(#[from] crate::table_catalog::VectorMaintenanceBudgetError),
    #[error("vector batch repeats a key at position {position}")]
    DuplicateInputKey { position: usize },
}

impl DatasetError {
    pub(crate) fn is_definite_conflict(&self) -> bool {
        matches!(
            self,
            Self::Manifest(LanceManifestError::ExpectedHeadConflict) | Self::Engine(lance_core::Error::CommitConflict { .. })
        )
    }
}

pub(crate) fn validate_record_keys(records: &[VectorRecord]) -> Result<(), DatasetError> {
    let mut keys = BTreeSet::new();
    for (position, record) in records.iter().enumerate() {
        if !keys.insert(record.key.as_str()) {
            return Err(DatasetError::DuplicateInputKey { position });
        }
    }
    Ok(())
}

fn serialize_metadata(
    metadata: Option<&Map<String, Value>>,
    non_filterable_keys: &BTreeSet<String>,
) -> Result<Option<String>, DatasetError> {
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    // Reuse the persisted JSON representation when measuring the total metadata bytes.
    let json = serde_json::to_string(metadata)?;
    if json.len() > 40_960 {
        return Err(VectorInputError::MetadataTooLarge { size_bytes: json.len() }.into());
    }
    let filterable_bytes = if metadata.keys().any(|key| non_filterable_keys.contains(key)) {
        let filterable: BTreeMap<_, _> = metadata
            .iter()
            .filter(|(key, _)| !non_filterable_keys.contains(*key))
            .collect();
        serde_json::to_vec(&filterable)?.len()
    } else {
        json.len()
    };
    if filterable_bytes > 2_048 {
        return Err(VectorInputError::FilterableMetadataTooLarge {
            size_bytes: filterable_bytes,
        }
        .into());
    }
    Ok(Some(json))
}

pub(super) fn record_batch(records: &[VectorRecord], index: &VectorIndexManifestSpec) -> Result<RecordBatch, DatasetError> {
    validate_record_keys(records)?;
    for record in records {
        if let Some(metadata) = &record.metadata {
            validate_metadata_key_count(metadata)?;
        }
        validate_vector(&record.data, index.dimension, index.distance_metric)?;
    }
    let dimension = i32::try_from(index.dimension.get()).map_err(|_| DatasetError::InvalidColumn { column: "vector" })?;
    let vector_field = Arc::new(Field::new("item", DataType::Float32, false));
    let data = Float32Array::from_iter_values(records.iter().flat_map(|record| record.data.iter().copied()));
    let vectors = FixedSizeListArray::try_new(vector_field.clone(), dimension, Arc::new(data), None)?;
    let metadata = records
        .iter()
        .map(|record| serialize_metadata(record.metadata.as_ref(), &index.non_filterable_metadata_keys))
        .collect::<Result<Vec<_>, _>>()?;
    let metadata = lance_arrow::json::JsonArray::try_from_iter(metadata)?.into_inner();
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("vector", DataType::FixedSizeList(vector_field, dimension), false),
        lance_arrow::json::json_field("metadata", true),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(records.iter().map(|record| record.key.as_str()))),
            Arc::new(vectors),
            Arc::new(metadata),
        ],
    )?)
}

pub(super) async fn put(
    dataset: Arc<Dataset>,
    batch: RecordBatch,
    handler: Arc<dyn CommitHandler>,
) -> Result<Dataset, DatasetError> {
    let uncommitted = MergeInsertBuilder::try_new(dataset.clone(), vec!["key".to_string()])?
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .write_mode(MergeInsertWriteMode::RewriteRows)
        .data_storage_version(LanceFileVersion::V2_2)
        .conflict_retries(0)
        .commit_retries(0)
        .skip_auto_cleanup(true)
        .try_build()?
        .execute_uncommitted_batches(vec![batch])
        .await?;
    let mut commit = commit_builder(dataset, handler);
    if let Some(affected_rows) = uncommitted.affected_rows {
        commit = commit.with_affected_rows(affected_rows);
    }
    Ok(commit.execute(uncommitted.transaction).await?)
}

pub(super) async fn delete(
    dataset: Arc<Dataset>,
    keys: &[String],
    handler: Arc<dyn CommitHandler>,
) -> Result<Dataset, DatasetError> {
    let uncommitted = DeleteBuilder::from_expr(dataset.clone(), key_filter(keys))
        .conflict_retries(0)
        .execute_uncommitted()
        .await?;
    let mut commit = commit_builder(dataset, handler);
    if let Some(affected_rows) = uncommitted.affected_rows {
        commit = commit.with_affected_rows(affected_rows);
    }
    Ok(commit.execute(uncommitted.transaction).await?)
}

fn commit_builder(dataset: Arc<Dataset>, handler: Arc<dyn CommitHandler>) -> CommitBuilder<'static> {
    CommitBuilder::new(dataset)
        .with_commit_handler(handler)
        .with_storage_format(LanceFileVersion::V2_2)
        .with_max_retries(0)
        .with_skip_auto_cleanup(true)
}

fn key_filter(keys: &[String]) -> Expr {
    col("key").in_list(keys.iter().cloned().map(lit).collect(), false)
}

pub(super) async fn get(
    dataset: &Dataset,
    keys: &[String],
    projection: VectorProjection,
) -> Result<Vec<ProjectedVector>, DatasetError> {
    let mut scanner = dataset.scan();
    scanner.project(&projection.columns())?.filter_expr(key_filter(keys));
    let batch = scanner.try_into_batch().await?;
    decode_records(&batch, projection)
}

pub(super) async fn list_batch(
    dataset: &Dataset,
    offset: u64,
    max_rows: NonZeroUsize,
    projection: VectorProjection,
) -> Result<Vec<ProjectedVector>, DatasetError> {
    let offset = i64::try_from(offset).map_err(|_| DatasetError::InvalidOffset)?;
    let limit = i64::try_from(max_rows.get()).map_err(|_| DatasetError::InvalidOffset)?;
    let mut scanner = dataset.scan();
    scanner
        .project(&projection.columns())?
        .scan_in_order(true)
        .use_scalar_index(false)
        .limit(Some(limit), Some(offset))?;
    let batch = scanner.try_into_batch().await?;
    decode_records(&batch, projection)
}

pub(super) async fn query_matches(
    dataset: &Dataset,
    query: &[f32],
    index: &VectorIndexManifestSpec,
    top_k: NonZeroUsize,
    filter: Option<ValidatedFilter>,
    ann_partitions: Option<usize>,
    #[cfg(test)] ann_completed: &std::sync::atomic::AtomicUsize,
) -> Result<Vec<QueryMatch>, DatasetError> {
    validate_vector(query, index.dimension, index.distance_metric)?;
    if top_k.get() > 10_000 {
        return Err(DatasetError::InvalidQueryLimit);
    }
    let query_norm = if index.distance_metric == VectorDistanceMetric::Cosine {
        query.iter().map(|value| f64::from(*value).powi(2)).sum::<f64>().sqrt()
    } else {
        0.0
    };

    if let Some(partitions) = ann_partitions.filter(|_| numeric_vector_eligible(query, index.distance_metric))
        && let Some(matches) = ann_query_matches(dataset, query, index, top_k, filter.clone(), partitions).await?
    {
        #[cfg(test)]
        ann_completed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        return Ok(matches);
    }
    let mut scanner = dataset.scan();
    scanner
        .project(&["key", "vector"])?
        .with_row_id()
        .prefilter(true)
        .use_scalar_index(false)
        .scan_in_order(true)
        .batch_size(256)
        .batch_size_bytes(4 * 1024 * 1024)
        .batch_readahead(1)
        .fragment_readahead(1)
        .io_buffer_size(8 * 1024 * 1024);
    if let Some(filter) = filter {
        scanner.filter_expr(filter_expr(filter));
    }
    let mut stream = scanner.try_into_stream().await?;
    let mut matches = BinaryHeap::<RankedQueryMatch>::with_capacity(top_k.get());
    while let Some(batch) = stream.try_next().await? {
        let keys = required_array::<StringArray>(&batch, "key")?;
        let row_ids = required_array::<UInt64Array>(&batch, "_rowid")?;
        let vectors = required_array::<FixedSizeListArray>(&batch, "vector")?;
        for row in 0..batch.num_rows() {
            let values = vectors.value(row);
            let values = values
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or(DatasetError::InvalidColumn { column: "vector" })?;
            if values.null_count() != 0 {
                return Err(DatasetError::InvalidColumn { column: "vector" });
            }
            validate_vector(values.values(), index.dimension, index.distance_metric)
                .map_err(|_| DatasetError::InvalidColumn { column: "vector" })?;
            let candidate = RankedQueryMatch(QueryMatch {
                key: keys.value(row).to_string(),
                row_id: row_ids.value(row),
                distance_f64: distance_f64(query, values.values(), index.distance_metric, query_norm),
            });
            if matches.len() < top_k.get() {
                matches.push(candidate);
            } else if let Some(mut farthest) = matches.peek_mut()
                && candidate < *farthest
            {
                *farthest = candidate;
            }
        }
        tokio::task::yield_now().await;
    }
    Ok(matches.into_sorted_vec().into_iter().map(|value| value.0).collect())
}

fn distance_f64(query: &[f32], vector: &[f32], metric: VectorDistanceMetric, query_norm: f64) -> f64 {
    // At most 4096 finite float32 components keep all products and sums within
    // float64's finite range, including the smallest float32 subnormal norms.
    let distance = match metric {
        VectorDistanceMetric::Euclidean => query
            .iter()
            .zip(vector)
            .map(|(query_value, vector_value)| (f64::from(*query_value) - f64::from(*vector_value)).powi(2))
            .sum::<f64>()
            .sqrt(),
        VectorDistanceMetric::Cosine => {
            let (dot, norm_squared) = query.iter().zip(vector).fold((0.0, 0.0), |(dot, norm_squared), (q, v)| {
                let q = f64::from(*q);
                let v = f64::from(*v);
                (dot + q * v, norm_squared + v * v)
            });
            (1.0 - dot / (query_norm * norm_squared.sqrt())).clamp(0.0, 2.0)
        }
    };
    if distance == 0.0 { 0.0 } else { distance }
}

pub(super) async fn take_rows(
    dataset: &Dataset,
    row_ids: &[u64],
    projection: VectorProjection,
) -> Result<Vec<ProjectedVector>, DatasetError> {
    let projection_request = ProjectionRequest::from_columns(projection.columns(), dataset.schema());
    let batch = dataset.take_rows(row_ids, projection_request).await?;
    decode_records(&batch, projection)
}

fn required_array<'a, T: Array + 'static>(batch: &'a RecordBatch, column: &'static str) -> Result<&'a T, DatasetError> {
    let values = batch
        .column_by_name(column)
        .and_then(|values| values.as_any().downcast_ref::<T>())
        .ok_or(DatasetError::InvalidColumn { column })?;
    if values.null_count() > 0 {
        return Err(DatasetError::InvalidColumn { column });
    }
    Ok(values)
}

fn decode_records(batch: &RecordBatch, projection: VectorProjection) -> Result<Vec<ProjectedVector>, DatasetError> {
    let keys = required_array::<StringArray>(batch, "key")?;
    let vectors = projection
        .data
        .then(|| required_array::<FixedSizeListArray>(batch, "vector"))
        .transpose()?;
    let metadata = projection
        .metadata
        .then(|| {
            batch
                .column_by_name("metadata")
                .ok_or(DatasetError::InvalidColumn { column: "metadata" })
        })
        .transpose()?;
    (0..batch.num_rows())
        .map(|row| {
            let data = vectors
                .map(|vectors| {
                    let values = vectors.value(row);
                    let values = values
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .ok_or(DatasetError::InvalidColumn { column: "vector" })?;
                    if values.null_count() != 0 || values.values().iter().any(|value| !value.is_finite()) {
                        return Err(DatasetError::InvalidColumn { column: "vector" });
                    }
                    Ok(values.values().to_vec())
                })
                .transpose()?;
            Ok(ProjectedVector {
                key: keys.value(row).to_string(),
                data,
                metadata: metadata.map(|values| decode_metadata(values, row)).transpose()?.flatten(),
            })
        })
        .collect()
}

fn decode_metadata(values: &ArrayRef, row: usize) -> Result<Option<Map<String, Value>>, DatasetError> {
    if values.is_null(row) {
        return Ok(None);
    }
    let json = if let Some(values) = values.as_any().downcast_ref::<LargeBinaryArray>() {
        lance_arrow::json::decode_json(values.value(row))
    } else if let Some(values) = values.as_any().downcast_ref::<StringArray>() {
        values.value(row).to_string()
    } else if let Some(values) = values.as_any().downcast_ref::<LargeStringArray>() {
        values.value(row).to_string()
    } else {
        return Err(DatasetError::InvalidColumn { column: "metadata" });
    };
    Ok(Some(serde_json::from_str(&json)?))
}

pub(super) fn numeric_vector_eligible(vector: &[f32], metric: VectorDistanceMetric) -> bool {
    vector.iter().all(|value| value.is_finite() && value.abs() <= 1_048_576.0)
        && (metric != VectorDistanceMetric::Cosine
            || vector.iter().map(|value| f64::from(*value).powi(2)).sum::<f64>() >= 2f64.powi(-40))
}

pub(super) async fn numeric_eligibility(
    dataset: &Dataset,
    index: &VectorIndexManifestSpec,
    control: Option<&crate::table_catalog::VectorMaintenanceControl>,
) -> Result<Option<u64>, DatasetError> {
    let mut scanner = dataset.scan();
    scanner
        .project(&["vector"])?
        .use_scalar_index(false)
        .batch_size(128)
        .batch_size_bytes(4 * 1024 * 1024)
        .batch_readahead(1)
        .fragment_readahead(1)
        .io_buffer_size(8 * 1024 * 1024);
    let mut stream = scanner.try_into_stream().await?;
    let mut count = 0u64;
    while let Some(batch) = stream.try_next().await? {
        let _reservation = control
            .map(|control| control.reserve_batch(batch.get_array_memory_size() as u64))
            .transpose()?;
        let vectors = required_array::<FixedSizeListArray>(&batch, "vector")?;
        for row in 0..batch.num_rows() {
            let values = vectors.value(row);
            let values = values
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or(DatasetError::InvalidColumn { column: "vector" })?;
            if values.null_count() != 0 || !numeric_vector_eligible(values.values(), index.distance_metric) {
                return Ok(None);
            }
        }
        count = count
            .checked_add(batch.num_rows() as u64)
            .ok_or(DatasetError::InvalidOffset)?;
        tokio::task::yield_now().await;
    }
    Ok(Some(count))
}

struct MaintenanceIndexProgress(crate::table_catalog::VectorMaintenanceControl);

impl std::fmt::Debug for MaintenanceIndexProgress {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("MaintenanceIndexProgress").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl lance_index::progress::IndexBuildProgress for MaintenanceIndexProgress {
    async fn stage_start(&self, _: &str, _: Option<u64>, _: &str) -> lance_core::Result<()> {
        self.stage_complete("").await
    }
    async fn stage_progress(&self, _: &str, _: u64) -> lance_core::Result<()> {
        self.stage_complete("").await
    }
    async fn stage_complete(&self, _: &str) -> lance_core::Result<()> {
        self.0
            .check_admission()
            .map_err(|error| lance_core::Error::invalid_input(error.to_string()))
    }
}

pub(super) async fn build_ann(
    dataset: Arc<Dataset>,
    index: &VectorIndexManifestSpec,
    handler: Arc<dyn CommitHandler>,
    control: &crate::table_catalog::VectorMaintenanceControl,
) -> Result<Option<Dataset>, DatasetError> {
    use lance::index::{CreateIndexBuilder, DatasetIndexExt, vector::VectorIndexParams};
    use lance_index::{IndexType, vector::ivf::builder::IvfBuildParams};
    use lance_table::transaction::{Operation, TransactionBuilder};
    control.check_admission()?;
    let Some(rows) = numeric_eligibility(&dataset, index, Some(control)).await? else {
        return Ok(None);
    };
    if rows < 1024 {
        return Ok(None);
    }
    let partitions = usize::try_from((rows / 256).clamp(1, 64)).map_err(|_| DatasetError::InvalidOffset)?;
    let old_indices = dataset.load_indices().await?;
    if let Some(old) = old_indices
        .iter()
        .find(|metadata| metadata.name == super::files::ANN_INDEX_NAME)
    {
        let coverage = old
            .fragment_bitmap
            .as_ref()
            .ok_or(DatasetError::InvalidColumn { column: "ANN coverage" })?;
        let uncovered = dataset
            .get_fragments()
            .into_iter()
            .filter(|fragment| !coverage.contains(fragment.id() as u32))
            .try_fold(0usize, |sum, fragment| {
                sum.checked_add(fragment.metadata().num_rows().unwrap_or(0))
                    .ok_or(DatasetError::InvalidOffset)
            })?;
        if (uncovered as u64) < 1024.max(rows.div_ceil(5)) {
            return Ok(None);
        }
    }
    #[cfg(test)]
    crate::vector::fault_test::ann_input_opened(
        dataset
            .get_fragments()
            .iter()
            .map(|fragment| fragment.metadata().id)
            .collect(),
        rows,
    )
    .await;
    let params = VectorIndexParams::with_ivf_flat_params(
        match index.distance_metric {
            VectorDistanceMetric::Cosine => "cosine",
            VectorDistanceMetric::Euclidean => "l2",
        }
        .try_into()?,
        IvfBuildParams {
            max_iters: 20,
            sample_rate: 64,
            ..IvfBuildParams::new(partitions)
        },
    );
    let mut working = dataset.as_ref().clone();
    let metadata = CreateIndexBuilder::new(&mut working, &["vector"], IndexType::IvfFlat, &params)
        .name(super::files::ANN_INDEX_NAME.to_owned())
        .replace(true)
        .train(true)
        .progress(Arc::new(MaintenanceIndexProgress(control.clone())))
        .execute_uncommitted()
        .await?;
    control.check_admission()?;
    let removed_indices = old_indices
        .iter()
        .filter(|metadata| metadata.name == super::files::ANN_INDEX_NAME)
        .cloned()
        .collect();
    let transaction = TransactionBuilder::new(
        dataset.version().version,
        Operation::CreateIndex {
            new_indices: vec![metadata],
            removed_indices,
        },
    )
    .build();
    Ok(Some(commit_builder(dataset, handler).execute(transaction).await?))
}

async fn ann_query_matches(
    dataset: &Dataset,
    query: &[f32],
    index: &VectorIndexManifestSpec,
    top_k: NonZeroUsize,
    filter: Option<ValidatedFilter>,
    partitions: usize,
) -> Result<Option<Vec<QueryMatch>>, DatasetError> {
    let rows = dataset.count_rows(None).await?;
    let candidate_k = top_k.get().checked_mul(4).ok_or(DatasetError::InvalidQueryLimit)?.min(rows);
    if candidate_k == 0 {
        return Ok(Some(Vec::new()));
    }
    for probes in [partitions.min(8), partitions] {
        let mut scanner = dataset.scan();
        scanner
            .project(&["key"])?
            .with_row_id()
            .nearest("vector", &Float32Array::from(query.to_vec()), candidate_k)?
            .distance_metric(
                match index.distance_metric {
                    VectorDistanceMetric::Cosine => "cosine",
                    VectorDistanceMetric::Euclidean => "l2",
                }
                .try_into()?,
            )
            .prefilter(true)
            .use_index(true)
            .use_scalar_index(false)
            .nprobes(probes);
        if let Some(filter) = filter.clone() {
            scanner.filter_expr(filter_expr(filter));
        }
        let mut stream = scanner.try_into_stream().await?;
        let mut candidates = Vec::with_capacity(candidate_k);
        let mut ids = BTreeSet::new();
        while let Some(batch) = stream.try_next().await? {
            let keys = required_array::<StringArray>(&batch, "key")?;
            let row_ids = required_array::<UInt64Array>(&batch, "_rowid")?;
            let distances = required_array::<Float32Array>(&batch, "_distance")?;
            for row in 0..batch.num_rows() {
                let distance = distances.value(row);
                if !distance.is_finite() || !ids.insert(row_ids.value(row)) || candidates.len() == candidate_k {
                    return Ok(None);
                }
                candidates.push((row_ids.value(row), keys.value(row).to_owned(), distance));
            }
        }
        if candidates.len() < top_k.get() {
            continue;
        }
        candidates.sort_by(|left, right| left.2.total_cmp(&right.2));
        if candidates.len() == candidate_k
            && candidates.len() > 1
            && candidates
                .last()
                .is_some_and(|last| candidates[candidates.len() - 2].2 == last.2)
        {
            return Ok(None);
        }
        let query_norm = query.iter().map(|value| f64::from(*value).powi(2)).sum::<f64>().sqrt();
        let mut matches = BinaryHeap::<RankedQueryMatch>::with_capacity(top_k.get());
        for chunk in candidates.chunks(128) {
            let row_ids = chunk.iter().map(|candidate| candidate.0).collect::<Vec<_>>();
            let originals = take_rows(
                dataset,
                &row_ids,
                VectorProjection {
                    data: true,
                    metadata: false,
                },
            )
            .await?;
            if originals.len() != chunk.len() {
                return Ok(None);
            }
            for (original, (row_id, key, _)) in originals.into_iter().zip(chunk) {
                let Some(vector) = original.data else {
                    return Ok(None);
                };
                if &original.key != key || !numeric_vector_eligible(&vector, index.distance_metric) {
                    return Ok(None);
                }
                let candidate = RankedQueryMatch(QueryMatch {
                    key: original.key,
                    row_id: *row_id,
                    distance_f64: distance_f64(query, &vector, index.distance_metric, query_norm),
                });
                if matches.len() < top_k.get() {
                    matches.push(candidate);
                } else if let Some(mut farthest) = matches.peek_mut()
                    && candidate < *farthest
                {
                    *farthest = candidate;
                }
            }
        }
        return Ok(Some(matches.into_sorted_vec().into_iter().map(|value| value.0).collect()));
    }
    Ok(None)
}

pub(super) fn compaction_options() -> lance::dataset::optimize::CompactionOptions {
    use lance::dataset::optimize::{CompactionOptions, IndexRemapMode};
    CompactionOptions {
        target_rows_per_fragment: 65_536,
        max_rows_per_group: 128,
        max_bytes_per_file: Some(67_108_864),
        num_threads: Some(1),
        batch_size: Some(128),
        io_buffer_size: Some(8_388_608),
        max_source_fragments: Some(8),
        max_source_rows: Some(65_536),
        max_source_bytes: Some(67_108_864),
        index_remap_mode: IndexRemapMode::Compact,
        defer_index_remap: false,
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..CompactionOptions::default()
    }
}
