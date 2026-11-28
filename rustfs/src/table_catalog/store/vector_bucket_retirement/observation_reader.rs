// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::super::*;
use crate::storage_api::startup::services::ECStore;
use crate::storage_api::table::test_support::BucketOperations as _;
use serde_json::{Value, json};

const MAX_OBSERVATION_ITEMS: usize = 8;
const MAX_OBSERVATION_BYTES: usize = 3 * 1024 * 1024;
const MAX_DATA_OBJECT_BYTES: usize = 1024 * 1024;

/// Read-only evidence authorized by the private test operator channel.
/// This owner has no lifecycle guard and exposes no mutation or business permit.
pub(crate) struct CatalogRetirementEvidenceReader {
    catalog: ObjectTableCatalogStore<EcStoreTableCatalogObjectBackend<ECStore>>,
    bucket: VectorBucketBindingV1,
    resource: Option<VectorMaintenanceResourceV1>,
    expected_authority: Option<CatalogMetadataRefRecordV1>,
}

pub(crate) struct CatalogRetirementEvidenceCursor {
    bucket: VectorBucketBindingV1,
    authority: Option<Box<CatalogBucketRetirementAuthority>>,
    step: Step,
    pending: Option<(&'static str, Value)>,
    native: Option<Value>,
}

enum Step {
    Data(Option<String>),
    UnplannedSources(Option<String>),
    Plan(Option<CatalogMetadataRefRecordV1>),
    Page(Box<PageCursor>),
    Done,
}
struct PageCursor {
    reference: CatalogMetadataRefRecordV1,
    page: VectorBucketCatalogRetirementPlanPageV1,
    progress_returned: bool,
    target: usize,
    resource: usize,
    proof_next: Option<CatalogMetadataRefRecordV1>,
    proof_started: bool,
}

impl CatalogRetirementEvidenceReader {
    pub(crate) fn new(
        backend: EcStoreTableCatalogObjectBackend<ECStore>,
        bucket: VectorBucketBindingV1,
        resource: Option<VectorMaintenanceResourceV1>,
        expected_authority: Option<CatalogMetadataRefRecordV1>,
    ) -> TableCatalogStoreResult<Self> {
        let handle = bucket.to_handle()?;
        if let Some(resource) = &resource {
            resource.validate()?;
            if resource.bucket != bucket {
                return Err(maintenance_error("Catalog observation resource has another bucket identity"));
            }
        }
        let paths = TableCatalogObjectPaths::for_vector();
        if let Some(reference) = &expected_authority
            && reference.object_key != paths.vector_bucket_retirement_authority_path(&handle)?
        {
            return Err(maintenance_error(
                "Catalog observation authority path differs from the complete bucket identity",
            ));
        }
        Ok(Self {
            catalog: ObjectTableCatalogStore {
                backend,
                paths,
                vector_bucket: Some(handle),
                vector_owner: Arc::new(()),
            },
            bucket,
            resource,
            expected_authority,
        })
    }

    pub(crate) async fn start(&self) -> TableCatalogStoreResult<CatalogRetirementEvidenceCursor> {
        // No mutation uses this empty signal set. Exact immutable references and
        // before/after physical identity reads protect this test-only observation.
        let fence = TableCatalogObjectMutationFence::default();
        let authority = self
            .catalog
            .read_bucket_retirement_authority(&self.bucket.to_handle()?, &fence)
            .await?;
        if let Some(expected) = &self.expected_authority
            && authority
                .as_ref()
                .map(|a| CatalogMetadataRefRecordV1::from(&a.reference))
                .as_ref()
                != Some(expected)
        {
            return Err(TableCatalogStoreError::Conflict(
                "Catalog observation authority changed or disappeared".into(),
            ));
        }
        if let Some(authority) = &authority {
            let (inventory, closure) = self
                .catalog
                .bucket_retirement_complete_digests(&authority.record, &fence)
                .await?;
            if inventory != authority.record.inventory_sha256 || closure != authority.record.closure_sha256 {
                return Err(maintenance_error("Catalog observation authority digest mismatch"));
            }
            if let Some(resource) = &self.resource {
                self.catalog
                    .bucket_retirement_resource_by_identity(&authority.record, resource, &fence)
                    .await?;
            }
        }
        Ok(CatalogRetirementEvidenceCursor {
            bucket: self.bucket.clone(),
            authority: authority.map(Box::new),
            step: Step::Data(None),
            pending: None,
            native: None,
        })
    }

    async fn native_bucket(&self) -> Value {
        let handle = match self.bucket.to_handle() {
            Ok(handle) => handle,
            Err(error) => return json!({"status":"Unavailable","current_bucket":null,"error":error.to_string()}),
        };
        let store = &self.catalog.backend.store;
        let sets = store.all_set_disks();
        if sets.is_empty() || sets.len() > 64 {
            return json!({"status":"Unavailable","current_bucket":null,"error":"native set observation bound"});
        }
        let mut present = 0;
        let mut absent = 0;
        // Each set applies its existing quorum. This bypasses only the ECStore
        // lifecycle-reader fallback, which cannot run inside a paused writer.
        for set in sets {
            match set
                .get_bucket_info(handle.storage_bucket_id().as_str(), &Default::default())
                .await
            {
                Ok(_) => present += 1,
                Err(StorageError::VolumeNotFound | StorageError::BucketNotFound(_)) => absent += 1,
                Err(error) => return json!({"status":"Unavailable","current_bucket":null,"error":error.to_string()}),
            }
        }
        match store.get_managed_bucket_metadata(handle.storage_bucket_id().as_str()).await {
            Ok(metadata) if present > 0 => match metadata.bucket_handle(handle.account_namespace_id(), handle.region()) {
                Ok(current) => match VectorBucketBindingV1::from_handle(&current) {
                    Ok(binding) => {
                        json!({"status":if current == handle {"PresentSame"} else {"Replaced"},"current_bucket":binding,"error":null,"sets_present":present,"sets_absent":absent})
                    }
                    Err(error) => json!({"status":"Unavailable","current_bucket":null,"error":error.to_string()}),
                },
                Err(error) => json!({"status":"Unavailable","current_bucket":null,"error":error.to_string()}),
            },
            Err(StorageError::ConfigNotFound | StorageError::BucketNotFound(_)) if present == 0 => {
                json!({"status":"Absent","current_bucket":null,"error":null,"sets_present":present,"sets_absent":absent})
            }
            Ok(_) => {
                json!({"status":"Unavailable","current_bucket":null,"error":"native bucket is absent while its persisted identity remains"})
            }
            Err(error) => json!({"status":"Unavailable","current_bucket":null,"error":error.to_string()}),
        }
    }

    pub(crate) async fn next(
        &self,
        mut cursor: CatalogRetirementEvidenceCursor,
    ) -> TableCatalogStoreResult<(Value, Option<CatalogRetirementEvidenceCursor>)> {
        if cursor.bucket != self.bucket {
            return Err(maintenance_error("Catalog observation cursor has another bucket identity"));
        }
        let fence = TableCatalogObjectMutationFence::default();
        let current = self
            .catalog
            .read_bucket_retirement_authority(&self.bucket.to_handle()?, &fence)
            .await?;
        if current.as_ref().map(|a| CatalogMetadataRefRecordV1::from(&a.reference))
            != cursor
                .authority
                .as_ref()
                .map(|a| CatalogMetadataRefRecordV1::from(&a.reference))
        {
            return Err(TableCatalogStoreError::Conflict(
                "Catalog observation authority changed during pagination".into(),
            ));
        }
        let native = self.native_bucket().await;
        if cursor.native.as_ref().is_some_and(|previous| *previous != native) {
            return Err(TableCatalogStoreError::Conflict(
                "native bucket identity changed across observation pages".into(),
            ));
        }
        cursor.native = Some(native.clone());
        let status = native["status"]
            .as_str()
            .ok_or_else(|| maintenance_error("missing native observation state"))?;
        let authority = if let Some(authority) = &cursor.authority {
            Some(
                self.exact_record::<VectorBucketCatalogRetirementV1>(&(&authority.reference).into())
                    .await?
                    .1,
            )
        } else {
            None
        };
        let mut output = json!({"bucket":self.bucket,"resource":self.resource,"native_bucket":native,
            "data_objects":[],"catalog_sources":[],"authority":authority,"plan_pages":[],"proof_pages":[],"progress_pages":[]});
        for _ in 0..MAX_OBSERVATION_ITEMS {
            let item = match cursor.pending.take() {
                Some(item) => Some(item),
                None => self.next_item(&mut cursor, status).await?,
            };
            let Some((category, value)) = item else { break };
            output[category]
                .as_array_mut()
                .ok_or_else(|| maintenance_error("invalid observation collection"))?
                .push(value);
            if serde_json::to_vec(&output)
                .map_err(|error| maintenance_error(&error.to_string()))?
                .len()
                > MAX_OBSERVATION_BYTES
            {
                let value = output[category]
                    .as_array_mut()
                    .and_then(Vec::pop)
                    .ok_or_else(|| maintenance_error("missing bounded observation item"))?;
                let item_count: usize = [
                    "data_objects",
                    "catalog_sources",
                    "plan_pages",
                    "proof_pages",
                    "progress_pages",
                ]
                .iter()
                .filter_map(|key| output[*key].as_array().map(Vec::len))
                .sum();
                if item_count == 0 {
                    return Err(TableCatalogStoreError::Unavailable(
                        "Catalog observation record exceeds the worker response byte bound".into(),
                    ));
                }
                cursor.pending = Some((category, value));
                break;
            }
        }
        if self.native_bucket().await != native {
            return Err(TableCatalogStoreError::Conflict(
                "native bucket identity changed during observation".into(),
            ));
        }
        let done = matches!(cursor.step, Step::Done) && cursor.pending.is_none();
        Ok((output, (!done).then_some(cursor)))
    }

    async fn exact_record<T: VectorMaintenanceRecord>(
        &self,
        reference: &CatalogMetadataRefRecordV1,
    ) -> TableCatalogStoreResult<(T, Value)> {
        let fence = TableCatalogObjectMutationFence::default();
        let record: T = self.catalog.read_bucket_retirement_record(reference, &fence).await?;
        let object = self
            .catalog
            .backend
            .read_object_limited(self.catalog.catalog_bucket(), &reference.object_key, T::MAX_BYTES)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound("Catalog observation exact record".into()))?;
        let actual = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(reference.object_key.clone())?, &object)?;
        if CatalogMetadataRefRecordV1::from(&actual) != *reference {
            return Err(TableCatalogStoreError::Conflict("Catalog observation exact record changed".into()));
        }
        Ok((record, record_value(reference.clone(), &object)?))
    }

    async fn next_item(
        &self,
        cursor: &mut CatalogRetirementEvidenceCursor,
        status: &str,
    ) -> TableCatalogStoreResult<Option<(&'static str, Value)>> {
        let fence = TableCatalogObjectMutationFence::default();
        loop {
            let step = std::mem::replace(&mut cursor.step, Step::Done);
            match step {
                Step::Data(after) => {
                    if status == "PresentSame"
                        && let Some(resource) = &self.resource
                    {
                        let prefix = resource.identity()?.dataset_prefix();
                        let page = self
                            .catalog
                            .backend
                            .list_objects_page(
                                self.bucket.storage_bucket_id.as_str(),
                                &prefix,
                                after.as_deref(),
                                NonZeroUsize::MIN,
                            )
                            .await?;
                        if page.objects.len() > 1 || (page.objects.is_empty() && page.is_truncated) {
                            return Err(maintenance_error("invalid bounded native data page"));
                        }
                        if let Some(key) = page.objects.into_iter().next() {
                            if !key.starts_with(&prefix) || after.as_ref().is_some_and(|after| key <= *after) {
                                return Err(maintenance_error("native data page escaped its captured resource"));
                            }
                            let object = self
                                .catalog
                                .backend
                                .read_object_limited(self.bucket.storage_bucket_id.as_str(), &key, MAX_DATA_OBJECT_BYTES)
                                .await?
                                .ok_or_else(|| {
                                    TableCatalogStoreError::Conflict("native data object disappeared after listing".into())
                                })?;
                            let value = json!({"object_key":key,"exists":true,"body_revision":object.body_revision,"etag":object.etag,
                                "content_length":object.data.len(),"content_sha256":CatalogContentSha256::digest(&object.data).to_hex(),"mod_time":object.mod_time.map(|value| json!({"unix_seconds":value.unix_timestamp(),"nanosecond":value.nanosecond()}))});
                            cursor.step = Step::Data(Some(key));
                            return Ok(Some(("data_objects", value)));
                        }
                    }
                    cursor.step = if let Some(authority) = &cursor.authority {
                        Step::Plan(Some(authority.record.plan_root.clone()))
                    } else {
                        Step::UnplannedSources(None)
                    };
                }
                Step::UnplannedSources(after) => {
                    if matches!(status, "PresentSame" | "Absent")
                        && let Some((key, object)) = self
                            .catalog
                            .next_bucket_catalog_source(&self.bucket.to_handle()?, after.as_deref(), &fence)
                            .await?
                    {
                        let reference = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(key.clone())?, &object)?;
                        let mut value = record_value((&reference).into(), &object)?;
                        value["object_key"] = json!(key);
                        value["exists"] = json!(true);
                        value["expected_reference"] = Value::Null;
                        value["not_read_reason"] = Value::Null;
                        cursor.step = Step::UnplannedSources(Some(key));
                        return Ok(Some(("catalog_sources", value)));
                    }
                }
                Step::Plan(Some(reference)) => {
                    let (page, value) = self
                        .exact_record::<VectorBucketCatalogRetirementPlanPageV1>(&reference)
                        .await?;
                    cursor.step = Step::Page(Box::new(PageCursor {
                        reference,
                        page,
                        progress_returned: false,
                        target: 0,
                        resource: 0,
                        proof_next: None,
                        proof_started: false,
                    }));
                    return Ok(Some(("plan_pages", value)));
                }
                Step::Page(mut page) => {
                    let authority = cursor
                        .authority
                        .as_ref()
                        .ok_or_else(|| maintenance_error("observation plan has no authority"))?;
                    if !page.progress_returned {
                        let path = self.catalog.paths.vector_bucket_retirement_page_path(
                            &self.bucket.to_handle()?,
                            authority.record.retirement_id,
                            "progress",
                            page.page.page_id,
                        )?;
                        let object = self
                            .catalog
                            .backend
                            .read_object_limited(self.catalog.catalog_bucket(), &path, VECTOR_RETIREMENT_PROGRESS_MAX_BYTES)
                            .await?
                            .ok_or_else(|| {
                                TableCatalogStoreError::Unavailable("Catalog observation progress page is missing".into())
                            })?;
                        let progress: VectorBucketCatalogRetirementProgressPageV1 =
                            decode_vector_maintenance_record(&object.data)?;
                        if progress.bucket != self.bucket
                            || progress.retirement_id != authority.record.retirement_id
                            || progress.plan_page != page.reference
                            || progress.targets.len() != page.page.targets.len()
                        {
                            return Err(maintenance_error("Catalog observation progress does not bind its complete plan"));
                        }
                        let reference = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(path)?, &object)?;
                        if reference.etag().is_none_or(str::is_empty) {
                            return Err(maintenance_error("Catalog observation progress has no exact native ETag"));
                        }
                        let value = record_value((&reference).into(), &object)?;
                        page.progress_returned = true;
                        cursor.step = Step::Page(page);
                        return Ok(Some(("progress_pages", value)));
                    }
                    if let Some(target) = page.page.targets.get(page.target) {
                        let expected = catalog_retirement_target_reference(target);
                        let mut value = json!({"object_key":target.object_key,"expected_reference":expected,"exists":null,"reference":null,"content":null,"bytes_base64":null,"not_read_reason":null});
                        if matches!(status, "Replaced" | "Unavailable") {
                            value["not_read_reason"] = json!(status);
                        } else {
                            let object = self
                                .catalog
                                .backend
                                .read_object_limited(
                                    self.catalog.catalog_bucket(),
                                    &target.object_key,
                                    VECTOR_BUCKET_RETIREMENT_PAGE_MAX_BYTES,
                                )
                                .await?;
                            value["exists"] = json!(object.is_some());
                            if let Some(object) = object {
                                let reference = CatalogMetadataRef::from_read(
                                    CatalogObjectKey::try_new(target.object_key.clone())?,
                                    &object,
                                )?;
                                let record = record_value((&reference).into(), &object)?;
                                for key in ["reference", "content", "bytes_base64"] {
                                    value[key] = record[key].clone();
                                }
                            }
                        }
                        page.target += 1;
                        cursor.step = Step::Page(page);
                        return Ok(Some(("catalog_sources", value)));
                    }
                    if let Some(resource) = page.page.resources.get(page.resource) {
                        if !page.proof_started {
                            page.proof_next = Some(resource.proof_root.clone());
                            page.proof_started = true;
                        }
                        if let Some(reference) = page.proof_next.take() {
                            let (proof, value) = self
                                .exact_record::<VectorCatalogRetiredResourceProofPageV1>(&reference)
                                .await?;
                            page.proof_next = proof.next_page;
                            cursor.step = Step::Page(page);
                            return Ok(Some(("proof_pages", value)));
                        }
                        page.resource += 1;
                        page.proof_started = false;
                        cursor.step = Step::Page(page);
                    } else {
                        cursor.step = Step::Plan(page.page.next_page);
                    }
                }
                Step::Plan(None) | Step::Done => return Ok(None),
            }
        }
    }
}

fn record_value(reference: CatalogMetadataRefRecordV1, object: &TableCatalogObject) -> TableCatalogStoreResult<Value> {
    let content: Value = serde_json::from_slice(&object.data).map_err(|error| maintenance_error(&error.to_string()))?;
    Ok(json!({"reference":reference,"content":content,"bytes_base64":base64_simd::STANDARD.encode_to_string(&object.data)}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_catalog::tests::prepared_commit::run_native_vector_delete_test;

    fn check_raw_record(value: &Value) {
        let bytes = base64_simd::STANDARD
            .decode_to_vec(value["bytes_base64"].as_str().unwrap())
            .unwrap();
        let reference: CatalogMetadataRefRecordV1 = serde_json::from_value(value["reference"].clone()).unwrap();
        assert_eq!(reference.content_length, u64::try_from(bytes.len()).unwrap());
        assert_eq!(reference.content_sha256, CatalogContentSha256::digest(&bytes).to_hex());
        assert_eq!(value["content"], serde_json::from_slice::<Value>(&bytes).unwrap());
    }

    #[test]
    #[serial_test::serial]
    fn catalog_retirement_observation_reopens_exact_proof_after_physical_deletion() {
        run_native_vector_delete_test("retirement-observe-deleted", || async {
            let (store, resource, _fixture) = evidence::tests::completed_native_resource().await;
            let bucket = resource.bucket.to_handle().unwrap();
            let physical = store.backend.store.clone();
            let instance = physical.id;
            let owner = store.clone();
            let captured = bucket.clone();
            physical
                .delete_managed_bucket_with_preparation(&bucket, move |writer| async move {
                    owner
                        .prepare_vector_bucket_delete(&captured, writer, CatalogCleanupExecution::new(instance).unwrap())
                        .await
                        .map_err(crate::storage_api::table::StorageError::other)
                })
                .await
                .unwrap();
            let authority = store
                .read_bucket_retirement_authority(&bucket, &TableCatalogObjectMutationFence::default())
                .await
                .unwrap()
                .unwrap();
            let reference: CatalogMetadataRefRecordV1 = (&authority.reference).into();
            let reader = CatalogRetirementEvidenceReader::new(
                store.backend.clone(),
                resource.bucket.clone(),
                Some(resource.clone()),
                Some(reference.clone()),
            )
            .unwrap();
            let mut cursor = reader.start().await.unwrap();
            let mut sources = 0_u64;
            let mut plans = 0;
            let mut proofs = 0;
            loop {
                let (page, next) = reader.next(cursor).await.unwrap();
                assert_eq!(page["native_bucket"]["status"], "Absent");
                assert_eq!(page["authority"]["reference"], serde_json::to_value(&reference).unwrap());
                check_raw_record(&page["authority"]);
                assert!(page["data_objects"].as_array().unwrap().is_empty());
                for source in page["catalog_sources"].as_array().unwrap() {
                    assert_eq!(source["exists"], false);
                    assert!(source["reference"].is_null());
                    assert!(!source["expected_reference"].is_null());
                    sources += 1;
                }
                for family in ["plan_pages", "proof_pages", "progress_pages"] {
                    for record in page[family].as_array().unwrap() {
                        check_raw_record(record);
                        match family {
                            "plan_pages" => plans += 1,
                            "proof_pages" => proofs += 1,
                            _ => {
                                for target in record["content"]["targets"].as_array().unwrap() {
                                    assert_eq!(target["state"], "DELETE_ACKNOWLEDGED");
                                }
                            }
                        }
                    }
                }
                let Some(next) = next else { break };
                cursor = next;
            }
            assert_eq!(sources, authority.record.target_count);
            assert_eq!(plans, authority.record.plan_page_count);
            assert!(proofs > 0);
            let mut outside = resource.clone();
            outside.table_id = Uuid::new_v4().to_string();
            outside.validate().unwrap();
            assert_eq!(outside.bucket, resource.bucket);
            assert_ne!(outside.table_id, resource.table_id);
            let outside = CatalogRetirementEvidenceReader::new(
                store.backend.clone(),
                resource.bucket.clone(),
                Some(outside),
                Some(reference.clone()),
            )
            .unwrap();
            assert!(matches!(outside.start().await, Err(TableCatalogStoreError::Invalid(message))
                if message == "Catalog target has no resource proof"));
            let bucket_only = CatalogRetirementEvidenceReader::new(
                store.backend.clone(),
                resource.bucket.clone(),
                None,
                Some(reference.clone()),
            )
            .unwrap();
            bucket_only.start().await.unwrap();
            let mut wrong = reference.clone();
            wrong.content_sha256 = "0".repeat(64);
            let wrong = CatalogRetirementEvidenceReader::new(
                store.backend.clone(),
                resource.bucket.clone(),
                Some(resource.clone()),
                Some(wrong),
            )
            .unwrap();
            assert!(matches!(wrong.start().await, Err(TableCatalogStoreError::Conflict(_))));
            let mut foreign = reference;
            foreign.object_key = "s3vectors/catalog-retirements/v1/foreign/authority.json".into();
            assert!(
                CatalogRetirementEvidenceReader::new(
                    store.backend.clone(),
                    resource.bucket.clone(),
                    Some(resource),
                    Some(foreign)
                )
                .is_err()
            );
        });
    }

    #[test]
    #[serial_test::serial]
    fn catalog_retirement_observation_preserves_unconfirmed_native_result_and_exact_bytes() {
        run_native_vector_delete_test("retirement-observe-unconfirmed", || async {
            let (store, resource, _fixture) = evidence::tests::completed_native_resource().await;
            let bucket = resource.bucket.to_handle().unwrap();
            let observed = Arc::new(parking_lot::Mutex::new(None));
            let output = observed.clone();
            let registration = observe_catalog_retirement(
                resource.bucket.clone(),
                Arc::new(move |event| {
                    let stop = event.phase == CatalogRetirementObservedPhase::TargetNativeReturned
                        && event.target.as_ref().is_some_and(|target| {
                            matches!(target.fenced_delete, Some(CatalogRetirementObservedOutcome::Succeeded))
                                && matches!(target.subsequent_fence, Some(CatalogRetirementObservedOutcome::Succeeded))
                        });
                    if stop {
                        *output.lock() = Some(event);
                    }
                    Box::pin(async move {
                        if stop {
                            Err(TableCatalogStoreError::Unavailable(
                                "observed actual native return before durable ACK".into(),
                            ))
                        } else {
                            Ok(())
                        }
                    })
                }),
            )
            .unwrap();
            let physical = store.backend.store.clone();
            let instance = physical.id;
            let owner = store.clone();
            let captured = bucket.clone();
            assert!(
                physical
                    .delete_managed_bucket_with_preparation(&bucket, move |writer| async move {
                        owner
                            .prepare_vector_bucket_delete(&captured, writer, CatalogCleanupExecution::new(instance).unwrap())
                            .await
                            .map_err(crate::storage_api::table::StorageError::other)
                    })
                    .await
                    .is_err()
            );
            drop(registration);
            let event = observed
                .lock()
                .clone()
                .expect("actual native and final fence results observed");
            let original = event.target.as_ref().unwrap();
            let reader = CatalogRetirementEvidenceReader::new(
                store.backend.clone(),
                resource.bucket.clone(),
                Some(resource),
                Some(event.authority_ref),
            )
            .unwrap();
            let mut cursor = reader.start().await.unwrap();
            let mut saw_source = false;
            let mut saw_progress = false;
            loop {
                let (page, next) = reader.next(cursor).await.unwrap();
                assert_eq!(page["native_bucket"]["status"], "PresentSame");
                for source in page["catalog_sources"].as_array().unwrap() {
                    if source["object_key"] == original.target.object_key {
                        assert_eq!(source["exists"], false);
                        saw_source = true;
                    }
                }
                for record in page["progress_pages"].as_array().unwrap() {
                    if record["reference"]["object_key"] == original.progress_ref.object_key {
                        check_raw_record(record);
                        assert_eq!(record["reference"], serde_json::to_value(&original.progress_ref).unwrap());
                        assert_eq!(record["content"], serde_json::to_value(&original.progress).unwrap());
                        assert_eq!(record["content"]["targets"][usize::from(original.ordinal)]["state"], "IN_FLIGHT");
                        saw_progress = true;
                    }
                }
                let Some(next) = next else { break };
                cursor = next;
            }
            assert!(saw_source && saw_progress);
            let retained = store
                .backend
                .read_object_limited(
                    store.catalog_bucket(),
                    &original.progress_ref.object_key,
                    VECTOR_RETIREMENT_PROGRESS_MAX_BYTES,
                )
                .await
                .unwrap()
                .unwrap();
            let reference = CatalogMetadataRef::from_read(
                CatalogObjectKey::try_new(original.progress_ref.object_key.clone()).unwrap(),
                &retained,
            )
            .unwrap();
            assert_eq!(CatalogMetadataRefRecordV1::from(&reference), original.progress_ref);
        });
    }
}
