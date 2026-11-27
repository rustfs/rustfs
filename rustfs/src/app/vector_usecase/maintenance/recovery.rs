// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Recovery consumes exact reservations and original stage identities.

use super::*;
use crate::table_catalog::*;
use crate::vector::lance::{
    ManifestPrepareRequest, VectorIndexManifestSpec, decode_initialization_payload, plan_initial_manifest,
};

struct RecoveryDataset<'a> {
    definition: &'a VectorIndexDefinitionV1,
    index: VectorIndexManifestSpec,
    allow_create: bool,
}

impl MaintenanceExecutor {
    pub(super) async fn execute_create_recovery(
        &self,
        catalog: Arc<Catalog>,
        work: VectorMaintenanceWork,
        control: VectorMaintenanceControl,
    ) -> Result<VectorMaintenanceExecutionOutcome, VectorMaintenanceExecutionError> {
        let dispatch = checked_dispatch(&work, &control, VectorMaintenanceKind::CreateRecovery)?;
        self.source
            .authorize_actions_with_guard(&catalog, &dispatch, &[S3VectorsAction::CreateIndexAction])
            .await?;
        let (reservation, operation) = catalog.resume_vector_index_creation_with_dispatch(&dispatch).await?;
        let operation = Arc::new(operation);
        if reservation.resource_identity() != work.lease().resource() {
            return Err(TableCatalogStoreError::Conflict("creation recovery reservation changed resource".into()).into());
        }
        if reservation.published_entry().is_none() {
            control.check_admission()?;
            let index = decode_initialization_payload(
                reservation.initialization_payload(),
                &reservation.definition().effective_encryption,
            )
            .map_err(maintenance_execution_error)?;
            let plan = plan_initial_manifest(reservation.resource_identity(), &index, reservation.create_id())
                .await
                .map_err(maintenance_execution_error)?;
            let _plan_reservation = control.reserve_memory(plan.bytes.len() as u64)?;
            plan.verify_reserved_reference(reservation.planned_manifest())
                .map_err(maintenance_execution_error)?;
            let (catalog, adapter, access) = self
                .recovery_adapter(
                    catalog.clone(),
                    operation.clone(),
                    &work,
                    control.clone(),
                    RecoveryDataset {
                        definition: reservation.definition(),
                        index: index.clone(),
                        allow_create: true,
                    },
                )
                .await?;
            let prepared = adapter
                .prepare_initial_manifest(
                    &access,
                    ManifestPrepareRequest {
                        parts: CatalogCommitParts {
                            resource_identity: reservation.resource_identity().clone(),
                            namespace: reservation.namespace().clone(),
                            name: reservation.name().clone(),
                            expected_head: reservation.expected_head().clone(),
                            commit_id: reservation.create_id().clone(),
                            operation: "vector-create".into(),
                            idempotency_key: None,
                            writer: None,
                        },
                        index,
                    },
                    &plan,
                    reservation.planned_manifest(),
                )
                .await
                .map_err(maintenance_execution_error)?;
            control.check_admission()?;
            dispatch.check_held()?;
            self.source
                .authorize_actions_with_guard(&catalog, &dispatch, &[S3VectorsAction::CreateIndexAction])
                .await?;
            catalog
                .publish_vector_index_initial_head(&operation, &reservation, prepared)
                .await?;
        }
        control.check_admission()?;
        let output = catalog.copy_vector_maintenance_completed_source_with_guard(&dispatch).await?;
        Ok(VectorMaintenanceExecutionOutcome::Completed {
            result: Box::new(VectorMaintenanceResultV1::try_for_work(
                &work,
                VectorMaintenanceResultEffect::Recovered,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                vec![(&output).into()],
                None,
            )?),
        })
    }

    pub(super) async fn execute_commit_recovery(
        &self,
        catalog: Arc<Catalog>,
        work: VectorMaintenanceWork,
        control: VectorMaintenanceControl,
    ) -> Result<VectorMaintenanceExecutionOutcome, VectorMaintenanceExecutionError> {
        let dispatch = checked_dispatch(&work, &control, VectorMaintenanceKind::CommitRecovery)?;
        self.source
            .authorize_actions_with_guard(
                &catalog,
                &dispatch,
                &[S3VectorsAction::GetVectorsAction, S3VectorsAction::PutVectorsAction],
            )
            .await?;
        let source = work
            .snapshot()
            .job()
            .input()
            .source()
            .as_ref()
            .ok_or_else(|| TableCatalogStoreError::Invalid("commit recovery source is absent".into()))?;
        if *source.kind() == VectorMaintenanceSourceKind::CommitLog {
            return self.execute_catalog_commit_recovery(catalog, work, control).await;
        }
        if *source.kind() != VectorMaintenanceSourceKind::MaintenanceJob {
            return Err(TableCatalogStoreError::Invalid("unsupported commit recovery source".into()).into());
        }
        if let Some(output) = catalog
            .read_vector_maintenance_completed_original_with_guard(&dispatch)
            .await?
        {
            return Ok(VectorMaintenanceExecutionOutcome::Completed {
                result: Box::new(VectorMaintenanceResultV1::try_for_work(
                    &work,
                    VectorMaintenanceResultEffect::Recovered,
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    vec![(&output).into()],
                    None,
                )?),
            });
        }
        let original_reference = CatalogMetadataRef::try_from(source.reference())?;
        let (original, _) = catalog.read_vector_maintenance_original_job_with_guard(&dispatch).await?;
        let resource = original.job().resource().identity()?;
        let namespace = Namespace::parse(original.job().resource().namespace()).map_err(maintenance_execution_error)?;
        let name = CatalogName::parse(original.job().resource().name()).map_err(maintenance_execution_error)?;
        let bucket = resource
            .vector_bucket()
            .ok_or_else(|| TableCatalogStoreError::Invalid("recovery requires a Vector bucket".into()))?;
        let operation = Arc::new(
            catalog
                .begin_vector_operation_with_lifecycle(bucket, &namespace, &name, dispatch.lifecycle_owner())
                .await?,
        );
        if operation.resource_identity() != &resource {
            return Err(TableCatalogStoreError::Conflict("recovery resource lifetime changed".into()).into());
        }
        let entry = catalog.read_vector_entry_with_guard(&operation).await?;
        let index =
            LanceManifestAdapter::<super::super::Backend>::index_from_catalog(&entry).map_err(maintenance_execution_error)?;
        let (catalog, adapter, _) = self
            .recovery_adapter(
                catalog,
                operation.clone(),
                &work,
                control.clone(),
                RecoveryDataset {
                    definition: entry.definition(),
                    index,
                    allow_create: false,
                },
            )
            .await?;
        let names: &[&str] = match original.job().kind() {
            VectorMaintenanceKind::AnnBuild => &["ann-build"],
            VectorMaintenanceKind::Compaction => &["reserve-fragments", "rewrite"],
            _ => return Err(TableCatalogStoreError::Invalid("recovery source is not an engine task".into()).into()),
        };
        let mut expected = original
            .job()
            .input()
            .base_head()
            .clone()
            .ok_or_else(|| TableCatalogStoreError::Invalid("original engine task has no base".into()))?;
        let mut stages = Vec::with_capacity(names.len());
        for stage in names {
            control.check_admission()?;
            let Some((log, reference)) = catalog
                .read_vector_maintenance_stage_commit(&dispatch, &original, stage)
                .await?
            else {
                return Ok(unknown_recovery());
            };
            // Catalog must prove publication from the complete head history, including response loss.
            if log.status == CommitLogStatus::Failed {
                return Ok(unknown_recovery());
            }
            let request = adapter
                .maintenance_stage_request(
                    &original,
                    stage,
                    CatalogHead::from_maintenance_snapshot(original.job().resource(), &expected)?.expected_head(),
                )
                .map_err(maintenance_execution_error)?;
            let Some(prepared) = adapter
                .prepare_existing_maintenance_candidate(request)
                .await
                .map_err(maintenance_execution_error)?
            else {
                return Err(TableCatalogStoreError::Conflict("published stage candidate is absent".into()).into());
            };
            let candidate = prepared.candidate_metadata().clone();
            let next = VectorMaintenanceHeadV1::from_stage_commit(&expected, &log)?;
            if CatalogMetadataRef::try_from(&next.binding().metadata)? != candidate
                || CatalogFormatDescriptor::try_from(&next.binding().descriptor)? != *prepared.format_descriptor()
            {
                return Err(TableCatalogStoreError::Conflict("published stage candidate differs from its log".into()).into());
            }
            let copied = catalog
                .copy_vector_maintenance_source_with_guard(&dispatch, &reference)
                .await?;
            stages.push(VectorMaintenanceStageResultV1::try_new(
                (*stage).into(),
                prepared.commit_id().clone(),
                Some(expected),
                Some((&candidate).into()),
                VectorMaintenanceStageOutcome::Committed,
                Some((&copied).into()),
            )?);
            expected = next;
        }
        let recovery =
            PreparedVectorMaintenanceRecovery::try_from_published_stages(original_reference, work.lease().clone(), stages)?;
        control.check_admission()?;
        self.source
            .authorize_actions_with_guard(&catalog, &dispatch, &[S3VectorsAction::PutVectorsAction])
            .await?;
        let recovered = catalog
            .recover_vector_maintenance_job_with_operation_guard(&dispatch, &operation, &original, recovery)
            .await?;
        let result = recovered
            .job()
            .result_ref()
            .as_ref()
            .ok_or_else(|| TableCatalogStoreError::Invalid("published recovery has no original result".into()))?
            .clone();
        Ok(VectorMaintenanceExecutionOutcome::Completed {
            result: Box::new(VectorMaintenanceResultV1::try_for_work(
                &work,
                VectorMaintenanceResultEffect::Recovered,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                vec![result],
                None,
            )?),
        })
    }

    async fn execute_catalog_commit_recovery(
        &self,
        catalog: Arc<Catalog>,
        work: VectorMaintenanceWork,
        control: VectorMaintenanceControl,
    ) -> Result<VectorMaintenanceExecutionOutcome, VectorMaintenanceExecutionError> {
        let dispatch = checked_dispatch(&work, &control, VectorMaintenanceKind::CommitRecovery)?;
        let job = work.snapshot().job();
        let resource = job.resource().identity()?;
        let namespace = Namespace::parse(job.resource().namespace()).map_err(maintenance_execution_error)?;
        let name = CatalogName::parse(job.resource().name()).map_err(maintenance_execution_error)?;
        let bucket = resource
            .vector_bucket()
            .ok_or_else(|| TableCatalogStoreError::Invalid("recovery requires a Vector bucket".into()))?;
        let operation = Arc::new(
            catalog
                .begin_vector_operation_with_lifecycle(bucket, &namespace, &name, dispatch.lifecycle_owner())
                .await?,
        );
        if operation.resource_identity() != &resource {
            return Err(TableCatalogStoreError::Conflict("commit recovery resource lifetime changed".into()).into());
        }
        let (log, parts) = catalog
            .read_vector_maintenance_commit_source_with_operation_guard(&dispatch, &operation)
            .await?;
        if log.status == CommitLogStatus::Failed {
            return Ok(unknown_recovery());
        }
        let binding = log
            .vector_commit_binding
            .as_ref()
            .ok_or_else(|| TableCatalogStoreError::Invalid("commit recovery binding is absent".into()))?;
        let reference = CatalogMetadataRef::try_from(&binding.candidate_metadata)?;
        let descriptor = CatalogFormatDescriptor::try_from(&binding.descriptor)?;
        let entry = catalog.read_vector_entry_with_guard(&operation).await?;
        let index =
            LanceManifestAdapter::<super::super::Backend>::index_from_catalog(&entry).map_err(maintenance_execution_error)?;
        let (catalog, adapter, _) = self
            .recovery_adapter(
                catalog,
                operation.clone(),
                &work,
                control.clone(),
                RecoveryDataset {
                    definition: entry.definition(),
                    index: index.clone(),
                    allow_create: false,
                },
            )
            .await?;
        let prepared = adapter
            .prepare_exact_catalog_candidate(ManifestPrepareRequest { parts, index }, &reference, &descriptor)
            .await
            .map_err(maintenance_execution_error)?;
        control.check_admission()?;
        self.source
            .authorize_actions_with_guard(&catalog, &dispatch, &[S3VectorsAction::PutVectorsAction])
            .await?;
        let Some(completed) = catalog
            .finalize_vector_maintenance_commit_with_operation_guard(&dispatch, &operation, prepared)
            .await?
        else {
            return Ok(unknown_recovery());
        };
        Ok(VectorMaintenanceExecutionOutcome::Completed {
            result: Box::new(VectorMaintenanceResultV1::try_for_work(
                &work,
                VectorMaintenanceResultEffect::Recovered,
                Vec::new(),
                vec![(&completed).into()],
                Vec::new(),
                Vec::new(),
                None,
            )?),
        })
    }

    async fn recovery_adapter(
        &self,
        catalog: Arc<Catalog>,
        operation: Arc<VectorCatalogOperationGuard>,
        work: &VectorMaintenanceWork,
        control: VectorMaintenanceControl,
        dataset: RecoveryDataset<'_>,
    ) -> Result<(Arc<Catalog>, LanceManifestAdapter<super::super::Backend>, ManagedObjectAccess), VectorMaintenanceExecutionError>
    {
        control.check_admission()?;
        let RecoveryDataset {
            definition,
            index,
            allow_create,
        } = dataset;
        let dispatch = Arc::new(control.dispatch_guard()?.clone());
        let resource = operation.resource_identity();
        let bucket = resource
            .vector_bucket()
            .ok_or_else(|| TableCatalogStoreError::Invalid("recovery requires a Vector bucket".into()))?;
        let reader = Arc::new(super::super::dataset::CandidateReader::with_maintenance_control(control.clone()));
        let catalog = Arc::new(catalog.with_vector_managed_candidate_reader(reader.clone())?);
        let authorizer = Arc::new(MaintenanceObjectAuthorizer::new(
            self.app.clone(),
            catalog.clone(),
            dispatch,
            self.identity.clone(),
        ));
        let job = work.snapshot().job();
        let audit = ManagedMaintenanceAuditContext::new(
            self.identity.clone(),
            *job.job_id(),
            u32::from(*job.attempt()),
            work.lease().worker_id().into(),
            work.lease().worker_boot_id(),
            job.initiator_ref().clone(),
        );
        let mut operations = std::collections::BTreeSet::from([
            ManagedObjectOperation::Get,
            ManagedObjectOperation::Head,
            ManagedObjectOperation::Range,
        ]);
        if allow_create {
            operations.insert(ManagedObjectOperation::CreateOnly);
        }
        let access = ManagedObjectAccess::try_from_parts(
            bucket.clone(),
            resource.table_id().clone(),
            resource.dataset_prefix(),
            operations,
        )
        .map_err(maintenance_execution_error)?;
        let context = ManagedObjectContext::from_maintenance(
            access.clone(),
            managed_encryption(&definition.effective_encryption),
            Arc::new(ManagedMaintenanceContext::new(audit, authorizer)),
        )
        .map_err(maintenance_execution_error)?;
        let objects = super::super::managed_objects::maintenance_managed_lance_objects(
            self.app.clone(),
            context,
            operation.clone(),
            self.limits,
        )
        .map_err(maintenance_execution_error)?;
        reader.bind(&objects);
        let name = operation.name().clone();
        let adapter = LanceManifestAdapter::new_guarded(
            LanceOperationContext {
                catalog: catalog.clone(),
                objects,
                access: access.clone(),
                guard: operation,
                definition: definition.clone(),
            },
            name,
            index,
            self.limits,
        )
        .map_err(maintenance_execution_error)?
        .with_numeric_proof_cache(self.app.vector_numeric_proof_cache())
        .map_err(maintenance_execution_error)?
        .with_maintenance_control(control)
        .map_err(maintenance_execution_error)?;
        Ok((catalog, adapter, access))
    }
}

fn checked_dispatch(
    work: &VectorMaintenanceWork,
    control: &VectorMaintenanceControl,
    kind: VectorMaintenanceKind,
) -> Result<Arc<VectorMaintenanceDispatchGuard>, VectorMaintenanceExecutionError> {
    control.check_admission()?;
    let dispatch = control.dispatch_guard()?;
    dispatch.check_held()?;
    if work.lease() != dispatch.lease() || *work.snapshot().job().kind() != kind {
        return Err(TableCatalogStoreError::Conflict("recovery work differs from its dispatch".into()).into());
    }
    Ok(Arc::new(dispatch.clone()))
}

fn unknown_recovery() -> VectorMaintenanceExecutionOutcome {
    VectorMaintenanceExecutionOutcome::Unknown {
        reason: VectorMaintenanceFailureCode::UnknownOutcome,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::storage_api::bucket_usecase::contract::bucket::{BucketOperations, DeleteBucketOptions};
    use crate::vector::protocol::Request;
    use serde_json::json;
    use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
    use std::time::Duration;
    use uuid::Uuid;

    #[test]
    #[serial_test::serial]
    fn maintenance_create_recovery_reuses_lifecycle_with_waiting_writer() {
        crate::app::gating_test_env::run_large_stack_test("maintenance-recovery-lifecycle", || async {
            let fixture = crate::app::vector_usecase::tests::Fixture::new().await;
            let bucket_name = format!("recovery-{}", Uuid::new_v4().simple());
            fixture
                .execute(Request::CreateVectorBucket(
                    serde_json::from_value(json!({
                        "vectorBucketName": bucket_name,
                    }))
                    .unwrap(),
                ))
                .await
                .unwrap();
            let create = Request::CreateIndex(serde_json::from_value(json!({
                "vectorBucketName":bucket_name,"indexName":"recovery.index","dataType":"float32","dimension":2,"distanceMetric":"euclidean",
            })).unwrap());
            let admitted = fixture.service.admit(fixture.verified(create)).await.unwrap();
            let bucket = admitted.bucket.as_ref().unwrap().clone();
            let catalog = admitted.catalog.as_ref().unwrap().clone();
            let namespace = Namespace::parse("indexes").unwrap();
            let name = CatalogName::parse("recovery.index").unwrap();
            fixture.service.initialize_index_catalog(&admitted, &name).await.unwrap();
            let resource = CatalogResourceIdentity::try_new(
                bucket.clone(),
                crate::app::storage_api::managed::ManagedTableId::try_new(Uuid::new_v4().to_string()).unwrap(),
                CatalogFormat::Lance,
            )
            .unwrap();
            let definition = VectorIndexDefinitionV1 {
                version: 1,
                effective_encryption: VectorEffectiveEncryptionV1::Unencrypted,
                tags: Default::default(),
            };
            let index = VectorIndexManifestSpec {
                schema_version: 1,
                dimension: NonZeroU32::new(2).unwrap(),
                distance_metric: crate::vector::model::VectorDistanceMetric::Euclidean,
                non_filterable_metadata_keys: Default::default(),
                encryption_descriptor_sha256: definition.effective_encryption.descriptor_sha256().unwrap(),
            };
            let create_id = CatalogCommitId::try_new(Uuid::new_v4().to_string()).unwrap();
            let plan = plan_initial_manifest(&resource, &index, &create_id).await.unwrap();
            let (_, reservation_owner) = catalog
                .reserve_vector_index(
                    &bucket,
                    VectorIndexCreateRequest {
                        resource_identity: resource.clone(),
                        namespace: namespace.clone(),
                        name: name.clone(),
                        create_id,
                        request_id: Uuid::new_v4(),
                        definition,
                        initialization_payload: crate::vector::lance::encode_initialization_payload(&index).unwrap(),
                        planned_manifest: plan.reference,
                    },
                )
                .await
                .unwrap();
            drop(reservation_owner);
            drop(admitted);
            let snapshot = catalog
                .read_vector_maintenance_resource(&resource, &namespace, &name)
                .await
                .unwrap();
            let copied = catalog
                .copy_vector_maintenance_source(&resource, snapshot.source_intent().unwrap())
                .await
                .unwrap();
            let (identity, _) = resolve_identity(&fixture.app, &fixture.credentials.access_key).await.unwrap();
            let execution_identity =
                VectorMaintenanceExecutionIdentityV1::try_new(identity.account().into(), *identity.user_generation()).unwrap();
            let policy = VectorMaintenancePolicyV1::try_new(300, 30, 3, 1, 60).unwrap();
            let semantic = VectorMaintenanceSemanticSpec::try_new(
                VectorMaintenanceResourceV1::try_new(&resource, &namespace, &name).unwrap(),
                VectorMaintenanceKind::CreateRecovery,
                VectorMaintenanceOrigin::Recovery,
                VectorMaintenanceInputV1::try_new(
                    None,
                    Some(
                        VectorMaintenanceSourceV1::try_new(VectorMaintenanceSourceKind::CreateIntent, (&copied).into()).unwrap(),
                    ),
                    None,
                )
                .unwrap(),
                execution_identity.clone(),
                None,
                policy.clone(),
            )
            .unwrap();
            let job = catalog
                .create_vector_maintenance_job(VectorMaintenanceJobSpec::from_semantic(semantic).unwrap())
                .await
                .unwrap();
            let worker =
                VectorMaintenanceWorker::try_new("recovery-test".into(), Uuid::new_v4(), execution_identity.clone()).unwrap();
            let claimed = catalog
                .claim_vector_maintenance_job(
                    &resource,
                    *job.job().job_id(),
                    VectorMaintenanceClaimCondition::from_snapshot(&job, snapshot),
                    worker,
                    Uuid::new_v4(),
                )
                .await
                .unwrap();
            let VectorMaintenanceClaimOutcome::Acquired(work) = claimed else {
                panic!("creation recovery must be claimed");
            };
            let dispatch = Arc::new(catalog.begin_vector_maintenance_dispatch(work.lease()).await.unwrap());
            let config = VectorMaintenanceRuntimeConfig::default();
            let control = VectorMaintenanceControl::new(
                tokio_util::sync::CancellationToken::new(),
                tokio::time::Instant::now() + Duration::from_secs(120),
                VectorMaintenanceBudgets::new(&config, None).unwrap(),
            )
            .with_dispatch(dispatch.clone());
            let source = Arc::new(source::MaintenanceSource {
                app: fixture.app.clone(),
                backend: super::super::super::Backend::new_with_managed_candidate_reader(
                    fixture.app.object_store(),
                    Arc::new(super::super::super::dataset::CandidateReader::default()),
                ),
                identity: execution_identity,
                policy,
                gc_scan: parking_lot::Mutex::new(None),
            });
            let executor = MaintenanceExecutor::new(
                fixture.app.clone(),
                identity,
                ManifestIoLimits {
                    max_manifest_bytes: NonZeroU64::new(64 * 1024 * 1024).unwrap(),
                    max_concurrent_reads: NonZeroUsize::new(2).unwrap(),
                },
                source,
            );
            let lock_counts = || {
                crate::app::storage_api::test::bootstrap_instance_ctx()
                    .lock_manager()
                    .as_fast_lock_manager()
                    .expect("fixture fast locks")
                    .current_lock_counts()
            };
            let before = lock_counts().1;
            let store = fixture.app.object_store();
            let storage_bucket = resource.storage_bucket_id().to_owned();
            let writer = tokio::spawn(async move { store.delete_bucket(&storage_bucket, &DeleteBucketOptions::default()).await });
            tokio::time::timeout(Duration::from_secs(5), async {
                while lock_counts().1 <= before {
                    assert!(!writer.is_finished(), "bucket writer must reach the queue");
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("bucket writer queues");
            let result =
                tokio::time::timeout(Duration::from_secs(30), executor.execute_create_recovery(catalog.clone(), *work, control))
                    .await
                    .expect("recovery must reuse its lifecycle owner")
                    .unwrap();
            assert!(matches!(result, VectorMaintenanceExecutionOutcome::Completed { .. }));
            assert!(!writer.is_finished(), "original dispatch must retain lifecycle protection");
            drop(dispatch);
            tokio::time::timeout(Duration::from_secs(10), writer)
                .await
                .expect("writer must resume after original owners finish")
                .unwrap()
                .expect_err("published Dataset keeps the bucket nonempty");
            let entry = catalog.read_catalog_head(&bucket, &namespace, &name).await.unwrap();
            assert_eq!(entry.resource_identity(), &resource);
        });
    }
}
