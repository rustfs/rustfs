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

//! CompleteMultipartUpload tests (batch E owns CompleteMultipartUpload).

use super::common::*;
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
async fn complete_multipart_upload_transitions_immediately_via_usecase() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultMultipartUsecase::from_global();

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let bucket = format!("test-api-mpu-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/multipart.txt";
    let payload = b"multipart immediate transition through complete API";

    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set lifecycle configuration");

    let upload = ecstore
        .new_multipart_upload(bucket.as_str(), object, &ObjectOptions::default())
        .await
        .expect("Failed to create multipart upload");

    let mut reader = PutObjReader::from_vec(payload.to_vec());
    let uploaded_part = ecstore
        .put_object_part(bucket.as_str(), object, &upload.upload_id, 1, &mut reader, &ObjectOptions::default())
        .await
        .expect("Failed to upload multipart part");

    let complete_input = CompleteMultipartUploadInput::builder()
        .bucket(bucket.clone())
        .key(object.to_string())
        .upload_id(upload.upload_id.clone())
        .multipart_upload(Some(CompletedMultipartUpload {
            parts: Some(vec![CompletedPart {
                part_number: Some(1),
                e_tag: uploaded_part.etag.clone().map(|etag| to_s3s_etag(&etag)),
                ..Default::default()
            }]),
        }))
        .build()
        .unwrap();

    let complete_output = Box::pin(usecase.execute_complete_multipart_upload(build_request(complete_input, Method::POST)))
        .await
        .expect("Failed to complete multipart upload through usecase");
    assert!(
        complete_output.output.version_id.is_some(),
        "versioned CompleteMultipartUpload should return the created version ID"
    );

    let info = wait_for_transition(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("multipart object should transition immediately after complete usecase");

    assert_eq!(info.transitioned_object.status, "complete");
    assert_eq!(info.transitioned_object.tier, tier_name);
    assert!(backend.contains(&info.transitioned_object.name).await);
}

#[serial]
#[ignore = "global-state ILM integration test: runs serialized in the CI ILM Integration (serial) lane, see ci.yml test-ilm-integration-serial and rustfs/backlog#1148 (ilm-1)"]
#[test]
fn compensation_driven_complete_multipart_upload_still_transitions() {
    std::thread::Builder::new()
        .name("lifecycle-compensation-complete-multipart".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("compensation multipart test runtime should build");

            runtime.block_on(compensation_driven_complete_multipart_upload_still_transitions_inner());
        })
        .expect("compensation multipart test thread should spawn")
        .join()
        .expect("compensation multipart test thread should finish");
}

async fn compensation_driven_complete_multipart_upload_still_transitions_inner() {
    let (_disk_paths, ecstore) = setup_test_env().await;
    let usecase = DefaultMultipartUsecase::from_global();

    let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
    let backend = register_mock_tier(&tier_name).await;

    let bucket = format!("test-comp-mpu-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let object = "test/multipart.txt";
    let payload = b"multipart should still transition after compensation";

    create_test_bucket(&ecstore, bucket.as_str()).await;
    set_bucket_lifecycle_transition_with_tier(bucket.as_str(), &tier_name)
        .await
        .expect("Failed to set lifecycle configuration");

    let upload = ecstore
        .new_multipart_upload(bucket.as_str(), object, &ObjectOptions::default())
        .await
        .expect("Failed to create multipart upload");

    let mut reader = PutObjReader::from_vec(payload.to_vec());
    let uploaded_part = ecstore
        .put_object_part(bucket.as_str(), object, &upload.upload_id, 1, &mut reader, &ObjectOptions::default())
        .await
        .expect("Failed to upload multipart part");

    let complete_input = CompleteMultipartUploadInput::builder()
        .bucket(bucket.clone())
        .key(object.to_string())
        .upload_id(upload.upload_id.clone())
        .multipart_upload(Some(CompletedMultipartUpload {
            parts: Some(vec![CompletedPart {
                part_number: Some(1),
                e_tag: uploaded_part.etag.clone().map(|etag| to_s3s_etag(&etag)),
                ..Default::default()
            }]),
        }))
        .build()
        .unwrap();

    with_forced_immediate_enqueue_timeout(&backend, || async {
        Box::pin(usecase.execute_complete_multipart_upload(build_request(complete_input, Method::POST)))
            .await
            .expect("Failed to complete multipart upload through usecase");
    })
    .await;

    let info = wait_for_transition(&ecstore, bucket.as_str(), object, TRANSITION_WAIT_TIMEOUT)
        .await
        .expect("multipart object should eventually transition after compensation backfill");

    assert_eq!(backend.put_count().await, 1, "compensation should upload the version once");
    assert_eq!(info.transitioned_object.status, "complete");
    assert_eq!(info.transitioned_object.tier, tier_name);
    assert!(backend.contains(&info.transitioned_object.name).await);
}
