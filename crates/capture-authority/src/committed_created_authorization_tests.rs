//! Reuse the existing actual Raft owner fixture, never a caller-constructed result proof.
use super::*;

async fn decide(cluster: &Cluster, value: u8) -> (DecideCreated, LogId) {
    let input = command(value);
    let expected = input.created().clone();
    let response = cluster
        .node(0)
        .owner()
        .client_write(input)
        .await
        .expect("actual Raft Created decision");
    let Some(CreatedResult::Created { decision_log_id, .. }) = response.data.created_result() else {
        panic!("successful Created result required")
    };
    (expected, *decision_log_id)
}

#[tokio::test]
async fn committed_created_authorization_exact() {
    let cluster = Cluster::start(&[0], 100).await;
    let (expected, first) = decide(&cluster, 1).await;
    let authorization = cluster
        .node(0)
        .owner()
        .authorize_created(expected.clone(), first)
        .await
        .expect("durable exact authorization");
    assert_eq!(authorization.identity(), identity(0));
    assert_eq!(authorization.operation_id(), expected.operation_id);
    assert_eq!(authorization.object(), &expected.object);
    assert_eq!(authorization.prepared(), &expected.prepared);
    assert_eq!(authorization.binding(), &binding());
    assert_eq!(authorization.decision_log_id(), first);
    assert_eq!(cluster.node(0).page().await.events[0].captured.decision_log_id, first);
    cluster.stop().await;
}

#[tokio::test]
async fn committed_created_authorization_retry_first_decision() {
    let cluster = Cluster::start(&[0], 100).await;
    let (expected, first) = decide(&cluster, 2).await;
    let before = cluster
        .node(0)
        .owner()
        .authorize_created(expected.clone(), first)
        .await
        .expect("first authorization");
    let response = cluster
        .node(0)
        .owner()
        .client_write(CaptureCommand::try_created(expected.clone()).expect("retry command"))
        .await
        .expect("real retry");
    assert_ne!(LogId::from(response.log_id), first);
    assert!(
        matches!(response.data.created_result(), Some(CreatedResult::Created{decision_log_id,..}) if *decision_log_id==first)
    );
    let after = cluster
        .node(0)
        .owner()
        .authorize_created(expected.clone(), first)
        .await
        .expect("first decision retained");
    assert_eq!(before, after);
    assert!(
        cluster
            .node(0)
            .owner()
            .authorize_created(expected, response.log_id.into())
            .await
            .is_err()
    );
    assert_eq!(cluster.node(0).page().await.events.len(), 1);
    cluster.stop().await;
}

#[tokio::test]
async fn committed_created_authorization_rejects_mismatch() {
    let cluster = Cluster::start(&[0], 100).await;
    let (expected, first) = decide(&cluster, 3).await;
    let before = cluster.node(0).page().await;
    let durable_before = cluster
        .node(0)
        .writer
        .write(|store| {
            Ok((
                store.read_vote()?,
                store.read_committed()?,
                store.read_entries(0, u64::MAX)?,
                store.native_application_state()?,
                store.created_events_page(None, 256)?,
            ))
        })
        .await
        .expect("durable state before rejected authorization");
    let mut variants = Vec::new();
    let mut altered = expected.clone();
    altered.operation_id = [4; 16];
    variants.push(altered);
    let mut altered = expected.clone();
    altered.object.key.push(9);
    variants.push(altered);
    let mut altered = expected.clone();
    altered.object.bucket_incarnation = [9; 16];
    variants.push(altered);
    let mut altered = expected.clone();
    altered.expected_head = Some([9; 16]);
    variants.push(altered);
    let mut altered = expected.clone();
    altered.prepared.preparation_id = [9; 16];
    variants.push(altered);
    let mut altered = expected.clone();
    altered.prepared.version_id = [9; 16];
    variants.push(altered);
    let mut altered = expected.clone();
    altered.prepared.content_digest = [9; 32];
    variants.push(altered);
    let mut altered = expected.clone();
    altered.prepared.content_length += 1;
    variants.push(altered);
    let mut altered = expected.clone();
    altered.binding_revision += 1;
    variants.push(altered);
    for altered in variants {
        assert!(
            cluster.node(0).owner().authorize_created(altered, first).await.is_err(),
            "AUTHORIZATION_REQUIRES_EXACT_PREPARED_IDENTITY_AND_COMMAND"
        );
    }
    for wrong in [
        LogId {
            term: first.term + 1,
            ..first
        },
        LogId {
            leader_node: first.leader_node + 1,
            ..first
        },
        LogId {
            index: first.index + 1,
            ..first
        },
    ] {
        assert!(
            cluster
                .node(0)
                .owner()
                .authorize_created(expected.clone(), wrong)
                .await
                .is_err()
        );
    }
    let durable_after = cluster
        .node(0)
        .writer
        .write(|store| {
            Ok((
                store.read_vote()?,
                store.read_committed()?,
                store.read_entries(0, u64::MAX)?,
                store.native_application_state()?,
                store.created_events_page(None, 256)?,
            ))
        })
        .await
        .expect("durable state after rejected authorization");
    assert_eq!(durable_before, durable_after, "REJECTED_AUTHORIZATION_DOES_NOT_WRITE");
    let mut failed = expected.clone();
    failed.operation_id = [5; 16];
    failed.prepared.version_id = [5; 16];
    let response = cluster
        .node(0)
        .owner()
        .client_write(CaptureCommand::try_created(failed.clone()).expect("failed command"))
        .await
        .expect("real head mismatch decision");
    assert!(matches!(response.data.created_result(), Some(CreatedResult::HeadMismatch { .. })));
    assert!(
        cluster
            .node(0)
            .owner()
            .authorize_created(failed, response.log_id.into())
            .await
            .is_err()
    );
    assert_eq!(cluster.node(0).page().await, before);
    cluster.stop().await;
}

#[tokio::test]
async fn committed_created_authorization_restart() {
    let routes = Arc::new(Routes::default());
    let mut node = Node::start(0, routes.clone(), config(100)).await;
    node.owner().bootstrap(members(&[0])).await.expect("bootstrap actual owner");
    node.owner()
        .raft()
        .wait(Some(Duration::from_secs(15)))
        .state(ServerState::Leader, "leader")
        .await
        .expect("leader ready");
    let expected = command(6).created().clone();
    let response = node.owner().client_write(command(6)).await.expect("real first write");
    let Some(CreatedResult::Created {
        decision_log_id: first, ..
    }) = response.data.created_result()
    else {
        panic!("Created")
    };
    let first = *first;
    let before = node
        .owner()
        .authorize_created(expected.clone(), first)
        .await
        .expect("before restart");
    node.stop(&routes).await;
    let Node {
        directory,
        writer,
        owner: _,
        id,
    } = node;
    drop(writer);
    let writer = NativeLogWriter::new(Store::open(directory.path().join("db"), identity(id)).expect("retained redb"));
    // A retained commit-before-apply crash fixture is not a client-provided proof.
    // Only real owner initialization may replay it before authorization can succeed.
    use openraft::storage::{RaftLogStorage, RaftLogStorageExt};
    let pending = command(7).created().clone();
    let mut logs = NativeRaftLogStore::new(writer.clone());
    let last = logs.get_log_state().await.expect("retained tail").last_log_id.expect("tail");
    let pending_id = LogId {
        term: last.leader_id.term,
        leader_node: last.leader_id.node_id,
        index: last.index + 1,
    };
    logs.blocking_append(vec![CaptureRaftEntry {
        log_id: pending_id.into(),
        payload: openraft::EntryPayload::Normal(CaptureCommand::try_created(pending.clone()).expect("pending command")),
    }])
    .await
    .expect("retained crash suffix");
    logs.save_committed(Some(pending_id.into()))
        .await
        .expect("retained commit marker");
    assert!(
        writer.authorize_created(pending.clone(), pending_id).await.is_err(),
        "UNAPPLIED_COMMIT_IS_NOT_AUTHORIZATION"
    );
    drop(logs);
    let owner = NativeRaftOwner::start(
        writer.clone(),
        id,
        config(100),
        CheckpointLimits::default(),
        DirectFactory {
            source: id,
            routes: routes.clone(),
        },
    )
    .await
    .expect("real recovery");
    routes.register(id, owner.raft().clone());
    let after = owner
        .authorize_created(expected, first)
        .await
        .expect("durable authorization after restart");
    assert_eq!(before, after);
    let replayed = owner
        .authorize_created(pending.clone(), pending_id)
        .await
        .expect("actual owner committed recovery authorizes");
    assert_eq!(replayed.prepared(), &pending.prepared);
    assert_eq!(replayed.decision_log_id(), pending_id);
    owner.shutdown().await.expect("drain recovered owner");
    routes.remove(id);
    drop(owner);
    drop(writer);
}
