// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::storage::rpc::node_service::mrf::{self, MrfOperation, MrfReply};
use futures_util::{StreamExt, stream};
use uuid::Uuid;

const ROOT: &str = "/v4/heal/mrf/cluster";
const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const SUMMARY_NODE_BUDGET: usize = 32;
const PAGE_NODE_BUDGET: usize = 4;

pub(super) fn register(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(Method::GET, &format!("{ADMIN_PREFIX}{ROOT}"), AdminOperation(&SummaryHandler))?;
    r.insert(
        Method::GET,
        &format!("{ADMIN_PREFIX}{ROOT}/responsibilities"),
        AdminOperation(&ListHandler),
    )?;
    r.insert(
        Method::GET,
        &format!("{ADMIN_PREFIX}{ROOT}/responsibilities/{{node_id}}/{{id}}"),
        AdminOperation(&GetHandler),
    )?;
    r.insert(
        Method::POST,
        &format!("{ADMIN_PREFIX}{ROOT}/responsibilities/actions"),
        AdminOperation(&ActionHandler),
    )?;
    Ok(())
}

#[derive(Clone)]
struct Target {
    id: String,
    host: String,
    local: bool,
}

struct Route {
    context: Arc<crate::admin::runtime_sources::AppContext>,
    fingerprint: String,
    nodes: Vec<Target>,
}

impl Route {
    fn prepare(req: &S3Request<Body>) -> S3Result<Self> {
        let context =
            app_context_from_req(req).ok_or_else(|| admin_error(S3ErrorCode::ServiceUnavailable, "MRF topology unavailable"))?;
        let endpoints = context
            .endpoints()
            .handle()
            .ok_or_else(|| admin_error(S3ErrorCode::ServiceUnavailable, "MRF topology unavailable"))?;
        let fingerprint = heal_topology_fingerprint(&endpoints)
            .map_err(|_| admin_error(S3ErrorCode::ServiceUnavailable, "MRF topology unavailable"))?;
        let nodes: Vec<_> = endpoints
            .get_nodes()
            .into_iter()
            .map(|node| Target {
                id: mrf::node_id(&node.grid_host),
                host: node.grid_host,
                local: node.is_local,
            })
            .collect();
        if nodes.is_empty() {
            return Err(admin_error(S3ErrorCode::ServiceUnavailable, "MRF topology is empty"));
        }
        Ok(Self {
            context,
            fingerprint,
            nodes,
        })
    }

    fn target(&self, id: &str) -> S3Result<&Target> {
        self.nodes
            .iter()
            .find(|node| node.id == id)
            .ok_or_else(|| admin_error(S3ErrorCode::InvalidArgument, "unknown MRF nodeId; refresh topology"))
    }

    async fn observe(&self, target: &Target, operation: MrfOperation, budget: Duration) -> NodeObservation {
        let result = timeout(budget, async {
            if target.local {
                return Ok(Some(mrf::execute(operation.clone()).await));
            }
            let notification = self.context.notification_system().handle().ok_or(())?;
            let client = notification.peer_client_for_grid_host(&target.host).ok_or(())?;
            let command = serde_json::to_vec(&operation).map_err(|_| ())?;
            let bytes = client
                .mrf_lifecycle(self.fingerprint.clone(), target.id.clone(), command, budget)
                .await
                .map_err(|_| ())?;
            bytes
                .map(|bytes| serde_json::from_slice::<MrfReply>(&bytes).map_err(|_| ()))
                .transpose()
        })
        .await;
        let (state, reply) = match result {
            Ok(Ok(Some(reply))) if !valid_reply(&operation, &reply) => ("invalid_peer_response", None),
            Ok(Ok(Some(reply))) => (if reply.is_success() { "ok" } else { "error" }, Some(reply)),
            Ok(Ok(None)) => ("unsupported", None),
            Ok(Err(())) => ("unavailable", None),
            Err(_) => ("timed_out", None),
        };
        NodeObservation {
            node_id: target.id.clone(),
            host: target.host.clone(),
            state: state.into(),
            reply,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeObservation {
    node_id: String,
    host: String,
    state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply: Option<MrfReply>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Observation {
    contract_version: u32,
    topology_fingerprint: String,
    observation_started_at_ms: u64,
    observation_finished_at_ms: u64,
    expected_nodes: usize,
    /// Completeness is only for this response. Pages are not a frozen cluster snapshot.
    page_complete: bool,
    nodes: Vec<NodeObservation>,
    next_cursor: Option<String>,
}

fn response(
    route: &Route,
    started: u64,
    nodes: Vec<NodeObservation>,
    next_cursor: Option<String>,
) -> S3Result<S3Response<(StatusCode, Body)>> {
    let observation = Observation {
        contract_version: 1,
        topology_fingerprint: route.fingerprint.clone(),
        observation_started_at_ms: started,
        observation_finished_at_ms: mrf::now_ms(),
        expected_nodes: route.nodes.len(),
        page_complete: !nodes.is_empty() && nodes.iter().all(|node| node.state == "ok"),
        nodes,
        next_cursor,
    };
    let body = serde_json::to_vec(&observation)
        .map_err(|_| admin_error(S3ErrorCode::InternalError, "MRF observation encoding failed"))?;
    Ok(json_response(StatusCode::OK, body))
}

struct SummaryHandler;
#[async_trait::async_trait]
impl Operation for SummaryHandler {
    async fn call(&self, req: S3Request<Body>, _: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_heal_admin_request(&req).await?;
        let started = mrf::now_ms();
        let route = Route::prepare(&req)?;
        let (index, _, limit) = parse_query(&req.uri, &route, CursorKind::Summary)?;
        let deadline = tokio::time::Instant::now() + QUERY_TIMEOUT;
        let mut nodes: Vec<_> = stream::iter(route.nodes.iter().skip(index).take(limit).cloned().map(|node| {
            let route = &route;
            async move {
                if tokio::time::Instant::now() >= deadline {
                    return NodeObservation {
                        node_id: node.id,
                        host: node.host,
                        state: "budget_exceeded".into(),
                        reply: None,
                    };
                }
                route
                    .observe(
                        &node,
                        MrfOperation::Summary {},
                        deadline.saturating_duration_since(tokio::time::Instant::now()),
                    )
                    .await
            }
        }))
        .buffered(4)
        .collect()
        .await;
        let next_cursor = summary_continuation(&route, index, &mut nodes)?;
        response(&route, started, nodes, next_cursor)
    }
}

fn summary_continuation(route: &Route, index: usize, nodes: &mut Vec<NodeObservation>) -> S3Result<Option<String>> {
    // A timed-out node has been attempted. Unstarted nodes remain resumable,
    // so a slow prefix cannot permanently hide the rest of the topology.
    let consumed = nodes
        .iter()
        .position(|node| node.state == "budget_exceeded")
        .unwrap_or(nodes.len());
    nodes.truncate((consumed + 1).min(nodes.len()));
    continuation(route, index + consumed, None, CursorKind::Summary)
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum CursorKind {
    Summary,
    Responsibilities,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Cursor {
    kind: CursorKind,
    topology: String,
    node_id: String,
    node_index: usize,
    after: Option<Uuid>,
}

fn encode_cursor(cursor: &Cursor) -> S3Result<String> {
    serde_json::to_vec(cursor)
        .map(|bytes| base64_simd::URL_SAFE_NO_PAD.encode_to_string(bytes))
        .map_err(|_| admin_error(S3ErrorCode::InternalError, "MRF cursor encoding failed"))
}

fn parse_query(uri: &Uri, route: &Route, kind: CursorKind) -> S3Result<(usize, Option<Uuid>, usize)> {
    let mut seen = HashSet::new();
    let mut cursor = None;
    let (mut limit, maximum) = match kind {
        CursorKind::Summary => (SUMMARY_NODE_BUDGET, SUMMARY_NODE_BUDGET),
        CursorKind::Responsibilities => (MRF_RESPONSIBILITY_LIST_DEFAULT_LIMIT, MRF_RESPONSIBILITY_LIST_MAX_LIMIT),
    };
    for (key, value) in url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes()) {
        if !seen.insert(key.to_string()) {
            return Err(invalid_cursor());
        }
        match key.as_ref() {
            "limit" => {
                limit = value.parse().map_err(|_| invalid_cursor())?;
                if !(1..=maximum).contains(&limit) {
                    return Err(invalid_cursor());
                }
            }
            "cursor" if value.len() <= 1024 => {
                let bytes = base64_simd::URL_SAFE_NO_PAD
                    .decode_to_vec(value.as_bytes())
                    .map_err(|_| invalid_cursor())?;
                cursor = Some(serde_json::from_slice::<Cursor>(&bytes).map_err(|_| invalid_cursor())?);
            }
            _ => return Err(invalid_cursor()),
        }
    }
    let Some(cursor) = cursor else {
        return Ok((0, None, limit));
    };
    if cursor.kind != kind || (kind == CursorKind::Summary && cursor.after.is_some()) {
        return Err(invalid_cursor());
    }
    validate_cursor(&cursor, &route.fingerprint, &route.nodes)?;
    Ok((cursor.node_index, cursor.after, limit))
}

fn continuation(route: &Route, index: usize, after: Option<Uuid>, kind: CursorKind) -> S3Result<Option<String>> {
    route
        .nodes
        .get(index)
        .map(|node| {
            encode_cursor(&Cursor {
                kind,
                topology: route.fingerprint.clone(),
                node_id: node.id.clone(),
                node_index: index,
                after,
            })
        })
        .transpose()
}

fn validate_cursor(cursor: &Cursor, fingerprint: &str, nodes: &[Target]) -> S3Result<()> {
    if cursor.topology != fingerprint
        || cursor.after.is_some_and(|id| id.is_nil())
        || nodes.get(cursor.node_index).is_none_or(|node| node.id != cursor.node_id)
    {
        return Err(invalid_cursor());
    }
    Ok(())
}

fn invalid_cursor() -> crate::admin::storage_api::s3::S3Error {
    admin_error(S3ErrorCode::InvalidArgument, "invalid MRF page or topology changed; restart observation")
}

fn reject_query(uri: &Uri) -> S3Result<()> {
    if uri.query().is_some_and(|query| !query.is_empty()) {
        return Err(invalid_cursor());
    }
    Ok(())
}

struct ListHandler;
#[async_trait::async_trait]
impl Operation for ListHandler {
    async fn call(&self, req: S3Request<Body>, _: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_heal_admin_request(&req).await?;
        let started = mrf::now_ms();
        let route = Route::prepare(&req)?;
        let (mut index, mut after, limit) = parse_query(&req.uri, &route, CursorKind::Responsibilities)?;
        let deadline = tokio::time::Instant::now() + QUERY_TIMEOUT;
        let mut remaining = limit;
        let mut nodes = Vec::new();
        while index < route.nodes.len()
            && nodes.len() < PAGE_NODE_BUDGET
            && remaining > 0
            && tokio::time::Instant::now() < deadline
        {
            let mut node = route
                .observe(
                    &route.nodes[index],
                    MrfOperation::List {
                        cursor: after,
                        limit: remaining,
                    },
                    deadline.saturating_duration_since(tokio::time::Instant::now()),
                )
                .await;
            if let Some(MrfReply::List { snapshot, .. }) = &node.reply {
                remaining -= snapshot.responsibilities.len();
                if let Some(next) = snapshot.next_cursor {
                    after = Some(next);
                    nodes.push(node);
                    break;
                }
            } else if node.state == "ok" {
                node.state = "invalid_peer_page".into();
                node.reply = None;
            }
            nodes.push(node);
            index += 1;
            after = None;
        }
        let next_cursor = continuation(&route, index, after, CursorKind::Responsibilities)?;
        response(&route, started, nodes, next_cursor)
    }
}

fn valid_reply(operation: &MrfOperation, reply: &MrfReply) -> bool {
    match (operation, reply) {
        (_, MrfReply::Error { .. }) => true,
        (MrfOperation::Summary {}, MrfReply::Summary { summary }) => summary.contract_version == 1,
        (MrfOperation::List { cursor, limit }, MrfReply::List { snapshot, .. }) => valid_page(snapshot, *cursor, *limit),
        (MrfOperation::Get { id }, MrfReply::Get { observation }) => {
            observation.contract_version == 1
                && observation
                    .responsibility
                    .as_ref()
                    .is_none_or(|row| row.responsibility_id == *id)
        }
        (MrfOperation::Action { request, .. }, MrfReply::Action { action }) => {
            let expected = match request.action {
                mrf::MrfLegacyResponsibilityAction::Refresh => "refreshed",
                mrf::MrfLegacyResponsibilityAction::Recheck => "active",
                mrf::MrfLegacyResponsibilityAction::AcceptUnverifiedRisk => "operatorAcceptedUnverified",
            };
            action.responsibility_id == request.responsibility_id
                && action.state == expected
                && !action.data_verified
                && action.durable_responsibility_retained
        }
        _ => false,
    }
}

fn valid_page(
    snapshot: &rustfs_heal::heal::mrf_queue::MrfLegacyResponsibilitySnapshot,
    after: Option<Uuid>,
    limit: usize,
) -> bool {
    if snapshot.contract_version != 2 || !snapshot.node_local || snapshot.responsibilities.len() > limit {
        return false;
    }
    let mut previous = after;
    for row in &snapshot.responsibilities {
        if row.responsibility_id.is_nil() || previous.is_some_and(|id| row.responsibility_id <= id) {
            return false;
        }
        previous = Some(row.responsibility_id);
    }
    snapshot.next_cursor.is_none() || (!snapshot.responsibilities.is_empty() && snapshot.next_cursor == previous)
}

struct GetHandler;
#[async_trait::async_trait]
impl Operation for GetHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_heal_admin_request(&req).await?;
        reject_query(&req.uri)?;
        let route = Route::prepare(&req)?;
        let target = route.target(params.get("node_id").unwrap_or_default())?;
        let id: Uuid = params.get("id").unwrap_or_default().parse().map_err(|_| invalid_cursor())?;
        if id.is_nil() {
            return Err(invalid_cursor());
        }
        let started = mrf::now_ms();
        let node = route.observe(target, MrfOperation::Get { id }, QUERY_TIMEOUT).await;
        response(&route, started, vec![node], None)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RoutedAction {
    node_id: String,
    request: MrfLegacyResponsibilityActionRequest,
}

struct ActionHandler;
#[async_trait::async_trait]
impl Operation for ActionHandler {
    async fn call(&self, mut req: S3Request<Body>, _: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let actor = authenticate_heal_admin_request(&req).await?;
        reject_query(&req.uri)?;
        let route = Route::prepare(&req)?;
        let bytes = req
            .input
            .store_all_limited(rustfs_protos::MRF_LIFECYCLE_MAX_COMMAND_BYTES)
            .await
            .map_err(|_| admin_error(S3ErrorCode::InvalidRequest, "MRF action body too large or unreadable"))?;
        let action: RoutedAction =
            serde_json::from_slice(&bytes).map_err(|_| admin_error(S3ErrorCode::InvalidRequest, "invalid MRF action"))?;
        validate_mrf_legacy_responsibility_action(&action.request)
            .map_err(|reason| admin_error(S3ErrorCode::InvalidRequest, reason))?;
        let target = route.target(&action.node_id)?;
        let started = mrf::now_ms();
        let node = route
            .observe(
                target,
                MrfOperation::Action {
                    actor,
                    request: Box::new(action.request),
                },
                Duration::from_secs(30),
            )
            .await;
        response(&route, started, vec![node], None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::admin::runtime_sources::{AppContext, IamInterface, KmsInterface, publish_test_app_context};
    use rustfs_heal::heal::mrf_queue::management::{MrfLifecycleSummary, MrfResponsibilityObservation};
    use rustfs_heal::heal::mrf_queue::{MrfLegacyResponsibility, MrfLegacyResponsibilitySnapshot};
    use rustfs_iam::store::{
        Store as _, UserType,
        object::{IAM_CONFIG_PREFIX, ObjectStore},
    };
    use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
    use rustfs_policy::policy::Policy;
    use serde_json::json;

    fn test_request(method: Method, path: String, key: Option<&str>, body: &'static str) -> S3Request<Body> {
        S3Request {
            input: Body::from(Bytes::from_static(body.as_bytes())),
            method,
            uri: path.parse().expect("MRF request URI"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: key.map(|key| crate::admin::storage_api::s3::auth::Credentials {
                access_key: key.into(),
                secret_key: "mrf-test-user-secret".into(),
            }),
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    struct ReadyIam(Arc<rustfs_iam::sys::IamSys<ObjectStore>>);

    impl IamInterface for ReadyIam {
        fn handle(&self) -> Arc<rustfs_iam::sys::IamSys<ObjectStore>> {
            self.0.clone()
        }

        fn is_ready(&self) -> bool {
            true
        }
    }

    struct TestKms;

    impl KmsInterface for TestKms {
        fn handle(&self) -> Arc<rustfs_kms::KmsServiceManager> {
            Arc::new(rustfs_kms::KmsServiceManager::new())
        }
    }

    async fn test_context() -> (tempfile::TempDir, Arc<AppContext>) {
        let (directory, _paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
        ObjectStore::new(store.clone())
            .save_iam_config(json!({"version": 1}), format!("{}/format.json", *IAM_CONFIG_PREFIX))
            .await
            .expect("persist IAM format");
        let iam = rustfs_iam::build_iam_sys(store.clone()).await.expect("build isolated IAM");
        (directory, Arc::new(AppContext::new(store, Arc::new(ReadyIam(iam)), Arc::new(TestKms))))
    }

    fn responsibility(id: u128) -> MrfLegacyResponsibility {
        MrfLegacyResponsibility {
            responsibility_id: Uuid::from_u128(id),
            bucket: "test-bucket".into(),
            object: "object".into(),
            source_bucket_incarnation_id: None,
            version_id: None,
            pool_index: Some(0),
            set_index: Some(0),
            bucket_incarnation_id: None,
            enqueued_at_ms: 1,
            status: "legacy_generation_unobserved".into(),
            held_since_ms: Some(1),
            accepted: None,
            last_identity_probe_result: None,
            last_identity_probe_at_ms: None,
            next_identity_probe_at_ms: None,
        }
    }

    fn page(ids: &[u128]) -> MrfLegacyResponsibilitySnapshot {
        MrfLegacyResponsibilitySnapshot {
            contract_version: 2,
            node_local: true,
            checkpoint_owner: Some(Uuid::from_u128(100)),
            checkpoint_sequence: Some(1),
            held_count: 0,
            legacy_generation_unknown_count: ids.len(),
            bucket_incarnation_changed_count: 0,
            operator_accepted_count: 0,
            responsibilities: ids.iter().map(|id| responsibility(*id)).collect(),
            next_cursor: ids.last().map(|id| Uuid::from_u128(*id)),
        }
    }

    fn summary() -> MrfLifecycleSummary {
        MrfLifecycleSummary {
            contract_version: 1,
            sampled_at_ms: 1,
            checkpoint_readable: true,
            checkpoint_owner: Some(Uuid::from_u128(100)),
            checkpoint_sequence: Some(1),
            active_responsibility_count: 0,
            held_count: 0,
            legacy_generation_unknown_count: 1,
            bucket_incarnation_changed_count: 0,
            operator_accepted_count: 0,
            best_effort_queue_depth: 0,
            responsibility_bytes: 128,
        }
    }

    fn refresh_operation() -> MrfOperation {
        MrfOperation::Action {
            actor: "test-operator".into(),
            request: Box::new(
                serde_json::from_value(json!({
                    "action": "refresh",
                    "responsibilityId": Uuid::from_u128(1)
                }))
                .expect("valid refresh request"),
            ),
        }
    }

    fn action_result(state: &str) -> mrf::MrfActionResult {
        mrf::MrfActionResult {
            responsibility_id: Uuid::from_u128(1),
            state: state.into(),
            data_verified: false,
            durable_responsibility_retained: true,
        }
    }

    fn node_observation(index: usize, state: &str) -> NodeObservation {
        NodeObservation {
            node_id: format!("node-{index}"),
            host: format!("node-{index}:9000"),
            state: state.into(),
            reply: None,
        }
    }

    #[test]
    fn cursor_cannot_switch_node_or_topology_or_use_nil_progress() {
        let nodes = vec![Target {
            id: "node-a".into(),
            host: "a".into(),
            local: true,
        }];
        let mut cursor = Cursor {
            kind: CursorKind::Summary,
            topology: "topology".into(),
            node_id: "node-a".into(),
            node_index: 0,
            after: None,
        };
        assert!(validate_cursor(&cursor, "topology", &nodes).is_ok());
        assert!(validate_cursor(&cursor, "changed", &nodes).is_err());
        cursor.node_id = "arbitrary-url".into();
        assert!(validate_cursor(&cursor, "topology", &nodes).is_err());
        cursor.node_id = "node-a".into();
        cursor.after = Some(Uuid::nil());
        assert!(validate_cursor(&cursor, "topology", &nodes).is_err());
        cursor.after = None;
        cursor.node_index = nodes.len();
        assert!(validate_cursor(&cursor, "topology", &nodes).is_err());
    }

    #[test]
    #[serial_test::serial]
    fn summary_cursor_resumes_past_32_nodes_and_timed_out_prefix() {
        crate::app::gating_test_env::run_large_stack_test("mrf-summary-cursor", || async {
            let (_directory, context) = test_context().await;
            let route = Route {
                context,
                fingerprint: "topology".into(),
                nodes: (0..65)
                    .map(|index| Target {
                        id: format!("node-{index}"),
                        host: format!("node-{index}:9000"),
                        local: false,
                    })
                    .collect(),
            };
            let uri: Uri = ROOT.parse().expect("summary URI");
            assert_eq!(parse_query(&uri, &route, CursorKind::Summary).unwrap(), (0, None, 32));
            for (index, count, next_index) in [(0, 32, Some(32)), (32, 32, Some(64)), (64, 1, None)] {
                let mut nodes = (index..index + count).map(|index| node_observation(index, "ok")).collect();
                let cursor = summary_continuation(&route, index, &mut nodes).unwrap();
                assert_eq!(nodes.len(), count);
                if let Some(next_index) = next_index {
                    let cursor = cursor.expect("remaining topology must have a cursor");
                    let uri: Uri = format!("{ROOT}?cursor={cursor}").parse().unwrap();
                    assert_eq!(parse_query(&uri, &route, CursorKind::Summary).unwrap(), (next_index, None, 32));
                    assert!(parse_query(&uri, &route, CursorKind::Responsibilities).is_err());
                } else {
                    assert!(cursor.is_none(), "the last node ends pagination");
                }
            }

            let mut nodes = vec![
                node_observation(32, "timed_out"),
                node_observation(33, "timed_out"),
                node_observation(34, "budget_exceeded"),
                node_observation(35, "budget_exceeded"),
            ];
            let cursor = summary_continuation(&route, 32, &mut nodes).unwrap().unwrap();
            assert_eq!(
                nodes.iter().map(|node| node.node_id.as_str()).collect::<Vec<_>>(),
                ["node-32", "node-33", "node-34"]
            );
            let uri: Uri = format!("{ROOT}?cursor={cursor}").parse().unwrap();
            assert_eq!(parse_query(&uri, &route, CursorKind::Summary).unwrap(), (34, None, 32));
            let (_, mut body) = response(&route, 1, nodes, Some(cursor)).unwrap().output;
            let observation: serde_json::Value = serde_json::from_slice(&body.store_all_limited(8192).await.unwrap()).unwrap();
            assert_eq!(observation["pageComplete"], false);
            assert_eq!(observation["expectedNodes"], 65);

            let mut unstarted = vec![
                node_observation(10, "budget_exceeded"),
                node_observation(11, "budget_exceeded"),
            ];
            let cursor = summary_continuation(&route, 10, &mut unstarted).unwrap().unwrap();
            assert_eq!(unstarted.len(), 1);
            let uri: Uri = format!("{ROOT}?cursor={cursor}").parse().unwrap();
            assert_eq!(parse_query(&uri, &route, CursorKind::Summary).unwrap(), (10, None, 32));
            let (_, mut body) = response(&route, 1, Vec::new(), Some(cursor)).unwrap().output;
            let observation: serde_json::Value = serde_json::from_slice(&body.store_all_limited(8192).await.unwrap()).unwrap();
            assert_eq!(observation["pageComplete"], false, "no observed nodes cannot establish completeness");

            let after = Some(Uuid::from_u128(7));
            let cursor = continuation(&route, 32, after, CursorKind::Responsibilities)
                .unwrap()
                .unwrap();
            let uri: Uri = format!("{ROOT}/responsibilities?cursor={cursor}&limit=1").parse().unwrap();
            assert_eq!(parse_query(&uri, &route, CursorKind::Responsibilities).unwrap(), (32, after, 1));
            assert!(parse_query(&uri, &route, CursorKind::Summary).is_err());
            let cursor = continuation(&route, 32, after, CursorKind::Summary).unwrap().unwrap();
            let uri: Uri = format!("{ROOT}?cursor={cursor}").parse().unwrap();
            assert!(parse_query(&uri, &route, CursorKind::Summary).is_err());

            let valid_cursor = continuation(&route, 32, None, CursorKind::Summary).unwrap().unwrap();
            for query in [
                "limit=0".to_owned(),
                "limit=33".to_owned(),
                "limit=1&limit=2".to_owned(),
                "unknown=1".to_owned(),
                "cursor=not-json".to_owned(),
                format!("cursor={valid_cursor}&cursor={valid_cursor}"),
            ] {
                let uri: Uri = format!("{ROOT}?{query}").parse().unwrap();
                let error = parse_query(&uri, &route, CursorKind::Summary).expect_err("invalid query must fail");
                assert_eq!(error.code(), &S3ErrorCode::InvalidArgument, "query: {query}");
            }
        });
    }

    #[test]
    fn valid_reply_rejects_mismatched_operation_even_when_reply_is_successful() {
        let cases = [
            (MrfOperation::Summary {}, MrfReply::Summary { summary: summary() }),
            (
                MrfOperation::List { cursor: None, limit: 2 },
                MrfReply::List {
                    snapshot: page(&[1, 2]),
                    observed_at_ms: 1,
                },
            ),
            (
                MrfOperation::Get { id: Uuid::from_u128(1) },
                MrfReply::Get {
                    observation: Box::new(MrfResponsibilityObservation {
                        contract_version: 1,
                        observed_at_ms: 1,
                        responsibility: Some(responsibility(1)),
                    }),
                },
            ),
            (
                refresh_operation(),
                MrfReply::Action {
                    action: action_result("refreshed"),
                },
            ),
        ];
        for (operation_index, (operation, _)) in cases.iter().enumerate() {
            for (reply_index, (_, reply)) in cases.iter().enumerate() {
                assert!(reply.is_success());
                assert_eq!(valid_reply(operation, reply), operation_index == reply_index);
            }
            let error = MrfReply::Error {
                code: "unavailable".into(),
            };
            assert!(valid_reply(operation, &error));
            assert!(!error.is_success());
        }
    }

    #[test]
    fn valid_reply_rejects_unknown_versions_and_wrong_responsibility_identity() {
        let mut invalid_summary = summary();
        invalid_summary.contract_version = 2;
        assert!(!valid_reply(
            &MrfOperation::Summary {},
            &MrfReply::Summary {
                summary: invalid_summary
            }
        ));

        let unreadable = MrfReply::Summary {
            summary: MrfLifecycleSummary {
                checkpoint_readable: false,
                ..summary()
            },
        };
        assert!(valid_reply(&MrfOperation::Summary {}, &unreadable));
        assert!(!unreadable.is_success(), "unreadable cached counts must remain a partial observation");

        let operation = MrfOperation::Get { id: Uuid::from_u128(1) };
        for (version, row) in [
            (2, Some(responsibility(1))),
            (1, Some(responsibility(2))),
            (1, Some(responsibility(0))),
        ] {
            let reply = MrfReply::Get {
                observation: Box::new(MrfResponsibilityObservation {
                    contract_version: version,
                    observed_at_ms: 1,
                    responsibility: row,
                }),
            };
            assert!(!valid_reply(&operation, &reply));
        }
        let absent = MrfReply::Get {
            observation: Box::new(MrfResponsibilityObservation {
                contract_version: 1,
                observed_at_ms: 1,
                responsibility: None,
            }),
        };
        assert!(valid_reply(&operation, &absent));
        let encoded = serde_json::to_value(absent).unwrap();
        assert!(encoded["observation"]["responsibility"].is_null());
        assert!(encoded["observation"].get("dataVerified").is_none());
    }

    #[test]
    fn valid_reply_rejects_invalid_peer_page_order_bounds_and_cursor() {
        let operation = MrfOperation::List {
            cursor: Some(Uuid::from_u128(1)),
            limit: 2,
        };
        let valid = page(&[2, 3]);
        assert!(valid_reply(
            &operation,
            &MrfReply::List {
                snapshot: valid.clone(),
                observed_at_ms: 1
            }
        ));

        let mut wrong_version = valid.clone();
        wrong_version.contract_version = 1;
        let mut not_node_local = valid.clone();
        not_node_local.node_local = false;
        let mut wrong_cursor = valid.clone();
        wrong_cursor.next_cursor = Some(Uuid::from_u128(4));
        let mut empty_with_cursor = page(&[]);
        empty_with_cursor.next_cursor = Some(Uuid::from_u128(1));
        for snapshot in [
            wrong_version,
            not_node_local,
            wrong_cursor,
            empty_with_cursor,
            page(&[2, 3, 4]),
            page(&[0, 2]),
            page(&[1, 2]),
            page(&[2, 2]),
            page(&[3, 2]),
        ] {
            assert!(!valid_reply(
                &operation,
                &MrfReply::List {
                    snapshot,
                    observed_at_ms: 1
                }
            ));
        }
        assert!(valid_reply(
            &operation,
            &MrfReply::List {
                snapshot: page(&[]),
                observed_at_ms: 1
            }
        ));
        let mut final_page = valid;
        final_page.next_cursor = None;
        assert!(valid_reply(
            &operation,
            &MrfReply::List {
                snapshot: final_page,
                observed_at_ms: 1
            }
        ));
    }

    #[test]
    fn valid_reply_rejects_action_identity_state_or_unverified_retention_claim_changes() {
        for (action, state) in [
            ("refresh", "refreshed"),
            ("recheck", "active"),
            ("acceptUnverifiedRisk", "operatorAcceptedUnverified"),
        ] {
            let mut value = json!({
                "action": action,
                "responsibilityId": Uuid::from_u128(1),
                "expectedBucketIncarnationId": Uuid::from_u128(2)
            });
            if action == "acceptUnverifiedRisk" {
                value["acknowledgeUnverifiedDataRisk"] = json!(true);
                value["acknowledgeUnknownSourceIncarnation"] = json!(true);
                value["acknowledgeBucketIncarnationMismatch"] = json!(false);
                value["reason"] = json!("operator accepted legacy risk");
                value["reference"] = json!("incident-1");
                value["requestId"] = json!(Uuid::from_u128(3));
            }
            let request = serde_json::from_value(value).expect("valid action fixture");
            validate_mrf_legacy_responsibility_action(&request).expect("action is accepted by production validation");
            let operation = MrfOperation::Action {
                actor: "test-operator".into(),
                request: Box::new(request),
            };
            let result = action_result(state);
            assert!(valid_reply(&operation, &MrfReply::Action { action: result.clone() }));
            let mut wrong_id = result.clone();
            wrong_id.responsibility_id = Uuid::from_u128(4);
            for other_state in ["refreshed", "active", "operatorAcceptedUnverified", "completed"] {
                if other_state != state {
                    let mut wrong_state = result.clone();
                    wrong_state.state = other_state.into();
                    assert!(!valid_reply(&operation, &MrfReply::Action { action: wrong_state }));
                }
            }
            let mut verified = result.clone();
            verified.data_verified = true;
            let mut retired = result;
            retired.durable_responsibility_retained = false;
            for action in [wrong_id, verified, retired] {
                assert!(!valid_reply(&operation, &MrfReply::Action { action }));
            }
        }
    }

    #[tokio::test]
    async fn cluster_management_routes_require_handler_authentication() {
        let mut router = S3Router::new(false);
        register(&mut router).expect("register MRF cluster routes");
        assert_eq!(router.registered_routes().len(), 4);
        for prefix in ["/rustfs/admin", "/minio/admin"] {
            let handlers: [(Method, &str, &dyn Operation); 4] = [
                (Method::GET, "", &SummaryHandler),
                (Method::GET, "/responsibilities", &ListHandler),
                (Method::GET, "/responsibilities/node/00000000-0000-4000-8000-000000000001", &GetHandler),
                (Method::POST, "/responsibilities/actions", &ActionHandler),
            ];
            for (method, suffix, handler) in handlers {
                let path = format!("{prefix}{ROOT}{suffix}");
                assert!(router.contains_compatible_route(method.clone(), &path));
                let req = test_request(method, path, None, "");
                let error = handler
                    .call(req, Params::new())
                    .await
                    .expect_err("handler must require authenticated credentials");
                assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
                assert!(error.to_string().contains("authentication required"));
            }
        }
    }

    #[test]
    #[serial_test::serial]
    fn cluster_management_handlers_require_heal_permission() {
        crate::app::gating_test_env::run_large_stack_test("mrf-cluster-permission", || async {
            let previous = crate::runtime_sources::current_app_context();
            let (_directory, context) = test_context().await;
            let iam = context.iam().handle();
            for (key, action) in [("mrf-observer", "admin:ServerInfo"), ("mrf-operator", "admin:Heal")] {
                iam.create_user(
                    key,
                    &AddOrUpdateUserReq {
                        secret_key: "mrf-test-user-secret".into(),
                        policy: None,
                        status: AccountStatus::Enabled,
                    },
                )
                .await
                .expect("create real IAM user");
                let policy = Policy::parse_config(
                    &serde_json::to_vec(&json!({
                        "Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": action}]
                    }))
                    .unwrap(),
                )
                .expect("valid admin policy");
                iam.set_policy(key, policy).await.expect("save IAM policy");
                iam.policy_db_set(key, UserType::Reg, false, key)
                    .await
                    .expect("attach IAM policy");
            }
            assert!(context.publish_action_credentials(rustfs_credentials::Credentials {
                access_key: "mrf-test-root".into(),
                secret_key: "mrf-test-root-secret".into(),
                status: "on".into(),
                ..Default::default()
            }));
            publish_test_app_context(context);

            let handlers: [(Method, &str, &dyn Operation); 4] = [
                (Method::GET, "", &SummaryHandler),
                (Method::GET, "/responsibilities", &ListHandler),
                (Method::GET, "/responsibilities/node/00000000-0000-4000-8000-000000000001", &GetHandler),
                (Method::POST, "/responsibilities/actions", &ActionHandler),
            ];
            for (method, suffix, handler) in handlers {
                let req = test_request(
                    method,
                    format!("{ADMIN_PREFIX}{ROOT}{suffix}"),
                    Some("mrf-observer"),
                    "invalid body must not be inspected before authorization",
                );
                let error = handler
                    .call(req, Params::new())
                    .await
                    .expect_err("ServerInfo does not authorize Heal");
                assert_eq!(error.code(), &S3ErrorCode::AccessDenied, "route: {suffix}");
            }
            let authorized = test_request(Method::GET, ROOT.into(), Some("mrf-operator"), "");
            validate_heal_admin_request(&authorized)
                .await
                .expect("Heal policy must pass the same IAM gate");
            if let Some(previous) = previous {
                publish_test_app_context(previous);
            }
        });
    }

    #[test]
    fn forwarded_actor_cannot_be_supplied_by_admin_client() {
        let body = r#"{"nodeId":"node","actor":"root","request":{"action":"refresh","responsibilityId":"00000000-0000-4000-8000-000000000001"}}"#;
        assert!(serde_json::from_str::<RoutedAction>(body).is_err());
    }
}
