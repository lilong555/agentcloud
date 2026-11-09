// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Authenticated execution-feedback listener shared by native frontend classifiers.
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Semaphore;

use super::{
    ExecutionBinding, ExecutionFeedbackControl, ExecutionFeedbackError, SandboxStateUpdate,
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    pub token: String,
    pub source_id: String,
    pub tenant_id: String,
    pub workspace_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlConfig {
    pub listen: SocketAddr,
    pub credentials: Vec<Credential>,
    #[serde(default = "default_rps")]
    pub requests_per_second: u32,
    #[serde(default = "default_sessions")]
    pub max_sessions: usize,
}
fn default_rps() -> u32 {
    100
}
fn default_sessions() -> usize {
    10000
}

#[derive(Clone)]
struct Session {
    binding: ExecutionBinding,
    routing_session_id: String,
    inference_lease: String,
    classifier_id: usize,
    generation: String,
}

pub struct ControlServer {
    config: ControlConfig,
    classifiers: Mutex<Vec<ExecutionFeedbackControl>>,
    sessions: Mutex<HashMap<(String, String, String), Session>>,
    rate: Mutex<Vec<(Instant, u32)>>,
    tombstones: Mutex<VecDeque<Value>>,
    concurrent: Arc<Semaphore>,
}

impl ControlServer {
    pub fn attach(&self, classifier: ExecutionFeedbackControl) {
        self.classifiers.lock().push(classifier);
    }
    pub fn router(self: Arc<Self>) -> Router {
        Router::new()
            .route("/v1/execution/capabilities", get(capabilities))
            .route("/v1/execution/bindings", post(register))
            .route("/v1/execution/state", post(update))
            .route("/v1/execution/session", post(session))
            .route("/v1/execution/finalize", post(finalize))
            .layer(DefaultBodyLimit::max(8192))
            .layer(middleware::from_fn_with_state(self.clone(), authenticate))
            .with_state(self)
    }
    fn create(config: ControlConfig) -> Result<Arc<Self>, String> {
        if config.credentials.is_empty()
            || config.credentials.len() > 256
            || config.requests_per_second == 0
            || config.max_sessions == 0
            || config.max_sessions > 100000
        {
            return Err("invalid execution control capacity or credentials".into());
        }
        for (i, c) in config.credentials.iter().enumerate() {
            if c.token.len() < 32
                || c.token.len() > 512
                || !c.token.bytes().all(|b| b.is_ascii_graphic())
                || config.credentials[..i].iter().any(|p| p.token == c.token)
            {
                return Err(
                    "execution control tokens must be unique printable strings of 32..512 bytes"
                        .into(),
                );
            }
            ExecutionBinding {
                source_id: c.source_id.clone(),
                tenant_id: c.tenant_id.clone(),
                workspace_id: if c.workspace_id == "*" {
                    "validate".into()
                } else {
                    c.workspace_id.clone()
                },
                session_id: "validate".into(),
                actor_uid: "validate".into(),
                epoch: 1,
            }
            .validate()
            .map_err(|e| e.to_string())?;
        }
        let rate = Mutex::new(vec![(Instant::now(), 0); config.credentials.len()]);
        Ok(Arc::new(Self {
            config,
            classifiers: Mutex::new(Vec::new()),
            sessions: Mutex::new(HashMap::new()),
            rate,
            tombstones: Mutex::new(VecDeque::new()),
            concurrent: Arc::new(Semaphore::new(32)),
        }))
    }
    fn authorized(c: &Credential, b: &ExecutionBinding) -> bool {
        c.source_id == b.source_id
            && c.tenant_id == b.tenant_id
            && (c.workspace_id == "*" || c.workspace_id == b.workspace_id)
    }
    fn key(b: &ExecutionBinding) -> (String, String, String) {
        (
            b.tenant_id.clone(),
            b.workspace_id.clone(),
            b.session_id.clone(),
        )
    }
    fn classifier(&self, id: usize) -> Result<ExecutionFeedbackControl, ApiError> {
        self.classifiers.lock().get(id).cloned().ok_or(ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "classifier_unavailable",
        ))
    }
}

/// Loaded once per frontend process. Invalid configuration fails catalog resolution.
pub fn from_environment() -> Result<Option<Arc<ControlServer>>, String> {
    static SERVER: OnceLock<Result<Option<Arc<ControlServer>>, String>> = OnceLock::new();
    SERVER
        .get_or_init(|| {
            let Some(path) = std::env::var_os("DYN_EXECUTION_CONTROL_CONFIG") else {
                return Ok(None);
            };
            let bytes =
                std::fs::read(path).map_err(|e| format!("execution control config: {e}"))?;
            if bytes.len() > 262144 {
                return Err("execution control config exceeds 256 KiB".into());
            }
            let config: ControlConfig = serde_json::from_slice(&bytes)
                .map_err(|e| format!("execution control config: {e}"))?;
            let listener = std::net::TcpListener::bind(config.listen)
                .map_err(|e| format!("execution control listen: {e}"))?;
            listener.set_nonblocking(true).map_err(|e| e.to_string())?;
            let server = ControlServer::create(config)?;
            let cloned = server.clone();
            std::thread::Builder::new()
                .name("execution-control".into())
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("execution control runtime");
                    runtime.block_on(async move {
                        let listener = tokio::net::TcpListener::from_std(listener)
                            .expect("execution control listener");
                        if let Err(error) = axum::serve(listener, cloned.router()).await {
                            tracing::error!(%error, "execution control listener stopped");
                        }
                    });
                })
                .map_err(|e| e.to_string())?;
            Ok(Some(server))
        })
        .clone()
}

struct ApiError(StatusCode, &'static str);
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.0,
            Json(json!({"protocol_version":1,"accepted":false,"applied":false,"error":self.1})),
        )
            .into_response()
    }
}
impl From<ExecutionFeedbackError> for ApiError {
    fn from(e: ExecutionFeedbackError) -> Self {
        let code = match e {
            ExecutionFeedbackError::InvalidBinding => "invalid_binding",
            ExecutionFeedbackError::UnknownSession => "unknown_session",
            ExecutionFeedbackError::StaleEpoch => "stale_epoch",
            ExecutionFeedbackError::IdentityMismatch => "identity_mismatch",
            ExecutionFeedbackError::Replay => "replay",
            ExecutionFeedbackError::InvalidUpdate => "invalid_update",
            ExecutionFeedbackError::Expired => "expired",
            ExecutionFeedbackError::SessionActive => "session_active",
        };
        Self(StatusCode::CONFLICT, code)
    }
}

pub(crate) fn constant_equal(a: &str, b: &str) -> bool {
    let mut mismatch = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        mismatch |= usize::from(
            a.as_bytes().get(i).copied().unwrap_or(0) ^ b.as_bytes().get(i).copied().unwrap_or(0),
        );
    }
    mismatch == 0
}

async fn authenticate(
    State(server): State<Arc<ControlServer>>,
    mut request: Request,
    next: Next,
) -> Response {
    let token = request
        .headers()
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .unwrap_or("");
    let mut found = None;
    for (id, c) in server.config.credentials.iter().enumerate() {
        if constant_equal(token, &c.token) {
            found = Some(id);
        }
    }
    let Some(id) = found else {
        return ApiError(StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    };
    {
        let mut rates = server.rate.lock();
        let (since, count) = &mut rates[id];
        if since.elapsed() >= Duration::from_secs(1) {
            *since = Instant::now();
            *count = 0;
        }
        if *count >= server.config.requests_per_second {
            return ApiError(StatusCode::TOO_MANY_REQUESTS, "rate_limited").into_response();
        }
        *count += 1;
    }
    let Ok(_permit) = server.concurrent.clone().try_acquire_owned() else {
        return ApiError(StatusCode::TOO_MANY_REQUESTS, "concurrency_limited").into_response();
    };
    request
        .extensions_mut()
        .insert(server.config.credentials[id].clone());
    match tokio::time::timeout(Duration::from_secs(3), next.run(request)).await {
        Ok(response) => response,
        Err(_) => ApiError(StatusCode::REQUEST_TIMEOUT, "request_timeout").into_response(),
    }
}

async fn capabilities(State(server): State<Arc<ControlServer>>) -> Json<Value> {
    Json(
        json!({"protocol_version":1,"binding_finalization":true,"active_bindings":server.sessions.lock().len(),"retired_bindings_retained":server.tombstones.lock().len(),"execution_feedback":true,"trusted_bindings":true,"opaque_routing_sessions":true,"admission_history":true,"admission_attempt_header":"x-agentcloud-attempt-id","inference_lease_header":"x-agentcloud-inference-lease","classifier_count":server.classifiers.lock().len(),"max_body_bytes":8192,"requests_per_second":server.config.requests_per_second,"ttl_ms":{"min":100,"max":60000},"native_frontend":true,"gpu_execution":false}),
    )
}

fn binding(body: &[u8]) -> Result<(ExecutionBinding, usize, Option<String>), ApiError> {
    let mut value: Value = serde_json::from_slice(body)
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_json"))?;
    let obj = value
        .as_object_mut()
        .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid_json"))?;
    let version = obj.remove("protocol_version").and_then(|v| v.as_u64());
    if version != Some(1) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_version"));
    }
    let generation = obj
        .remove("registration_generation")
        .map(|v| {
            v.as_str()
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid_generation"))
        })
        .transpose()?;
    let classifier_id = obj
        .remove("classifier_id")
        .map(|v| {
            v.as_u64()
                .and_then(|i| usize::try_from(i).ok())
                .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid_classifier"))
        })
        .transpose()?
        .unwrap_or(0);
    if obj.keys().any(|key| {
        ![
            "source_id",
            "tenant_id",
            "workspace_id",
            "session_id",
            "actor_uid",
            "epoch",
        ]
        .contains(&key.as_str())
    }) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "unknown_field"));
    }
    let b: ExecutionBinding = serde_json::from_value(value)
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_binding"))?;
    b.validate()?;
    Ok((b, classifier_id, generation))
}

fn receipt(s: &Session, snapshot: Value, include_lease: bool) -> Json<Value> {
    let mut value = json!({"protocol_version":1,"accepted":true,"applied":snapshot["program_present"],"reconciled":true,"binding":s.binding,"routing_session_id":s.routing_session_id,"classifier_id":s.classifier_id,"binding_generation":s.generation,"sequence":snapshot["sequence"],"received_at":chrono::Utc::now(),"snapshot":snapshot});
    if include_lease {
        value["inference_lease"] = Value::String(s.inference_lease.clone());
    }
    Json(value)
}

async fn register(
    State(server): State<Arc<ControlServer>>,
    Extension(c): Extension<Credential>,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let (b, classifier_id, generation) = binding(&body)?;
    if !ControlServer::authorized(&c, &b) {
        return Err(ApiError(StatusCode::FORBIDDEN, "scope_mismatch"));
    }
    let classifier = server.classifier(classifier_id)?;
    let mut sessions = server.sessions.lock();
    let key = ControlServer::key(&b);
    let existing = sessions.get(&key);
    if let Some(old) = existing {
        if generation.as_ref().is_some_and(|g| g != &old.generation) {
            return Err(ApiError(StatusCode::CONFLICT, "stale_generation"));
        }
        if old.classifier_id != classifier_id {
            return Err(ApiError(StatusCode::CONFLICT, "classifier_mismatch"));
        }
        if old.binding != b && b.epoch <= old.binding.epoch {
            return Err(ApiError(StatusCode::CONFLICT, "stale_epoch"));
        }
    } else if generation.is_some() {
        return Err(ApiError(StatusCode::CONFLICT, "stale_generation"));
    } else if sessions.len() >= server.config.max_sessions {
        return Err(ApiError(StatusCode::TOO_MANY_REQUESTS, "session_limit"));
    }
    let mut s = existing.cloned().unwrap_or_else(|| Session {
        binding: b.clone(),
        routing_session_id: format!("ac:{}", uuid::Uuid::new_v4().simple()),
        inference_lease: uuid::Uuid::new_v4().simple().to_string(),
        classifier_id,
        generation: uuid::Uuid::new_v4().simple().to_string(),
    });
    if s.binding != b {
        s.generation = uuid::Uuid::new_v4().simple().to_string();
        s.inference_lease = uuid::Uuid::new_v4().simple().to_string();
    }
    s.binding = b;
    let mut internal = s.binding.clone();
    internal.session_id = s.routing_session_id.clone();
    classifier.register_binding(internal)?;
    classifier.authorize_session(&s.routing_session_id, &s.inference_lease);
    let snapshot = classifier.session_snapshot(&s.routing_session_id);
    let result = receipt(&s, snapshot, true);
    sessions.insert(key, s);
    Ok(result)
}

async fn session(
    State(server): State<Arc<ControlServer>>,
    Extension(c): Extension<Credential>,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let (b, _, generation) = binding(&body)?;
    if !ControlServer::authorized(&c, &b) {
        return Err(ApiError(StatusCode::FORBIDDEN, "scope_mismatch"));
    }
    let s = server
        .sessions
        .lock()
        .get(&ControlServer::key(&b))
        .cloned()
        .ok_or(ApiError(StatusCode::NOT_FOUND, "unknown_session"))?;
    if generation.as_deref() != Some(s.generation.as_str()) {
        return Err(ApiError(StatusCode::CONFLICT, "stale_generation"));
    }
    if s.binding != b {
        return Err(ApiError(StatusCode::CONFLICT, "identity_mismatch"));
    }
    let snapshot = server
        .classifier(s.classifier_id)?
        .session_snapshot(&s.routing_session_id);
    Ok(receipt(&s, snapshot, false))
}

async fn finalize(
    State(server): State<Arc<ControlServer>>,
    Extension(c): Extension<Credential>,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let (b, _, generation) = binding(&body)?;
    if !ControlServer::authorized(&c, &b) {
        return Err(ApiError(StatusCode::FORBIDDEN, "scope_mismatch"));
    }
    let mut sessions = server.sessions.lock();
    let key = ControlServer::key(&b);
    let s = sessions
        .get(&key)
        .ok_or(ApiError(StatusCode::NOT_FOUND, "unknown_session"))?;
    if generation.as_deref() != Some(s.generation.as_str()) {
        return Err(ApiError(StatusCode::CONFLICT, "stale_generation"));
    }
    if s.binding != b {
        return Err(ApiError(StatusCode::CONFLICT, "identity_mismatch"));
    }
    let classifier = server.classifier(s.classifier_id)?;
    let snapshot = classifier.session_snapshot(&s.routing_session_id);
    classifier.retire_binding(&s.routing_session_id)?;
    let mut tombstones = server.tombstones.lock();
    if tombstones.len() == server.config.max_sessions {
        tombstones.pop_front();
    }
    tombstones.push_back(
        json!({"generation":s.generation,"binding":s.binding,"sequence":snapshot["sequence"]}),
    );
    sessions.remove(&key);
    Ok(Json(
        json!({"protocol_version":1,"accepted":true,"finalized":true,"binding_generation":generation,"snapshot":snapshot}),
    ))
}

async fn update(
    State(server): State<Arc<ControlServer>>,
    Extension(c): Extension<Credential>,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let mut value: Value = serde_json::from_slice(&body)
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_json"))?;
    let generation = value
        .as_object_mut()
        .and_then(|o| o.remove("registration_generation"))
        .and_then(|v| v.as_str().map(str::to_owned))
        .filter(|s| !s.is_empty())
        .ok_or(ApiError(StatusCode::BAD_REQUEST, "missing_generation"))?;
    let obj = value
        .as_object()
        .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid_json"))?;
    if obj.keys().any(|key| {
        ![
            "protocol_version",
            "source_id",
            "tenant_id",
            "workspace_id",
            "session_id",
            "actor_uid",
            "epoch",
            "sequence",
            "observed_at",
            "ttl_ms",
            "state",
            "estimated_return_ms",
        ]
        .contains(&key.as_str())
    }) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "unknown_field"));
    }
    let mut u: SandboxStateUpdate = serde_json::from_value(value)
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_update"))?;
    if !ControlServer::authorized(&c, &u.binding) {
        return Err(ApiError(StatusCode::FORBIDDEN, "scope_mismatch"));
    }
    // Hold the mapping through validation and application so concurrent epoch rotation is fenced.
    let sessions = server.sessions.lock();
    let s = sessions
        .get(&ControlServer::key(&u.binding))
        .ok_or(ApiError(StatusCode::NOT_FOUND, "unknown_session"))?;
    if generation != s.generation {
        return Err(ApiError(StatusCode::CONFLICT, "stale_generation"));
    }
    if s.binding != u.binding {
        return Err(ApiError(StatusCode::CONFLICT, "identity_mismatch"));
    }
    u.binding.session_id = s.routing_session_id.clone();
    let classifier = server.classifier(s.classifier_id)?;
    classifier.update_state(u)?;
    let snapshot = classifier.session_snapshot(&s.routing_session_id);
    Ok(receipt(s, snapshot, false))
}

#[cfg(test)]
mod tests {
    use super::super::request_classifier::WorkerCapacitySnapshot;
    use super::super::{ThunderAgentClassifier, ThunderAgentConfig};
    use super::*;
    use axum::body::{Body, to_bytes};
    use tower::ServiceExt;

    const TOKEN: &str = "contract-test-token-0123456789abcdef";
    fn server(rps: u32) -> Arc<ControlServer> {
        let config = ControlConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            credentials: vec![Credential {
                token: TOKEN.into(),
                source_id: "coordinator".into(),
                tenant_id: "tenant-a".into(),
                workspace_id: "*".into(),
            }],
            requests_per_second: rps,
            max_sessions: 2,
        };
        let server = ControlServer::create(config).unwrap();
        let classifier = ThunderAgentClassifier::new(
            ThunderAgentConfig::default(),
            Arc::new(|| Arc::new(WorkerCapacitySnapshot::new([]))),
        )
        .unwrap();
        server.attach(classifier.execution_feedback());
        server
    }
    fn bind(tenant: &str, workspace: &str, epoch: u64) -> Value {
        json!({"protocol_version":1,"source_id":"coordinator","tenant_id":tenant,"workspace_id":workspace,"session_id":"session","actor_uid":"actor","epoch":epoch})
    }
    async fn call(
        server: Arc<ControlServer>,
        path: &str,
        mut value: Value,
        token: &str,
    ) -> (StatusCode, Value) {
        if path != "/v1/execution/bindings" && value.get("registration_generation").is_none() {
            let key = (
                value["tenant_id"].as_str().unwrap_or("").to_owned(),
                value["workspace_id"].as_str().unwrap_or("").to_owned(),
                value["session_id"].as_str().unwrap_or("").to_owned(),
            );
            if let Some(session) = server.sessions.lock().get(&key) {
                value["registration_generation"] = json!(session.generation);
            }
        }
        let response = server
            .router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from(value.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 65536).await.unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }
    #[tokio::test]
    async fn finalization_reclaims_binding_capacity_and_rejects_retired_generation_after_churn() {
        let server = server(10000);
        let identity = bind("tenant-a", "retired", 1);
        let (_, retired) = call(
            server.clone(),
            "/v1/execution/bindings",
            identity.clone(),
            TOKEN,
        )
        .await;
        let mut stale = identity.clone();
        stale["registration_generation"] = retired["binding_generation"].clone();
        assert_eq!(
            call(
                server.clone(),
                "/v1/execution/finalize",
                stale.clone(),
                TOKEN
            )
            .await
            .0,
            StatusCode::OK
        );
        for n in 0..64 {
            let mut b = bind("tenant-a", &format!("workspace-{n}"), 1);
            let (status, receipt) =
                call(server.clone(), "/v1/execution/bindings", b.clone(), TOKEN).await;
            assert_eq!(status, StatusCode::OK);
            b["registration_generation"] = receipt["binding_generation"].clone();
            assert_eq!(
                call(server.clone(), "/v1/execution/finalize", b, TOKEN)
                    .await
                    .0,
                StatusCode::OK
            );
            assert!(server.sessions.lock().is_empty());
            assert!(server.tombstones.lock().len() <= 2);
        }
        assert_eq!(
            call(
                server.clone(),
                "/v1/execution/bindings",
                stale.clone(),
                TOKEN
            )
            .await
            .1["error"],
            "stale_generation"
        );
        let (_, fresh) = call(
            server.clone(),
            "/v1/execution/bindings",
            identity.clone(),
            TOKEN,
        )
        .await;
        assert_ne!(retired["binding_generation"], fresh["binding_generation"]);
        assert_ne!(retired["routing_session_id"], fresh["routing_session_id"]);
        assert_ne!(retired["inference_lease"], fresh["inference_lease"]);
        assert_eq!(
            call(
                server.clone(),
                "/v1/execution/session",
                stale.clone(),
                TOKEN
            )
            .await
            .1["error"],
            "stale_generation"
        );
        assert_eq!(
            call(
                server.clone(),
                "/v1/execution/finalize",
                stale.clone(),
                TOKEN
            )
            .await
            .1["error"],
            "stale_generation"
        );
        stale["sequence"] = json!(1);
        stale["state"] = json!("SUSPENDED");
        stale["observed_at"] = json!(chrono::Utc::now());
        stale["ttl_ms"] = json!(2000);
        stale["estimated_return_ms"] = json!(1000);
        assert_eq!(
            call(server.clone(), "/v1/execution/state", stale, TOKEN)
                .await
                .1["error"],
            "stale_generation"
        );
    }

    #[tokio::test]
    async fn concurrent_final_register_and_state_keep_one_generation_and_tenant_scope() {
        let server = server(10000);
        let identity = bind("tenant-a", "w1", 1);
        let (_, receipt) = call(
            server.clone(),
            "/v1/execution/bindings",
            identity.clone(),
            TOKEN,
        )
        .await;
        let mut old = identity.clone();
        old["registration_generation"] = receipt["binding_generation"].clone();
        let mut state = old.clone();
        state["sequence"] = json!(1);
        state["state"] = json!("SUSPENDED");
        state["observed_at"] = json!(chrono::Utc::now());
        state["ttl_ms"] = json!(2000);
        state["estimated_return_ms"] = json!(1000);
        let (finalized, registered, update) = tokio::join!(
            call(server.clone(), "/v1/execution/finalize", old.clone(), TOKEN),
            call(
                server.clone(),
                "/v1/execution/bindings",
                identity.clone(),
                TOKEN
            ),
            call(server.clone(), "/v1/execution/state", state, TOKEN),
        );
        assert_eq!(finalized.0, StatusCode::OK);
        assert_eq!(registered.0, StatusCode::OK);
        assert!([StatusCode::OK, StatusCode::NOT_FOUND, StatusCode::CONFLICT].contains(&update.0));
        let (_, fresh) = call(server.clone(), "/v1/execution/bindings", identity, TOKEN).await;
        assert_ne!(fresh["binding_generation"], receipt["binding_generation"]);
        assert!(server.sessions.lock().len() <= 1);
        let mut foreign = bind("tenant-b", "w1", 1);
        foreign["registration_generation"] = fresh["binding_generation"].clone();
        assert_eq!(
            call(server.clone(), "/v1/execution/finalize", foreign, TOKEN)
                .await
                .0,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            call(server.clone(), "/v1/execution/session", old, TOKEN)
                .await
                .1["error"],
            "stale_generation"
        );
    }

    #[tokio::test]
    async fn authenticated_bindings_isolate_same_session_across_workspaces_and_rotate_epoch_lease()
    {
        let server = server(100);
        assert_eq!(
            call(
                server.clone(),
                "/v1/execution/bindings",
                bind("tenant-a", "w1", 1),
                "bad"
            )
            .await
            .0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            call(
                server.clone(),
                "/v1/execution/bindings",
                bind("tenant-b", "w1", 1),
                TOKEN
            )
            .await
            .0,
            StatusCode::FORBIDDEN
        );
        let (status, first) = call(
            server.clone(),
            "/v1/execution/bindings",
            bind("tenant-a", "w1", 1),
            TOKEN,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(first["applied"], false);
        let (_, same) = call(
            server.clone(),
            "/v1/execution/bindings",
            bind("tenant-a", "w1", 1),
            TOKEN,
        )
        .await;
        assert_eq!(first["inference_lease"], same["inference_lease"]);
        let (_, other) = call(
            server.clone(),
            "/v1/execution/bindings",
            bind("tenant-a", "w2", 1),
            TOKEN,
        )
        .await;
        assert_ne!(first["routing_session_id"], other["routing_session_id"]);
        let (_, rotated) = call(
            server.clone(),
            "/v1/execution/bindings",
            bind("tenant-a", "w1", 2),
            TOKEN,
        )
        .await;
        assert_ne!(first["inference_lease"], rotated["inference_lease"]);
        assert_eq!(first["routing_session_id"], rotated["routing_session_id"]);
        assert_eq!(
            call(
                server.clone(),
                "/v1/execution/session",
                bind("tenant-a", "w1", 1),
                TOKEN
            )
            .await
            .1["error"],
            "identity_mismatch"
        );
        assert_eq!(
            call(
                server.clone(),
                "/v1/execution/bindings",
                bind("tenant-a", "w3", 1),
                TOKEN
            )
            .await
            .0,
            StatusCode::TOO_MANY_REQUESTS
        );
    }
    #[tokio::test]
    async fn native_control_rejects_replay_expired_scope_and_unknown_fields() {
        let server = server(100);
        call(
            server.clone(),
            "/v1/execution/bindings",
            bind("tenant-a", "w1", 1),
            TOKEN,
        )
        .await;
        let mut update = bind("tenant-a", "w1", 1);
        let obj = update.as_object_mut().unwrap();
        obj.insert("sequence".into(), json!(1));
        obj.insert("observed_at".into(), json!(chrono::Utc::now()));
        obj.insert("ttl_ms".into(), json!(2000));
        obj.insert("state".into(), json!("SUSPENDED"));
        obj.insert("estimated_return_ms".into(), json!(10000));
        let (status, receipt) =
            call(server.clone(), "/v1/execution/state", update.clone(), TOKEN).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(receipt["sequence"], 1);
        update["registration_generation"] = receipt["binding_generation"].clone();
        assert_eq!(receipt["snapshot"]["fresh"], true);
        assert_eq!(
            call(server.clone(), "/v1/execution/state", update.clone(), TOKEN)
                .await
                .1["error"],
            "replay"
        );
        update["sequence"] = json!(2);
        update["observed_at"] = json!(chrono::Utc::now() - chrono::Duration::seconds(3));
        assert_eq!(
            call(server.clone(), "/v1/execution/state", update.clone(), TOKEN)
                .await
                .0,
            StatusCode::CONFLICT
        );
        update["tenant_id"] = json!("tenant-b");
        assert_eq!(
            call(server.clone(), "/v1/execution/state", update.clone(), TOKEN)
                .await
                .1["error"],
            "scope_mismatch"
        );
        update["extra"] = json!(true);
        assert_eq!(
            call(server.clone(), "/v1/execution/state", update, TOKEN)
                .await
                .1["error"],
            "unknown_field"
        );
    }
    #[tokio::test]
    async fn body_size_and_per_credential_rate_are_bounded() {
        let server = server(2);
        assert_eq!(
            call(
                server.clone(),
                "/v1/execution/bindings",
                json!({"large":"x".repeat(9000)}),
                TOKEN
            )
            .await
            .0,
            StatusCode::PAYLOAD_TOO_LARGE
        );
        call(
            server.clone(),
            "/v1/execution/bindings",
            bind("tenant-a", "w1", 1),
            TOKEN,
        )
        .await;
        assert_eq!(
            call(
                server,
                "/v1/execution/bindings",
                bind("tenant-a", "w1", 1),
                TOKEN
            )
            .await
            .0,
            StatusCode::TOO_MANY_REQUESTS
        );
    }
}
