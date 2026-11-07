mod auth;
mod bootstrap;
mod contract;
mod media;
mod openapi;

use auth::{LocalAuth, authorize, security_error};
use axum::body::Body;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router, middleware};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use simulmt_api::*;
use simulmt_core::{
    CoreError, CoreOptions, DeploymentConfig, SessionManager, SubtitleExportFormat,
    list_macos_sources,
};
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::sync::Semaphore;
use tower_http::services::{ServeDir, ServeFile};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    manager: Arc<SessionManager>,
    audio_host: String,
    runtime_dir: PathBuf,
    media_limits: media::MediaLimits,
    media_gate: Arc<Semaphore>,
    auth: LocalAuth,
}

#[derive(Serialize, JsonSchema)]
struct Health {
    api_version: &'static str,
    instance_id: Uuid,
    status: &'static str,
}

#[tokio::main]
async fn main() {
    if std::env::args().any(|argument| argument == "--print-openapi") {
        print!("{}", openapi::document_yaml());
        return;
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "simulmt_app=info,simulmt_core=info".into()),
        )
        .init();
    let address = std::env::var("SIMULMT_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8780".into())
        .parse::<std::net::SocketAddr>()
        .expect("SIMULMT_ADDR must be a numeric loopback IP and port");
    if !address.ip().is_loopback() {
        panic!("SIMULMT_ADDR must be loopback; remote control-plane access is disabled");
    }
    let media_limits = media::MediaLimits::from_env().expect("valid media resource limits");
    let root = std::env::var("SIMULMT_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::current_dir().expect("current directory"));
    let demo = std::env::var("SIMULMT_DEMO_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let vendor = root.join("vendor");
            if vendor.join("models").is_dir() {
                vendor
            } else {
                root.parent().unwrap_or(&root).to_path_buf()
            }
        });
    let runtime = std::env::var("SIMULMT_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| root.join("runtime"));
    let audio_host = std::env::var("SIMULMT_AUDIO_HOST").unwrap_or_else(|_| {
        root.join("target/native/simulmt-audio-host")
            .to_string_lossy()
            .into_owned()
    });
    let web = root.join("web");
    tokio::fs::create_dir_all(&runtime)
        .await
        .expect("create runtime directory");
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("bind local API");
    let identity = bootstrap::publish(&runtime, address).expect("publish owner-only bootstrap");
    let manager = Arc::new(
        SessionManager::load(CoreOptions {
            audio_host: audio_host.clone(),
            runtime_dir: runtime.to_string_lossy().into_owned(),
            instance_id: identity.instance_id,
            deployment: DeploymentConfig {
                model_executable: demo.join("llama.cpp-hyv3/build/bin/llama-mtmd-cli"),
                model_path: demo.join("models/Hy-SimulMT-30B-A3B-Interleave-Q4_K_M.gguf"),
                player_path: root.join("target/native/simulmt-tts-host"),
            },
        })
        .await
        .expect("restore local sessions"),
    );
    let auth = LocalAuth::new(address, identity.token.clone());
    let state = AppState {
        manager: Arc::clone(&manager),
        audio_host,
        runtime_dir: runtime,
        media_gate: Arc::new(Semaphore::new(media_limits.concurrency)),
        media_limits,
        auth: auth.clone(),
    };
    let app = Router::new()
        .route("/api/health", get(health))
        .route("/api/sources", get(sources))
        .route(
            "/api/media",
            post(import_media).layer(DefaultBodyLimit::disable()),
        )
        .route("/api/default-config", get(default_config))
        .route("/api/sessions", post(create_session).get(list_sessions))
        .route(
            "/api/sessions/{id}",
            get(get_session).delete(delete_session),
        )
        .route("/api/sessions/{id}/title", put(rename_session))
        .route("/api/sessions/{id}/config", put(reconfigure))
        .route("/api/sessions/{id}/prepare", post(prepare))
        .route("/api/sessions/{id}/start", post(start))
        .route("/api/sessions/{id}/pause", post(pause))
        .route("/api/sessions/{id}/resume", post(resume))
        .route("/api/sessions/{id}/stop", post(stop))
        .route("/api/sessions/{id}/stop-model", post(stop_model))
        .route("/api/sessions/{id}/source", put(set_source))
        .route("/api/sessions/{id}/latency-mode", put(set_latency_mode))
        .route("/api/sessions/{id}/direction", put(set_direction))
        .route(
            "/api/sessions/{id}/history",
            get(history).delete(clear_history),
        )
        .route("/api/sessions/{id}/export.srt", get(export_srt))
        .route("/api/sessions/{id}/export.vtt", get(export_vtt))
        .route("/api/sessions/{id}/export.wav", get(export_wav))
        .route("/api/sessions/{id}/ws", get(websocket))
        .fallback_service(ServeDir::new(&web).fallback(ServeFile::new(web.join("index.html"))))
        .with_state(state)
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .layer(middleware::from_fn_with_state(auth, authorize))
        .layer(middleware::from_fn(contract::request_context));
    tracing::info!("EchoNote listening on http://{address}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            #[cfg(unix)]
            {
                let mut terminate =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("install SIGTERM handler");
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
            }
            manager.stop_all().await;
        })
        .await
        .expect("serve local API");
}

async fn health(State(state): State<AppState>) -> Json<Health> {
    Json(Health {
        api_version: API_VERSION,
        instance_id: state.manager.instance_id(),
        status: "ok",
    })
}

async fn sources(State(state): State<AppState>) -> Result<Json<Vec<SourceInfo>>, ApiError> {
    Ok(Json(list_macos_sources(&state.audio_host).await?))
}

#[derive(Deserialize, JsonSchema)]
struct MediaUploadQuery {
    name: Option<String>,
}

async fn import_media(
    State(state): State<AppState>,
    Query(query): Query<MediaUploadQuery>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<media::ImportedMedia>, Response> {
    let _permit = Arc::clone(&state.media_gate)
        .try_acquire_owned()
        .map_err(|_| {
            security_error(
                StatusCode::TOO_MANY_REQUESTS,
                "media_decoder_busy",
                "the media decoder concurrency limit has been reached; retry later",
            )
        })?;
    media::import(
        &state.runtime_dir,
        query.name,
        headers,
        body,
        state.media_limits,
    )
    .await
    .map(Json)
}

async fn default_config() -> Json<PipelineConfig> {
    Json(PipelineConfig {
        source: SourceConfig::MacosSystem {
            exclude_pids: vec![],
            mute_original: true,
        },
        translator: TranslatorConfig::HySimulmt {
            direction: "en-zh".into(),
            context: Some(16_384),
            n_predict: Some(160),
        },
        output: OutputConfig::MacosSystem {
            voice: Some("Tingting".into()),
            // Synthesize near natural speed; playback fits each phrase to its
            // source duration and only adds catch-up when real backlog exists.
            speech_rate: Some(220),
        },
        latency_mode: LatencyMode::Balanced,
        queue: QueuePolicy::default(),
        mute_original: true,
    })
}

fn validate_public_config(config: &PipelineConfig, runtime_dir: &FsPath) -> Result<(), ApiError> {
    validate_public_source(&config.source, runtime_dir)
}

fn validate_public_source(source: &SourceConfig, runtime_dir: &FsPath) -> Result<(), ApiError> {
    let SourceConfig::File { path, .. } = source else {
        return Ok(());
    };
    let media_root = std::fs::canonicalize(runtime_dir.join("media")).map_err(|_| {
        CoreError::InvalidConfig("import media before selecting a file source".into())
    })?;
    let candidate = std::fs::canonicalize(path).map_err(|_| {
        CoreError::InvalidConfig("the selected media is no longer available".into())
    })?;
    let valid_name = candidate
        .file_stem()
        .and_then(|value| value.to_str())
        .is_some_and(|value| Uuid::parse_str(value).is_ok())
        && candidate.extension().and_then(|value| value.to_str()) == Some("wav");
    if candidate.parent() != Some(media_root.as_path()) || !valid_name || !candidate.is_file() {
        return Err(CoreError::InvalidConfig(
            "file sources must reference a WAV imported through /api/media".into(),
        )
        .into());
    }
    Ok(())
}

async fn create_session(
    State(state): State<AppState>,
    Json(config): Json<PipelineConfig>,
) -> Result<Json<SessionSnapshot>, ApiError> {
    validate_public_config(&config, &state.runtime_dir)?;
    let session = state.manager.create(config).await?;
    Ok(Json(session.snapshot().await))
}

async fn list_sessions(State(state): State<AppState>) -> Json<Vec<SessionSnapshot>> {
    let sessions = state.manager.list().await;
    let mut snapshots = Vec::with_capacity(sessions.len());
    for session in sessions {
        snapshots.push(session.snapshot().await);
    }
    Json(snapshots)
}

async fn get_session(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<SessionSnapshot>, ApiError> {
    Ok(Json(state.manager.get(id).await?.snapshot().await))
}

async fn delete_session(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    state.manager.delete(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize, JsonSchema)]
struct SessionTitle {
    title: String,
}

async fn rename_session(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(title): Json<SessionTitle>,
) -> Result<Json<SessionSnapshot>, ApiError> {
    let session = state.manager.get(id).await?;
    session.rename(title.title).await?;
    Ok(Json(session.snapshot().await))
}

async fn reconfigure(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(config): Json<PipelineConfig>,
) -> Result<Json<SessionSnapshot>, ApiError> {
    let session = state.manager.get(id).await?;
    validate_public_config(&config, &state.runtime_dir)?;
    session.reconfigure(config).await?;
    Ok(Json(session.snapshot().await))
}

async fn prepare(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<SessionSnapshot>, ApiError> {
    let session = state.manager.prepare(id).await?;
    Ok(Json(session.snapshot().await))
}

async fn start(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<SessionSnapshot>, ApiError> {
    let session = state.manager.get(id).await?;
    session.start().await?;
    Ok(Json(session.snapshot().await))
}

async fn pause(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<SessionSnapshot>, ApiError> {
    let session = state.manager.get(id).await?;
    session.pause().await?;
    Ok(Json(session.snapshot().await))
}

async fn resume(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<SessionSnapshot>, ApiError> {
    let session = state.manager.get(id).await?;
    session.resume().await?;
    Ok(Json(session.snapshot().await))
}

async fn stop(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<SessionSnapshot>, ApiError> {
    let session = state.manager.get(id).await?;
    session.stop().await?;
    Ok(Json(session.snapshot().await))
}

#[derive(Deserialize, JsonSchema)]
struct SourceSelection {
    source: SourceConfig,
    mute_original: bool,
}

async fn stop_model(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<SessionSnapshot>, ApiError> {
    let session = state.manager.get(id).await?;
    session.stop_model().await?;
    Ok(Json(session.snapshot().await))
}

async fn set_source(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(selection): Json<SourceSelection>,
) -> Result<Json<SessionSnapshot>, ApiError> {
    let session = state.manager.get(id).await?;
    validate_public_source(&selection.source, &state.runtime_dir)?;
    session
        .set_source(selection.source, selection.mute_original)
        .await?;
    Ok(Json(session.snapshot().await))
}

#[derive(Deserialize, JsonSchema)]
struct DirectionSelection {
    direction: String,
}

async fn set_direction(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(selection): Json<DirectionSelection>,
) -> Result<Json<SessionSnapshot>, ApiError> {
    let session = state.manager.get(id).await?;
    session.set_direction(selection.direction).await?;
    Ok(Json(session.snapshot().await))
}

#[derive(Deserialize, JsonSchema)]
struct LatencySelection {
    latency_mode: LatencyMode,
}

async fn set_latency_mode(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(selection): Json<LatencySelection>,
) -> Result<Json<SessionSnapshot>, ApiError> {
    let session = state.manager.get(id).await?;
    session.set_latency_mode(selection.latency_mode).await?;
    Ok(Json(session.snapshot().await))
}

async fn history(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<SubtitleCue>>, ApiError> {
    Ok(Json(state.manager.get(id).await?.history().await?))
}

async fn clear_history(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<SubtitleCue>>, ApiError> {
    let session = state.manager.get(id).await?;
    session.clear_history().await?;
    Ok(Json(Vec::new()))
}

async fn export_srt(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let document = state
        .manager
        .get(id)
        .await?
        .export_subtitles(SubtitleExportFormat::Srt)
        .await?;
    Ok(download_response(
        "simulmt-subtitles.srt",
        "application/x-subrip; charset=utf-8",
        document.into_bytes(),
    ))
}

async fn export_vtt(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let document = state
        .manager
        .get(id)
        .await?
        .export_subtitles(SubtitleExportFormat::Vtt)
        .await?;
    Ok(download_response(
        "simulmt-subtitles.vtt",
        "text/vtt; charset=utf-8",
        document.into_bytes(),
    ))
}

async fn export_wav(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let export_path = state.manager.get(id).await?.export_wav().await?;
    let file = tokio::fs::File::open(export_path)
        .await
        .map_err(|error| CoreError::Output(error.to_string()))?;
    let content_length = file
        .metadata()
        .await
        .map_err(|error| CoreError::Output(error.to_string()))?
        .len();
    let stream = futures_util::stream::try_unfold(file, |mut file| async move {
        let mut chunk = vec![0_u8; 64 * 1024];
        let read = file.read(&mut chunk).await?;
        if read == 0 {
            Ok::<Option<(Vec<u8>, tokio::fs::File)>, std::io::Error>(None)
        } else {
            chunk.truncate(read);
            Ok(Some((chunk, file)))
        }
    });
    let mut headers = download_headers("simulmt-translation.wav", "audio/wav");
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string()).expect("valid content length"),
    );
    Ok((headers, Body::from_stream(stream)).into_response())
}

fn download_response(
    filename: &'static str,
    content_type: &'static str,
    body: Vec<u8>,
) -> Response {
    (download_headers(filename, content_type), body).into_response()
}

fn download_headers(filename: &'static str, content_type: &'static str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"{filename}\""))
            .expect("valid download header"),
    );
    headers
}

async fn websocket(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let session = state.manager.get(id).await?;
    let auth = state.auth.clone();
    Ok(ws
        .max_message_size(4 * 1024)
        .max_frame_size(4 * 1024)
        .on_upgrade(move |socket| stream_events(socket, session, auth)))
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SocketAuthentication {
    #[serde(rename = "type")]
    kind: String,
    token: String,
}

async fn stream_events(
    mut socket: WebSocket,
    session: Arc<simulmt_core::Session>,
    auth: LocalAuth,
) {
    let authenticated = tokio::time::timeout(std::time::Duration::from_secs(5), socket.recv())
        .await
        .ok()
        .flatten()
        .and_then(Result::ok)
        .and_then(|message| match message {
            Message::Text(text) => serde_json::from_str::<SocketAuthentication>(&text).ok(),
            _ => None,
        })
        .is_some_and(|authentication| {
            authentication.kind == "authenticate" && auth.accepts_token(&authentication.token)
        });
    if !authenticated {
        let _ = socket
            .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                code: 1008,
                reason: "valid first-message authentication required".into(),
            })))
            .await;
        return;
    }
    let (baseline, mut events) = match session.subscribe_baseline().await {
        Ok(subscription) => subscription,
        Err(error) => {
            tracing::warn!(session_id = %session.id, "failed to create WS baseline: {error}");
            let _ = socket
                .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                    code: 1011,
                    reason: "baseline unavailable".into(),
                })))
                .await;
            return;
        }
    };
    if send_stream_message(
        &mut socket,
        SessionStreamMessage::Baseline(Box::new(baseline)),
    )
    .await
    .is_err()
    {
        return;
    }
    loop {
        match events.recv().await {
            Ok(event) => {
                if send_stream_message(&mut socket, SessionStreamMessage::Event(event))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!(session_id = %session.id, "UI event receiver lagged by {skipped} events");
                match session.subscribe_baseline().await {
                    Ok((baseline, replacement)) => {
                        let notice = SessionStreamMessage::ResyncRequired {
                            instance_id: baseline.instance_id,
                            session_id: baseline.session_id,
                            sequence: baseline.sequence,
                            reason: format!("event buffer overflowed; skipped {skipped} events"),
                        };
                        if send_stream_message(&mut socket, notice).await.is_err()
                            || send_stream_message(
                                &mut socket,
                                SessionStreamMessage::Baseline(Box::new(baseline)),
                            )
                            .await
                            .is_err()
                        {
                            break;
                        }
                        events = replacement;
                    }
                    Err(error) => {
                        tracing::warn!(session_id = %session.id, "failed to resync WS baseline: {error}");
                        break;
                    }
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

async fn send_stream_message(
    socket: &mut WebSocket,
    message: SessionStreamMessage,
) -> Result<(), axum::Error> {
    let text = serde_json::to_string(&message).expect("serialize event");
    socket.send(Message::Text(text.into())).await
}

struct ApiError(CoreError);

impl From<CoreError> for ApiError {
    fn from(value: CoreError) -> Self {
        Self(value)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, component_id, retryable) = match &self.0 {
            CoreError::SessionNotFound(_) => {
                (StatusCode::NOT_FOUND, "session_not_found", "session", false)
            }
            CoreError::InvalidState(_) => (StatusCode::CONFLICT, "invalid_state", "session", false),
            CoreError::InvalidConfig(_) => {
                (StatusCode::BAD_REQUEST, "invalid_config", "session", false)
            }
            CoreError::PermissionDenied(_) => {
                (StatusCode::FORBIDDEN, "permission_denied", "source", false)
            }
            CoreError::RoutingUnsafe(_) => {
                (StatusCode::CONFLICT, "routing_unsafe", "source", false)
            }
            CoreError::Source(_) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "source_unavailable",
                "source",
                true,
            ),
            CoreError::Translator(_) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "translator_unavailable",
                "translator",
                true,
            ),
            CoreError::Output(_) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "output_unavailable",
                "output",
                true,
            ),
            CoreError::Storage(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "storage_error",
                "storage",
                false,
            ),
            CoreError::Io(_) => (StatusCode::INTERNAL_SERVER_ERROR, "io_error", "core", false),
            CoreError::Protocol(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "protocol_error",
                "core",
                false,
            ),
            CoreError::Policy(_) => (StatusCode::BAD_REQUEST, "invalid_policy", "policy", false),
        };
        let error = ErrorInfo {
            code: code.into(),
            component_id: component_id.into(),
            message: self.0.to_string(),
            retryable,
            fatal: false,
            request_id: String::new(),
        };
        (status, Json(error)).into_response()
    }
}
