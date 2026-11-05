use axum::body::{Body, Bytes};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use simulmt_api::*;
use simulmt_core::{CoreError, CoreOptions, SessionManager, list_macos_sources};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tower_http::services::{ServeDir, ServeFile};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    manager: Arc<SessionManager>,
    audio_host: String,
    paths: ProjectPaths,
}

#[derive(Clone)]
struct ProjectPaths {
    root: PathBuf,
    demo: PathBuf,
    runtime: PathBuf,
}

#[derive(Serialize)]
struct Health {
    api_version: &'static str,
    status: &'static str,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "simulmt_app=info,simulmt_core=info".into()),
        )
        .init();
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
    let manager = Arc::new(
        SessionManager::load(CoreOptions {
            audio_host: audio_host.clone(),
            runtime_dir: runtime.to_string_lossy().into_owned(),
        })
        .await
        .expect("restore local sessions"),
    );
    let state = AppState {
        manager: Arc::clone(&manager),
        audio_host,
        paths: ProjectPaths {
            root: root.clone(),
            demo,
            runtime,
        },
    };
    let app = Router::new()
        .route("/api/health", get(health))
        .route("/api/sources", get(sources))
        .route("/api/media", post(import_media))
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
        .layer(DefaultBodyLimit::max(1024 * 1024 * 1024));
    let address = std::env::var("SIMULMT_ADDR").unwrap_or_else(|_| "127.0.0.1:8780".into());
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .expect("bind local API");
    tracing::info!("SimulMT listening on http://{address}");
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

async fn health() -> Json<Health> {
    Json(Health {
        api_version: API_VERSION,
        status: "ok",
    })
}

async fn sources(State(state): State<AppState>) -> Result<Json<Vec<SourceInfo>>, ApiError> {
    Ok(Json(list_macos_sources(&state.audio_host).await?))
}

#[derive(Deserialize)]
struct MediaUploadQuery {
    name: Option<String>,
}

#[derive(Serialize)]
struct ImportedMedia {
    name: String,
    path: String,
}

async fn import_media(
    State(state): State<AppState>,
    Query(query): Query<MediaUploadQuery>,
    body: Bytes,
) -> Result<Json<ImportedMedia>, ApiError> {
    if body.is_empty() {
        return Err(CoreError::InvalidConfig("the selected media file is empty".into()).into());
    }
    let name = query.name.unwrap_or_else(|| "local-media.wav".into());
    let extension = std::path::Path::new(&name)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("wav")
        .to_ascii_lowercase();
    let supported = ["wav", "mp3", "m4a", "aac", "flac", "mp4", "mov", "m4v"];
    if !supported.contains(&extension.as_str()) {
        return Err(CoreError::InvalidConfig(format!(
            "unsupported media format .{extension}; use WAV, MP3, M4A, AAC, FLAC, MP4, MOV or M4V"
        ))
        .into());
    }
    let directory = state.paths.runtime.join("media");
    tokio::fs::create_dir_all(&directory)
        .await
        .map_err(CoreError::from)?;
    let stem = Uuid::new_v4().to_string();
    let input = directory.join(format!("{stem}-input.{extension}"));
    let output = directory.join(format!("{stem}.wav"));
    tokio::fs::write(&input, body)
        .await
        .map_err(CoreError::from)?;
    let converted = tokio::process::Command::new("/usr/bin/afconvert")
        .arg("-f")
        .arg("WAVE")
        .arg("-d")
        .arg("LEI16@16000")
        .arg("-c")
        .arg("1")
        .arg(&input)
        .arg(&output)
        .output()
        .await
        .map_err(|error| CoreError::Source(format!("failed to start media decoder: {error}")))?;
    if !converted.status.success() {
        let _ = tokio::fs::remove_file(&input).await;
        let _ = tokio::fs::remove_file(&output).await;
        return Err(CoreError::InvalidConfig(format!(
            "cannot decode this media file: {}",
            String::from_utf8_lossy(&converted.stderr).trim()
        ))
        .into());
    }
    Ok(Json(ImportedMedia {
        name,
        path: output.to_string_lossy().into_owned(),
    }))
}

async fn default_config(State(state): State<AppState>) -> Json<PipelineConfig> {
    let demo = &state.paths.demo;
    Json(PipelineConfig {
        source: SourceConfig::MacosSystem {
            exclude_pids: vec![],
            mute_original: true,
        },
        translator: TranslatorConfig::HySimulmt {
            executable: demo
                .join("llama.cpp-hyv3/build/bin/llama-mtmd-cli")
                .to_string_lossy()
                .into_owned(),
            model: demo
                .join("models/Hy-SimulMT-30B-A3B-Interleave-Q4_K_M.gguf")
                .to_string_lossy()
                .into_owned(),
            direction: "en-zh".into(),
            context: Some(16_384),
            n_predict: Some(160),
        },
        output: OutputConfig::MacosSystem {
            player_path: state
                .paths
                .root
                .join("target/native/simulmt-tts-host")
                .to_string_lossy()
                .into_owned(),
            cache_dir: state
                .paths
                .runtime
                .join("cache")
                .to_string_lossy()
                .into_owned(),
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

async fn create_session(
    State(state): State<AppState>,
    Json(config): Json<PipelineConfig>,
) -> Result<Json<SessionSnapshot>, ApiError> {
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

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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
    session
        .set_source(selection.source, selection.mute_original)
        .await?;
    Ok(Json(session.snapshot().await))
}

#[derive(Deserialize)]
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

#[derive(Deserialize)]
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
    let cues = state.manager.get(id).await?.history().await?;
    Ok(download_response(
        "simulmt-subtitles.srt",
        "application/x-subrip; charset=utf-8",
        subtitle_document(&cues, false).into_bytes(),
    ))
}

async fn export_vtt(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let cues = state.manager.get(id).await?.history().await?;
    Ok(download_response(
        "simulmt-subtitles.vtt",
        "text/vtt; charset=utf-8",
        subtitle_document(&cues, true).into_bytes(),
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

fn subtitle_document(cues: &[SubtitleCue], vtt: bool) -> String {
    let mut output = if vtt {
        "WEBVTT\n\n".to_string()
    } else {
        String::new()
    };
    for (index, cue) in cues.iter().enumerate() {
        if !vtt {
            output.push_str(&(index + 1).to_string());
            output.push('\n');
        }
        output.push_str(&format!(
            "{} --> {}\n{}\n{}\n\n",
            subtitle_time(cue.source_start_ns, vtt),
            subtitle_time(
                cue.source_end_ns.max(cue.source_start_ns + 400_000_000),
                vtt
            ),
            cue.source_text,
            cue.translated_text
        ));
    }
    output
}

fn subtitle_time(ns: i64, vtt: bool) -> String {
    let total_ms = (ns.max(0) / 1_000_000) as u64;
    let hours = total_ms / 3_600_000;
    let minutes = total_ms / 60_000 % 60;
    let seconds = total_ms / 1_000 % 60;
    let millis = total_ms % 1_000;
    let separator = if vtt { '.' } else { ',' };
    format!("{hours:02}:{minutes:02}:{seconds:02}{separator}{millis:03}")
}

async fn websocket(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let session = state.manager.get(id).await?;
    Ok(ws.on_upgrade(move |socket| stream_events(socket, session.subscribe())))
}

async fn stream_events(
    mut socket: WebSocket,
    mut events: tokio::sync::broadcast::Receiver<SessionEvent>,
) {
    loop {
        match events.recv().await {
            Ok(event) => {
                let text = serde_json::to_string(&event).expect("serialize event");
                if socket.send(Message::Text(text.into())).await.is_err() {
                    break;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!("UI event receiver lagged by {skipped} events");
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

struct ApiError(CoreError);

impl From<CoreError> for ApiError {
    fn from(value: CoreError) -> Self {
        Self(value)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.0 {
            CoreError::SessionNotFound(_) => StatusCode::NOT_FOUND,
            CoreError::InvalidState(_) | CoreError::InvalidConfig(_) => StatusCode::BAD_REQUEST,
            CoreError::PermissionDenied(_) => StatusCode::FORBIDDEN,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let error = ErrorInfo {
            code: "core_error".into(),
            component_id: "core".into(),
            message: self.0.to_string(),
            retryable: false,
            fatal: false,
        };
        (status, Json(error)).into_response()
    }
}
