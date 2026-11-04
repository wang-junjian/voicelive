use crate::error::{CoreError, Result};
use crate::output::{OutputRunner, OutputSignal, PlaybackControl};
use crate::source::RunningSource;
use crate::source::{SourceEvent, SourceStartOptions, start_source};
use crate::time::monotonic_ns;
use crate::translator::{RunningTranslator, TranslatorSignal, start_translator};
use simulmt_api::*;
use simulmt_policy::{AudioChunker, SubtitleAssembler};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, RwLock, broadcast, mpsc};
use tokio::task::JoinHandle;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct CoreOptions {
    pub audio_host: String,
    pub runtime_dir: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SessionMetadata {
    title: String,
    created_at_ms: i64,
    started_at_ms: Option<i64>,
    ended_at_ms: Option<i64>,
    #[serde(default)]
    audio_duration_us: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredSession {
    id: Uuid,
    config: PipelineConfig,
    metadata: SessionMetadata,
}

pub struct SessionManager {
    options: CoreOptions,
    sessions: Mutex<HashMap<Uuid, Arc<Session>>>,
    model_gate: Mutex<()>,
}

impl SessionManager {
    pub fn new(options: CoreOptions) -> Self {
        Self {
            options,
            sessions: Mutex::new(HashMap::new()),
            model_gate: Mutex::new(()),
        }
    }

    /// Restores session metadata and configuration, never live capture or model processes.
    pub async fn load(options: CoreOptions) -> Result<Self> {
        let manager = Self::new(options.clone());
        let directory = PathBuf::from(&options.runtime_dir).join("sessions");
        let mut entries = match tokio::fs::read_dir(&directory).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(manager),
            Err(error) => return Err(error.into()),
        };
        let mut restored = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            if entry.path().extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            match tokio::fs::read_to_string(entry.path()).await {
                Ok(contents) => match serde_json::from_str::<StoredSession>(&contents) {
                    Ok(stored) => restored.push(Session::restore(stored, options.clone())),
                    Err(error) => tracing::warn!("ignored invalid session manifest: {error}"),
                },
                Err(error) => tracing::warn!("failed to read session manifest: {error}"),
            }
        }
        restored.sort_by_key(|session| session.metadata_created_at());
        let mut sessions = manager.sessions.lock().await;
        for session in restored {
            sessions.insert(session.id, Arc::new(session));
        }
        drop(sessions);
        Ok(manager)
    }

    pub async fn create(&self, config: PipelineConfig) -> Arc<Session> {
        // Only one live tap may mute the original audio at a time.
        for existing in self.list().await {
            if *existing.state.lock().await == SessionState::Preparing {
                let _ = existing.stop_model().await;
            } else {
                let _ = existing.stop().await;
            }
        }
        let session = Arc::new(Session::new(config, self.options.clone()));
        session.persist().await;
        self.sessions
            .lock()
            .await
            .insert(session.id, Arc::clone(&session));
        session
    }

    pub async fn prepare(&self, id: Uuid) -> Result<Arc<Session>> {
        let _gate = self.model_gate.lock().await;
        let session = self.get(id).await?;
        let state = *session.state.lock().await;
        if matches!(state, SessionState::Ready | SessionState::Preparing) {
            return Ok(session);
        }

        // Move the resident process between sessions instead of unloading and
        // reloading identical weights. Only capture and playback stay session-local.
        for existing in self.list().await {
            if existing.id == id {
                continue;
            }
            let state = *existing.state.lock().await;
            if matches!(state, SessionState::Running | SessionState::Paused) {
                existing.stop().await?;
            }
            if let Some(translator) = existing.detach_translator().await? {
                session.adopt_translator(translator).await?;
                return Ok(session);
            }
        }
        session.prepare().await?;
        Ok(session)
    }

    pub async fn get(&self, id: Uuid) -> Result<Arc<Session>> {
        self.sessions
            .lock()
            .await
            .get(&id)
            .cloned()
            .ok_or(CoreError::SessionNotFound(id))
    }

    pub async fn list(&self) -> Vec<Arc<Session>> {
        let mut sessions = self
            .sessions
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        sessions.sort_by_key(|session| std::cmp::Reverse(session.metadata_created_at()));
        sessions
    }

    pub async fn delete(&self, id: Uuid) -> Result<()> {
        let session = self.get(id).await?;
        session.stop_model().await?;
        self.sessions.lock().await.remove(&id);
        session.delete_files().await
    }

    pub async fn stop_all(&self) {
        for session in self.list().await {
            let _ = session.stop_model().await;
        }
    }
}

const SUBTITLE_MEMORY_LIMIT: usize = 512;

struct Runtime {
    translator: Option<Box<dyn RunningTranslator>>,
    output: Option<Box<dyn PlaybackControl>>,
    source: Option<Box<dyn RunningSource>>,
    translator_task: Option<JoinHandle<()>>,
    output_task: Option<JoinHandle<()>>,
    source_task: Option<JoinHandle<()>>,
}

pub struct Session {
    pub id: Uuid,
    config: RwLock<PipelineConfig>,
    metadata: Mutex<SessionMetadata>,
    state: Mutex<SessionState>,
    epoch: AtomicU64,
    metrics: Mutex<SessionMetrics>,
    last_error: Mutex<Option<String>>,
    runtime: Mutex<Option<Runtime>>,
    io_recovery: Mutex<()>,
    subtitles: Mutex<SubtitleAssembler>,
    history: Mutex<VecDeque<SubtitleCue>>,
    events: broadcast::Sender<SessionEvent>,
    options: CoreOptions,
}

impl Session {
    fn new(config: PipelineConfig, options: CoreOptions) -> Self {
        Self::restore(
            StoredSession {
                id: Uuid::new_v4(),
                config,
                metadata: SessionMetadata {
                    title: "未命名同传".into(),
                    created_at_ms: now_ms(),
                    started_at_ms: None,
                    ended_at_ms: None,
                    audio_duration_us: 0,
                },
            },
            options,
        )
    }

    fn restore(stored: StoredSession, options: CoreOptions) -> Self {
        let (events, _) = broadcast::channel(512);
        let audio_duration_us = stored.metadata.audio_duration_us;
        Self {
            id: stored.id,
            config: RwLock::new(stored.config),
            metadata: Mutex::new(stored.metadata),
            state: Mutex::new(SessionState::Idle),
            epoch: AtomicU64::new(1),
            metrics: Mutex::new(SessionMetrics {
                captured_duration_us: audio_duration_us,
                ..SessionMetrics::default()
            }),
            last_error: Mutex::new(None),
            runtime: Mutex::new(None),
            io_recovery: Mutex::new(()),
            subtitles: Mutex::new(SubtitleAssembler::default()),
            history: Mutex::new(VecDeque::new()),
            events,
            options,
        }
    }

    fn metadata_created_at(&self) -> i64 {
        self.metadata
            .try_lock()
            .map(|metadata| metadata.created_at_ms)
            .unwrap_or(0)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<SessionEvent> {
        self.events.subscribe()
    }

    pub async fn snapshot(&self) -> SessionSnapshot {
        let metadata = self.metadata.lock().await.clone();
        SessionSnapshot {
            api_version: API_VERSION.into(),
            session_id: self.id,
            epoch: self.epoch.load(Ordering::Relaxed),
            state: *self.state.lock().await,
            title: metadata.title,
            created_at_ms: metadata.created_at_ms,
            started_at_ms: metadata.started_at_ms,
            ended_at_ms: metadata.ended_at_ms,
            config: self.config.read().await.clone(),
            metrics: self.metrics.lock().await.clone(),
            last_error: self.last_error.lock().await.clone(),
        }
    }

    fn event(&self, kind: SessionEventKind) -> SessionEvent {
        SessionEvent {
            api_version: API_VERSION.into(),
            session_id: self.id,
            epoch: self.epoch.load(Ordering::Relaxed),
            emitted_mono_ns: monotonic_ns(),
            kind,
        }
    }

    fn emit(&self, kind: SessionEventKind) {
        let _ = self.events.send(self.event(kind));
    }

    async fn set_state(&self, state: SessionState, message: Option<String>) {
        *self.state.lock().await = state;
        self.emit(SessionEventKind::State { state, message });
    }

    async fn fault(&self, code: &str, message: String, fatal: bool) {
        *self.last_error.lock().await = Some(message.clone());
        if fatal {
            self.set_state(SessionState::Faulted, Some(message.clone()))
                .await;
        }
        self.emit(SessionEventKind::Error {
            code: code.into(),
            message,
            fatal,
        });
    }

    async fn detach_translator(self: &Arc<Self>) -> Result<Option<Box<dyn RunningTranslator>>> {
        if *self.state.lock().await != SessionState::Ready {
            return Ok(None);
        }
        let (translator, task) = {
            let mut guard = self.runtime.lock().await;
            let Some(runtime) = guard.as_mut() else {
                return Ok(None);
            };
            (runtime.translator.take(), runtime.translator_task.take())
        };
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
        if translator.is_some() {
            self.set_state(
                SessionState::Idle,
                Some("resident model moved to another session".into()),
            )
            .await;
        }
        Ok(translator)
    }

    async fn adopt_translator(
        self: &Arc<Self>,
        mut translator: Box<dyn RunningTranslator>,
    ) -> Result<()> {
        let state = *self.state.lock().await;
        if !matches!(state, SessionState::Idle | SessionState::Faulted) {
            return Err(CoreError::InvalidState(format!(
                "cannot attach a model while {state:?}"
            )));
        }
        if self.runtime.lock().await.is_some() {
            self.stop_model().await?;
        }
        self.set_state(
            SessionState::Preparing,
            Some("attaching resident translator".into()),
        )
        .await;
        let config = self.config.read().await.clone();
        let direction = match config.translator {
            TranslatorConfig::HySimulmt { direction, .. } => Some(direction),
            TranslatorConfig::Mock => None,
        };
        let epoch = self.epoch.load(Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(256);
        let attached = async {
            translator.reattach(self.id, epoch, sender).await?;
            if let Some(direction) = direction {
                translator.set_direction(&direction).await?;
            }
            Ok::<(), CoreError>(())
        }
        .await;
        if let Err(error) = attached {
            let _ = translator.stop().await;
            self.fault("model_not_ready", error.to_string(), true).await;
            return Err(error);
        }
        *self.runtime.lock().await = Some(Runtime {
            translator: Some(translator),
            output: None,
            source: None,
            translator_task: None,
            output_task: None,
            source_task: None,
        });
        let weak = Arc::downgrade(self);
        let task = tokio::spawn(forward_translator(weak, receiver));
        if let Some(runtime) = self.runtime.lock().await.as_mut() {
            runtime.translator_task = Some(task);
        }
        *self.last_error.lock().await = None;
        self.set_state(
            SessionState::Ready,
            Some("resident model attached without reloading".into()),
        )
        .await;
        Ok(())
    }

    pub async fn prepare(self: &Arc<Self>) -> Result<()> {
        let state = *self.state.lock().await;
        if state == SessionState::Ready || state == SessionState::Preparing {
            return Ok(());
        }
        if state != SessionState::Idle && state != SessionState::Faulted {
            return Err(CoreError::InvalidState(format!(
                "cannot prepare while {state:?}"
            )));
        }
        if self.runtime.lock().await.is_some() {
            self.stop().await?;
        }
        self.set_state(SessionState::Preparing, Some("loading translator".into()))
            .await;
        let config = self.config.read().await.clone();
        let epoch = self.epoch.load(Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(256);
        let translator = match start_translator(
            &config.translator,
            self.id,
            epoch,
            sender,
            &self.options.runtime_dir,
        )
        .await
        {
            Ok(translator) => translator,
            Err(error) => {
                self.fault("model_not_ready", error.to_string(), true).await;
                return Err(error);
            }
        };
        {
            let mut guard = self.runtime.lock().await;
            *guard = Some(Runtime {
                translator: Some(translator),
                output: None,
                source: None,
                translator_task: None,
                output_task: None,
                source_task: None,
            });
        }
        let weak = Arc::downgrade(self);
        let task = tokio::spawn(async move {
            forward_translator(weak, receiver).await;
        });
        if let Some(runtime) = self.runtime.lock().await.as_mut() {
            runtime.translator_task = Some(task);
        }
        Ok(())
    }

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        let state = *self.state.lock().await;
        if state != SessionState::Ready {
            return Err(CoreError::InvalidState(format!(
                "cannot start while {state:?}; prepare and wait for ready"
            )));
        }
        let config = self.config.read().await.clone();
        let epoch = self.epoch.load(Ordering::Relaxed);
        let (output_sender, output_receiver) = mpsc::channel(256);
        let mut runtime_guard = self.runtime.lock().await;
        let runtime = runtime_guard
            .as_mut()
            .ok_or_else(|| CoreError::InvalidState("runtime missing".into()))?;
        let output = match OutputRunner::start(
            &config.output,
            config.latency_mode,
            self.id,
            epoch,
            output_sender,
        )
        .await
        {
            Ok(output) => output,
            Err(error) => {
                drop(runtime_guard);
                self.fault("output_unavailable", error.to_string(), true)
                    .await;
                return Err(error);
            }
        };
        let playback_pid = output.playback_pid();
        runtime.output = Some(output);
        let (source_sender, source_receiver) = mpsc::channel(config.queue.max_frames.max(2));
        let source = match start_source(
            &config.source,
            &SourceStartOptions {
                audio_host: self.options.audio_host.clone(),
                playback_pid,
            },
            self.id,
            epoch,
            source_sender,
        )
        .await
        {
            Ok(source) => source,
            Err(error) => {
                if let Some(output) = runtime.output.as_mut() {
                    let _ = output.stop().await;
                }
                runtime.output = None;
                drop(runtime_guard);
                self.fault("source_unavailable", error.to_string(), true)
                    .await;
                return Err(error);
            }
        };
        runtime.source = Some(source);
        drop(runtime_guard);

        let weak = Arc::downgrade(self);
        let output_task = tokio::spawn(forward_output(weak, output_receiver));
        let weak = Arc::downgrade(self);
        let source_task = tokio::spawn(forward_source(weak, source_receiver, config.queue));
        if let Some(runtime) = self.runtime.lock().await.as_mut() {
            runtime.output_task = Some(output_task);
            runtime.source_task = Some(source_task);
        }
        {
            let mut metadata = self.metadata.lock().await;
            if metadata.started_at_ms.is_none() {
                metadata.started_at_ms = Some(now_ms());
            }
            metadata.ended_at_ms = None;
        }
        self.persist().await;
        self.set_state(SessionState::Running, Some("capturing".into()))
            .await;
        Ok(())
    }

    pub async fn pause(&self) -> Result<()> {
        if *self.state.lock().await != SessionState::Running {
            return Err(CoreError::InvalidState(
                "only a running session can pause".into(),
            ));
        }
        self.set_state(
            SessionState::Paused,
            Some("live input is discarded while paused".into()),
        )
        .await;
        let mut runtime = self.runtime.lock().await;
        if let Some(output) = runtime.as_mut().and_then(|r| r.output.as_mut()) {
            output.pause().await?;
        }
        Ok(())
    }

    pub async fn resume(&self) -> Result<()> {
        if *self.state.lock().await != SessionState::Paused {
            return Err(CoreError::InvalidState(
                "only a paused session can resume".into(),
            ));
        }
        let mut runtime = self.runtime.lock().await;
        if let Some(output) = runtime.as_mut().and_then(|r| r.output.as_mut()) {
            output.resume().await?;
        }
        drop(runtime);
        self.set_state(
            SessionState::Running,
            Some("capturing from the live edge".into()),
        )
        .await;
        Ok(())
    }

    /// Stops capture and playback while keeping the expensive model process warm.
    pub async fn stop(&self) -> Result<()> {
        let state = *self.state.lock().await;
        if state == SessionState::Idle || state == SessionState::Stopping {
            return Ok(());
        }
        self.set_state(SessionState::Stopping, None).await;
        let (mut source, mut output, source_task, output_task, has_translator) = {
            let mut guard = self.runtime.lock().await;
            let has_translator = guard
                .as_ref()
                .is_some_and(|runtime| runtime.translator.is_some());
            if let Some(runtime) = guard.as_mut() {
                (
                    runtime.source.take(),
                    runtime.output.take(),
                    runtime.source_task.take(),
                    runtime.output_task.take(),
                    has_translator,
                )
            } else {
                (None, None, None, None, has_translator)
            }
        };
        if let Some(task) = source_task {
            task.abort();
            let _ = task.await;
        }
        if let Some(task) = output_task {
            task.abort();
            let _ = task.await;
        }
        if let Some(source) = source.as_mut() {
            source.stop().await?;
        }
        if let Some(output) = output.as_mut() {
            output.stop().await?;
        }
        self.flush_subtitle().await;
        if matches!(state, SessionState::Running | SessionState::Paused) {
            self.metadata.lock().await.ended_at_ms = Some(now_ms());
            self.persist().await;
        }
        let next_state = if has_translator {
            SessionState::Ready
        } else {
            SessionState::Idle
        };
        self.set_state(
            next_state,
            Some("capture stopped; original audio restored; model kept warm".into()),
        )
        .await;
        Ok(())
    }

    /// Stops the complete pipeline and unloads the model.
    pub async fn stop_model(&self) -> Result<()> {
        let state = *self.state.lock().await;
        if state == SessionState::Stopping {
            return Ok(());
        }
        if matches!(state, SessionState::Running | SessionState::Paused) {
            self.metadata.lock().await.ended_at_ms = Some(now_ms());
            self.persist().await;
        }
        self.set_state(SessionState::Stopping, None).await;
        self.epoch.fetch_add(1, Ordering::Relaxed);
        let runtime = self.runtime.lock().await.take();
        if let Some(mut runtime) = runtime {
            for task in [
                runtime.source_task.take(),
                runtime.output_task.take(),
                runtime.translator_task.take(),
            ]
            .into_iter()
            .flatten()
            {
                task.abort();
                let _ = task.await;
            }
            if let Some(source) = runtime.source.as_mut() {
                let _ = source.stop().await;
            }
            if let Some(output) = runtime.output.as_mut() {
                let _ = output.stop().await;
            }
            if let Some(translator) = runtime.translator.as_mut() {
                let _ = translator.stop().await;
            }
        }
        self.flush_subtitle().await;
        self.set_state(
            SessionState::Idle,
            Some("model stopped; original audio restored".into()),
        )
        .await;
        Ok(())
    }

    pub async fn set_source(
        self: &Arc<Self>,
        source: SourceConfig,
        mute_original: bool,
    ) -> Result<()> {
        let state = *self.state.lock().await;
        if !matches!(
            state,
            SessionState::Idle | SessionState::Ready | SessionState::Running | SessionState::Paused
        ) {
            return Err(CoreError::InvalidState(format!(
                "cannot change source while {state:?}"
            )));
        }
        {
            let mut config = self.config.write().await;
            config.source = source.clone();
            config.mute_original = mute_original;
        }
        self.persist().await;
        if !matches!(state, SessionState::Running | SessionState::Paused) {
            return Ok(());
        }

        let (mut old_source, old_task, playback_pid, queue) = {
            let mut guard = self.runtime.lock().await;
            let runtime = guard
                .as_mut()
                .ok_or_else(|| CoreError::InvalidState("runtime missing".into()))?;
            (
                runtime.source.take(),
                runtime.source_task.take(),
                runtime
                    .output
                    .as_ref()
                    .and_then(|output| output.playback_pid()),
                self.config.read().await.queue.clone(),
            )
        };
        if let Some(task) = old_task {
            task.abort();
            let _ = task.await;
        }
        if let Some(source) = old_source.as_mut() {
            source.stop().await?;
        }
        self.flush_subtitle().await;
        {
            let mut guard = self.runtime.lock().await;
            if let Some(runtime) = guard.as_mut() {
                if let Some(translator) = runtime.translator.as_mut() {
                    translator.reset_source().await?;
                }
                if let Some(output) = runtime.output.as_mut() {
                    output.flush().await?;
                }
            }
        }

        let epoch = self.epoch.load(Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(queue.max_frames.max(2));
        let new_source = match start_source(
            &source,
            &SourceStartOptions {
                audio_host: self.options.audio_host.clone(),
                playback_pid,
            },
            self.id,
            epoch,
            sender,
        )
        .await
        {
            Ok(source) => source,
            Err(error) => {
                self.fault("source_unavailable", error.to_string(), true)
                    .await;
                return Err(error);
            }
        };
        let weak = Arc::downgrade(self);
        let task = tokio::spawn(forward_source(weak, receiver, queue));
        let mut guard = self.runtime.lock().await;
        if let Some(runtime) = guard.as_mut() {
            runtime.source = Some(new_source);
            runtime.source_task = Some(task);
        }
        self.emit(SessionEventKind::State {
            state,
            message: Some("audio source switched".into()),
        });
        Ok(())
    }

    fn recover_output(
        self: Arc<Self>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
        Box::pin(async move {
            let _recovery = self.io_recovery.lock().await;
            let state = *self.state.lock().await;
            if !matches!(state, SessionState::Running | SessionState::Paused) {
                return Ok(());
            }
            let config = self.config.read().await.clone();
            let epoch = self.epoch.load(Ordering::Relaxed);
            let (mut old_source, mut old_output, source_task, output_task) = {
                let mut guard = self.runtime.lock().await;
                let Some(runtime) = guard.as_mut() else {
                    return Ok(());
                };
                (
                    runtime.source.take(),
                    runtime.output.take(),
                    runtime.source_task.take(),
                    runtime.output_task.take(),
                )
            };
            for task in [source_task, output_task].into_iter().flatten() {
                task.abort();
                let _ = task.await;
            }
            if let Some(source) = old_source.as_mut() {
                let _ = source.stop().await;
            }
            if let Some(output) = old_output.as_mut() {
                let _ = output.stop().await;
            }

            let (output_sender, output_receiver) = mpsc::channel(256);
            let mut output = match OutputRunner::start(
                &config.output,
                config.latency_mode,
                self.id,
                epoch,
                output_sender,
            )
            .await
            {
                Ok(output) => output,
                Err(error) => {
                    self.fault("output_recovery", error.to_string(), true).await;
                    return Err(error);
                }
            };
            let playback_pid = output.playback_pid();
            let (source_sender, source_receiver) = mpsc::channel(config.queue.max_frames.max(2));
            let source = match start_source(
                &config.source,
                &SourceStartOptions {
                    audio_host: self.options.audio_host.clone(),
                    playback_pid,
                },
                self.id,
                epoch,
                source_sender,
            )
            .await
            {
                Ok(source) => source,
                Err(error) => {
                    let _ = output.stop().await;
                    self.fault("source_recovery", error.to_string(), true).await;
                    return Err(error);
                }
            };
            let output_task = tokio::spawn(forward_output(Arc::downgrade(&self), output_receiver));
            let source_task = tokio::spawn(forward_source(
                Arc::downgrade(&self),
                source_receiver,
                config.queue,
            ));
            if let Some(runtime) = self.runtime.lock().await.as_mut() {
                runtime.output = Some(output);
                runtime.source = Some(source);
                runtime.output_task = Some(output_task);
                runtime.source_task = Some(source_task);
            }
            self.emit(SessionEventKind::Warning {
                code: "output_recovered".into(),
                message: "audio input and output recovered; translator kept warm".into(),
            });
            Ok(())
        })
    }

    fn manifest_path(&self) -> PathBuf {
        PathBuf::from(&self.options.runtime_dir)
            .join("sessions")
            .join(format!("{}.json", self.id))
    }

    async fn persist(&self) {
        let audio_duration_us = self.metrics.lock().await.captured_duration_us;
        self.metadata.lock().await.audio_duration_us = audio_duration_us;
        let stored = StoredSession {
            id: self.id,
            config: self.config.read().await.clone(),
            metadata: self.metadata.lock().await.clone(),
        };
        let result = async {
            let path = self.manifest_path();
            tokio::fs::create_dir_all(path.parent().expect("session directory")).await?;
            let temporary = path.with_extension("json.tmp");
            let contents = serde_json::to_vec_pretty(&stored)?;
            tokio::fs::write(&temporary, contents).await?;
            tokio::fs::rename(temporary, path).await?;
            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
        if let Err(error) = result {
            tracing::warn!(session_id = %self.id, "failed to persist session: {error}");
        }
    }

    pub async fn rename(&self, title: String) -> Result<()> {
        let title = title.trim();
        if title.is_empty() || title.chars().count() > 80 {
            return Err(CoreError::InvalidConfig(
                "session title must contain 1 to 80 characters".into(),
            ));
        }
        self.metadata.lock().await.title = title.to_string();
        self.persist().await;
        self.emit(SessionEventKind::State {
            state: *self.state.lock().await,
            message: Some("session renamed".into()),
        });
        Ok(())
    }

    async fn delete_files(&self) -> Result<()> {
        for path in [self.manifest_path(), self.history_path()] {
            match tokio::fs::remove_file(path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        let cache = match &*self.config.read().await {
            PipelineConfig {
                output: OutputConfig::MacosSystem { cache_dir, .. },
                ..
            } => PathBuf::from(cache_dir),
            _ => return Ok(()),
        };
        let mut entries = match tokio::fs::read_dir(cache).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let marker = self.id.to_string();
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_name().to_string_lossy().contains(&marker) {
                let _ = tokio::fs::remove_file(entry.path()).await;
            }
        }
        Ok(())
    }

    fn history_path(&self) -> PathBuf {
        PathBuf::from(&self.options.runtime_dir)
            .join("subtitles")
            .join(format!("{}.jsonl", self.id))
    }

    pub async fn history(&self) -> Vec<SubtitleCue> {
        if let Ok(contents) = tokio::fs::read_to_string(self.history_path()).await {
            if let Some(history) = fold_subtitle_history(&contents) {
                return history;
            }
        }
        self.history.lock().await.iter().cloned().collect()
    }

    pub async fn clear_history(&self) {
        self.history.lock().await.clear();
        *self.subtitles.lock().await = SubtitleAssembler::default();
        if let Err(error) = tokio::fs::remove_file(self.history_path()).await {
            if error.kind() != std::io::ErrorKind::NotFound {
                self.emit(SessionEventKind::Warning {
                    code: "subtitle_storage".into(),
                    message: format!("failed to remove subtitle history: {error}"),
                });
            }
        }
        self.emit(SessionEventKind::HistoryCleared);
    }

    async fn record_subtitle(&self, cue: SubtitleCue) {
        {
            let mut history = self.history.lock().await;
            if let Some(existing) = history.iter_mut().find(|item| item.cue_id == cue.cue_id) {
                *existing = cue.clone();
            } else {
                history.push_back(cue.clone());
                while history.len() > SUBTITLE_MEMORY_LIMIT {
                    history.pop_front();
                }
            }
        }
        let persisted = async {
            let path = self.history_path();
            tokio::fs::create_dir_all(path.parent().expect("subtitle directory"))
                .await
                .map_err(|error| error.to_string())?;
            let mut file = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .await
                .map_err(|error| error.to_string())?;
            let line = serde_json::to_string(&cue).map_err(|error| error.to_string())?;
            file.write_all(format!("{line}\n").as_bytes())
                .await
                .map_err(|error| error.to_string())
        }
        .await;
        if let Err(message) = persisted {
            self.emit(SessionEventKind::Warning {
                code: "subtitle_storage".into(),
                message: format!("subtitle history is memory-only: {message}"),
            });
        }
        self.emit(SessionEventKind::Subtitle(cue));
    }

    async fn flush_subtitle(&self) {
        let cue = self.subtitles.lock().await.flush();
        if let Some(cue) = cue {
            self.record_subtitle(cue).await;
        }
    }

    pub async fn set_direction(&self, direction: String) -> Result<()> {
        let state = *self.state.lock().await;
        if matches!(state, SessionState::Stopping | SessionState::Preparing) {
            return Err(CoreError::InvalidState(format!(
                "cannot change translation direction while {state:?}"
            )));
        }
        if !matches!(direction.as_str(), "en-zh" | "zh-en") {
            return Err(CoreError::InvalidConfig(
                "unsupported translation direction".into(),
            ));
        }
        let voice = if direction == "en-zh" {
            "Tingting"
        } else {
            "Ava (Enhanced)"
        };
        {
            let mut config = self.config.write().await;
            match &mut config.translator {
                TranslatorConfig::HySimulmt {
                    direction: current, ..
                } => *current = direction.clone(),
                TranslatorConfig::Mock => {}
            }
            match &mut config.output {
                OutputConfig::MacosSystem { voice: current, .. } => {
                    *current = Some(voice.to_string())
                }
                OutputConfig::Mock => {}
            }
        }
        let mut runtime = self.runtime.lock().await;
        if let Some(runtime) = runtime.as_mut() {
            if let Some(output) = runtime.output.as_mut() {
                output.flush().await?;
                output.set_voice(voice.to_string()).await?;
            }
            if let Some(translator) = runtime.translator.as_mut() {
                translator.set_direction(&direction).await?;
            }
        }
        drop(runtime);
        self.persist().await;
        self.emit(SessionEventKind::State {
            state,
            message: Some("translation direction and language context updated".into()),
        });
        Ok(())
    }

    pub async fn set_latency_mode(&self, mode: LatencyMode) -> Result<()> {
        let state = *self.state.lock().await;
        if matches!(state, SessionState::Stopping | SessionState::Preparing) {
            return Err(CoreError::InvalidState(format!(
                "cannot change voice policy while {state:?}"
            )));
        }
        self.config.write().await.latency_mode = mode;
        let mut runtime = self.runtime.lock().await;
        if let Some(output) = runtime.as_mut().and_then(|runtime| runtime.output.as_mut()) {
            output.set_latency_mode(mode).await?;
        }
        drop(runtime);
        self.persist().await;
        self.emit(SessionEventKind::State {
            state,
            message: Some("voice policy updated".into()),
        });
        Ok(())
    }

    pub async fn reconfigure(self: &Arc<Self>, config: PipelineConfig) -> Result<()> {
        let state = *self.state.lock().await;
        if state != SessionState::Idle && state != SessionState::Faulted {
            return Err(CoreError::InvalidState(
                "stop the session before reconfiguring".into(),
            ));
        }
        if state == SessionState::Faulted {
            self.stop_model().await?;
        }
        *self.config.write().await = config;
        *self.metrics.lock().await = SessionMetrics::default();
        *self.last_error.lock().await = None;
        self.persist().await;
        self.emit(SessionEventKind::State {
            state: SessionState::Idle,
            message: Some("configuration updated".into()),
        });
        Ok(())
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn fold_subtitle_history(contents: &str) -> Option<Vec<SubtitleCue>> {
    let mut history = Vec::<SubtitleCue>::new();
    let mut positions = HashMap::<String, usize>::new();
    for line in contents.lines().filter(|line| !line.trim().is_empty()) {
        let cue = serde_json::from_str::<SubtitleCue>(line).ok()?;
        if let Some(position) = positions.get(&cue.cue_id).copied() {
            history[position] = cue;
        } else {
            positions.insert(cue.cue_id.clone(), history.len());
            history.push(cue);
        }
    }
    Some(history)
}

async fn forward_translator(
    weak: std::sync::Weak<Session>,
    mut receiver: mpsc::Receiver<TranslatorSignal>,
) {
    while let Some(signal) = receiver.recv().await {
        let Some(session) = weak.upgrade() else { break };
        match signal {
            TranslatorSignal::Ready => {
                if *session.state.lock().await == SessionState::Preparing {
                    session
                        .set_state(SessionState::Ready, Some("translator ready".into()))
                        .await;
                }
            }
            TranslatorSignal::Translation(event) => {
                if event.epoch != session.epoch.load(Ordering::Relaxed) {
                    continue;
                }
                let state = *session.state.lock().await;
                if !matches!(state, SessionState::Running | SessionState::Paused) {
                    // A model may finish an in-flight turn after capture stops. It must
                    // not mutate a closed timeline or leak into the next capture run.
                    continue;
                }
                session.metrics.lock().await.translations += 1;
                session.emit(SessionEventKind::Translation(event.clone()));
                let cue = session.subtitles.lock().await.push(&event);
                session.record_subtitle(cue).await;
                if event.speakable {
                    let mut runtime = session.runtime.lock().await;
                    if let Some(output) = runtime.as_mut().and_then(|r| r.output.as_mut()) {
                        if let Err(error) = output.submit(event).await {
                            session.emit(SessionEventKind::Warning {
                                code: "tts_queue".into(),
                                message: error.to_string(),
                            });
                        }
                    }
                }
            }
            TranslatorSignal::Completed {
                duration_us,
                context_position,
            } => {
                let mut metrics = session.metrics.lock().await;
                metrics.completed_duration_us += duration_us as u64;
                metrics.context_position = context_position;
                metrics.model_backlog_us = metrics
                    .sent_duration_us
                    .saturating_sub(metrics.completed_duration_us);
                let snapshot = metrics.clone();
                drop(metrics);
                session.emit(SessionEventKind::Metrics(snapshot));
            }
            TranslatorSignal::ContextRotated => {
                let snapshot = {
                    let mut metrics = session.metrics.lock().await;
                    metrics.context_rotations += 1;
                    metrics.clone()
                };
                session.emit(SessionEventKind::Metrics(snapshot));
            }
            TranslatorSignal::ModelRestarted => {
                let snapshot = {
                    let mut metrics = session.metrics.lock().await;
                    metrics.model_restarts += 1;
                    metrics.clone()
                };
                session.emit(SessionEventKind::Metrics(snapshot));
            }
            TranslatorSignal::SourceReset => {
                let snapshot = {
                    let mut metrics = session.metrics.lock().await;
                    metrics.source_resets += 1;
                    metrics.clone()
                };
                session.emit(SessionEventKind::Metrics(snapshot));
            }
            TranslatorSignal::Warning(message) => {
                session.emit(SessionEventKind::Warning {
                    code: "translator".into(),
                    message,
                });
            }
        }
    }
}

async fn forward_output(
    weak: std::sync::Weak<Session>,
    mut receiver: mpsc::Receiver<OutputSignal>,
) {
    while let Some(signal) = receiver.recv().await {
        let Some(session) = weak.upgrade() else { break };
        match signal {
            OutputSignal::Playback(event) => {
                if event.token != 0 {
                    let snapshot = {
                        let mut metrics = session.metrics.lock().await;
                        match event.event_type {
                            PlaybackEventType::Started => {
                                metrics.playback_started += 1;
                                metrics.output_latency_us = event.queue_duration_us;
                                metrics.max_output_latency_us =
                                    metrics.max_output_latency_us.max(event.queue_duration_us);
                                if let Some(source_end_ns) = event.source_end_ns {
                                    let source_end_us = source_end_ns.max(0) as u64 / 1_000;
                                    metrics.playback_lag_us =
                                        metrics.captured_duration_us.saturating_sub(source_end_us);
                                    metrics.max_playback_lag_us =
                                        metrics.max_playback_lag_us.max(metrics.playback_lag_us);
                                }
                            }
                            PlaybackEventType::Finished => metrics.playback_finished += 1,
                            PlaybackEventType::Cancelled => metrics.playback_cancelled += 1,
                            _ => {}
                        }
                        metrics.clone()
                    };
                    session.emit(SessionEventKind::Metrics(snapshot));
                    session.emit(SessionEventKind::Playback(event));
                }
            }
            OutputSignal::QueueDepth(depth) => {
                let snapshot = {
                    let mut metrics = session.metrics.lock().await;
                    metrics.tts_queue_depth = depth;
                    metrics.clone()
                };
                session.emit(SessionEventKind::Metrics(snapshot));
            }
            OutputSignal::Warning(message) => {
                session.emit(SessionEventKind::Warning {
                    code: "output".into(),
                    message,
                });
            }
            OutputSignal::Error(message) => {
                session.emit(SessionEventKind::Warning {
                    code: "output".into(),
                    message,
                });
            }
            OutputSignal::Exited => {
                tokio::spawn(async move {
                    let _ = session.recover_output().await;
                });
                break;
            }
        }
    }
}

async fn forward_source(
    weak: std::sync::Weak<Session>,
    mut receiver: mpsc::Receiver<SourceEvent>,
    queue_policy: QueuePolicy,
) {
    let mut chunker = AudioChunker::default();
    while let Some(source_event) = receiver.recv().await {
        let Some(session) = weak.upgrade() else { break };
        match source_event {
            SourceEvent::Frame(frame) => {
                if frame.epoch != session.epoch.load(Ordering::Relaxed) {
                    continue;
                }
                if *session.state.lock().await != SessionState::Running {
                    let mut metrics = session.metrics.lock().await;
                    metrics.dropped_frames += 1;
                    metrics.dropped_duration_us += frame.duration_us as u64;
                    continue;
                }
                {
                    let mut metrics = session.metrics.lock().await;
                    metrics.captured_frames += 1;
                    metrics.captured_duration_us += frame.duration_us as u64;
                }
                let blocks = match chunker.push(&frame) {
                    Ok(blocks) => blocks,
                    Err(error) => {
                        session
                            .fault("invalid_audio", error.to_string(), true)
                            .await;
                        break;
                    }
                };
                for block in blocks {
                    let backlog = {
                        let metrics = session.metrics.lock().await;
                        metrics
                            .sent_duration_us
                            .saturating_sub(metrics.completed_duration_us)
                    };
                    if backlog > queue_policy.max_model_backlog_us as u64 {
                        let mut metrics = session.metrics.lock().await;
                        metrics.dropped_frames += 1;
                        metrics.dropped_duration_us += block.duration_us as u64;
                        session.emit(SessionEventKind::Warning {
                            code: "backlog_overflow".into(),
                            message: format!("dropped one audio block at {backlog} us backlog"),
                        });
                        continue;
                    }
                    let mut runtime = session.runtime.lock().await;
                    let result = match runtime.as_mut().and_then(|r| r.translator.as_mut()) {
                        Some(translator) => translator.push_audio(block.clone()).await,
                        None => Err(CoreError::InvalidState("translator unavailable".into())),
                    };
                    drop(runtime);
                    match result {
                        Ok(()) => {
                            let mut metrics = session.metrics.lock().await;
                            metrics.sent_frames += 1;
                            metrics.sent_duration_us += block.duration_us as u64;
                            metrics.model_backlog_us = metrics
                                .sent_duration_us
                                .saturating_sub(metrics.completed_duration_us);
                        }
                        Err(error) => {
                            session
                                .fault("translator_write", error.to_string(), true)
                                .await;
                            break;
                        }
                    }
                }
            }
            SourceEvent::Dropped { duration_us } => {
                let mut metrics = session.metrics.lock().await;
                metrics.dropped_frames += 1;
                metrics.dropped_duration_us += duration_us as u64;
            }
            SourceEvent::Ended => {
                let mut runtime = session.runtime.lock().await;
                if let Some(output) = runtime.as_mut().and_then(|r| r.output.as_mut()) {
                    if let Err(error) = output.finish().await {
                        session.emit(SessionEventKind::Warning {
                            code: "tts_finish".into(),
                            message: error.to_string(),
                        });
                    }
                }
                drop(runtime);
                session.emit(SessionEventKind::Warning {
                    code: "source_ended".into(),
                    message: "audio source ended; stop the session to release remaining resources"
                        .into(),
                });
            }
            SourceEvent::Warning(message) => {
                session.emit(SessionEventKind::Warning {
                    code: "source_recovery".into(),
                    message,
                });
            }
            SourceEvent::Error(message) => {
                session.fault("source_lost", message, true).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn wait_for_state(session: &Session, expected: SessionState) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if session.snapshot().await.state == expected {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("state transition timed out");
    }

    #[tokio::test]
    async fn mock_session_covers_complete_lifecycle() {
        let session = Session::new(
            PipelineConfig::default(),
            CoreOptions {
                audio_host: "unused".into(),
                runtime_dir: "/tmp".into(),
            },
        );
        let session = Arc::new(session);
        let mut events = session.subscribe();
        session.prepare().await.unwrap();
        wait_for_state(&session, SessionState::Ready).await;
        session.start().await.unwrap();
        wait_for_state(&session, SessionState::Running).await;
        tokio::time::sleep(std::time::Duration::from_millis(950)).await;
        session.pause().await.unwrap();
        wait_for_state(&session, SessionState::Paused).await;
        session.resume().await.unwrap();
        session
            .set_source(SourceConfig::Mock { frames: Some(30) }, true)
            .await
            .unwrap();
        session.stop().await.unwrap();
        wait_for_state(&session, SessionState::Ready).await;
        let snapshot = session.snapshot().await;
        assert!(snapshot.metrics.translations >= 1);
        assert_eq!(snapshot.epoch, 1);
        assert!(session.history().await.iter().all(|cue| cue.is_final));
        session.start().await.unwrap();
        session.stop().await.unwrap();
        session.stop_model().await.unwrap();
        wait_for_state(&session, SessionState::Idle).await;
        assert!(session.snapshot().await.epoch >= 2);
        let mut saw_translation = false;
        let mut saw_subtitle = false;
        while let Ok(event) = events.try_recv() {
            match event.kind {
                SessionEventKind::Translation(_) => saw_translation = true,
                SessionEventKind::Subtitle(_) => saw_subtitle = true,
                _ => {}
            }
        }
        assert!(saw_translation && saw_subtitle);
    }

    #[tokio::test]
    async fn resident_mock_model_moves_between_sessions() {
        let root = std::env::temp_dir().join(format!("simulmt-model-handoff-{}", Uuid::new_v4()));
        let manager = SessionManager::new(CoreOptions {
            audio_host: "unused".into(),
            runtime_dir: root.to_string_lossy().into_owned(),
        });
        let first = manager.create(PipelineConfig::default()).await;
        manager.prepare(first.id).await.unwrap();
        wait_for_state(&first, SessionState::Ready).await;

        let second = manager.create(PipelineConfig::default()).await;
        manager.prepare(second.id).await.unwrap();
        wait_for_state(&second, SessionState::Ready).await;
        assert_eq!(first.snapshot().await.state, SessionState::Idle);
        assert!(
            first
                .runtime
                .lock()
                .await
                .as_ref()
                .unwrap()
                .translator
                .is_none()
        );
        assert!(
            second
                .runtime
                .lock()
                .await
                .as_ref()
                .unwrap()
                .translator
                .is_some()
        );

        second.start().await.unwrap();
        wait_for_state(&second, SessionState::Running).await;
        second.stop().await.unwrap();
        manager.stop_all().await;
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn subtitle_history_is_bounded_in_memory_and_complete_on_disk() {
        let root = std::env::temp_dir().join(format!("simulmt-history-{}", Uuid::new_v4()));
        let session = Session::new(
            PipelineConfig::default(),
            CoreOptions {
                audio_host: "unused".into(),
                runtime_dir: root.to_string_lossy().into_owned(),
            },
        );
        for index in 0..600 {
            session
                .record_subtitle(SubtitleCue {
                    cue_id: format!("cue-{index}"),
                    revision: 0,
                    source_text: format!("source {index}"),
                    translated_text: format!("translation {index}"),
                    source_start_ns: index * 1_000_000_000,
                    source_end_ns: (index + 1) * 1_000_000_000,
                    is_final: true,
                    segment_ids: vec![format!("segment-{index}")],
                })
                .await;
        }
        let cached = session.history.lock().await;
        assert_eq!(cached.len(), SUBTITLE_MEMORY_LIMIT);
        assert_eq!(cached.front().unwrap().cue_id, "cue-88");
        assert_eq!(cached.back().unwrap().cue_id, "cue-599");
        drop(cached);

        let history = session.history().await;
        assert_eq!(history.len(), 600);
        assert_eq!(history.first().unwrap().cue_id, "cue-0");
        assert_eq!(history.last().unwrap().cue_id, "cue-599");
        assert!(session.history_path().exists());

        session.clear_history().await;
        assert!(session.history().await.is_empty());
        assert!(!session.history_path().exists());
        tokio::fs::remove_dir_all(root).await.unwrap();
    }
}
