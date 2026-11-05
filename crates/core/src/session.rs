use crate::error::{CoreError, Result};
use crate::export::SubtitleExportFormat;
use crate::lifecycle::Lifecycle;
use crate::output::{OutputPaths, OutputRunner, OutputSignal, PlaybackControl};
use crate::source::RunningSource;
use crate::source::{SourceEvent, SourceStartOptions, start_source};
use crate::store::{SessionMetadata, Store, StoredSession};
use crate::time::monotonic_ns;
use crate::translator::{RunningTranslator, TranslatorSignal, start_translator};
use simulmt_api::*;
use simulmt_policy::{AudioChunker, SubtitleAssembler};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, RwLock, broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub struct DeploymentConfig {
    pub model_executable: PathBuf,
    pub model_path: PathBuf,
    pub player_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct CoreOptions {
    pub audio_host: String,
    pub runtime_dir: String,
    pub deployment: DeploymentConfig,
}

impl CoreOptions {
    pub fn cache_dir(&self) -> PathBuf {
        PathBuf::from(&self.runtime_dir).join("cache")
    }
}

pub struct SessionManager {
    options: CoreOptions,
    store: Arc<Store>,
    sessions: Mutex<HashMap<Uuid, Arc<Session>>>,
    model_gate: Mutex<()>,
}

impl SessionManager {
    pub fn new(options: CoreOptions) -> Self {
        let store = Store::open(std::path::Path::new(&options.runtime_dir))
            .expect("open local session database");
        Self {
            options,
            store,
            sessions: Mutex::new(HashMap::new()),
            model_gate: Mutex::new(()),
        }
    }

    /// Restores session metadata and configuration, never live capture or model processes.
    pub async fn load(options: CoreOptions) -> Result<Self> {
        let store = Store::open(std::path::Path::new(&options.runtime_dir))?;
        store
            .migrate_legacy(PathBuf::from(&options.runtime_dir))
            .await?;
        let manager = Self {
            options: options.clone(),
            store: Arc::clone(&store),
            sessions: Mutex::new(HashMap::new()),
            model_gate: Mutex::new(()),
        };
        let stored_sessions = store.load_sessions().await?;
        for stored in &stored_sessions {
            store.recover_speech(stored.id, options.cache_dir()).await?;
        }
        let mut restored = stored_sessions
            .into_iter()
            .map(|stored| Session::restore_with_store(stored, options.clone(), Arc::clone(&store)))
            .collect::<Vec<_>>();
        restored.sort_by_key(|session| session.metadata_created_at());
        let mut sessions = manager.sessions.lock().await;
        for session in restored {
            sessions.insert(session.id, Arc::new(session));
        }
        drop(sessions);
        Ok(manager)
    }

    pub async fn create(&self, config: PipelineConfig) -> Result<Arc<Session>> {
        // Creating a draft has no side effect on an active meeting. Capture ownership
        // changes only when prepare/start explicitly moves the resident model.
        let session = Arc::new(Session::new_with_store(
            config,
            self.options.clone(),
            Arc::clone(&self.store),
        ));
        session.persist().await?;
        self.sessions
            .lock()
            .await
            .insert(session.id, Arc::clone(&session));
        Ok(session)
    }

    pub async fn prepare(&self, id: Uuid) -> Result<Arc<Session>> {
        let _gate = self.model_gate.lock().await;
        let session = self.get(id).await?;
        let state = *session.state.lock().await;
        if state == SessionState::Ready {
            return Ok(session);
        }
        if state == SessionState::Preparing {
            Self::wait_for_model_ready(&session).await?;
            return Ok(session);
        }

        // Move the resident process between sessions instead of unloading and
        // reloading identical weights. Only capture and playback stay session-local.
        for existing in self.list().await {
            if existing.id == id {
                continue;
            }
            let mut state = *existing.state.lock().await;
            if state == SessionState::Preparing {
                if let Err(error) = Self::wait_for_model_ready(&existing).await {
                    let _ = existing.stop_model().await;
                    return Err(error);
                }
                state = SessionState::Ready;
            }
            if state == SessionState::Configuring {
                existing.stop().await?;
            }
            if matches!(state, SessionState::Stopping | SessionState::Faulted) {
                existing.stop_model().await?;
                continue;
            }
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

    async fn wait_for_model_ready(session: &Arc<Session>) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(180), async {
            loop {
                match *session.state.lock().await {
                    SessionState::Ready => return Ok(()),
                    SessionState::Preparing => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    SessionState::Faulted => {
                        let message = session
                            .last_error
                            .lock()
                            .await
                            .clone()
                            .unwrap_or_else(|| "translator failed before ready".into());
                        return Err(CoreError::Translator(message));
                    }
                    state => {
                        return Err(CoreError::InvalidState(format!(
                            "translator cannot become ready while {state:?}"
                        )));
                    }
                }
            }
        })
        .await
        .map_err(|_| CoreError::Translator("translator ready timed out".into()))?
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
        session.delete_files().await?;
        self.sessions.lock().await.remove(&id);
        Ok(())
    }

    pub async fn stop_all(&self) {
        for session in self.list().await {
            if let Err(error) = session.stop_model().await {
                tracing::warn!(session_id = %session.id, "failed to stop session: {error}");
            }
        }
    }
}

const SUBTITLE_MEMORY_LIMIT: usize = 512;
const LIFECYCLE_IO_TIMEOUT: Duration = Duration::from_secs(15);
const RESOURCE_STOP_TIMEOUT: Duration = Duration::from_secs(5);
const PENDING_SPEECH_LIMIT: usize = 32;

struct Runtime {
    translator: Option<Box<dyn RunningTranslator>>,
    output: Option<Box<dyn PlaybackControl>>,
    source: Option<Box<dyn RunningSource>>,
    translator_task: Option<JoinHandle<()>>,
    output_task: Option<JoinHandle<()>>,
    source_task: Option<JoinHandle<()>>,
}

struct PlaybackUpdate {
    pending: u64,
    count_event: bool,
}

#[derive(Default)]
struct PlaybackLedger {
    states: HashMap<u64, PlaybackEventType>,
    terminal_order: VecDeque<u64>,
    pending: u64,
}

impl PlaybackLedger {
    fn apply(&mut self, token: u64, event_type: PlaybackEventType) -> PlaybackUpdate {
        let previous = self.states.get(&token).copied();
        let next = if previous.is_some_and(is_terminal_playback) {
            previous.expect("terminal playback state")
        } else if previous == Some(PlaybackEventType::Started)
            && event_type == PlaybackEventType::Queued
        {
            PlaybackEventType::Started
        } else {
            event_type
        };
        let was_pending = previous.is_some_and(is_pending_playback);
        let is_pending = is_pending_playback(next);
        if was_pending && !is_pending {
            self.pending = self.pending.saturating_sub(1);
        } else if !was_pending && is_pending {
            self.pending += 1;
        }
        let count_event = previous != Some(next) && next == event_type;
        if count_event && is_terminal_playback(next) {
            self.terminal_order.push_back(token);
            while self.terminal_order.len() > 64 {
                if let Some(expired) = self.terminal_order.pop_front() {
                    self.states.remove(&expired);
                }
            }
        }
        self.states.insert(token, next);
        PlaybackUpdate {
            pending: self.pending,
            count_event,
        }
    }

    fn clear(&mut self) {
        self.states.clear();
        self.terminal_order.clear();
        self.pending = 0;
    }
}

fn is_terminal_playback(event_type: PlaybackEventType) -> bool {
    matches!(
        event_type,
        PlaybackEventType::Finished
            | PlaybackEventType::Cancelled
            | PlaybackEventType::Failed
            | PlaybackEventType::Expired
    )
}

fn is_pending_playback(event_type: PlaybackEventType) -> bool {
    !is_terminal_playback(event_type)
}

pub struct Session {
    pub id: Uuid,
    config: RwLock<PipelineConfig>,
    metadata: Mutex<SessionMetadata>,
    state: Mutex<SessionState>,
    epoch: AtomicU64,
    timeline_offset_ns: AtomicU64,
    timeline_offsets: Mutex<HashMap<(u64, u64), i64>>,
    metrics: Mutex<SessionMetrics>,
    playback: Mutex<PlaybackLedger>,
    pending_speech: Mutex<VecDeque<TranslationEvent>>,
    last_error: Mutex<Option<String>>,
    runtime: Mutex<Option<Runtime>>,
    lifecycle: Lifecycle,
    subtitles: Mutex<SubtitleAssembler>,
    subtitle_writes: Mutex<()>,
    history: Mutex<VecDeque<SubtitleCue>>,
    events: broadcast::Sender<SessionEvent>,
    options: CoreOptions,
    store: Arc<Store>,
}

impl Session {
    #[cfg(test)]
    fn new(config: PipelineConfig, options: CoreOptions) -> Self {
        let store = Store::open(std::path::Path::new(&options.runtime_dir))
            .expect("open local session database");
        Self::new_with_store(config, options, store)
    }

    fn new_with_store(config: PipelineConfig, options: CoreOptions, store: Arc<Store>) -> Self {
        Self::restore_with_store(
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
            store,
        )
    }

    #[cfg(test)]
    fn restore(stored: StoredSession, options: CoreOptions) -> Self {
        let store = Store::open(std::path::Path::new(&options.runtime_dir))
            .expect("open local session database");
        Self::restore_with_store(stored, options, store)
    }

    fn restore_with_store(stored: StoredSession, options: CoreOptions, store: Arc<Store>) -> Self {
        let (events, _) = broadcast::channel(512);
        let audio_duration_us = stored.metadata.audio_duration_us;
        Self {
            id: stored.id,
            config: RwLock::new(stored.config),
            metadata: Mutex::new(stored.metadata),
            state: Mutex::new(SessionState::Idle),
            epoch: AtomicU64::new(1),
            timeline_offset_ns: AtomicU64::new(0),
            timeline_offsets: Mutex::new(HashMap::new()),
            metrics: Mutex::new(SessionMetrics {
                captured_duration_us: audio_duration_us,
                ..SessionMetrics::default()
            }),
            playback: Mutex::new(PlaybackLedger::default()),
            pending_speech: Mutex::new(VecDeque::new()),
            last_error: Mutex::new(None),
            runtime: Mutex::new(None),
            lifecycle: Lifecycle::new(),
            subtitles: Mutex::new(SubtitleAssembler::default()),
            subtitle_writes: Mutex::new(()),
            history: Mutex::new(VecDeque::new()),
            events,
            options,
            store,
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

    async fn fault(self: &Arc<Self>, code: &str, message: String, fatal: bool) {
        *self.last_error.lock().await = Some(message.clone());
        let previous_state = *self.state.lock().await;
        if fatal {
            self.lifecycle.cancel_current().await;
            self.set_state(SessionState::Faulted, Some(message.clone()))
                .await;
        }
        self.emit(SessionEventKind::Error {
            code: code.into(),
            message,
            fatal,
        });
        if fatal
            && !matches!(
                previous_state,
                SessionState::Faulted | SessionState::Stopping
            )
        {
            let session = Arc::clone(self);
            tokio::spawn(async move {
                let operation = session.lifecycle.begin().await;
                let _ = session
                    .stop_model_with_operation(SessionState::Faulted, &operation)
                    .await;
            });
        }
    }

    async fn detach_translator(self: &Arc<Self>) -> Result<Option<Box<dyn RunningTranslator>>> {
        let _operation = self.lifecycle.begin().await;
        self.detach_translator_locked().await
    }

    async fn detach_translator_locked(
        self: &Arc<Self>,
    ) -> Result<Option<Box<dyn RunningTranslator>>> {
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
        translator: Box<dyn RunningTranslator>,
    ) -> Result<()> {
        let operation = self.lifecycle.begin().await;
        self.adopt_translator_with_operation(translator, &operation)
            .await
    }

    async fn adopt_translator_with_operation(
        self: &Arc<Self>,
        mut translator: Box<dyn RunningTranslator>,
        operation: &crate::lifecycle::Operation<'_>,
    ) -> Result<()> {
        let state = *self.state.lock().await;
        if !matches!(state, SessionState::Idle | SessionState::Faulted) {
            return Err(CoreError::InvalidState(format!(
                "cannot attach a model while {state:?}"
            )));
        }
        if self.runtime.lock().await.is_some() {
            self.stop_model_with_operation(SessionState::Idle, operation)
                .await?;
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
        let attached = operation
            .run_with_timeout(
                LIFECYCLE_IO_TIMEOUT,
                "attaching the resident translator",
                async {
                    translator.reattach(self.id, epoch, sender).await?;
                    if let Some(direction) = direction {
                        translator.set_direction(&direction).await?;
                    }
                    Ok::<(), CoreError>(())
                },
            )
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
        let task = tokio::spawn(forward_translator(weak, receiver, operation.token()));
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
        let operation = self.lifecycle.begin().await;
        self.prepare_with_operation(&operation).await
    }

    async fn prepare_with_operation(
        self: &Arc<Self>,
        operation: &crate::lifecycle::Operation<'_>,
    ) -> Result<()> {
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
            self.stop_model_with_operation(SessionState::Idle, operation)
                .await?;
        }
        self.set_state(SessionState::Preparing, Some("loading translator".into()))
            .await;
        let config = self.config.read().await.clone();
        let epoch = self.epoch.load(Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(256);
        let mut translator = match operation
            .run_with_timeout(
                LIFECYCLE_IO_TIMEOUT,
                "starting the translator process",
                start_translator(
                    &config.translator,
                    &self.options.deployment.model_executable,
                    &self.options.deployment.model_path,
                    self.id,
                    epoch,
                    sender,
                    &self.options.runtime_dir,
                ),
            )
            .await
        {
            Ok(translator) => translator,
            Err(error) => {
                if !operation.is_cancelled() {
                    self.fault("model_not_ready", error.to_string(), true).await;
                }
                return Err(error);
            }
        };
        if operation.is_cancelled() {
            let _ = translator.stop().await;
            return Err(CoreError::InvalidState(
                "translator start was cancelled before commit".into(),
            ));
        }
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
        let cancellation = operation.token();
        let task = tokio::spawn(async move {
            forward_translator(weak, receiver, cancellation).await;
        });
        if let Some(runtime) = self.runtime.lock().await.as_mut() {
            runtime.translator_task = Some(task);
        }
        Ok(())
    }

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        let operation = self.lifecycle.begin().await;
        self.start_with_operation(&operation).await
    }

    async fn start_with_operation(
        self: &Arc<Self>,
        operation: &crate::lifecycle::Operation<'_>,
    ) -> Result<()> {
        let state = *self.state.lock().await;
        if !matches!(state, SessionState::Ready | SessionState::Paused) {
            return Err(CoreError::InvalidState(format!(
                "cannot start while {state:?}; prepare and wait for ready"
            )));
        }
        self.set_state(
            SessionState::Configuring,
            Some("starting a new capture run".into()),
        )
        .await;
        let config = self.config.read().await.clone();
        let timeline_offset_ns = self
            .metadata
            .lock()
            .await
            .audio_duration_us
            .saturating_mul(1_000);
        self.timeline_offset_ns
            .store(timeline_offset_ns, Ordering::Release);
        self.timeline_offsets.lock().await.clear();
        self.pending_speech.lock().await.clear();
        let epoch = self.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        let (translator_sender, translator_receiver) = mpsc::channel(256);
        let old_translator_task = self
            .runtime
            .lock()
            .await
            .as_mut()
            .and_then(|runtime| runtime.translator_task.take());
        if let Some(task) = old_translator_task {
            task.abort();
            let _ = task.await;
        }
        let first_speech_token = self.store.next_speech_token(self.id).await?;
        let (output_sender, output_receiver) = mpsc::channel(256);
        let mut runtime_guard = self.runtime.lock().await;
        let runtime = runtime_guard
            .as_mut()
            .ok_or_else(|| CoreError::InvalidState("runtime missing".into()))?;
        let reattached = {
            let translator = runtime
                .translator
                .as_mut()
                .ok_or_else(|| CoreError::InvalidState("translator missing".into()))?;
            operation
                .run_with_timeout(
                    LIFECYCLE_IO_TIMEOUT,
                    "resetting the translator for a new run",
                    translator.reattach(self.id, epoch, translator_sender),
                )
                .await
        };
        if let Err(error) = reattached {
            drop(runtime_guard);
            if !operation.is_cancelled() {
                self.fault("model_reset", error.to_string(), true).await;
            }
            return Err(error);
        }
        let output = match operation
            .run_with_timeout(
                LIFECYCLE_IO_TIMEOUT,
                "starting the output process",
                OutputRunner::start(
                    &config.output,
                    OutputPaths {
                        player_path: &self.options.deployment.player_path,
                        cache_dir: &self.options.cache_dir(),
                    },
                    config.latency_mode,
                    self.id,
                    epoch,
                    first_speech_token,
                    output_sender,
                ),
            )
            .await
        {
            Ok(output) => output,
            Err(error) => {
                drop(runtime_guard);
                if !operation.is_cancelled() {
                    self.fault("output_unavailable", error.to_string(), true)
                        .await;
                }
                return Err(error);
            }
        };
        let playback_pid = output.playback_pid();
        runtime.output = Some(output);
        let (source_sender, source_receiver) = mpsc::channel(config.queue.max_frames.max(2));
        let source = match operation
            .run_with_timeout(
                LIFECYCLE_IO_TIMEOUT,
                "starting the audio source",
                start_source(
                    &config.source,
                    &SourceStartOptions {
                        audio_host: self.options.audio_host.clone(),
                        playback_pid,
                    },
                    self.id,
                    epoch,
                    source_sender,
                ),
            )
            .await
        {
            Ok(source) => source,
            Err(error) => {
                if let Some(output) = runtime.output.as_mut() {
                    let _ = tokio::time::timeout(RESOURCE_STOP_TIMEOUT, output.stop()).await;
                }
                runtime.output = None;
                drop(runtime_guard);
                if !operation.is_cancelled() {
                    self.fault("source_unavailable", error.to_string(), true)
                        .await;
                }
                return Err(error);
            }
        };
        runtime.source = Some(source);
        drop(runtime_guard);

        {
            let mut metadata = self.metadata.lock().await;
            if metadata.started_at_ms.is_none() {
                metadata.started_at_ms = Some(now_ms());
            }
            metadata.ended_at_ms = None;
        }
        self.playback.lock().await.clear();
        {
            let mut metrics = self.metrics.lock().await;
            metrics.playback_pending = 0;
            metrics.tts_queue_depth = 0;
            metrics.sent_duration_us = 0;
            metrics.completed_duration_us = 0;
            metrics.model_backlog_us = 0;
        }
        self.persist().await?;
        if operation.is_cancelled() {
            return Err(CoreError::InvalidState(
                "capture start was cancelled before commit".into(),
            ));
        }
        self.set_state(SessionState::Running, Some("capturing".into()))
            .await;
        let translator_task = tokio::spawn(forward_translator(
            Arc::downgrade(self),
            translator_receiver,
            operation.token(),
        ));
        let output_task = tokio::spawn(forward_output(
            Arc::downgrade(self),
            output_receiver,
            operation.token(),
        ));
        let source_task = tokio::spawn(forward_source(
            Arc::downgrade(self),
            source_receiver,
            config.queue,
            operation.token(),
        ));
        if let Some(runtime) = self.runtime.lock().await.as_mut() {
            runtime.translator_task = Some(translator_task);
            runtime.output_task = Some(output_task);
            runtime.source_task = Some(source_task);
        }
        Ok(())
    }

    pub async fn pause(self: &Arc<Self>) -> Result<()> {
        if *self.state.lock().await != SessionState::Running {
            return Err(CoreError::InvalidState(
                "only a running session can pause".into(),
            ));
        }
        self.lifecycle.cancel_current().await;
        let operation = self.lifecycle.begin().await;
        if *self.state.lock().await != SessionState::Running {
            return Err(CoreError::InvalidState(
                "session stopped before pause acquired the lifecycle gate".into(),
            ));
        }
        self.stop_capture_with_operation(Some(SessionState::Paused), &operation)
            .await
    }

    pub async fn resume(self: &Arc<Self>) -> Result<()> {
        self.start().await
    }

    /// Stops capture and playback while keeping the expensive model process warm.
    pub async fn stop(self: &Arc<Self>) -> Result<()> {
        self.lifecycle.cancel_current().await;
        let operation = self.lifecycle.begin().await;
        if *self.state.lock().await == SessionState::Preparing {
            return self
                .stop_model_with_operation(SessionState::Idle, &operation)
                .await;
        }
        self.stop_capture_with_operation(None, &operation).await
    }

    async fn stop_capture_with_operation(
        self: &Arc<Self>,
        requested_state: Option<SessionState>,
        operation: &crate::lifecycle::Operation<'_>,
    ) -> Result<()> {
        let state = *self.state.lock().await;
        if matches!(state, SessionState::Idle | SessionState::Stopping) {
            return Ok(());
        }
        self.set_state(SessionState::Stopping, None).await;
        self.epoch.fetch_add(1, Ordering::AcqRel);
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
        for task in [source_task, output_task].into_iter().flatten() {
            task.abort();
            let _ = task.await;
        }
        let mut cleanup_error = None;
        if let Some(source) = source.as_mut() {
            match tokio::time::timeout(RESOURCE_STOP_TIMEOUT, source.stop()).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => cleanup_error = Some(error.to_string()),
                Err(_) => cleanup_error = Some("audio source stop timed out".into()),
            }
        }
        if let Some(output) = output.as_mut() {
            match tokio::time::timeout(RESOURCE_STOP_TIMEOUT, output.stop()).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => cleanup_error = Some(error.to_string()),
                Err(_) => cleanup_error = Some("output stop timed out".into()),
            }
        }
        if let Err(error) = self
            .store
            .recover_speech(self.id, self.options.cache_dir())
            .await
        {
            cleanup_error = Some(error.to_string());
        }
        self.playback.lock().await.clear();
        self.pending_speech.lock().await.clear();
        {
            let mut metrics = self.metrics.lock().await;
            metrics.playback_pending = 0;
            metrics.tts_queue_depth = 0;
        }
        self.flush_subtitle().await;
        let captured_duration_us = self.metrics.lock().await.captured_duration_us;
        {
            let mut metadata = self.metadata.lock().await;
            metadata.audio_duration_us = captured_duration_us;
            if requested_state.is_none()
                && matches!(state, SessionState::Running | SessionState::Paused)
            {
                metadata.ended_at_ms = Some(now_ms());
            }
        }
        self.persist().await?;
        if let Some(message) = cleanup_error {
            self.fault("resource_stop", message.clone(), true).await;
            return Err(CoreError::Output(message));
        }
        let next_state = requested_state.unwrap_or(if has_translator {
            SessionState::Ready
        } else {
            SessionState::Idle
        });
        self.set_state(
            next_state,
            Some("capture stopped; original audio restored; model kept warm".into()),
        )
        .await;
        let _ = operation;
        Ok(())
    }

    /// Stops the complete pipeline and unloads the model.
    pub async fn stop_model(self: &Arc<Self>) -> Result<()> {
        self.lifecycle.cancel_current().await;
        let operation = self.lifecycle.begin().await;
        self.stop_model_with_operation(SessionState::Idle, &operation)
            .await
    }

    async fn stop_model_with_operation(
        self: &Arc<Self>,
        next_state: SessionState,
        operation: &crate::lifecycle::Operation<'_>,
    ) -> Result<()> {
        let state = *self.state.lock().await;
        let captured_duration_us = self.metrics.lock().await.captured_duration_us;
        {
            let mut metadata = self.metadata.lock().await;
            metadata.audio_duration_us = captured_duration_us;
            if matches!(state, SessionState::Running | SessionState::Paused) {
                metadata.ended_at_ms = Some(now_ms());
            }
        }
        if matches!(state, SessionState::Running | SessionState::Paused) {
            self.persist().await?;
        }
        self.set_state(SessionState::Stopping, None).await;
        self.epoch.fetch_add(1, Ordering::AcqRel);
        let runtime = self.runtime.lock().await.take();
        let mut cleanup_error = None;
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
                match tokio::time::timeout(RESOURCE_STOP_TIMEOUT, source.stop()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => cleanup_error = Some(error.to_string()),
                    Err(_) => cleanup_error = Some("audio source stop timed out".into()),
                }
            }
            if let Some(output) = runtime.output.as_mut() {
                match tokio::time::timeout(RESOURCE_STOP_TIMEOUT, output.stop()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => cleanup_error = Some(error.to_string()),
                    Err(_) => cleanup_error = Some("output stop timed out".into()),
                }
            }
            if let Some(translator) = runtime.translator.as_mut() {
                match tokio::time::timeout(RESOURCE_STOP_TIMEOUT, translator.stop()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => cleanup_error = Some(error.to_string()),
                    Err(_) => cleanup_error = Some("translator stop timed out".into()),
                }
            }
        }
        if let Err(error) = self
            .store
            .recover_speech(self.id, self.options.cache_dir())
            .await
        {
            cleanup_error = Some(error.to_string());
        }
        self.playback.lock().await.clear();
        self.pending_speech.lock().await.clear();
        {
            let mut metrics = self.metrics.lock().await;
            metrics.playback_pending = 0;
            metrics.tts_queue_depth = 0;
        }
        self.flush_subtitle().await;
        if let Some(message) = cleanup_error {
            *self.last_error.lock().await = Some(message.clone());
            self.emit(SessionEventKind::Error {
                code: "resource_stop".into(),
                message: message.clone(),
                fatal: true,
            });
            self.set_state(
                SessionState::Faulted,
                Some("resource cleanup failed".into()),
            )
            .await;
            return Err(CoreError::Output(message));
        } else {
            self.set_state(
                next_state,
                Some("model stopped; original audio restored".into()),
            )
            .await;
        }
        let _ = operation;
        Ok(())
    }

    pub async fn set_source(
        self: &Arc<Self>,
        source: SourceConfig,
        mute_original: bool,
    ) -> Result<()> {
        let operation = self.lifecycle.begin().await;
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
        self.persist().await?;
        if state != SessionState::Running {
            return Ok(());
        }

        self.stop_capture_with_operation(None, &operation).await?;
        self.start_with_operation(&operation).await?;
        self.emit(SessionEventKind::State {
            state: SessionState::Running,
            message: Some("audio source switched in a new capture run".into()),
        });
        Ok(())
    }

    async fn recover_translator(self: &Arc<Self>, message: String) {
        let operation = self.lifecycle.begin().await;
        let state = *self.state.lock().await;
        if matches!(state, SessionState::Preparing | SessionState::Ready) {
            self.fault("model_stream_ended", message, true).await;
            return;
        }
        if state != SessionState::Running {
            return;
        }
        let mut runtime = self.runtime.lock().await;
        let result = operation
            .run_with_timeout(LIFECYCLE_IO_TIMEOUT, "recovering the model stream", async {
                let translator = runtime
                    .as_mut()
                    .and_then(|runtime| runtime.translator.as_mut())
                    .ok_or_else(|| CoreError::InvalidState("translator missing".into()))?;
                translator.recover().await
            })
            .await;
        drop(runtime);
        if let Err(error) = result
            && !operation.is_cancelled()
        {
            self.fault("model_recovery", error.to_string(), true).await;
        }
    }

    async fn flush_pending_speech(
        self: &Arc<Self>,
        operation: &crate::lifecycle::Operation<'_>,
    ) -> Result<()> {
        loop {
            let Some(event) = self.pending_speech.lock().await.pop_front() else {
                return Ok(());
            };
            let cancellation = operation.token();
            let mut runtime = self.runtime.lock().await;
            let result = if let Some(output) = runtime.as_mut().and_then(|r| r.output.as_mut()) {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        Err(CoreError::InvalidState("speech recovery was cancelled".into()))
                    }
                    result = output.submit(event.clone()) => result,
                }
            } else {
                Err(CoreError::InvalidState(
                    "recovered output is unavailable".into(),
                ))
            };
            drop(runtime);
            if let Err(error) = result {
                self.pending_speech.lock().await.push_front(event);
                if !operation.is_cancelled() {
                    self.fault("tts_recovery", error.to_string(), true).await;
                }
                return Err(error);
            }
        }
    }

    fn recover_output(
        self: Arc<Self>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
        Box::pin(async move {
            let operation = self.lifecycle.begin().await;
            let state = *self.state.lock().await;
            if state != SessionState::Running {
                return Ok(());
            }
            let config = self.config.read().await.clone();
            if matches!(&config.source, SourceConfig::File { .. }) {
                let error = CoreError::Output(
                    "file input cannot safely resume after an output restart; start a new run explicitly".into(),
                );
                self.fault("output_recovery", error.to_string(), true).await;
                return Err(error);
            }
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
                let _ = tokio::time::timeout(RESOURCE_STOP_TIMEOUT, source.stop()).await;
            }
            if let Some(output) = old_output.as_mut() {
                let _ = tokio::time::timeout(RESOURCE_STOP_TIMEOUT, output.stop()).await;
            }
            if let Err(error) = self
                .store
                .recover_speech(self.id, self.options.cache_dir())
                .await
            {
                self.fault("speech_storage", error.to_string(), true).await;
                return Err(error);
            }

            self.playback.lock().await.clear();
            {
                let mut metrics = self.metrics.lock().await;
                metrics.playback_pending = 0;
                metrics.tts_queue_depth = 0;
            }

            let first_speech_token = self.store.next_speech_token(self.id).await?;
            let (output_sender, output_receiver) = mpsc::channel(256);
            let mut output = match operation
                .run_with_timeout(
                    LIFECYCLE_IO_TIMEOUT,
                    "restarting the output process",
                    OutputRunner::start(
                        &config.output,
                        OutputPaths {
                            player_path: &self.options.deployment.player_path,
                            cache_dir: &self.options.cache_dir(),
                        },
                        config.latency_mode,
                        self.id,
                        epoch,
                        first_speech_token,
                        output_sender,
                    ),
                )
                .await
            {
                Ok(output) => output,
                Err(error) => {
                    if !operation.is_cancelled() {
                        self.fault("output_recovery", error.to_string(), true).await;
                    }
                    return Err(error);
                }
            };
            let playback_pid = output.playback_pid();
            let (source_sender, source_receiver) = mpsc::channel(config.queue.max_frames.max(2));
            let mut source = match operation
                .run_with_timeout(
                    LIFECYCLE_IO_TIMEOUT,
                    "restarting the audio source",
                    start_source(
                        &config.source,
                        &SourceStartOptions {
                            audio_host: self.options.audio_host.clone(),
                            playback_pid,
                        },
                        self.id,
                        epoch,
                        source_sender,
                    ),
                )
                .await
            {
                Ok(source) => source,
                Err(error) => {
                    let _ = tokio::time::timeout(RESOURCE_STOP_TIMEOUT, output.stop()).await;
                    if !operation.is_cancelled() {
                        self.fault("source_recovery", error.to_string(), true).await;
                    }
                    return Err(error);
                }
            };
            let current_state = *self.state.lock().await;
            if operation.is_cancelled()
                || self.epoch.load(Ordering::Acquire) != epoch
                || current_state != SessionState::Running
            {
                let _ = tokio::time::timeout(RESOURCE_STOP_TIMEOUT, source.stop()).await;
                let _ = tokio::time::timeout(RESOURCE_STOP_TIMEOUT, output.stop()).await;
                return Err(CoreError::InvalidState(
                    "output recovery was cancelled before commit".into(),
                ));
            }
            let output_task = tokio::spawn(forward_output(
                Arc::downgrade(&self),
                output_receiver,
                operation.token(),
            ));
            let source_task = tokio::spawn(forward_source(
                Arc::downgrade(&self),
                source_receiver,
                config.queue,
                operation.token(),
            ));
            let mut guard = self.runtime.lock().await;
            if let Some(runtime) = guard.as_mut() {
                runtime.output = Some(output);
                runtime.source = Some(source);
                runtime.output_task = Some(output_task);
                runtime.source_task = Some(source_task);
            } else {
                output_task.abort();
                source_task.abort();
                drop(guard);
                let _ = tokio::time::timeout(RESOURCE_STOP_TIMEOUT, source.stop()).await;
                let _ = tokio::time::timeout(RESOURCE_STOP_TIMEOUT, output.stop()).await;
                return Err(CoreError::InvalidState(
                    "output recovery found no active runtime".into(),
                ));
            }
            drop(guard);
            self.flush_pending_speech(&operation).await?;
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

    async fn persist(&self) -> Result<()> {
        let audio_duration_us = self.metrics.lock().await.captured_duration_us;
        self.metadata.lock().await.audio_duration_us = audio_duration_us;
        let stored = StoredSession {
            id: self.id,
            config: self.config.read().await.clone(),
            metadata: self.metadata.lock().await.clone(),
        };
        self.store.save_session(stored).await
    }

    pub async fn rename(&self, title: String) -> Result<()> {
        let title = title.trim();
        if title.is_empty() || title.chars().count() > 80 {
            return Err(CoreError::InvalidConfig(
                "session title must contain 1 to 80 characters".into(),
            ));
        }
        self.metadata.lock().await.title = title.to_string();
        self.persist().await?;
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
        let cache = self.options.cache_dir();
        let mut entries = match tokio::fs::read_dir(cache).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return self.store.delete_session(self.id).await;
            }
            Err(error) => return Err(error.into()),
        };
        let marker = self.id.to_string();
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_name().to_string_lossy().contains(&marker) {
                let _ = tokio::fs::remove_file(entry.path()).await;
            }
        }
        self.store.delete_session(self.id).await
    }

    fn history_path(&self) -> PathBuf {
        PathBuf::from(&self.options.runtime_dir)
            .join("subtitles")
            .join(format!("{}.jsonl", self.id))
    }

    pub async fn history(&self) -> Result<Vec<SubtitleCue>> {
        self.store.history(self.id).await
    }

    pub async fn clear_history(&self) -> Result<()> {
        let _write = self.subtitle_writes.lock().await;
        self.store.clear_history(self.id).await?;
        self.history.lock().await.clear();
        *self.subtitles.lock().await = SubtitleAssembler::default();
        self.emit(SessionEventKind::HistoryCleared);
        Ok(())
    }

    #[cfg(test)]
    async fn record_subtitle(&self, cue: SubtitleCue) {
        let _write = self.subtitle_writes.lock().await;
        self.record_subtitle_locked(cue).await;
    }

    async fn record_subtitle_locked(&self, cue: SubtitleCue) {
        if let Err(error) = self.persist().await {
            self.emit(SessionEventKind::Warning {
                code: "subtitle_storage".into(),
                message: format!("subtitle revision was not published: {error}"),
            });
            return;
        }
        match self.store.record_cue(self.id, cue.clone()).await {
            Ok(true) => {}
            Ok(false) => return,
            Err(error) => {
                self.emit(SessionEventKind::Warning {
                    code: "subtitle_storage".into(),
                    message: format!("subtitle revision was not published: {error}"),
                });
                return;
            }
        }
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
        self.emit(SessionEventKind::Subtitle(cue));
    }

    pub async fn export_subtitles(&self, format: SubtitleExportFormat) -> Result<String> {
        let cues = self.store.export_snapshot(self.id).await?.cues;
        tokio::task::spawn_blocking(move || crate::export::render_subtitles(&cues, format))
            .await
            .map_err(|error| CoreError::Output(format!("subtitle export task failed: {error}")))
    }

    pub async fn export_wav(&self) -> Result<PathBuf> {
        if !matches!(
            &*self.config.read().await,
            PipelineConfig {
                output: OutputConfig::MacosSystem { .. },
                ..
            }
        ) {
            return Err(CoreError::InvalidConfig(
                "mock output does not create audio files".into(),
            ));
        }
        let cache_dir = self.options.cache_dir();
        let segments = self.store.export_snapshot(self.id).await?.speech_segments;
        crate::export::export_wav(self.id, segments, cache_dir).await
    }

    async fn flush_subtitle(&self) {
        let _write = self.subtitle_writes.lock().await;
        let cue = self.subtitles.lock().await.flush();
        if let Some(cue) = cue {
            self.record_subtitle_locked(cue).await;
        }
    }

    pub async fn set_direction(&self, direction: String) -> Result<()> {
        let operation = self.lifecycle.begin().await;
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
        operation
            .run_with_timeout(
                LIFECYCLE_IO_TIMEOUT,
                "changing translation direction",
                async {
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
                    Ok(())
                },
            )
            .await?;
        self.persist().await?;
        self.emit(SessionEventKind::State {
            state,
            message: Some("translation direction and language context updated".into()),
        });
        Ok(())
    }

    pub async fn set_latency_mode(&self, mode: LatencyMode) -> Result<()> {
        let operation = self.lifecycle.begin().await;
        let state = *self.state.lock().await;
        if matches!(state, SessionState::Stopping | SessionState::Preparing) {
            return Err(CoreError::InvalidState(format!(
                "cannot change voice policy while {state:?}"
            )));
        }
        self.config.write().await.latency_mode = mode;
        operation
            .run_with_timeout(LIFECYCLE_IO_TIMEOUT, "changing voice policy", async {
                let mut runtime = self.runtime.lock().await;
                if let Some(output) = runtime.as_mut().and_then(|runtime| runtime.output.as_mut()) {
                    output.set_latency_mode(mode).await?;
                }
                Ok(())
            })
            .await?;
        self.persist().await?;
        self.emit(SessionEventKind::State {
            state,
            message: Some("voice policy updated".into()),
        });
        Ok(())
    }

    pub async fn reconfigure(self: &Arc<Self>, config: PipelineConfig) -> Result<()> {
        let operation = self.lifecycle.begin().await;
        let state = *self.state.lock().await;
        if state != SessionState::Idle && state != SessionState::Faulted {
            return Err(CoreError::InvalidState(
                "stop the session before reconfiguring".into(),
            ));
        }
        if state == SessionState::Faulted {
            self.stop_model_with_operation(SessionState::Idle, &operation)
                .await?;
        }
        *self.config.write().await = config;
        *self.metrics.lock().await = SessionMetrics::default();
        *self.last_error.lock().await = None;
        self.persist().await?;
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

async fn forward_translator(
    weak: std::sync::Weak<Session>,
    mut receiver: mpsc::Receiver<TranslatorSignal>,
    cancellation: CancellationToken,
) {
    loop {
        let signal = tokio::select! {
            _ = cancellation.cancelled() => break,
            signal = receiver.recv() => signal,
        };
        let Some(signal) = signal else { break };
        let Some(session) = weak.upgrade() else { break };
        match signal {
            TranslatorSignal::Ready => {
                if *session.state.lock().await == SessionState::Preparing {
                    session
                        .set_state(SessionState::Ready, Some("translator ready".into()))
                        .await;
                }
            }
            TranslatorSignal::Translation(mut event) => {
                if event.epoch != session.epoch.load(Ordering::Relaxed) {
                    continue;
                }
                let state = *session.state.lock().await;
                if !matches!(state, SessionState::Running | SessionState::Paused) {
                    // A model may finish an in-flight turn after capture stops. It must
                    // not mutate a closed timeline or leak into the next capture run.
                    continue;
                }
                event.segment_id = format!(
                    "run-{}-source-{}-{}",
                    event.epoch, event.source_generation, event.segment_id
                );
                let timeline_offset_ns = session
                    .timeline_offsets
                    .lock()
                    .await
                    .get(&(event.epoch, event.source_generation))
                    .copied()
                    .unwrap_or(0);
                event.source_start_ns = event
                    .source_start_ns
                    .map(|timestamp| timestamp.saturating_add(timeline_offset_ns));
                event.source_end_ns = event
                    .source_end_ns
                    .map(|timestamp| timestamp.saturating_add(timeline_offset_ns));
                session.metrics.lock().await.translations += 1;
                session.emit(SessionEventKind::Translation(event.clone()));
                {
                    let _write = session.subtitle_writes.lock().await;
                    let cue = session.subtitles.lock().await.push(&event);
                    session.record_subtitle_locked(cue).await;
                }
                if event.speakable {
                    let mut runtime = session.runtime.lock().await;
                    if let Some(output) = runtime.as_mut().and_then(|r| r.output.as_mut()) {
                        let submitted = tokio::select! {
                            _ = cancellation.cancelled() => return,
                            result = output.submit(event) => result,
                        };
                        if let Err(error) = submitted {
                            session.emit(SessionEventKind::Warning {
                                code: "tts_queue".into(),
                                message: error.to_string(),
                            });
                        }
                    } else {
                        drop(runtime);
                        let mut pending = session.pending_speech.lock().await;
                        if pending.len() >= PENDING_SPEECH_LIMIT {
                            session.emit(SessionEventKind::Warning {
                                code: "tts_recovery_queue".into(),
                                message: "dropped new speech because the recovery queue is full"
                                    .into(),
                            });
                        } else {
                            pending.push_back(event);
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
            TranslatorSignal::SourceReset {
                epoch,
                source_generation,
            } => {
                let offset = session
                    .timeline_offset_ns
                    .load(Ordering::Acquire)
                    .min(i64::MAX as u64) as i64;
                let mut offsets = session.timeline_offsets.lock().await;
                offsets.clear();
                offsets.insert((epoch, source_generation), offset);
                drop(offsets);
                let snapshot = {
                    let mut metrics = session.metrics.lock().await;
                    metrics.source_resets += 1;
                    metrics.clone()
                };
                session.emit(SessionEventKind::Metrics(snapshot));
            }
            TranslatorSignal::StreamEnded(message) => {
                session.recover_translator(message).await;
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
    cancellation: CancellationToken,
) {
    loop {
        let signal = tokio::select! {
            _ = cancellation.cancelled() => break,
            signal = receiver.recv() => signal,
        };
        let Some(signal) = signal else { break };
        let Some(session) = weak.upgrade() else { break };
        match signal {
            OutputSignal::Playback(event) => {
                if event.token != 0 {
                    let cache_dir = session.options.cache_dir();
                    match session
                        .store
                        .record_playback(session.id, cache_dir, event.clone())
                        .await
                    {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(error) => {
                            session.emit(SessionEventKind::Warning {
                                code: "speech_storage".into(),
                                message: format!("playback state was not persisted: {error}"),
                            });
                            continue;
                        }
                    }
                    let snapshot = {
                        let update = session
                            .playback
                            .lock()
                            .await
                            .apply(event.token, event.event_type);
                        let mut metrics = session.metrics.lock().await;
                        metrics.playback_pending = update.pending;
                        if update.count_event {
                            match event.event_type {
                                PlaybackEventType::Queued => {}
                                PlaybackEventType::Started => {
                                    metrics.playback_started += 1;
                                    metrics.output_latency_us = event.queue_duration_us;
                                    metrics.max_output_latency_us =
                                        metrics.max_output_latency_us.max(event.queue_duration_us);
                                    if let Some(source_end_ns) = event.source_end_ns {
                                        let source_end_us = source_end_ns.max(0) as u64 / 1_000;
                                        metrics.playback_lag_us = metrics
                                            .captured_duration_us
                                            .saturating_sub(source_end_us);
                                        metrics.max_playback_lag_us = metrics
                                            .max_playback_lag_us
                                            .max(metrics.playback_lag_us);
                                    }
                                }
                                PlaybackEventType::Finished => {
                                    metrics.playback_finished += 1;
                                }
                                PlaybackEventType::Cancelled => {
                                    metrics.playback_cancelled += 1;
                                }
                                PlaybackEventType::Failed | PlaybackEventType::Expired => {}
                            }
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
            OutputSignal::DrainBarrier(ack) => {
                let _ = ack.send(());
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
    cancellation: CancellationToken,
) {
    let mut chunker = AudioChunker::default();
    loop {
        let source_event = tokio::select! {
            _ = cancellation.cancelled() => break,
            event = receiver.recv() => event,
        };
        let Some(source_event) = source_event else {
            break;
        };
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
                    if let Err(error) =
                        forward_model_block(&session, block, &queue_policy, &cancellation).await
                    {
                        if cancellation.is_cancelled() {
                            return;
                        }
                        session
                            .fault("translator_write", error.to_string(), true)
                            .await;
                        break;
                    }
                }
            }
            SourceEvent::Dropped {
                duration_us,
                frames,
            } => {
                let mut metrics = session.metrics.lock().await;
                metrics.dropped_frames += frames as u64;
                metrics.dropped_duration_us += duration_us as u64;
            }
            SourceEvent::Ended => {
                session.emit(SessionEventKind::Warning {
                    code: "source_ended".into(),
                    message: "audio source ended; draining translation and playback".into(),
                });
                if let Some(block) = chunker.finish()
                    && let Err(error) =
                        forward_model_block(&session, block, &queue_policy, &cancellation).await
                {
                    if !cancellation.is_cancelled() {
                        session
                            .fault("translator_write", error.to_string(), true)
                            .await;
                    }
                    break;
                }
                let drain_epoch = session.epoch.load(Ordering::Acquire);
                let session = Arc::clone(&session);
                tokio::spawn(async move {
                    let operation = session.lifecycle.begin().await;
                    let cancellation = operation.token();
                    if operation.is_cancelled()
                        || session.epoch.load(Ordering::Acquire) != drain_epoch
                    {
                        return;
                    }
                    if let Err(error) = wait_for_model_drain(&session, &cancellation).await {
                        if !cancellation.is_cancelled()
                            && session.epoch.load(Ordering::Acquire) == drain_epoch
                        {
                            session.fault("model_drain", error.to_string(), true).await;
                        }
                        return;
                    }
                    if cancellation.is_cancelled()
                        || session.epoch.load(Ordering::Acquire) != drain_epoch
                    {
                        return;
                    }
                    session.flush_subtitle().await;
                    let mut runtime = session.runtime.lock().await;
                    if let Some(output) = runtime.as_mut().and_then(|r| r.output.as_mut()) {
                        let finished = tokio::select! {
                            _ = cancellation.cancelled() => return,
                            result = output.finish() => result,
                        };
                        if let Err(error) = finished {
                            session.emit(SessionEventKind::Warning {
                                code: "tts_finish".into(),
                                message: error.to_string(),
                            });
                        }
                    }
                    drop(runtime);
                    if let Err(error) = wait_for_playback_drain(&session, &cancellation).await {
                        if !cancellation.is_cancelled()
                            && session.epoch.load(Ordering::Acquire) == drain_epoch
                        {
                            session
                                .fault("playback_drain", error.to_string(), true)
                                .await;
                        }
                        return;
                    }
                    if cancellation.is_cancelled()
                        || session.epoch.load(Ordering::Acquire) != drain_epoch
                    {
                        return;
                    }
                    if let Err(error) = session.stop_capture_with_operation(None, &operation).await
                    {
                        tracing::warn!("failed to stop drained session: {error}");
                    }
                });
                break;
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

async fn forward_model_block(
    session: &Arc<Session>,
    block: AudioFrame,
    queue_policy: &QueuePolicy,
    cancellation: &CancellationToken,
) -> Result<()> {
    let mut block = block;
    if block.payload.len() < 25_600 {
        block.payload.resize(25_600, 0);
        block.format.frame_duration_us = MODEL_BLOCK_US;
    }
    loop {
        let backlog = {
            let metrics = session.metrics.lock().await;
            metrics
                .sent_duration_us
                .saturating_sub(metrics.completed_duration_us)
        };
        if backlog <= queue_policy.max_model_backlog_us as u64 {
            break;
        }
        match queue_policy.overflow_behavior {
            OverflowBehavior::BlockProducer => {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        return Err(CoreError::InvalidState("audio backpressure was cancelled".into()));
                    }
                    _ = tokio::time::sleep(Duration::from_millis(25)) => {}
                }
            }
            OverflowBehavior::FailSession => {
                return Err(CoreError::InvalidState(format!(
                    "model backlog exceeded the configured limit at {backlog} us"
                )));
            }
            OverflowBehavior::DropOldestAudio | OverflowBehavior::SkipStaleSpeech => {
                reset_stale_backlog(
                    session,
                    backlog,
                    matches!(
                        queue_policy.overflow_behavior,
                        OverflowBehavior::SkipStaleSpeech
                    ),
                    cancellation,
                )
                .await?;
                break;
            }
            OverflowBehavior::DropNewestAudio => {
                let mut metrics = session.metrics.lock().await;
                metrics.dropped_frames += 1;
                metrics.dropped_duration_us += block.duration_us as u64;
                drop(metrics);
                session.emit(SessionEventKind::Warning {
                    code: "backlog_overflow".into(),
                    message: format!("dropped one audio block at {backlog} us backlog"),
                });
                return Ok(());
            }
        }
    }
    let mut runtime = tokio::select! {
        _ = cancellation.cancelled() => {
            return Err(CoreError::InvalidState("model write was cancelled".into()));
        }
        guard = session.runtime.lock() => guard,
    };
    let result = tokio::select! {
        _ = cancellation.cancelled() => {
            return Err(CoreError::InvalidState("model write was cancelled".into()));
        }
        result = async {
            match runtime.as_mut().and_then(|r| r.translator.as_mut()) {
                Some(translator) => translator.push_audio(block.clone()).await,
                None => Err(CoreError::InvalidState("translator unavailable".into())),
            }
        } => result,
    };
    drop(runtime);
    result?;
    let mut metrics = session.metrics.lock().await;
    metrics.sent_frames += 1;
    metrics.sent_duration_us += block.duration_us as u64;
    metrics.model_backlog_us = metrics
        .sent_duration_us
        .saturating_sub(metrics.completed_duration_us);
    Ok(())
}

async fn reset_stale_backlog(
    session: &Arc<Session>,
    backlog_us: u64,
    flush_speech: bool,
    cancellation: &CancellationToken,
) -> Result<()> {
    let mut runtime = tokio::select! {
        _ = cancellation.cancelled() => {
            return Err(CoreError::InvalidState("stale backlog reset was cancelled".into()));
        }
        guard = session.runtime.lock() => guard,
    };
    let result = tokio::select! {
        _ = cancellation.cancelled() => {
            return Err(CoreError::InvalidState("stale backlog reset was cancelled".into()));
        }
        result = async {
            let runtime = runtime
                .as_mut()
                .ok_or_else(|| CoreError::InvalidState("runtime missing".into()))?;
            let translator = runtime
                .translator
                .as_mut()
                .ok_or_else(|| CoreError::InvalidState("translator missing".into()))?;
            translator.reset_source().await?;
            if flush_speech
                && let Some(output) = runtime.output.as_mut()
            {
                output.flush().await?;
            }
            Ok::<(), CoreError>(())
        } => result,
    };
    drop(runtime);
    result?;

    let captured_duration_us = session.metrics.lock().await.captured_duration_us;
    session.timeline_offset_ns.store(
        captured_duration_us.saturating_mul(1_000),
        Ordering::Release,
    );
    if flush_speech {
        session.playback.lock().await.clear();
    }
    {
        let mut metrics = session.metrics.lock().await;
        metrics.sent_duration_us = 0;
        metrics.completed_duration_us = 0;
        metrics.model_backlog_us = 0;
        metrics.dropped_frames += 1;
        metrics.dropped_duration_us = metrics.dropped_duration_us.saturating_add(backlog_us);
        if flush_speech {
            metrics.playback_pending = 0;
            metrics.tts_queue_depth = 0;
        }
    }
    session.emit(SessionEventKind::Warning {
        code: "stale_backlog_dropped".into(),
        message: format!(
            "dropped {backlog_us} us of stale model backlog and resumed at the live edge"
        ),
    });
    Ok(())
}

async fn wait_for_model_drain(
    session: &Arc<Session>,
    cancellation: &CancellationToken,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let metrics = session.metrics.lock().await.clone();
            if metrics.completed_duration_us >= metrics.sent_duration_us {
                break;
            }
            tokio::select! {
                _ = cancellation.cancelled() => {
                    return Err(CoreError::InvalidState("model drain was cancelled".into()));
                }
                _ = tokio::time::sleep(Duration::from_millis(25)) => {}
            }
        }
        Ok(())
    })
    .await
    .map_err(|_| CoreError::Translator("model drain timed out".into()))?
}

async fn wait_for_playback_drain(
    session: &Arc<Session>,
    cancellation: &CancellationToken,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let metrics = session.metrics.lock().await.clone();
            if metrics.tts_queue_depth == 0 && metrics.playback_pending == 0 {
                break;
            }
            tokio::select! {
                _ = cancellation.cancelled() => {
                    return Err(CoreError::InvalidState("playback drain was cancelled".into()));
                }
                _ = tokio::time::sleep(Duration::from_millis(25)) => {}
            }
        }
        Ok(())
    })
    .await
    .map_err(|_| CoreError::Output("playback drain timed out".into()))?
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

    #[test]
    fn playback_ledger_tolerates_out_of_order_and_duplicate_events() {
        let mut ledger = PlaybackLedger::default();
        let terminal = ledger.apply(1, PlaybackEventType::Finished);
        assert_eq!(terminal.pending, 0);
        assert!(terminal.count_event);
        assert!(!ledger.apply(1, PlaybackEventType::Queued).count_event);
        assert_eq!(ledger.apply(1, PlaybackEventType::Started).pending, 0);
        assert_eq!(ledger.apply(2, PlaybackEventType::Queued).pending, 1);
        assert_eq!(ledger.apply(2, PlaybackEventType::Queued).pending, 1);
        assert_eq!(ledger.apply(2, PlaybackEventType::Started).pending, 1);
        let finished = ledger.apply(2, PlaybackEventType::Finished);
        assert_eq!(finished.pending, 0);
        assert!(finished.count_event);
        assert!(!ledger.apply(2, PlaybackEventType::Finished).count_event);
        for token in 10..1_000 {
            ledger.apply(token, PlaybackEventType::Finished);
        }
        assert!(ledger.states.len() <= 64);
    }

    #[tokio::test]
    async fn mock_session_covers_complete_lifecycle() {
        let session = Session::new(
            PipelineConfig::default(),
            CoreOptions {
                audio_host: "unused".into(),
                runtime_dir: "/tmp".into(),
                deployment: DeploymentConfig::default(),
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
        assert!(snapshot.epoch > 1);
        let history = session.history().await.unwrap();
        assert!(history.iter().all(|cue| cue.is_final));
        assert!(history.windows(2).all(|pair| {
            pair[0].source_start_ns <= pair[1].source_start_ns
                && pair[0].source_end_ns <= pair[1].source_end_ns
        }));
        let segment_ids = history
            .iter()
            .flat_map(|cue| cue.segment_ids.iter())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            segment_ids.len(),
            history
                .iter()
                .map(|cue| cue.segment_ids.len())
                .sum::<usize>()
        );
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
    async fn concurrent_prepare_and_start_are_single_flight() {
        let root = tempfile::tempdir().unwrap();
        let session = Arc::new(Session::new(
            PipelineConfig {
                source: SourceConfig::Mock { frames: None },
                ..PipelineConfig::default()
            },
            CoreOptions {
                audio_host: "unused".into(),
                runtime_dir: root.path().to_string_lossy().into_owned(),
                deployment: DeploymentConfig::default(),
            },
        ));
        let mut prepares = tokio::task::JoinSet::new();
        for _ in 0..16 {
            let session = Arc::clone(&session);
            prepares.spawn(async move { session.prepare().await });
        }
        while let Some(result) = prepares.join_next().await {
            result.unwrap().unwrap();
        }
        wait_for_state(&session, SessionState::Ready).await;

        let mut starts = tokio::task::JoinSet::new();
        for _ in 0..16 {
            let session = Arc::clone(&session);
            starts.spawn(async move { session.start().await });
        }
        let mut succeeded = 0;
        while let Some(result) = starts.join_next().await {
            if result.unwrap().is_ok() {
                succeeded += 1;
            }
        }
        assert_eq!(succeeded, 1);
        {
            let guard = session.runtime.lock().await;
            let runtime = guard.as_ref().unwrap();
            assert_eq!(runtime.translator.as_ref().map(|_| 1).unwrap_or(0), 1);
            assert_eq!(runtime.source.as_ref().map(|_| 1).unwrap_or(0), 1);
            assert_eq!(runtime.output.as_ref().map(|_| 1).unwrap_or(0), 1);
        }
        session.stop_model().await.unwrap();
    }

    #[tokio::test]
    async fn stop_cancels_a_blocked_model_write() {
        struct BlockingTranslator {
            entered: Arc<tokio::sync::Notify>,
            stopped: Arc<std::sync::atomic::AtomicBool>,
        }

        #[async_trait::async_trait]
        impl RunningTranslator for BlockingTranslator {
            async fn push_audio(&mut self, _frame: AudioFrame) -> Result<()> {
                self.entered.notify_one();
                std::future::pending::<Result<()>>().await
            }

            async fn recover(&mut self) -> Result<()> {
                Ok(())
            }

            async fn reset_source(&mut self) -> Result<()> {
                Ok(())
            }

            async fn reattach(
                &mut self,
                _session_id: Uuid,
                _epoch: u64,
                _sender: mpsc::Sender<TranslatorSignal>,
            ) -> Result<()> {
                Ok(())
            }

            async fn set_direction(&mut self, _direction: &str) -> Result<()> {
                Ok(())
            }

            async fn stop(&mut self) -> Result<()> {
                self.stopped
                    .store(true, std::sync::atomic::Ordering::Release);
                Ok(())
            }
        }

        let root = tempfile::tempdir().unwrap();
        let session = Arc::new(Session::new(
            PipelineConfig::default(),
            CoreOptions {
                audio_host: "unused".into(),
                runtime_dir: root.path().to_string_lossy().into_owned(),
                deployment: DeploymentConfig::default(),
            },
        ));
        let entered = Arc::new(tokio::sync::Notify::new());
        let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        *session.runtime.lock().await = Some(Runtime {
            translator: Some(Box::new(BlockingTranslator {
                entered: Arc::clone(&entered),
                stopped: Arc::clone(&stopped),
            })),
            output: None,
            source: None,
            translator_task: None,
            output_task: None,
            source_task: None,
        });
        *session.state.lock().await = SessionState::Running;
        let operation = session.lifecycle.begin().await;
        let cancellation = operation.token();
        drop(operation);
        let (sender, receiver) = mpsc::channel(2);
        let task = tokio::spawn(forward_source(
            Arc::downgrade(&session),
            receiver,
            QueuePolicy::default(),
            cancellation,
        ));
        sender
            .send(SourceEvent::Frame(AudioFrame {
                session_id: session.id,
                epoch: 1,
                sequence: 0,
                source_id: "test".into(),
                format: AudioFormat {
                    frame_duration_us: MODEL_BLOCK_US,
                    ..AudioFormat::default()
                },
                payload: vec![0; 25_600],
                source_pts_ns: None,
                capture_mono_ns: 0,
                ingress_mono_ns: 0,
                duration_us: MODEL_BLOCK_US,
                discontinuity: false,
            }))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), session.stop_model())
            .await
            .expect("blocked model write prevented stop")
            .unwrap();
        assert!(stopped.load(std::sync::atomic::Ordering::Acquire));
        let _ = task.await;
    }

    #[tokio::test]
    async fn recovery_and_stop_cannot_leave_capture_resources_behind() {
        let root = tempfile::tempdir().unwrap();
        let session = Arc::new(Session::new(
            PipelineConfig {
                source: SourceConfig::Mock { frames: None },
                ..PipelineConfig::default()
            },
            CoreOptions {
                audio_host: "unused".into(),
                runtime_dir: root.path().to_string_lossy().into_owned(),
                deployment: DeploymentConfig::default(),
            },
        ));
        session.prepare().await.unwrap();
        wait_for_state(&session, SessionState::Ready).await;
        for _ in 0..10 {
            session.start().await.unwrap();
            let recovering = {
                let session = Arc::clone(&session);
                tokio::spawn(async move { session.recover_output().await })
            };
            let stopping = {
                let session = Arc::clone(&session);
                tokio::spawn(async move { session.stop().await })
            };
            let _ = recovering.await.unwrap();
            stopping.await.unwrap().unwrap();
            wait_for_state(&session, SessionState::Ready).await;
            let guard = session.runtime.lock().await;
            let runtime = guard.as_ref().unwrap();
            assert!(runtime.source.is_none());
            assert!(runtime.output.is_none());
        }
        session.stop_model().await.unwrap();
    }

    #[tokio::test]
    async fn invalid_pause_does_not_cancel_preparing() {
        let root = tempfile::tempdir().unwrap();
        let session = Arc::new(Session::new(
            PipelineConfig::default(),
            CoreOptions {
                audio_host: "unused".into(),
                runtime_dir: root.path().to_string_lossy().into_owned(),
                deployment: DeploymentConfig::default(),
            },
        ));
        *session.state.lock().await = SessionState::Preparing;
        let operation = session.lifecycle.begin().await;
        let token = operation.token();
        drop(operation);
        assert!(session.pause().await.is_err());
        assert!(!token.is_cancelled());
        assert_eq!(session.snapshot().await.state, SessionState::Preparing);
    }

    #[tokio::test]
    async fn paused_source_change_does_not_restart_capture() {
        let root = tempfile::tempdir().unwrap();
        let session = Arc::new(Session::new(
            PipelineConfig {
                source: SourceConfig::Mock { frames: None },
                ..PipelineConfig::default()
            },
            CoreOptions {
                audio_host: "unused".into(),
                runtime_dir: root.path().to_string_lossy().into_owned(),
                deployment: DeploymentConfig::default(),
            },
        ));
        session.prepare().await.unwrap();
        wait_for_state(&session, SessionState::Ready).await;
        session.start().await.unwrap();
        session.pause().await.unwrap();
        session
            .set_source(SourceConfig::Mock { frames: Some(1) }, true)
            .await
            .unwrap();
        let guard = session.runtime.lock().await;
        let runtime = guard.as_ref().unwrap();
        assert!(runtime.source.is_none());
        assert!(runtime.output.is_none());
        drop(guard);
        session.stop_model().await.unwrap();
    }

    #[tokio::test]
    async fn configuring_session_cannot_be_bypassed_for_model_ownership() {
        let root = tempfile::tempdir().unwrap();
        let manager = SessionManager::new(CoreOptions {
            audio_host: "unused".into(),
            runtime_dir: root.path().to_string_lossy().into_owned(),
            deployment: DeploymentConfig::default(),
        });
        let first = manager.create(PipelineConfig::default()).await.unwrap();
        manager.prepare(first.id).await.unwrap();
        wait_for_state(&first, SessionState::Ready).await;
        *first.state.lock().await = SessionState::Configuring;
        let second = manager.create(PipelineConfig::default()).await.unwrap();
        manager.prepare(second.id).await.unwrap();
        wait_for_state(&second, SessionState::Ready).await;
        assert_eq!(first.snapshot().await.state, SessionState::Idle);
        assert!(
            first
                .runtime
                .lock()
                .await
                .as_ref()
                .is_some_and(|runtime| runtime.translator.is_none())
        );
        manager.stop_all().await;
    }

    #[tokio::test]
    async fn cancelled_drain_cannot_stop_a_later_run() {
        struct QuietTranslator;

        #[async_trait::async_trait]
        impl RunningTranslator for QuietTranslator {
            async fn push_audio(&mut self, _frame: AudioFrame) -> Result<()> {
                Ok(())
            }
            async fn recover(&mut self) -> Result<()> {
                Ok(())
            }

            async fn reset_source(&mut self) -> Result<()> {
                Ok(())
            }
            async fn reattach(
                &mut self,
                _session_id: Uuid,
                _epoch: u64,
                _sender: mpsc::Sender<TranslatorSignal>,
            ) -> Result<()> {
                Ok(())
            }
            async fn set_direction(&mut self, _direction: &str) -> Result<()> {
                Ok(())
            }
            async fn stop(&mut self) -> Result<()> {
                Ok(())
            }
        }

        let root = tempfile::tempdir().unwrap();
        let session = Arc::new(Session::new(
            PipelineConfig {
                source: SourceConfig::Mock { frames: Some(1) },
                ..PipelineConfig::default()
            },
            CoreOptions {
                audio_host: "unused".into(),
                runtime_dir: root.path().to_string_lossy().into_owned(),
                deployment: DeploymentConfig::default(),
            },
        ));
        *session.runtime.lock().await = Some(Runtime {
            translator: Some(Box::new(QuietTranslator)),
            output: None,
            source: None,
            translator_task: None,
            output_task: None,
            source_task: None,
        });
        *session.state.lock().await = SessionState::Ready;
        session.start().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while session.snapshot().await.metrics.sent_duration_us == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        session.stop().await.unwrap();
        session
            .set_source(SourceConfig::Mock { frames: None }, true)
            .await
            .unwrap();
        session.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(session.snapshot().await.state, SessionState::Running);
        session.stop_model().await.unwrap();
    }

    #[tokio::test]
    async fn stale_ahead_policy_resets_to_the_live_edge() {
        let root = tempfile::tempdir().unwrap();
        let session = Arc::new(Session::new(
            PipelineConfig::default(),
            CoreOptions {
                audio_host: "unused".into(),
                runtime_dir: root.path().to_string_lossy().into_owned(),
                deployment: DeploymentConfig::default(),
            },
        ));
        session.prepare().await.unwrap();
        wait_for_state(&session, SessionState::Ready).await;
        *session.state.lock().await = SessionState::Running;
        {
            let mut metrics = session.metrics.lock().await;
            metrics.sent_duration_us = 20_000_000;
            metrics.captured_duration_us = 20_000_000;
        }
        let policy = QueuePolicy {
            max_model_backlog_us: 10_000_000,
            overflow_behavior: OverflowBehavior::DropOldestAudio,
            ..QueuePolicy::default()
        };
        let result = forward_model_block(
            &session,
            AudioFrame {
                session_id: session.id,
                epoch: 1,
                sequence: 0,
                source_id: "test".into(),
                format: AudioFormat {
                    frame_duration_us: MODEL_BLOCK_US,
                    ..AudioFormat::default()
                },
                payload: vec![0; 25_600],
                source_pts_ns: None,
                capture_mono_ns: 0,
                ingress_mono_ns: 0,
                duration_us: MODEL_BLOCK_US,
                discontinuity: false,
            },
            &policy,
            &CancellationToken::new(),
        )
        .await;
        result.unwrap();
        let snapshot = session.snapshot().await;
        assert_eq!(snapshot.metrics.dropped_frames, 1);
        assert_eq!(snapshot.metrics.dropped_duration_us, 20_000_000);
        assert_eq!(snapshot.metrics.sent_duration_us, 800_000);
        tokio::time::timeout(Duration::from_secs(1), async {
            while session.history().await.unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            session.history().await.unwrap()[0].source_start_ns,
            20_000_000_000
        );
        session.stop_model().await.unwrap();
    }

    #[tokio::test]
    async fn competing_sessions_share_one_preparing_model() {
        let root = tempfile::tempdir().unwrap();
        let manager = Arc::new(SessionManager::new(CoreOptions {
            audio_host: "unused".into(),
            runtime_dir: root.path().to_string_lossy().into_owned(),
            deployment: DeploymentConfig::default(),
        }));
        let first = manager.create(PipelineConfig::default()).await.unwrap();
        let second = manager.create(PipelineConfig::default()).await.unwrap();
        let mut prepares = tokio::task::JoinSet::new();
        for id in [first.id, second.id] {
            let manager = Arc::clone(&manager);
            prepares.spawn(async move { manager.prepare(id).await });
        }
        while let Some(result) = prepares.join_next().await {
            result.unwrap().unwrap();
        }
        let mut resident_models = 0;
        for session in [first, second] {
            if session
                .runtime
                .lock()
                .await
                .as_ref()
                .is_some_and(|runtime| runtime.translator.is_some())
            {
                resident_models += 1;
            }
        }
        assert_eq!(resident_models, 1);
        manager.stop_all().await;
    }

    #[tokio::test]
    async fn stale_translation_is_rejected_after_a_new_run_starts() {
        let root = tempfile::tempdir().unwrap();
        let session = Arc::new(Session::new(
            PipelineConfig {
                source: SourceConfig::Mock { frames: None },
                ..PipelineConfig::default()
            },
            CoreOptions {
                audio_host: "unused".into(),
                runtime_dir: root.path().to_string_lossy().into_owned(),
                deployment: DeploymentConfig::default(),
            },
        ));
        session.prepare().await.unwrap();
        wait_for_state(&session, SessionState::Ready).await;
        session.start().await.unwrap();
        let old_epoch = session.snapshot().await.epoch;
        session.stop().await.unwrap();
        session.start().await.unwrap();
        let new_epoch = session.snapshot().await.epoch;
        assert!(new_epoch > old_epoch);

        let (sender, receiver) = mpsc::channel(8);
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(forward_translator(
            Arc::downgrade(&session),
            receiver,
            cancellation.clone(),
        ));
        sender
            .send(TranslatorSignal::Translation(test_translation(
                session.id,
                old_epoch,
                "stale-run",
                "must not appear",
            )))
            .await
            .unwrap();
        sender
            .send(TranslatorSignal::Translation(test_translation(
                session.id,
                new_epoch,
                "fresh-run",
                "fresh content",
            )))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if session
                    .history()
                    .await
                    .unwrap()
                    .iter()
                    .any(|cue| cue.segment_ids.iter().any(|id| id.ends_with("fresh-run")))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            session
                .history()
                .await
                .unwrap()
                .iter()
                .all(|cue| { cue.segment_ids.iter().all(|id| !id.ends_with("stale-run")) })
        );
        cancellation.cancel();
        let _ = task.await;
        session.stop_model().await.unwrap();
    }

    #[tokio::test]
    async fn translation_events_use_their_own_source_generation_timeline() {
        let root = tempfile::tempdir().unwrap();
        let session = Arc::new(Session::new(
            PipelineConfig::default(),
            CoreOptions {
                audio_host: "unused".into(),
                runtime_dir: root.path().to_string_lossy().into_owned(),
                deployment: DeploymentConfig::default(),
            },
        ));
        *session.state.lock().await = SessionState::Running;
        session
            .timeline_offsets
            .lock()
            .await
            .insert((1, 1), 10_000_000_000);
        session
            .timeline_offset_ns
            .store(20_000_000_000, Ordering::Release);
        let (sender, receiver) = mpsc::channel(8);
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(forward_translator(
            Arc::downgrade(&session),
            receiver,
            cancellation.clone(),
        ));
        let mut old = test_translation(session.id, 1, "old", "old.");
        old.source_generation = 1;
        old.speakable = false;
        sender
            .send(TranslatorSignal::Translation(old))
            .await
            .unwrap();
        sender
            .send(TranslatorSignal::SourceReset {
                epoch: 1,
                source_generation: 2,
            })
            .await
            .unwrap();
        let mut new = test_translation(session.id, 1, "new", "new.");
        new.source_generation = 2;
        new.speakable = false;
        sender
            .send(TranslatorSignal::Translation(new))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while session.history().await.unwrap().len() < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let history = session.history().await.unwrap();
        assert_eq!(history[0].source_start_ns, 10_000_000_000);
        assert_eq!(history[1].source_start_ns, 20_000_000_000);
        assert!(history[0].segment_ids[0].contains("source-1-"));
        assert!(history[1].segment_ids[0].contains("source-2-"));
        cancellation.cancel();
        let _ = task.await;
    }

    #[tokio::test]
    async fn speech_arriving_during_output_recovery_is_submitted_after_commit() {
        let root = tempfile::tempdir().unwrap();
        let session = Arc::new(Session::new(
            PipelineConfig {
                source: SourceConfig::Mock { frames: None },
                ..PipelineConfig::default()
            },
            CoreOptions {
                audio_host: "unused".into(),
                runtime_dir: root.path().to_string_lossy().into_owned(),
                deployment: DeploymentConfig::default(),
            },
        ));
        session.prepare().await.unwrap();
        wait_for_state(&session, SessionState::Ready).await;
        session.start().await.unwrap();
        let (output, mut source, source_task) = {
            let mut guard = session.runtime.lock().await;
            let runtime = guard.as_mut().unwrap();
            (
                runtime.output.take().unwrap(),
                runtime.source.take().unwrap(),
                runtime.source_task.take().unwrap(),
            )
        };
        source_task.abort();
        let _ = source_task.await;
        source.stop().await.unwrap();
        session.pending_speech.lock().await.clear();
        let before = session.snapshot().await.metrics.playback_finished;

        let (sender, receiver) = mpsc::channel(8);
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(forward_translator(
            Arc::downgrade(&session),
            receiver,
            cancellation.clone(),
        ));
        sender
            .send(TranslatorSignal::Translation(test_translation(
                session.id,
                session.snapshot().await.epoch,
                "during-recovery",
                "queued during recovery",
            )))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while session.pending_speech.lock().await.is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        session.runtime.lock().await.as_mut().unwrap().output = Some(output);
        let operation = session.lifecycle.begin().await;
        session.flush_pending_speech(&operation).await.unwrap();
        drop(operation);
        assert!(session.pending_speech.lock().await.is_empty());
        tokio::time::timeout(Duration::from_secs(1), async {
            while session.snapshot().await.metrics.playback_finished <= before {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        cancellation.cancel();
        let _ = task.await;
        session.stop_model().await.unwrap();
    }

    #[tokio::test]
    async fn stale_playback_is_not_broadcast_and_stop_settles_pending_speech() {
        let root = tempfile::tempdir().unwrap();
        let session = Arc::new(Session::new(
            PipelineConfig::default(),
            CoreOptions {
                audio_host: "unused".into(),
                runtime_dir: root.path().to_string_lossy().into_owned(),
                deployment: DeploymentConfig::default(),
            },
        ));
        session.persist().await.unwrap();
        let (sender, receiver) = mpsc::channel(512);
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(forward_output(
            Arc::downgrade(&session),
            receiver,
            cancellation.clone(),
        ));
        let mut events = session.subscribe();
        let playback = |token, event_type| PlaybackEvent {
            event_type,
            segment_id: format!("segment-{token}"),
            token,
            queue_duration_us: 0,
            actual_start_mono_ns: None,
            source_end_ns: None,
        };
        for token in 1..=70 {
            sender
                .send(OutputSignal::Playback(playback(
                    token,
                    PlaybackEventType::Finished,
                )))
                .await
                .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while session.snapshot().await.metrics.playback_finished < 70 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        while events.try_recv().is_ok() {}
        sender
            .send(OutputSignal::Playback(playback(
                1,
                PlaybackEventType::Started,
            )))
            .await
            .unwrap();
        sender
            .send(OutputSignal::Playback(playback(
                1,
                PlaybackEventType::Queued,
            )))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(session.snapshot().await.metrics.playback_pending, 0);
        while let Ok(event) = events.try_recv() {
            assert!(!matches!(event.kind, SessionEventKind::Playback(_)));
        }

        sender
            .send(OutputSignal::Playback(playback(
                71,
                PlaybackEventType::Queued,
            )))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while session.snapshot().await.metrics.playback_pending != 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        session.stop_model().await.unwrap();
        let speech = session.store.speech_segments(session.id).await.unwrap();
        assert_eq!(speech.last().unwrap().token, 71);
        assert_eq!(speech.last().unwrap().status, "cancelled");
        cancellation.cancel();
        let _ = task.await;
    }

    #[tokio::test]
    async fn clear_history_waits_for_the_subtitle_write_critical_section() {
        let root = tempfile::tempdir().unwrap();
        let session = Arc::new(Session::new(
            PipelineConfig::default(),
            CoreOptions {
                audio_host: "unused".into(),
                runtime_dir: root.path().to_string_lossy().into_owned(),
                deployment: DeploymentConfig::default(),
            },
        ));
        session.persist().await.unwrap();
        let write = session.subtitle_writes.lock().await;
        let clearing = {
            let session = Arc::clone(&session);
            tokio::spawn(async move { session.clear_history().await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!clearing.is_finished());
        drop(write);
        tokio::time::timeout(Duration::from_secs(1), clearing)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(session.history().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn restored_history_and_new_uuid_cues_coexist() {
        let root = tempfile::tempdir().unwrap();
        let options = CoreOptions {
            audio_host: "unused".into(),
            runtime_dir: root.path().to_string_lossy().into_owned(),
            deployment: DeploymentConfig::default(),
        };
        let session = Session::new(PipelineConfig::default(), options.clone());
        session
            .record_subtitle(SubtitleCue {
                cue_id: "cue-0".into(),
                revision: 1,
                source_text: "old source".into(),
                translated_text: "old target".into(),
                source_start_ns: 0,
                source_end_ns: 1,
                is_final: true,
                segment_ids: vec!["old".into()],
            })
            .await;
        let restored = Session::restore(
            StoredSession {
                id: session.id,
                config: PipelineConfig::default(),
                metadata: session.metadata.lock().await.clone(),
            },
            options,
        );
        let event = test_translation(session.id, 2, "new-run", "new content");
        let cue = restored.subtitles.lock().await.push(&event);
        assert_ne!(cue.cue_id, "cue-0");
        assert_eq!(cue.cue_id.len(), 36);
        restored.record_subtitle(cue).await;
        let history = restored.history().await.unwrap();
        assert_eq!(history.len(), 2);
        assert!(history.iter().any(|cue| cue.cue_id == "cue-0"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn model_exit_before_ready_faults_and_cleans_runtime() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().unwrap();
        let executable = root.path().join("fake-model.py");
        let model = root.path().join("model.gguf");
        tokio::fs::write(
            &executable,
            "#!/usr/bin/env python3\nimport sys\nsys.exit(3)\n",
        )
        .await
        .unwrap();
        tokio::fs::write(&model, b"test").await.unwrap();
        let mut permissions = std::fs::metadata(&executable).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&executable, permissions).unwrap();
        let session = Arc::new(Session::new(
            PipelineConfig {
                translator: TranslatorConfig::HySimulmt {
                    direction: "en-zh".into(),
                    context: Some(16_384),
                    n_predict: Some(160),
                },
                ..PipelineConfig::default()
            },
            CoreOptions {
                audio_host: "unused".into(),
                runtime_dir: root.path().to_string_lossy().into_owned(),
                deployment: DeploymentConfig {
                    model_executable: executable,
                    model_path: model,
                    player_path: PathBuf::new(),
                },
            },
        ));
        session.prepare().await.unwrap();
        wait_for_state(&session, SessionState::Faulted).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while session.runtime.lock().await.is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("model runtime was not cleaned after early exit");
        assert!(session.snapshot().await.last_error.is_some());
    }

    #[tokio::test]
    async fn finite_source_drains_tail_and_returns_to_ready() {
        let root = tempfile::tempdir().unwrap();
        let session = Arc::new(Session::new(
            PipelineConfig {
                source: SourceConfig::Mock { frames: Some(1) },
                ..PipelineConfig::default()
            },
            CoreOptions {
                audio_host: "unused".into(),
                runtime_dir: root.path().to_string_lossy().into_owned(),
                deployment: DeploymentConfig::default(),
            },
        ));
        session.prepare().await.unwrap();
        wait_for_state(&session, SessionState::Ready).await;
        session.start().await.unwrap();
        wait_for_state(&session, SessionState::Ready).await;
        let snapshot = session.snapshot().await;
        assert_eq!(snapshot.metrics.sent_duration_us, 100_000);
        assert_eq!(snapshot.metrics.playback_pending, 0);
        assert_eq!(
            session.history().await.unwrap()[0].source_end_ns,
            100_000_000
        );
        assert!(snapshot.metrics.playback_finished >= 1);
        assert!(
            session
                .history()
                .await
                .unwrap()
                .iter()
                .all(|cue| cue.is_final)
        );
        session.stop_model().await.unwrap();
    }

    fn test_translation(
        session_id: Uuid,
        epoch: u64,
        segment_id: &str,
        text: &str,
    ) -> TranslationEvent {
        TranslationEvent {
            session_id,
            epoch,
            source_generation: 0,
            segment_id: segment_id.into(),
            revision: 0,
            event_type: TranslationEventType::Append,
            source_text: text.into(),
            translated_text: text.into(),
            source_start_ns: Some(0),
            source_end_ns: Some(800_000_000),
            generation_started_ns: 0,
            generation_finished_ns: monotonic_ns(),
            generation_duration_us: 0,
            speakable: true,
        }
    }

    #[tokio::test]
    async fn resident_mock_model_moves_between_sessions() {
        let root = std::env::temp_dir().join(format!("simulmt-model-handoff-{}", Uuid::new_v4()));
        let manager = SessionManager::new(CoreOptions {
            audio_host: "unused".into(),
            runtime_dir: root.to_string_lossy().into_owned(),
            deployment: DeploymentConfig::default(),
        });
        let first = manager.create(PipelineConfig::default()).await.unwrap();
        manager.prepare(first.id).await.unwrap();
        wait_for_state(&first, SessionState::Ready).await;

        let second = manager.create(PipelineConfig::default()).await.unwrap();
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
                deployment: DeploymentConfig::default(),
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

        let history = session.history().await.unwrap();
        assert_eq!(history.len(), 600);
        assert_eq!(history.first().unwrap().cue_id, "cue-0");
        assert_eq!(history.last().unwrap().cue_id, "cue-599");
        assert!(root.join("echonote.db").exists());

        session.clear_history().await.unwrap();
        assert!(session.history().await.unwrap().is_empty());
        assert!(root.join("echonote.db").exists());
        assert_eq!(session.store.history(session.id).await.unwrap(), Vec::new());
        tokio::fs::remove_dir_all(root).await.unwrap();
    }
}
