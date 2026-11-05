use crate::error::{CoreError, Result};
use crate::time::monotonic_ns;
use async_trait::async_trait;
use serde::Deserialize;
use simulmt_api::{AudioFrame, TranslationEvent, TranslationEventType, TranslatorConfig};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, Command};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use uuid::Uuid;

const PROMPT_HEAD: &str =
    "<｜hy_begin▁of▁sentence｜><｜reasoning_mode｜>reasoning_effort:no_think<｜hy_User｜>";
const PROMPT_TAIL: &str =
    "<__media__><｜hy_Assistant｜><think></think><｜hy_place▁holder▁no▁790｜>";
const HANDOFF_TURNS: usize = 4;
const STREAM_AUDIO: u8 = 1;
const STREAM_RESET_SOURCE: u8 = 2;

#[derive(Debug)]
pub enum TranslatorSignal {
    Ready,
    Translation(TranslationEvent),
    Completed {
        duration_us: u32,
        context_position: u32,
    },
    ContextRotated,
    ModelRestarted,
    SourceReset {
        epoch: u64,
        source_generation: u64,
    },
    StreamEnded(String),
    Warning(String),
}

#[async_trait]
pub trait RunningTranslator: Send {
    async fn push_audio(&mut self, frame: AudioFrame) -> Result<()>;
    async fn recover(&mut self) -> Result<()>;
    async fn reset_source(&mut self) -> Result<()>;
    async fn reattach(
        &mut self,
        session_id: Uuid,
        epoch: u64,
        sender: mpsc::Sender<TranslatorSignal>,
    ) -> Result<()>;
    async fn set_direction(&mut self, direction: &str) -> Result<()>;
    async fn stop(&mut self) -> Result<()>;
}

#[derive(Clone)]
struct TranslatorRoute {
    session_id: Uuid,
    epoch: u64,
    sender: mpsc::Sender<TranslatorSignal>,
}

type SharedRoute = Arc<std::sync::Mutex<TranslatorRoute>>;

struct MockTranslator {
    session_id: Uuid,
    epoch: u64,
    sender: mpsc::Sender<TranslatorSignal>,
    index: u64,
    timeline_us: u64,
    source_generation: u64,
}

#[async_trait]
impl RunningTranslator for MockTranslator {
    async fn push_audio(&mut self, frame: AudioFrame) -> Result<()> {
        let now = monotonic_ns();
        let index = self.index;
        self.index += 1;
        let source_start_ns = self.timeline_us.saturating_mul(1_000) as i64;
        self.timeline_us = self.timeline_us.saturating_add(frame.duration_us as u64);
        let source_end_ns = self.timeline_us.saturating_mul(1_000) as i64;
        let event = TranslationEvent {
            session_id: self.session_id,
            epoch: self.epoch,
            source_generation: self.source_generation,
            segment_id: format!("mock-{index}"),
            revision: 0,
            event_type: TranslationEventType::Append,
            source_text: format!("Mock source segment {index}"),
            translated_text: format!("模拟译文片段 {index}"),
            source_start_ns: Some(source_start_ns),
            source_end_ns: Some(source_end_ns),
            generation_started_ns: now,
            generation_finished_ns: monotonic_ns(),
            generation_duration_us: 50,
            speakable: true,
        };
        let _ = self.sender.send(TranslatorSignal::Translation(event)).await;
        let _ = self
            .sender
            .send(TranslatorSignal::Completed {
                duration_us: frame.duration_us,
                context_position: index as u32,
            })
            .await;
        Ok(())
    }

    async fn recover(&mut self) -> Result<()> {
        Ok(())
    }

    async fn reset_source(&mut self) -> Result<()> {
        self.source_generation += 1;
        self.index = 0;
        self.timeline_us = 0;
        let _ = self
            .sender
            .send(TranslatorSignal::SourceReset {
                epoch: self.epoch,
                source_generation: self.source_generation,
            })
            .await;
        Ok(())
    }

    async fn reattach(
        &mut self,
        session_id: Uuid,
        epoch: u64,
        sender: mpsc::Sender<TranslatorSignal>,
    ) -> Result<()> {
        self.session_id = session_id;
        self.epoch = epoch;
        self.sender = sender;
        self.index = 0;
        self.timeline_us = 0;
        self.reset_source().await
    }

    async fn set_direction(&mut self, _direction: &str) -> Result<()> {
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct HyConfig {
    executable: String,
    model: String,
    direction: String,
    context: u32,
    n_predict: u32,
    runtime_dir: String,
}

#[derive(Default)]
struct HandoffTurn {
    source: String,
    target: String,
}

type SharedHandoff = Arc<std::sync::Mutex<VecDeque<HandoffTurn>>>;
type SharedPendingAudio = Arc<std::sync::Mutex<VecDeque<AudioFrame>>>;

struct HyShared {
    route: SharedRoute,
    rotate_requested: Arc<AtomicBool>,
    handoff: SharedHandoff,
    pending_audio: SharedPendingAudio,
    timeline_us: Arc<AtomicU64>,
    source_generation: Arc<AtomicU64>,
}

struct HyProcess {
    child: Child,
    stdin: ChildStdin,
    reader: JoinHandle<()>,
    logger: JoinHandle<()>,
}

impl HyProcess {
    async fn stop(&mut self) {
        self.reader.abort();
        self.logger.abort();
        let _ = (&mut self.reader).await;
        let _ = (&mut self.logger).await;
        let _ = self.stdin.shutdown().await;
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
}

struct HyTranslator {
    config: HyConfig,
    session_id: Uuid,
    epoch: u64,
    sender: mpsc::Sender<TranslatorSignal>,
    route: SharedRoute,
    process: HyProcess,
    rotate_requested: Arc<AtomicBool>,
    handoff: SharedHandoff,
    pending_audio: SharedPendingAudio,
    timeline_us: Arc<AtomicU64>,
    source_generation: Arc<AtomicU64>,
    generation: u64,
}

impl HyTranslator {
    async fn restart(&mut self, reason: &str) -> Result<()> {
        self.process.stop().await;
        self.generation += 1;
        self.rotate_requested.store(false, Ordering::Release);
        self.process = spawn_hy_process(
            &self.config,
            self.generation,
            HyShared {
                route: Arc::clone(&self.route),
                rotate_requested: Arc::clone(&self.rotate_requested),
                handoff: Arc::clone(&self.handoff),
                pending_audio: Arc::clone(&self.pending_audio),
                timeline_us: Arc::clone(&self.timeline_us),
                source_generation: Arc::clone(&self.source_generation),
            },
        )
        .await?;
        let replay = self
            .pending_audio
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        for frame in replay {
            write_stream_frame(
                &mut self.process.stdin,
                STREAM_AUDIO,
                self.source_generation.load(Ordering::Acquire),
                &frame.payload,
            )
            .await?;
        }
        self.process.stdin.flush().await?;
        let _ = self.sender.send(TranslatorSignal::ModelRestarted).await;
        let _ = self
            .sender
            .send(TranslatorSignal::Warning(format!(
                "model process restarted ({reason}); recent context retained"
            )))
            .await;
        Ok(())
    }

    fn record_audio_written(&self, frame: AudioFrame) {
        self.pending_audio
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push_back(frame);
    }
}

#[async_trait]
impl RunningTranslator for HyTranslator {
    async fn push_audio(&mut self, frame: AudioFrame) -> Result<()> {
        if self.rotate_requested.swap(false, Ordering::AcqRel) {
            self.restart("in-process context rotation unavailable")
                .await?;
        } else {
            let status = self.process.child.try_wait().map_err(|error| {
                CoreError::Translator(format!("failed to inspect model: {error}"))
            })?;
            if let Some(status) = status {
                self.restart(&format!("model exited with {status}")).await?;
            }
        }

        self.record_audio_written(frame.clone());
        let source_generation = self.source_generation.load(Ordering::Acquire);
        let first_write = async {
            write_stream_frame(
                &mut self.process.stdin,
                STREAM_AUDIO,
                source_generation,
                &frame.payload,
            )
            .await?;
            self.process.stdin.flush().await
        }
        .await;
        if let Err(first_error) = first_write {
            self.restart(&format!("model write failed: {first_error}"))
                .await?;
        }
        Ok(())
    }

    async fn recover(&mut self) -> Result<()> {
        self.restart("model stream ended").await
    }

    async fn reset_source(&mut self) -> Result<()> {
        let source_generation = self.source_generation.fetch_add(1, Ordering::AcqRel) + 1;
        self.pending_audio
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
        self.handoff
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
        self.timeline_us.store(0, Ordering::Release);
        self.rotate_requested.store(false, Ordering::Release);
        let result = async {
            write_stream_frame(
                &mut self.process.stdin,
                STREAM_RESET_SOURCE,
                source_generation,
                &[],
            )
            .await?;
            self.process.stdin.flush().await
        }
        .await;
        if let Err(error) = result {
            self.restart(&format!("source reset failed: {error}"))
                .await?;
        }
        Ok(())
    }

    async fn reattach(
        &mut self,
        session_id: Uuid,
        epoch: u64,
        sender: mpsc::Sender<TranslatorSignal>,
    ) -> Result<()> {
        // A new session must not inherit audio or linguistic context, but the
        // expensive model process and its loaded weights remain resident.
        self.reset_source().await?;
        self.session_id = session_id;
        self.epoch = epoch;
        self.sender = sender.clone();
        *self.route.lock().unwrap_or_else(|error| error.into_inner()) = TranslatorRoute {
            session_id,
            epoch,
            sender,
        };
        Ok(())
    }

    async fn set_direction(&mut self, direction: &str) -> Result<()> {
        if !matches!(direction, "en-zh" | "zh-en") {
            return Err(CoreError::InvalidConfig(format!(
                "unsupported translation direction: {direction}"
            )));
        }
        if self.config.direction == direction {
            return Ok(());
        }
        self.config.direction = direction.to_string();
        self.source_generation.fetch_add(1, Ordering::AcqRel);
        self.pending_audio
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
        self.handoff
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
        self.restart("translation direction changed").await
    }

    async fn stop(&mut self) -> Result<()> {
        self.process.stop().await;
        self.pending_audio
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
        Ok(())
    }
}

pub async fn start_translator(
    config: &TranslatorConfig,
    model_executable: &Path,
    model_path: &Path,
    session_id: Uuid,
    epoch: u64,
    sender: mpsc::Sender<TranslatorSignal>,
    runtime_dir: &str,
) -> Result<Box<dyn RunningTranslator>> {
    match config {
        TranslatorConfig::Mock => {
            let _ = sender.send(TranslatorSignal::Ready).await;
            Ok(Box::new(MockTranslator {
                session_id,
                epoch,
                sender,
                index: 0,
                timeline_us: 0,
                source_generation: 0,
            }))
        }
        TranslatorConfig::HySimulmt {
            direction,
            context,
            n_predict,
        } => {
            let config = HyConfig {
                executable: model_executable.to_string_lossy().into_owned(),
                model: model_path.to_string_lossy().into_owned(),
                direction: direction.clone(),
                context: context.unwrap_or(16_384),
                n_predict: n_predict.unwrap_or(160),
                runtime_dir: runtime_dir.to_string(),
            };
            let translator = start_hy(config, session_id, epoch, sender).await?;
            Ok(Box::new(translator))
        }
    }
}

async fn start_hy(
    config: HyConfig,
    session_id: Uuid,
    epoch: u64,
    sender: mpsc::Sender<TranslatorSignal>,
) -> Result<HyTranslator> {
    if !Path::new(&config.executable).exists() || !Path::new(&config.model).exists() {
        return Err(CoreError::InvalidConfig(
            "Hy-SimulMT executable or model does not exist".into(),
        ));
    }
    let rotate_requested = Arc::new(AtomicBool::new(false));
    let handoff = SharedHandoff::default();
    let pending_audio = SharedPendingAudio::default();
    let timeline_us = Arc::new(AtomicU64::new(0));
    let source_generation = Arc::new(AtomicU64::new(0));
    let route = Arc::new(std::sync::Mutex::new(TranslatorRoute {
        session_id,
        epoch,
        sender: sender.clone(),
    }));
    let process = spawn_hy_process(
        &config,
        0,
        HyShared {
            route: Arc::clone(&route),
            rotate_requested: Arc::clone(&rotate_requested),
            handoff: Arc::clone(&handoff),
            pending_audio: Arc::clone(&pending_audio),
            timeline_us: Arc::clone(&timeline_us),
            source_generation: Arc::clone(&source_generation),
        },
    )
    .await?;
    Ok(HyTranslator {
        config,
        session_id,
        epoch,
        sender,
        route,
        process,
        rotate_requested,
        handoff,
        pending_audio,
        timeline_us,
        source_generation,
        generation: 0,
    })
}

async fn write_stream_frame(
    stdin: &mut ChildStdin,
    kind: u8,
    source_generation: u64,
    payload: &[u8],
) -> std::io::Result<()> {
    let mut header = [0_u8; 20];
    header[..4].copy_from_slice(b"SMT2");
    header[4] = kind;
    header[8..12].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    header[12..20].copy_from_slice(&source_generation.to_le_bytes());
    stdin.write_all(&header).await?;
    stdin.write_all(payload).await
}

async fn spawn_hy_process(
    config: &HyConfig,
    generation: u64,
    shared: HyShared,
) -> Result<HyProcess> {
    let HyShared {
        route,
        rotate_requested,
        handoff,
        pending_audio,
        timeline_us,
        source_generation,
    } = shared;
    let prompt = build_prompt(&config.direction, &handoff)?;
    tokio::fs::create_dir_all(&config.runtime_dir).await?;
    let log_path = Path::new(&config.runtime_dir).join("model.log");
    let mut command = Command::new(&config.executable);
    command
        .arg("-m")
        .arg(&config.model)
        .arg("--stream-v2")
        .arg("-c")
        .arg(config.context.to_string())
        .arg("-n")
        .arg(config.n_predict.to_string())
        .arg("--temp")
        .arg("0")
        .arg("-p")
        .arg(prompt)
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .map_err(|error| CoreError::Translator(format!("failed to spawn model: {error}")))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| CoreError::Translator("missing model stdin".into()))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| CoreError::Translator("missing model stdout".into()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| CoreError::Translator("missing model stderr".into()))?;
    let logger = tokio::spawn(log_model_stderr(stderr, log_path));
    let reader = tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        let mut finished_normally = false;
        while let Ok(Some(line)) = lines.next_line().await {
            let (session_id, epoch, sender) = {
                let route = route.lock().unwrap_or_else(|error| error.into_inner());
                (route.session_id, route.epoch, route.sender.clone())
            };
            match serde_json::from_str::<HyLine>(&line) {
                Ok(message) if message.kind == "ready" => {
                    let _ = sender.send(TranslatorSignal::Ready).await;
                }
                Ok(message) if message.kind == "turn" => {
                    if message.source_generation.unwrap_or(0)
                        != source_generation.load(Ordering::Acquire)
                    {
                        continue;
                    }
                    let duration_us = message.ms.unwrap_or(800) * 1_000;
                    let acknowledged_duration_us =
                        acknowledge_audio(&pending_audio, duration_us as u64);
                    let source_start_ns = (timeline_us
                        .fetch_add(acknowledged_duration_us, Ordering::AcqRel)
                        * 1_000) as i64;
                    let source_end_ns = (timeline_us.load(Ordering::Acquire) * 1_000) as i64;
                    let position = message.pos.unwrap_or(0);
                    let src = message.src.unwrap_or_default().trim().to_string();
                    let tgt = message.tgt.unwrap_or_default().trim().to_string();
                    if !src.is_empty() || !tgt.is_empty() {
                        if let Ok(mut turns) = handoff.lock() {
                            turns.push_back(HandoffTurn {
                                source: src.clone(),
                                target: tgt.clone(),
                            });
                            while turns.len() > HANDOFF_TURNS {
                                turns.pop_front();
                            }
                        }
                        let index = message.i.unwrap_or(0);
                        let now = monotonic_ns();
                        let event = TranslationEvent {
                            session_id,
                            epoch,
                            source_generation: message.source_generation.unwrap_or(0),
                            segment_id: format!("hy-{generation}-{index}"),
                            revision: 0,
                            event_type: TranslationEventType::Append,
                            source_text: src.clone(),
                            translated_text: tgt.clone(),
                            source_start_ns: Some(source_start_ns),
                            source_end_ns: Some(source_end_ns),
                            generation_started_ns: now
                                - message.gen_ms.unwrap_or(0.0) as i64 * 1_000,
                            generation_finished_ns: now,
                            generation_duration_us: message.gen_ms.unwrap_or(0.0) as u32 * 1_000,
                            speakable: !tgt.is_empty(),
                        };
                        let _ = sender.send(TranslatorSignal::Translation(event)).await;
                    }
                    let _ = sender
                        .send(TranslatorSignal::Completed {
                            duration_us: acknowledged_duration_us.min(u32::MAX as u64) as u32,
                            context_position: position,
                        })
                        .await;
                }
                Ok(message) if message.kind == "rotate" => {
                    if message.source_generation.unwrap_or(0)
                        == source_generation.load(Ordering::Acquire)
                    {
                        let _ = sender.send(TranslatorSignal::ContextRotated).await;
                    }
                }
                Ok(message) if message.kind == "rotate_fallback" => {
                    if message.source_generation.unwrap_or(0)
                        == source_generation.load(Ordering::Acquire)
                    {
                        rotate_requested.store(true, Ordering::Release);
                        let _ = sender
                            .send(TranslatorSignal::Warning(
                                "in-process context rotation unavailable; recovery scheduled"
                                    .into(),
                            ))
                            .await;
                    }
                }
                Ok(message) if message.kind == "reset" => {
                    if message.source_generation.unwrap_or(0)
                        == source_generation.load(Ordering::Acquire)
                    {
                        let _ = sender
                            .send(TranslatorSignal::SourceReset {
                                epoch,
                                source_generation: message.source_generation.unwrap_or(0),
                            })
                            .await;
                    }
                }
                Ok(message) if message.kind == "done" => {
                    finished_normally = true;
                    break;
                }
                Ok(_) => {}
                Err(_) => {
                    tracing::debug!(target: "translator", "ignored non-JSON model output: {line}");
                }
            }
        }
        if !finished_normally {
            let sender = route
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .sender
                .clone();
            let _ = sender
                .send(TranslatorSignal::StreamEnded(
                    "model stream ended; the next audio block will trigger recovery".into(),
                ))
                .await;
        }
    });
    Ok(HyProcess {
        child,
        stdin,
        reader,
        logger,
    })
}

const MODEL_LOG_LIMIT_BYTES: u64 = 5 * 1024 * 1024;

struct ModelLog {
    path: PathBuf,
    backup: PathBuf,
    file: Option<File>,
    len: u64,
}

impl ModelLog {
    fn new(path: PathBuf) -> Self {
        Self {
            backup: path.with_extension("log.1"),
            path,
            file: None,
            len: 0,
        }
    }

    async fn open(&mut self) -> std::io::Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;
        self.len = file.metadata().await?.len();
        self.file = Some(file);
        Ok(())
    }

    async fn append(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        if self.file.is_none() {
            self.open().await?;
        }
        if self.len >= MODEL_LOG_LIMIT_BYTES {
            self.file.take();
            match tokio::fs::remove_file(&self.backup).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            tokio::fs::rename(&self.path, &self.backup).await?;
            self.open().await?;
        }
        let file = self.file.as_mut().expect("model log opened");
        file.write_all(bytes).await?;
        self.len += bytes.len() as u64;
        Ok(())
    }
}

async fn log_model_stderr(stderr: ChildStderr, path: PathBuf) {
    let mut lines = BufReader::new(stderr);
    let mut log = ModelLog::new(path);
    let mut buffer = Vec::new();
    loop {
        buffer.clear();
        match lines.read_until(b'\n', &mut buffer).await {
            Ok(0) => break,
            Ok(_) => {
                if let Err(error) = log.append(&buffer).await {
                    tracing::warn!("failed to write model log: {error}");
                }
            }
            Err(error) => {
                tracing::warn!("failed to read model log: {error}");
                break;
            }
        }
    }
}

fn acknowledge_audio(pending_audio: &SharedPendingAudio, mut duration_us: u64) -> u64 {
    let mut acknowledged_us = 0_u64;
    let mut pending = pending_audio
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    while duration_us > 0 {
        let Some(front) = pending.front_mut() else {
            break;
        };
        let front_duration = front.duration_us as u64;
        if front_duration == 0 {
            pending.pop_front();
            continue;
        }
        if duration_us >= front_duration {
            acknowledged_us += front_duration;
            duration_us -= front_duration;
            pending.pop_front();
            continue;
        }
        acknowledged_us += duration_us;
        let consumed_bytes = (front.payload.len() as u64 * duration_us / front_duration) as usize;
        let consumed_bytes = consumed_bytes - consumed_bytes % 2;
        front.payload.drain(..consumed_bytes);
        front.duration_us -= duration_us as u32;
        duration_us = 0;
    }
    acknowledged_us
}

fn build_prompt(direction: &str, handoff: &SharedHandoff) -> Result<String> {
    let turns = handoff.lock().unwrap_or_else(|error| error.into_inner());
    let source = turns
        .iter()
        .map(|turn| turn.source.as_str())
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    let target = turns
        .iter()
        .map(|turn| turn.target.as_str())
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    let has_handoff = !source.is_empty() || !target.is_empty();
    let instruction = match (direction, has_handoff) {
        ("en-zh", false) => {
            "请完成下面的同声传译任务，将陆续收到的语音翻译成中文。请结合上下文边听边译，输出能够确定的译文，不必等待整句结束。".to_string()
        }
        ("en-zh", true) => format!(
            "请完成下面的同声传译任务，将陆续收到的语音翻译成中文。请结合上下文边听边译，输出能够确定的译文，不必等待整句结束。\n以下最近内容只用于理解上下文，不要重复翻译或输出：\n最近原文：{source}\n最近译文：{target}\n请继续翻译接下来收到的新语音。"
        ),
        ("zh-en", false) => {
            "Please act as a simultaneous interpreter. Translate the incoming speech into English as you listen. Output what you are confident about instead of waiting for the whole sentence to finish.".to_string()
        }
        ("zh-en", true) => format!(
            "Please act as a simultaneous interpreter. Translate the incoming speech into English as you listen. Output what you are confident about instead of waiting for the whole sentence to finish.\nThe following recent text is context only. Do not translate or output it again:\nRecent source: {source}\nRecent translation: {target}\nContinue with the new incoming speech."
        ),
        (other, _) => {
            return Err(CoreError::InvalidConfig(format!(
                "unsupported translation direction: {other}"
            )));
        }
    };
    Ok(format!("{PROMPT_HEAD}{instruction}{PROMPT_TAIL}"))
}

#[derive(Debug, Deserialize)]
struct HyLine {
    #[serde(rename = "type")]
    kind: String,
    i: Option<u64>,
    ms: Option<u32>,
    src: Option<String>,
    tgt: Option<String>,
    gen_ms: Option<f64>,
    pos: Option<u32>,
    source_generation: Option<u64>,
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use simulmt_api::AudioFormat;
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    fn audio(sequence: u64) -> AudioFrame {
        AudioFrame {
            session_id: Uuid::nil(),
            epoch: 1,
            sequence,
            source_id: "test".into(),
            format: AudioFormat::default(),
            payload: vec![0; 3_200],
            source_pts_ns: None,
            capture_mono_ns: 0,
            ingress_mono_ns: 0,
            duration_us: 100_000,
            discontinuity: sequence == 0,
        }
    }

    async fn fake_model() -> (std::path::PathBuf, HyConfig, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!("simulmt-translator-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.unwrap();
        let executable = root.join("fake-model.py");
        let invocations = root.join("invocations.jsonl");
        let model = root.join("model.gguf");
        let script = format!(
            r#"#!/usr/bin/env python3
import json
import sys
from pathlib import Path

with Path({invocations:?}).open("a", encoding="utf-8") as output:
    output.write(json.dumps({{"prompt": sys.argv[sys.argv.index("-p") + 1]}}) + "\n")
print(json.dumps({{"type": "ready"}}), flush=True)
index = 0

def read_exact(size):
    data = bytearray()
    while len(data) < size:
        part = sys.stdin.buffer.read(size - len(data))
        if not part:
            return bytes(data)
        data.extend(part)
    return bytes(data)

while True:
    header = read_exact(20)
    if not header:
        break
    assert len(header) == 20 and header[:4] == b"SMT2"
    kind = header[4]
    size = int.from_bytes(header[8:12], "little")
    source_generation = int.from_bytes(header[12:20], "little")
    payload = read_exact(size)
    assert len(payload) == size
    if kind == 2:
        print(json.dumps({{"type": "reset", "source_generation": source_generation}}), flush=True)
        continue
    assert kind == 1 and size == 3200
    print(json.dumps({{
        "type": "turn", "i": index, "ms": 800,
        "src": "hello", "tgt": "你好", "pos": 100,
        "source_generation": source_generation
    }}), flush=True)
    if index == 0:
        print(json.dumps({{
            "type": "rotate", "pos": 100, "source_generation": source_generation
        }}), flush=True)
    index += 1
"#,
            invocations = invocations.to_string_lossy().to_string()
        );
        tokio::fs::write(&executable, script).await.unwrap();
        tokio::fs::write(&model, b"test").await.unwrap();
        let mut permissions = std::fs::metadata(&executable).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&executable, permissions).unwrap();
        let config = HyConfig {
            executable: executable.to_string_lossy().into_owned(),
            model: model.to_string_lossy().into_owned(),
            direction: "en-zh".into(),
            context: 16_384,
            n_predict: 160,
            runtime_dir: root.to_string_lossy().into_owned(),
        };
        (root, config, invocations)
    }

    async fn next_translation(receiver: &mut mpsc::Receiver<TranslatorSignal>) -> TranslationEvent {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match receiver.recv().await {
                    Some(TranslatorSignal::Translation(event)) => return event,
                    Some(_) => {}
                    None => panic!("translator channel closed"),
                }
            }
        })
        .await
        .expect("translation timed out")
    }

    async fn wait_for_invocations(path: &Path, expected: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(contents) = tokio::fs::read_to_string(path).await
                    && contents.lines().count() >= expected
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("model invocation timed out");
    }

    async fn wait_for_rotation(receiver: &mut mpsc::Receiver<TranslatorSignal>) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match receiver.recv().await {
                    Some(TranslatorSignal::ContextRotated) => return,
                    Some(_) => {}
                    None => panic!("translator channel closed"),
                }
            }
        })
        .await
        .expect("rotation timed out");
    }

    async fn wait_for_source_reset(receiver: &mut mpsc::Receiver<TranslatorSignal>) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match receiver.recv().await {
                    Some(TranslatorSignal::SourceReset { .. }) => return,
                    Some(_) => {}
                    None => panic!("translator channel closed"),
                }
            }
        })
        .await
        .expect("source reset timed out");
    }

    #[test]
    fn acknowledgement_uses_valid_tail_duration_instead_of_model_padding() {
        let pending = SharedPendingAudio::default();
        pending.lock().unwrap().push_back(audio(0));
        assert_eq!(acknowledge_audio(&pending, 800_000), 100_000);
        assert!(pending.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn rotates_in_process_and_recovers_after_process_exit() {
        let (root, config, invocations) = fake_model().await;
        let (sender, mut receiver) = mpsc::channel(64);
        let mut translator = start_hy(config, Uuid::nil(), 1, sender).await.unwrap();

        for sequence in 0..8 {
            translator.push_audio(audio(sequence)).await.unwrap();
        }
        let first = next_translation(&mut receiver).await;
        wait_for_rotation(&mut receiver).await;
        assert_eq!(first.segment_id, "hy-0-0");
        assert_eq!(first.source_start_ns, Some(0));

        for sequence in 8..16 {
            translator.push_audio(audio(sequence)).await.unwrap();
        }
        let second = next_translation(&mut receiver).await;
        assert_eq!(second.segment_id, "hy-0-1");
        assert_eq!(second.source_start_ns, Some(800_000_000));
        assert_eq!(translator.generation, 0);
        wait_for_invocations(&invocations, 1).await;

        translator.process.child.kill().await.unwrap();
        translator.process.child.wait().await.unwrap();
        for sequence in 16..20 {
            translator.push_audio(audio(sequence)).await.unwrap();
        }
        wait_for_invocations(&invocations, 2).await;
        translator.process.child.kill().await.unwrap();
        translator.process.child.wait().await.unwrap();
        for sequence in 20..28 {
            translator.push_audio(audio(sequence)).await.unwrap();
        }
        let third = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = next_translation(&mut receiver).await;
                if event.segment_id.starts_with("hy-2-") {
                    break event;
                }
            }
        })
        .await
        .expect("restarted translation timed out");
        assert_eq!(third.segment_id, "hy-2-0");
        assert!(third.source_start_ns.unwrap() >= 1_600_000_000);
        translator.stop().await.unwrap();

        let invocations = tokio::fs::read_to_string(invocations).await.unwrap();
        let prompts = invocations
            .lines()
            .map(|line| {
                serde_json::from_str::<serde_json::Value>(line).unwrap()["prompt"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect::<Vec<_>>();
        assert_eq!(prompts.len(), 3);
        assert!(!prompts[0].contains("最近原文"));
        assert!(prompts[1].contains("最近原文：hello"));
        assert!(prompts[1].contains("最近译文：你好"));
        assert!(prompts[2].contains("不要重复翻译或输出"));
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn source_reset_clears_recovery_state_without_restarting() {
        let (root, config, invocations) = fake_model().await;
        let (sender, mut receiver) = mpsc::channel(64);
        let mut translator = start_hy(config, Uuid::nil(), 1, sender).await.unwrap();
        translator.push_audio(audio(0)).await.unwrap();
        let first = next_translation(&mut receiver).await;
        assert_eq!(first.source_start_ns, Some(0));

        translator.reset_source().await.unwrap();
        wait_for_source_reset(&mut receiver).await;
        assert!(translator.pending_audio.lock().unwrap().is_empty());
        assert!(translator.handoff.lock().unwrap().is_empty());
        assert_eq!(translator.timeline_us.load(Ordering::Acquire), 0);
        assert_eq!(translator.source_generation.load(Ordering::Acquire), 1);
        assert_eq!(translator.generation, 0);

        translator.push_audio(audio(1)).await.unwrap();
        let second = next_translation(&mut receiver).await;
        assert_eq!(second.segment_id, "hy-0-1");
        assert_eq!(second.source_start_ns, Some(0));
        translator.stop().await.unwrap();
        wait_for_invocations(&invocations, 1).await;
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn reattach_routes_events_without_reloading_model() {
        let (root, config, invocations) = fake_model().await;
        let (sender, mut old_receiver) = mpsc::channel(64);
        let mut translator = start_hy(config, Uuid::nil(), 1, sender).await.unwrap();
        translator.push_audio(audio(0)).await.unwrap();
        let first = next_translation(&mut old_receiver).await;
        assert_eq!(first.session_id, Uuid::nil());

        let new_session = Uuid::new_v4();
        let (new_sender, mut new_receiver) = mpsc::channel(64);
        translator
            .reattach(new_session, 7, new_sender)
            .await
            .unwrap();
        assert!(translator.handoff.lock().unwrap().is_empty());
        translator.push_audio(audio(1)).await.unwrap();
        let second = next_translation(&mut new_receiver).await;
        assert_eq!(second.session_id, new_session);
        assert_eq!(second.epoch, 7);
        assert_eq!(second.source_start_ns, Some(0));

        translator.stop().await.unwrap();
        wait_for_invocations(&invocations, 1).await;
        assert_eq!(
            tokio::fs::read_to_string(invocations)
                .await
                .unwrap()
                .lines()
                .count(),
            1
        );
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[test]
    fn handoff_is_bounded_to_recent_turns() {
        let handoff = SharedHandoff::default();
        {
            let mut turns = handoff.lock().unwrap();
            for index in 0..6 {
                turns.push_back(HandoffTurn {
                    source: format!("source-{index}"),
                    target: format!("target-{index}"),
                });
                while turns.len() > HANDOFF_TURNS {
                    turns.pop_front();
                }
            }
        }
        let prompt = build_prompt("en-zh", &handoff).unwrap();
        assert!(!prompt.contains("source-1"));
        assert!(prompt.contains("source-2"));
        assert!(prompt.contains("source-5"));
        assert!(prompt.contains("target-5"));
    }

    #[tokio::test]
    async fn model_log_keeps_one_bounded_backup() {
        let root = std::env::temp_dir().join(format!("simulmt-model-log-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.unwrap();
        let path = root.join("model.log");
        let mut log = ModelLog::new(path.clone());
        log.append(&vec![b'x'; MODEL_LOG_LIMIT_BYTES as usize])
            .await
            .unwrap();
        log.append(b"current\n").await.unwrap();
        drop(log);

        let backup = tokio::fs::read(root.join("model.log.1")).await.unwrap();
        let current = tokio::fs::read_to_string(path).await.unwrap();
        assert_eq!(backup.len(), MODEL_LOG_LIMIT_BYTES as usize);
        assert!(backup.ends_with(b"xxxx"));
        assert_eq!(current, "current\n");
        tokio::fs::remove_dir_all(root).await.unwrap();
    }
}
