use crate::error::{CoreError, Result};
use crate::time::monotonic_ns;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use simulmt_api::{LatencyMode, OutputConfig, PlaybackEvent, PlaybackEventType, TranslationEvent};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use uuid::Uuid;

#[derive(Debug)]
pub enum OutputSignal {
    Playback(PlaybackEvent),
    QueueDepth(u32),
    Warning(String),
    Error(String),
    Exited,
}

#[async_trait]
pub trait PlaybackControl: Send {
    fn playback_pid(&self) -> Option<u32>;
    async fn submit(&mut self, event: TranslationEvent) -> Result<()>;
    async fn set_latency_mode(&mut self, mode: LatencyMode) -> Result<()>;
    async fn set_voice(&mut self, voice: String) -> Result<()>;
    async fn finish(&mut self) -> Result<()>;
    async fn pause(&mut self) -> Result<()>;
    async fn resume(&mut self) -> Result<()>;
    async fn flush(&mut self) -> Result<()>;
    async fn stop(&mut self) -> Result<()>;
}

pub struct OutputRunner;

impl OutputRunner {
    pub async fn start(
        config: &OutputConfig,
        latency_mode: LatencyMode,
        session_id: Uuid,
        epoch: u64,
        sender: mpsc::Sender<OutputSignal>,
    ) -> Result<Box<dyn PlaybackControl>> {
        match config {
            OutputConfig::Mock => Ok(Box::new(MockOutput {
                sender,
                next_token: 0,
            })),
            OutputConfig::MacosSystem {
                player_path,
                cache_dir,
                voice,
                speech_rate,
            } => MacOutput::start(
                player_path,
                cache_dir,
                voice.clone(),
                *speech_rate,
                latency_mode,
                session_id,
                epoch,
                sender,
            )
            .await
            .map(|output| Box::new(output) as Box<dyn PlaybackControl>),
        }
    }
}

struct MockOutput {
    sender: mpsc::Sender<OutputSignal>,
    next_token: u64,
}

#[async_trait]
impl PlaybackControl for MockOutput {
    fn playback_pid(&self) -> Option<u32> {
        None
    }

    async fn submit(&mut self, event: TranslationEvent) -> Result<()> {
        self.next_token += 1;
        let token = self.next_token;
        for event_type in [
            PlaybackEventType::Queued,
            PlaybackEventType::Started,
            PlaybackEventType::Finished,
        ] {
            let _ = self
                .sender
                .send(OutputSignal::Playback(PlaybackEvent {
                    event_type,
                    segment_id: event.segment_id.clone(),
                    token,
                    queue_duration_us: 0,
                    actual_start_mono_ns: Some(monotonic_ns()),
                    source_end_ns: event.source_end_ns,
                }))
                .await;
        }
        Ok(())
    }
    async fn set_latency_mode(&mut self, _mode: LatencyMode) -> Result<()> {
        Ok(())
    }
    async fn set_voice(&mut self, _voice: String) -> Result<()> {
        Ok(())
    }
    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }
    async fn pause(&mut self) -> Result<()> {
        Ok(())
    }
    async fn resume(&mut self) -> Result<()> {
        Ok(())
    }
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    async fn stop(&mut self) -> Result<()> {
        Ok(())
    }
}

enum OutputRequest {
    Speak(TranslationEvent),
    SetMode(LatencyMode),
    SetVoice(String),
    Finish,
    Flush,
}

struct SpeechAssembler {
    pending: Option<TranslationEvent>,
    emitted: u64,
    mode: LatencyMode,
}

impl Default for SpeechAssembler {
    fn default() -> Self {
        Self {
            pending: None,
            emitted: 0,
            mode: LatencyMode::Balanced,
        }
    }
}

impl SpeechAssembler {
    fn push(&mut self, event: TranslationEvent) -> Option<TranslationEvent> {
        if let Some(pending) = self.pending.as_mut() {
            append_delta(&mut pending.source_text, &event.source_text);
            append_delta(&mut pending.translated_text, &event.translated_text);
            pending.segment_id = event.segment_id;
            pending.source_end_ns = event.source_end_ns.or(pending.source_end_ns);
            pending.generation_started_ns = event.generation_started_ns;
            pending.generation_finished_ns = event.generation_finished_ns;
            pending.generation_duration_us = event.generation_duration_us;
        } else {
            self.pending = Some(event);
        }
        let pending = self.pending.as_ref().expect("speech pending");

        let duration_us = pending
            .source_start_ns
            .zip(pending.source_end_ns)
            .map(|(start, end)| (end - start).max(0) / 1_000)
            .unwrap_or(0);
        let target_us = match (self.mode, self.emitted) {
            (LatencyMode::LowLatency, _) => 1_600_000,
            (LatencyMode::Balanced, 0) => 1_600_000,
            (LatencyMode::Balanced, _) => 2_400_000,
            (LatencyMode::Complete, _) => 4_800_000,
        };
        let text = pending.translated_text.trim();
        let word_count = text.split_whitespace().count();
        let sentence_end = ends_with(text, &['.', '!', '?', '。', '！', '？']);
        let clause_end = ends_with(text, &[',', ';', ':', '，', '；', '：']);
        let (character_limit, word_limit) = match self.mode {
            LatencyMode::LowLatency => (28, 12),
            LatencyMode::Balanced => (36, 16),
            LatencyMode::Complete => (80, 32),
        };
        if sentence_end
            || (clause_end && duration_us >= 1_600_000)
            || duration_us >= target_us
            || text.chars().count() >= character_limit
            || word_count >= word_limit
        {
            self.emitted += 1;
            return self.pending.take();
        }
        None
    }

    fn finish(&mut self) -> Option<TranslationEvent> {
        self.pending.take()
    }

    fn clear(&mut self) {
        self.pending = None;
        self.emitted = 0;
    }
}

fn append_delta(text: &mut String, delta: &str) {
    let delta = delta.trim();
    if delta.is_empty() {
        return;
    }
    let needs_space = text
        .chars()
        .last()
        .zip(delta.chars().next())
        .is_some_and(|(left, right)| left.is_ascii_alphanumeric() && right.is_ascii_alphanumeric());
    if !text.is_empty() && needs_space {
        text.push(' ');
    }
    text.push_str(delta);
}

fn ends_with(text: &str, punctuation: &[char]) -> bool {
    text.trim_end_matches(|character: char| {
        matches!(character, '"' | '\'' | '”' | '’' | '）' | ')' | ']' | '}')
    })
    .trim_end()
    .chars()
    .last()
    .is_some_and(|character| punctuation.contains(&character))
}

fn latency_mode_name(mode: LatencyMode) -> &'static str {
    match mode {
        LatencyMode::LowLatency => "low_latency",
        LatencyMode::Balanced => "balanced",
        LatencyMode::Complete => "complete",
    }
}

struct MacOutput {
    child: Option<Child>,
    writer: Arc<Mutex<ChildStdin>>,
    worker: JoinHandle<()>,
    reader: JoinHandle<()>,
    queue: mpsc::Sender<OutputRequest>,
    pid: u32,
}

#[derive(Debug, Serialize)]
struct PlayerCommand<'a> {
    cmd: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    token: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    voice: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rate: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    latency_mode: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
struct PlayerEvent {
    #[serde(rename = "type")]
    kind: String,
    token: Option<u64>,
    message: Option<String>,
    depth: Option<u32>,
}

impl MacOutput {
    async fn start(
        player_path: &str,
        cache_dir: &str,
        voice: Option<String>,
        speech_rate: Option<u32>,
        latency_mode: LatencyMode,
        session_id: Uuid,
        epoch: u64,
        sender: mpsc::Sender<OutputSignal>,
    ) -> Result<Self> {
        if !Path::new(player_path).exists() {
            return Err(CoreError::InvalidConfig(format!(
                "TTS player not found: {player_path}"
            )));
        }
        tokio::fs::create_dir_all(cache_dir).await?;
        let mut child = Command::new(player_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| CoreError::Output(format!("failed to start player: {e}")))?;
        let pid = child
            .id()
            .ok_or_else(|| CoreError::Output("player PID unavailable".into()))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| CoreError::Output("missing player stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| CoreError::Output("missing player stdout".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| CoreError::Output("missing player stderr".into()))?;
        let writer = Arc::new(Mutex::new(stdin));
        let segments = Arc::new(Mutex::new(HashMap::<u64, TranslationEvent>::new()));
        let reader_segments = Arc::clone(&segments);
        let reader_sender = sender.clone();
        let reader = tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(event) = serde_json::from_str::<PlayerEvent>(&line) else {
                    continue;
                };
                let token = event.token.unwrap_or(0);
                let translation = reader_segments.lock().await.get(&token).cloned();
                let segment_id = translation
                    .as_ref()
                    .map(|item| item.segment_id.clone())
                    .unwrap_or_else(|| "prime".into());
                let kind = match event.kind.as_str() {
                    "started" => Some(PlaybackEventType::Started),
                    "done" => Some(PlaybackEventType::Finished),
                    "cancelled" => Some(PlaybackEventType::Cancelled),
                    _ => None,
                };
                if let Some(event_type) = kind {
                    let now = monotonic_ns();
                    let queue_duration_us = translation
                        .as_ref()
                        .filter(|_| event_type == PlaybackEventType::Started)
                        .map(|item| (now - item.generation_finished_ns).max(0) as u64)
                        .unwrap_or(0);
                    let _ = reader_sender
                        .send(OutputSignal::Playback(PlaybackEvent {
                            event_type,
                            segment_id,
                            token,
                            queue_duration_us,
                            actual_start_mono_ns: Some(now),
                            source_end_ns: translation.as_ref().and_then(|item| item.source_end_ns),
                        }))
                        .await;
                    if event.kind == "done" || event.kind == "cancelled" {
                        reader_segments.lock().await.remove(&token);
                    }
                } else if event.kind == "queue" {
                    let _ = reader_sender
                        .send(OutputSignal::QueueDepth(event.depth.unwrap_or(0)))
                        .await;
                } else if event.kind == "error" {
                    let _ = reader_sender
                        .send(OutputSignal::Error(
                            event.message.unwrap_or_else(|| "player error".into()),
                        ))
                        .await;
                }
            }
            let _ = reader_sender.send(OutputSignal::Exited).await;
        });
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::info!(target: "tts_host", "{line}");
            }
        });

        let (queue, mut receiver) = mpsc::channel::<OutputRequest>(32);
        let worker_writer = Arc::clone(&writer);
        let worker_sender = sender.clone();
        let worker_cache = PathBuf::from(cache_dir);
        let mut worker_voice = voice.unwrap_or_else(|| "Tingting".into());
        let worker = tokio::spawn(async move {
            let mut next_token = 1_u64;
            let mut assembler = SpeechAssembler {
                mode: latency_mode,
                ..SpeechAssembler::default()
            };
            while worker_cache
                .join(format!("speech-{session_id}-{next_token}.wav"))
                .exists()
            {
                next_token += 1;
            }
            while let Some(request) = receiver.recv().await {
                let event = match request {
                    OutputRequest::Speak(event)
                        if event.epoch == epoch
                            && event.speakable
                            && !event.translated_text.trim().is_empty() =>
                    {
                        assembler.push(event)
                    }
                    OutputRequest::Speak(_) => None,
                    OutputRequest::SetMode(mode) => {
                        assembler.mode = mode;
                        None
                    }
                    OutputRequest::SetVoice(voice) => {
                        worker_voice = voice;
                        None
                    }
                    OutputRequest::Finish => assembler.finish(),
                    OutputRequest::Flush => {
                        assembler.clear();
                        None
                    }
                };
                if let Some(event) = event {
                    let token = next_token;
                    next_token += 1;
                    let source_duration_ms = event
                        .source_start_ns
                        .zip(event.source_end_ns)
                        .map(|(start, end)| ((end - start).max(0) / 1_000_000) as u64);
                    segments.lock().await.insert(token, event.clone());
                    let path = worker_cache.join(format!("speech-{session_id}-{token}.wav"));
                    let command = PlayerCommand {
                        cmd: "speak",
                        token: Some(token),
                        path: path.to_str(),
                        text: Some(&event.translated_text),
                        voice: Some(&worker_voice),
                        rate: speech_rate,
                        source_duration_ms,
                        latency_mode: Some(latency_mode_name(assembler.mode)),
                    };
                    match serde_json::to_string(&command) {
                        Ok(mut line) => {
                            line.push('\n');
                            let mut guard = worker_writer.lock().await;
                            if let Err(error) = guard.write_all(line.as_bytes()).await {
                                let _ = worker_sender
                                    .send(OutputSignal::Error(error.to_string()))
                                    .await;
                            }
                        }
                        Err(error) => {
                            let _ = worker_sender
                                .send(OutputSignal::Error(error.to_string()))
                                .await;
                        }
                    }
                }
            }
        });

        let output = Self {
            child: Some(child),
            writer,
            worker,
            reader,
            queue,
            pid,
        };
        // The player is a separate process and defaults to balanced. Send the
        // session's initial policy explicitly; otherwise a policy selected
        // before startup never affects playback until the user changes it.
        output
            .send_command(PlayerCommand {
                cmd: "policy",
                token: None,
                path: None,
                text: None,
                voice: None,
                rate: None,
                source_duration_ms: None,
                latency_mode: Some(latency_mode_name(latency_mode)),
            })
            .await?;
        output.prime(cache_dir).await?;
        Ok(output)
    }

    async fn prime(&self, cache_dir: &str) -> Result<()> {
        let path = Path::new(cache_dir).join("player-prime.wav");
        if !path.exists() {
            let spec = hound::WavSpec {
                channels: 1,
                sample_rate: 24_000,
                bits_per_sample: 16,
                sample_format: hound::SampleFormat::Int,
            };
            let mut wav = hound::WavWriter::create(&path, spec)
                .map_err(|e| CoreError::Output(e.to_string()))?;
            for n in 0..48_000 {
                let value = (80.0
                    * (2.0 * std::f64::consts::PI * 440.0 * n as f64 / 24_000.0).sin())
                    as i16;
                wav.write_sample(value)
                    .map_err(|e| CoreError::Output(e.to_string()))?;
            }
            wav.finalize()
                .map_err(|e| CoreError::Output(e.to_string()))?;
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        self.send_command(PlayerCommand {
            cmd: "play",
            token: Some(0),
            path: path.to_str(),
            text: None,
            voice: None,
            rate: None,
            source_duration_ms: None,
            latency_mode: None,
        })
        .await?;
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        Ok(())
    }

    async fn send_command(&self, command: PlayerCommand<'_>) -> Result<()> {
        let mut line =
            serde_json::to_string(&command).map_err(|e| CoreError::Protocol(e.to_string()))?;
        line.push('\n');
        let mut writer = self.writer.lock().await;
        writer.write_all(line.as_bytes()).await?;
        writer.flush().await?;
        Ok(())
    }
}

#[async_trait]
impl PlaybackControl for MacOutput {
    fn playback_pid(&self) -> Option<u32> {
        Some(self.pid)
    }

    async fn submit(&mut self, event: TranslationEvent) -> Result<()> {
        self.queue
            .try_send(OutputRequest::Speak(event))
            .map_err(|_| CoreError::Output("TTS queue is full".into()))?;
        Ok(())
    }

    async fn set_latency_mode(&mut self, mode: LatencyMode) -> Result<()> {
        self.queue
            .send(OutputRequest::SetMode(mode))
            .await
            .map_err(|_| CoreError::Output("TTS worker stopped".into()))?;
        self.send_command(PlayerCommand {
            cmd: "policy",
            token: None,
            path: None,
            text: None,
            voice: None,
            rate: None,
            source_duration_ms: None,
            latency_mode: Some(latency_mode_name(mode)),
        })
        .await
    }

    async fn set_voice(&mut self, voice: String) -> Result<()> {
        self.queue
            .send(OutputRequest::SetVoice(voice))
            .await
            .map_err(|_| CoreError::Output("TTS worker stopped".into()))
    }

    async fn finish(&mut self) -> Result<()> {
        self.queue
            .send(OutputRequest::Finish)
            .await
            .map_err(|_| CoreError::Output("TTS worker stopped".into()))?;
        Ok(())
    }

    async fn pause(&mut self) -> Result<()> {
        self.send_command(PlayerCommand {
            cmd: "pause",
            token: None,
            path: None,
            text: None,
            voice: None,
            rate: None,
            source_duration_ms: None,
            latency_mode: None,
        })
        .await
    }

    async fn resume(&mut self) -> Result<()> {
        self.send_command(PlayerCommand {
            cmd: "resume",
            token: None,
            path: None,
            text: None,
            voice: None,
            rate: None,
            source_duration_ms: None,
            latency_mode: None,
        })
        .await
    }

    async fn flush(&mut self) -> Result<()> {
        self.queue
            .send(OutputRequest::Flush)
            .await
            .map_err(|_| CoreError::Output("TTS worker stopped".into()))?;
        self.send_command(PlayerCommand {
            cmd: "flush",
            token: None,
            path: None,
            text: None,
            voice: None,
            rate: None,
            source_duration_ms: None,
            latency_mode: None,
        })
        .await
    }

    async fn stop(&mut self) -> Result<()> {
        let _ = self
            .send_command(PlayerCommand {
                cmd: "flush",
                token: None,
                path: None,
                text: None,
                voice: None,
                rate: None,
                source_duration_ms: None,
                latency_mode: None,
            })
            .await;
        self.worker.abort();
        self.reader.abort();
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use simulmt_api::TranslationEventType;

    fn event(index: i64, source: &str, target: &str) -> TranslationEvent {
        TranslationEvent {
            session_id: Uuid::nil(),
            epoch: 1,
            segment_id: format!("segment-{index}"),
            revision: 0,
            event_type: TranslationEventType::Append,
            source_text: source.into(),
            translated_text: target.into(),
            source_start_ns: Some(index * 800_000_000),
            source_end_ns: Some((index + 1) * 800_000_000),
            generation_started_ns: index,
            generation_finished_ns: index + 1,
            generation_duration_us: 100,
            speakable: true,
        }
    }

    #[test]
    fn speech_emits_a_complete_sentence_without_waiting() {
        let mut assembler = SpeechAssembler::default();
        let speech = assembler
            .push(event(0, "A complete sentence.", "一个完整的句子。"))
            .expect("complete sentence");
        assert_eq!(speech.translated_text, "一个完整的句子。");
        assert_eq!(speech.source_end_ns, Some(800_000_000));
    }

    #[test]
    fn speech_uses_a_short_first_phrase_then_longer_follow_up_phrases() {
        let mut assembler = SpeechAssembler::default();
        assert!(assembler.push(event(0, "first", "第一")).is_none());
        let first = assembler
            .push(event(1, "second", "第二"))
            .expect("first phrase");
        assert_eq!(first.translated_text, "第一第二");
        assert_eq!(first.source_start_ns, Some(0));
        assert_eq!(first.source_end_ns, Some(1_600_000_000));
        assert_eq!(first.segment_id, "segment-1");

        assert!(assembler.push(event(2, "third", "第三")).is_none());
        assert!(assembler.push(event(3, "fourth", "第四")).is_none());
        let second = assembler
            .push(event(4, "fifth", "第五"))
            .expect("second phrase");
        assert_eq!(second.translated_text, "第三第四第五");
        assert_eq!(second.source_end_ns, Some(4_000_000_000));
        assert_eq!(second.source_start_ns, Some(1_600_000_000));
    }

    #[test]
    fn finish_releases_a_short_trailing_phrase_and_flush_discards_it() {
        let mut assembler = SpeechAssembler::default();
        assert!(assembler.push(event(0, "short", "短句")).is_none());
        assert_eq!(assembler.finish().unwrap().translated_text, "短句");
        assert!(assembler.push(event(1, "stale", "旧内容")).is_none());
        assembler.clear();
        assert!(assembler.finish().is_none());
    }
}
