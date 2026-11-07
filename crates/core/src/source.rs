use crate::error::{CoreError, Result};
use crate::time::monotonic_ns;
use async_trait::async_trait;
use serde::Deserialize;
use simulmt_api::{AudioFormat, AudioFrame, SourceConfig, SourceInfo};
use std::path::Path;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use uuid::Uuid;

#[derive(Debug)]
pub enum SourceEvent {
    Frame(AudioFrame),
    Dropped { duration_us: u32, frames: u32 },
    Ended,
    Warning(String),
    Error(String),
}

#[async_trait]
pub trait RunningSource: Send {
    async fn stop(&mut self) -> Result<()>;
}

struct TaskSource {
    task: JoinHandle<()>,
}

#[async_trait]
impl RunningSource for TaskSource {
    async fn stop(&mut self) -> Result<()> {
        self.task.abort();
        let _ = (&mut self.task).await;
        Ok(())
    }
}

struct ProcessSource {
    child: std::sync::Arc<tokio::sync::Mutex<Option<Child>>>,
    task: JoinHandle<()>,
    stopped: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait]
impl RunningSource for ProcessSource {
    async fn stop(&mut self) -> Result<()> {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Release);
        {
            let mut child = self.child.lock().await;
            if let Some(child) = child.as_mut() {
                let _ = child.kill().await;
                let _ = child.wait().await;
            }
        }
        self.task.abort();
        let _ = (&mut self.task).await;
        Ok(())
    }
}

pub struct SourceStartOptions {
    pub audio_host: String,
    pub playback_pid: Option<u32>,
}

pub async fn start_source(
    config: &SourceConfig,
    options: &SourceStartOptions,
    session_id: Uuid,
    epoch: u64,
    sender: mpsc::Sender<SourceEvent>,
) -> Result<Box<dyn RunningSource>> {
    match config {
        SourceConfig::Mock { frames } => start_mock(*frames, session_id, epoch, sender).await,
        SourceConfig::File { path, realtime } => {
            start_file(path, *realtime, session_id, epoch, sender).await
        }
        SourceConfig::MacosProcess { .. }
        | SourceConfig::MacosSystem { .. }
        | SourceConfig::MacosMicrophone => {
            start_macos(config, options, session_id, epoch, sender).await
        }
    }
}

fn make_frame(
    session_id: Uuid,
    epoch: u64,
    sequence: u64,
    source_id: &str,
    payload: Vec<u8>,
) -> AudioFrame {
    let now = monotonic_ns();
    AudioFrame {
        session_id,
        epoch,
        sequence,
        source_id: source_id.to_string(),
        format: AudioFormat::default(),
        payload,
        source_pts_ns: None,
        capture_mono_ns: now,
        ingress_mono_ns: now,
        duration_us: 100_000,
        discontinuity: sequence == 0,
    }
}

async fn start_mock(
    frames: Option<u32>,
    session_id: Uuid,
    epoch: u64,
    sender: mpsc::Sender<SourceEvent>,
) -> Result<Box<dyn RunningSource>> {
    let task = tokio::spawn(async move {
        let mut sequence = 0_u64;
        loop {
            if frames.is_some_and(|n| sequence >= n as u64) {
                let _ = sender.send(SourceEvent::Ended).await;
                break;
            }
            // Non-zero low-energy samples make capture tests distinguish a stream from no input.
            let mut payload = Vec::with_capacity(3_200);
            for i in 0..1_600 {
                let value = (((sequence as usize * 1_600 + i) % 200) as i16) - 100;
                payload.extend_from_slice(&value.to_le_bytes());
            }
            if sender
                .send(SourceEvent::Frame(make_frame(
                    session_id, epoch, sequence, "mock", payload,
                )))
                .await
                .is_err()
            {
                break;
            }
            sequence += 1;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    });
    Ok(Box::new(TaskSource { task }))
}

async fn start_file(
    path: &str,
    realtime: bool,
    session_id: Uuid,
    epoch: u64,
    sender: mpsc::Sender<SourceEvent>,
) -> Result<Box<dyn RunningSource>> {
    let mut reader = hound::WavReader::open(path).map_err(|e| CoreError::Source(e.to_string()))?;
    let spec = reader.spec();
    if spec.sample_rate != 16_000 || spec.channels != 1 || spec.bits_per_sample != 16 {
        return Err(CoreError::InvalidConfig(
            "file source currently requires 16 kHz mono PCM16 WAV".into(),
        ));
    }
    let samples: Vec<i16> = reader
        .samples::<i16>()
        .filter_map(std::result::Result::ok)
        .collect();
    let task = tokio::spawn(async move {
        for (sequence, chunk) in samples.chunks(1_600).enumerate() {
            let mut payload = Vec::with_capacity(chunk.len() * 2);
            for sample in chunk {
                payload.extend_from_slice(&sample.to_le_bytes());
            }
            let duration_us = (chunk.len() as u32 * 1_000_000) / 16_000;
            let mut frame = make_frame(session_id, epoch, sequence as u64, "file", payload);
            frame.duration_us = duration_us;
            frame.format.frame_duration_us = duration_us;
            if sender.send(SourceEvent::Frame(frame)).await.is_err() {
                return;
            }
            if realtime {
                tokio::time::sleep(std::time::Duration::from_micros(duration_us as u64)).await;
            }
        }
        let _ = sender.send(SourceEvent::Ended).await;
    });
    Ok(Box::new(TaskSource { task }))
}

async fn start_macos(
    config: &SourceConfig,
    options: &SourceStartOptions,
    session_id: Uuid,
    epoch: u64,
    sender: mpsc::Sender<SourceEvent>,
) -> Result<Box<dyn RunningSource>> {
    let child = spawn_macos_host(config, options).await?;
    let child_slot = std::sync::Arc::new(tokio::sync::Mutex::new(None));
    let stopped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let task_child = std::sync::Arc::clone(&child_slot);
    let task_stopped = std::sync::Arc::clone(&stopped);
    let config = config.clone();
    let options = SourceStartOptions {
        audio_host: options.audio_host.clone(),
        playback_pid: options.playback_pid,
    };
    let task = tokio::spawn(async move {
        let mut next_child = Some(child);
        let mut sequence = 0_u64;
        let mut discontinuity = true;
        let mut dropped_duration_us = 0_u32;
        let mut dropped_frames = 0_u32;
        let mut failures = 0_u32;
        loop {
            if task_stopped.load(std::sync::atomic::Ordering::Acquire) {
                break;
            }
            let mut child = if let Some(child) = next_child.take() {
                child
            } else {
                match spawn_macos_host(&config, &options).await {
                    Ok(child) => child,
                    Err(error) => {
                        let _ = sender
                            .send(SourceEvent::Warning(format!(
                                "audio host restart failed: {error}; retrying"
                            )))
                            .await;
                        failures = failures.saturating_add(1).min(5);
                        let delay = 250_u64 << failures.saturating_sub(1);
                        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                        continue;
                    }
                }
            };
            let Some(stdout) = child.stdout.take() else {
                let _ = sender
                    .send(SourceEvent::Error("missing audio stdout".into()))
                    .await;
                break;
            };
            let Some(stderr) = child.stderr.take() else {
                let _ = sender
                    .send(SourceEvent::Error("missing audio stderr".into()))
                    .await;
                break;
            };
            *task_child.lock().await = Some(child);
            let logger = tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::info!(target: "audio_host", "{line}");
                }
            });
            let mut stdout = BufReader::new(stdout);
            loop {
                let mut payload = vec![0_u8; 3_200];
                match stdout.read_exact(&mut payload).await {
                    Ok(_) => {
                        if dropped_duration_us > 0
                            && sender
                                .try_send(SourceEvent::Dropped {
                                    duration_us: dropped_duration_us,
                                    frames: dropped_frames,
                                })
                                .is_ok()
                        {
                            dropped_duration_us = 0;
                            dropped_frames = 0;
                        }
                        let mut frame = make_frame(session_id, epoch, sequence, "macos", payload);
                        frame.discontinuity = discontinuity;
                        discontinuity = false;
                        failures = 0;
                        match sender.try_send(SourceEvent::Frame(frame)) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                dropped_duration_us = dropped_duration_us.saturating_add(100_000);
                                dropped_frames = dropped_frames.saturating_add(1);
                                discontinuity = true;
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => return,
                        }
                        sequence += 1;
                    }
                    Err(_) => break,
                }
            }
            logger.abort();
            {
                let mut child = task_child.lock().await;
                if let Some(child) = child.as_mut() {
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                }
            }
            if task_stopped.load(std::sync::atomic::Ordering::Acquire) {
                break;
            }
            let _ = sender
                .send(SourceEvent::Warning(
                    "audio host exited; reconnecting with a discontinuous timeline".into(),
                ))
                .await;
            discontinuity = true;
            failures = failures.saturating_add(1).min(5);
            let delay = 250_u64 << failures.saturating_sub(1);
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }
    });
    Ok(Box::new(ProcessSource {
        child: child_slot,
        task,
        stopped,
    }))
}

async fn spawn_macos_host(config: &SourceConfig, options: &SourceStartOptions) -> Result<Child> {
    if !Path::new(&options.audio_host).exists() {
        return Err(CoreError::InvalidConfig(format!(
            "audio host not found: {}",
            options.audio_host
        )));
    }
    let mut command = Command::new(&options.audio_host);
    match config {
        SourceConfig::MacosProcess {
            pids,
            mute_original,
        } => {
            if pids.is_empty() {
                return Err(CoreError::InvalidConfig(
                    "at least one audio process PID is required".into(),
                ));
            }
            for pid in pids {
                command.arg("--pid").arg(pid.to_string());
            }
            if let Some(pid) = options.playback_pid {
                command.arg("--exclude-pid").arg(pid.to_string());
            }
            if !mute_original {
                command.arg("--no-mute");
            }
        }
        SourceConfig::MacosSystem {
            exclude_pids,
            mute_original,
        } => {
            for pid in exclude_pids.iter().copied().chain(options.playback_pid) {
                command.arg("--exclude-pid").arg(pid.to_string());
            }
            if !mute_original {
                command.arg("--no-mute");
            }
        }
        SourceConfig::MacosMicrophone => {
            command.arg("--microphone");
        }
        _ => unreachable!(),
    }
    command
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| CoreError::Source(format!("failed to start audio host: {e}")))
}

#[derive(Debug, Deserialize)]
struct NativeSourceInfo {
    pid: u32,
    name: String,
    bundle_id: Option<String>,
    #[serde(default)]
    is_running: bool,
}

pub async fn list_macos_sources(audio_host: &str) -> Result<Vec<SourceInfo>> {
    let output = Command::new(audio_host)
        .arg("--list")
        .output()
        .await
        .map_err(|e| CoreError::Source(format!("failed to enumerate audio sources: {e}")))?;
    if !output.status.success() {
        return Err(CoreError::PermissionDenied(
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ));
    }
    let native: Vec<NativeSourceInfo> = serde_json::from_slice(&output.stdout)
        .map_err(|e| CoreError::Protocol(format!("invalid source list: {e}")))?;
    let mut result: Vec<SourceInfo> = native
        .into_iter()
        .map(|item| SourceInfo {
            source_id: format!("process:{}", item.pid),
            kind: "process".into(),
            name: item.name,
            pid: Some(item.pid),
            bundle_id: item.bundle_id,
            can_mute: true,
            can_exclude_sink: true,
            is_running: item.is_running,
        })
        .collect();
    result.sort_by_key(|item| !item.is_running);
    result.push(SourceInfo {
        source_id: "system".into(),
        kind: "system".into(),
        name: "全局系统声音".into(),
        pid: None,
        bundle_id: None,
        can_mute: true,
        can_exclude_sink: true,
        is_running: result.iter().any(|item| item.is_running),
    });
    Ok(result)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn host_restarts_with_continuous_sequence_and_discontinuity() {
        let root = std::env::temp_dir().join(format!("simulmt-source-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.unwrap();
        let executable = root.join("fake-audio-host.py");
        let invocations = root.join("invocations");
        let script = format!(
            r#"#!/usr/bin/env python3
import sys
import time
from pathlib import Path

with Path({invocations:?}).open("a") as output:
    output.write("start\n")
sys.stdout.buffer.write(bytes(3200))
sys.stdout.buffer.flush()
if len(Path({invocations:?}).read_text().splitlines()) == 1:
    sys.exit(3)
time.sleep(30)
"#,
            invocations = invocations.to_string_lossy().to_string()
        );
        tokio::fs::write(&executable, script).await.unwrap();
        let mut permissions = std::fs::metadata(&executable).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&executable, permissions).unwrap();
        let config = SourceConfig::MacosSystem {
            exclude_pids: vec![],
            mute_original: true,
        };
        let options = SourceStartOptions {
            audio_host: executable.to_string_lossy().into_owned(),
            playback_pid: None,
        };
        let (sender, mut receiver) = mpsc::channel(16);
        let mut source = start_source(&config, &options, Uuid::nil(), 1, sender)
            .await
            .unwrap();
        let first = tokio::time::timeout(std::time::Duration::from_secs(5), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let Some(SourceEvent::Frame(first)) = Some(first) else {
            panic!("expected first audio frame")
        };
        assert_eq!(first.sequence, 0);
        assert!(first.discontinuity);
        let second = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Some(SourceEvent::Frame(frame)) = receiver.recv().await {
                    break frame;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(second.sequence, 1);
        assert!(second.discontinuity);
        source.stop().await.unwrap();
        tokio::fs::remove_dir_all(root).await.unwrap();
    }
}
