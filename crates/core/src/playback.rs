use crate::error::{CoreError, Result};
use crate::store::{SpeechSegment, Store};
use simulmt_api::{PlaybackEvent, PlaybackEventType, SessionMetrics};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

pub(crate) struct PlaybackFacts {
    session_id: Uuid,
    cache_dir: PathBuf,
    store: Arc<dyn PlaybackStore>,
    ledger: Mutex<PlaybackLedger>,
}

#[async_trait::async_trait]
pub(crate) trait PlaybackStore: Send + Sync {
    async fn playback_status(&self, session_id: Uuid, token: u64) -> Result<Option<String>>;
    async fn persist_playback(&self, session_id: Uuid, segment: SpeechSegment) -> Result<()>;
    async fn highest_speech_token(&self, session_id: Uuid) -> Result<u64>;
    async fn speech_segments(&self, session_id: Uuid) -> Result<Vec<SpeechSegment>>;
    async fn persist_speech_recovery(
        &self,
        session_id: Uuid,
        segments: Vec<SpeechSegment>,
    ) -> Result<()>;
}

#[async_trait::async_trait]
impl PlaybackStore for Store {
    async fn playback_status(&self, session_id: Uuid, token: u64) -> Result<Option<String>> {
        Store::playback_status(self, session_id, token).await
    }

    async fn persist_playback(&self, session_id: Uuid, segment: SpeechSegment) -> Result<()> {
        Store::persist_playback(self, session_id, segment).await
    }

    async fn highest_speech_token(&self, session_id: Uuid) -> Result<u64> {
        Store::highest_speech_token(self, session_id).await
    }

    async fn speech_segments(&self, session_id: Uuid) -> Result<Vec<SpeechSegment>> {
        Store::speech_segments(self, session_id).await
    }

    async fn persist_speech_recovery(
        &self,
        session_id: Uuid,
        segments: Vec<SpeechSegment>,
    ) -> Result<()> {
        Store::persist_speech_recovery(self, session_id, segments).await
    }
}

pub(crate) fn speech_path(cache_dir: &std::path::Path, session_id: Uuid, token: u64) -> PathBuf {
    cache_dir.join(format!("speech-{session_id}-{token}.wav"))
}

fn cached_speech_files(
    cache_dir: &std::path::Path,
    session_id: Uuid,
) -> Result<Vec<(u64, PathBuf)>> {
    let entries = match std::fs::read_dir(cache_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let prefix = format!("speech-{session_id}-");
    let mut files = Vec::new();
    for entry in entries {
        let path = entry?.path();
        let Some(token) = path
            .file_name()
            .and_then(|value| value.to_str())
            .and_then(|value| value.strip_prefix(&prefix))
            .and_then(|value| value.strip_suffix(".wav"))
            .and_then(|value| value.parse::<u64>().ok())
        else {
            continue;
        };
        files.push((token, path));
    }
    files.sort_by_key(|(token, _)| *token);
    Ok(files)
}

fn speech_status(event_type: PlaybackEventType) -> &'static str {
    match event_type {
        PlaybackEventType::Queued => "pending",
        PlaybackEventType::Started => "playing",
        PlaybackEventType::Finished => "ready",
        PlaybackEventType::Cancelled => "cancelled",
        PlaybackEventType::Expired => "expired",
        PlaybackEventType::Failed => "failed",
    }
}

fn playback_event_type(status: &str) -> Result<PlaybackEventType> {
    match status {
        "pending" => Ok(PlaybackEventType::Queued),
        "playing" => Ok(PlaybackEventType::Started),
        "ready" => Ok(PlaybackEventType::Finished),
        "cancelled" => Ok(PlaybackEventType::Cancelled),
        "expired" => Ok(PlaybackEventType::Expired),
        "failed" => Ok(PlaybackEventType::Failed),
        unknown => Err(CoreError::Storage(format!(
            "unknown persisted playback status: {unknown}"
        ))),
    }
}

fn is_terminal_speech(status: &str) -> bool {
    matches!(status, "ready" | "cancelled" | "expired" | "failed")
}

fn validate_wav(path: &std::path::Path) -> std::result::Result<(), String> {
    let mut reader = hound::WavReader::open(path).map_err(|error| error.to_string())?;
    for sample in reader.samples::<i16>() {
        sample.map_err(|error| error.to_string())?;
    }
    Ok(())
}

pub(crate) fn is_exportable_speech(status: &str) -> bool {
    status == "ready"
}

fn recover_speech_segments(
    session_id: Uuid,
    cache_dir: PathBuf,
    current: Vec<SpeechSegment>,
) -> Result<Vec<SpeechSegment>> {
    let current_by_token = current
        .iter()
        .map(|segment| (segment.token, segment))
        .collect::<HashMap<_, _>>();
    let mut recovered = Vec::new();
    let mut recovered_tokens = HashMap::new();
    for (token, path) in cached_speech_files(&cache_dir, session_id)? {
        let previous = current_by_token.get(&token).copied();
        if previous.is_some_and(|segment| is_terminal_speech(&segment.status)) {
            continue;
        }
        let status = match validate_wav(&path) {
            Ok(()) => "ready",
            Err(error) => {
                tracing::warn!(path = %path.display(), "invalid recovered speech WAV: {error}");
                "failed"
            }
        };
        recovered.push(SpeechSegment {
            token,
            segment_id: previous
                .map(|segment| segment.segment_id.clone())
                .unwrap_or_else(|| format!("recovered-{token}")),
            path,
            status: status.into(),
            source_end_ns: previous.and_then(|segment| segment.source_end_ns),
        });
        recovered_tokens.insert(token, ());
    }
    for segment in current {
        if matches!(segment.status.as_str(), "pending" | "playing")
            && !recovered_tokens.contains_key(&segment.token)
        {
            recovered.push(SpeechSegment {
                status: "cancelled".into(),
                ..segment
            });
        }
    }
    Ok(recovered)
}

pub(crate) struct PlaybackPublication {
    pub metrics: SessionMetrics,
    pub event: PlaybackEvent,
}

impl PlaybackFacts {
    pub(crate) fn new(session_id: Uuid, cache_dir: PathBuf, store: Arc<dyn PlaybackStore>) -> Self {
        Self {
            session_id,
            cache_dir,
            store,
            ledger: Mutex::new(PlaybackLedger::default()),
        }
    }

    pub async fn apply(
        &self,
        event: PlaybackEvent,
        metrics: &mut SessionMetrics,
    ) -> Result<Option<PlaybackPublication>> {
        let mut ledger = self.ledger.lock().await;
        let previous = match ledger.states.get(&event.token).copied() {
            Some(previous) => Some(previous),
            None => self
                .store
                .playback_status(self.session_id, event.token)
                .await?
                .as_deref()
                .map(playback_event_type)
                .transpose()?,
        };
        if previous.is_some_and(is_terminal_playback)
            || previous == Some(event.event_type)
            || (previous == Some(PlaybackEventType::Started)
                && event.event_type == PlaybackEventType::Queued)
        {
            return Ok(None);
        }
        self.store
            .persist_playback(
                self.session_id,
                SpeechSegment {
                    token: event.token,
                    segment_id: event.segment_id.clone(),
                    path: speech_path(&self.cache_dir, self.session_id, event.token),
                    status: speech_status(event.event_type).into(),
                    source_end_ns: event.source_end_ns,
                },
            )
            .await?;
        let update = ledger.apply(event.token, event.event_type);
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
                        metrics.playback_lag_us =
                            metrics.captured_duration_us.saturating_sub(source_end_us);
                        metrics.max_playback_lag_us =
                            metrics.max_playback_lag_us.max(metrics.playback_lag_us);
                    }
                }
                PlaybackEventType::Finished => metrics.playback_finished += 1,
                PlaybackEventType::Cancelled => metrics.playback_cancelled += 1,
                PlaybackEventType::Failed | PlaybackEventType::Expired => {}
            }
        }
        Ok(Some(PlaybackPublication {
            metrics: metrics.clone(),
            event,
        }))
    }

    pub async fn recover(&self) -> Result<()> {
        let current = self.store.speech_segments(self.session_id).await?;
        let session_id = self.session_id;
        let cache_dir = self.cache_dir.clone();
        let recovered = tokio::task::spawn_blocking(move || {
            recover_speech_segments(session_id, cache_dir, current)
        })
        .await
        .map_err(|error| CoreError::Storage(format!("speech recovery task failed: {error}")))??;
        self.store
            .persist_speech_recovery(self.session_id, recovered)
            .await
    }

    pub async fn next_token(&self) -> Result<u64> {
        let database = self.store.highest_speech_token(self.session_id).await?;
        let cache_dir = self.cache_dir.clone();
        let session_id = self.session_id;
        let files = tokio::task::spawn_blocking(move || {
            cached_speech_files(&cache_dir, session_id)
                .map(|files| files.last().map(|(token, _)| *token).unwrap_or(0))
        })
        .await
        .map_err(|error| CoreError::Storage(format!("speech token scan failed: {error}")))??;
        Ok(database.max(files).saturating_add(1))
    }

    pub async fn clear(&self) {
        self.ledger.lock().await.clear();
    }
}

#[derive(Default)]
struct PlaybackLedger {
    states: HashMap<u64, PlaybackEventType>,
    terminal_order: VecDeque<u64>,
    pending: u64,
}

struct PlaybackUpdate {
    pending: u64,
    count_event: bool,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{SessionMetadata, Store, StoredSession};
    use simulmt_api::{PipelineConfig, PlaybackEvent, PlaybackEventType, SessionMetrics};
    use uuid::Uuid;

    async fn facts(root: &std::path::Path) -> (PlaybackFacts, Arc<Store>) {
        let store = Store::open(root).unwrap();
        let session_id = Uuid::new_v4();
        store
            .save_session(StoredSession {
                id: session_id,
                config: PipelineConfig::default(),
                metadata: SessionMetadata {
                    title: "播放事实测试".into(),
                    created_at_ms: 1,
                    started_at_ms: None,
                    ended_at_ms: None,
                    audio_duration_us: 0,
                },
            })
            .await
            .unwrap();
        let facts = PlaybackFacts::new(session_id, root.join("cache"), store.clone());
        (facts, store)
    }

    fn event(token: u64, event_type: PlaybackEventType) -> PlaybackEvent {
        PlaybackEvent {
            event_type,
            segment_id: format!("segment-{token}"),
            token,
            queue_duration_us: 0,
            actual_start_mono_ns: None,
            source_end_ns: Some(800_000_000),
        }
    }

    fn write_wav(path: &std::path::Path) {
        let mut writer = hound::WavWriter::create(
            path,
            hound::WavSpec {
                channels: 1,
                sample_rate: 16_000,
                bits_per_sample: 16,
                sample_format: hound::SampleFormat::Int,
            },
        )
        .unwrap();
        writer.write_sample(7_i16).unwrap();
        writer.finalize().unwrap();
    }

    #[tokio::test]
    async fn recovery_settles_files_and_unfinished_facts() {
        let root = tempfile::tempdir().unwrap();
        let (facts, store) = facts(root.path()).await;
        let cache = root.path().join("cache");
        std::fs::create_dir_all(&cache).unwrap();
        let mut metrics = SessionMetrics::default();
        for token in 1..=4 {
            let event_type = if token == 4 {
                PlaybackEventType::Finished
            } else {
                PlaybackEventType::Started
            };
            facts
                .apply(event(token, event_type), &mut metrics)
                .await
                .unwrap();
        }
        write_wav(&speech_path(&cache, facts.session_id, 2));
        std::fs::write(speech_path(&cache, facts.session_id, 3), b"RIFF").unwrap();
        write_wav(&speech_path(&cache, facts.session_id, 5));

        facts.recover().await.unwrap();

        let segments = store.speech_segments(facts.session_id).await.unwrap();
        assert_eq!(
            segments
                .iter()
                .find(|segment| segment.token == 2)
                .unwrap()
                .segment_id,
            "segment-2"
        );
        assert_eq!(
            segments
                .iter()
                .find(|segment| segment.token == 5)
                .unwrap()
                .segment_id,
            "recovered-5"
        );
        let statuses = segments
            .into_iter()
            .map(|segment| (segment.token, segment.status))
            .collect::<HashMap<_, _>>();
        assert_eq!(statuses.get(&1).map(String::as_str), Some("cancelled"));
        assert_eq!(statuses.get(&2).map(String::as_str), Some("ready"));
        assert_eq!(statuses.get(&3).map(String::as_str), Some("failed"));
        assert_eq!(statuses.get(&4).map(String::as_str), Some("ready"));
        assert_eq!(statuses.get(&5).map(String::as_str), Some("ready"));
        assert_eq!(statuses.len(), 5);
    }

    #[tokio::test]
    async fn next_token_preserves_database_and_orphaned_file_history() {
        let root = tempfile::tempdir().unwrap();
        let (facts, _store) = facts(root.path()).await;
        let mut metrics = SessionMetrics::default();
        facts
            .apply(event(3, PlaybackEventType::Finished), &mut metrics)
            .await
            .unwrap();
        std::fs::create_dir_all(root.path().join("cache")).unwrap();
        std::fs::write(
            root.path()
                .join("cache")
                .join(format!("speech-{}-7.wav", facts.session_id)),
            b"orphan",
        )
        .unwrap();

        assert_eq!(facts.next_token().await.unwrap(), 8);
    }

    #[tokio::test]
    async fn failed_commit_leaves_metrics_and_publication_untouched() {
        struct FailingStore;

        #[async_trait::async_trait]
        impl PlaybackStore for FailingStore {
            async fn playback_status(
                &self,
                _session_id: Uuid,
                _token: u64,
            ) -> Result<Option<String>> {
                Ok(None)
            }

            async fn persist_playback(
                &self,
                _session_id: Uuid,
                _segment: SpeechSegment,
            ) -> Result<()> {
                Err(crate::CoreError::Storage("injected commit failure".into()))
            }

            async fn highest_speech_token(&self, _session_id: Uuid) -> Result<u64> {
                Ok(0)
            }

            async fn speech_segments(&self, _session_id: Uuid) -> Result<Vec<SpeechSegment>> {
                Ok(Vec::new())
            }

            async fn persist_speech_recovery(
                &self,
                _session_id: Uuid,
                _segments: Vec<SpeechSegment>,
            ) -> Result<()> {
                Ok(())
            }
        }

        let root = tempfile::tempdir().unwrap();
        let facts = PlaybackFacts::new(
            Uuid::new_v4(),
            root.path().join("cache"),
            Arc::new(FailingStore),
        );
        let mut metrics = SessionMetrics::default();
        let result = facts
            .apply(event(1, PlaybackEventType::Started), &mut metrics)
            .await;

        assert!(result.is_err());
        assert_eq!(metrics.playback_pending, 0);
        assert_eq!(metrics.playback_started, 0);
        assert_eq!(metrics.output_latency_us, 0);
    }

    #[tokio::test]
    async fn duplicate_and_out_of_order_events_are_silent_noops() {
        let root = tempfile::tempdir().unwrap();
        let (facts, _store) = facts(root.path()).await;
        let mut metrics = SessionMetrics::default();

        assert!(
            facts
                .apply(event(1, PlaybackEventType::Finished), &mut metrics)
                .await
                .unwrap()
                .is_some()
        );
        for event_type in [PlaybackEventType::Queued, PlaybackEventType::Started] {
            assert!(
                facts
                    .apply(event(1, event_type), &mut metrics)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(metrics.playback_pending, 0);

        assert!(
            facts
                .apply(event(2, PlaybackEventType::Queued), &mut metrics)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(metrics.playback_pending, 1);
        assert!(
            facts
                .apply(event(2, PlaybackEventType::Queued), &mut metrics)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            facts
                .apply(event(2, PlaybackEventType::Started), &mut metrics)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            facts
                .apply(event(2, PlaybackEventType::Queued), &mut metrics)
                .await
                .unwrap()
                .is_none()
        );
        let publication = facts
            .apply(event(2, PlaybackEventType::Finished), &mut metrics)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(publication.metrics.playback_pending, 0);
        assert_eq!(publication.event.token, 2);
        assert_eq!(metrics.playback_finished, 2);
    }

    #[tokio::test]
    async fn terminal_history_survives_an_empty_memory_cache() {
        let root = tempfile::tempdir().unwrap();
        let (facts, _store) = facts(root.path()).await;
        let mut metrics = SessionMetrics::default();

        assert!(
            facts
                .apply(event(1, PlaybackEventType::Finished), &mut metrics)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(metrics.playback_finished, 1);

        facts.clear().await;
        assert!(
            facts
                .apply(event(1, PlaybackEventType::Finished), &mut metrics)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(metrics.playback_finished, 1);
    }
}
