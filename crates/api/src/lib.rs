use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const API_VERSION: &str = "simulmt.api.v1";
pub const NORMALIZED_SAMPLE_RATE: u32 = 16_000;
pub const MODEL_BLOCK_US: u32 = 800_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Idle,
    Configuring,
    Preparing,
    Ready,
    Running,
    Paused,
    Stopping,
    Faulted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthState {
    Unknown,
    Loading,
    Healthy,
    Degraded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SampleEncoding {
    PcmS16le,
    PcmF32le,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioFormat {
    pub sample_rate: u32,
    pub channels: u16,
    pub encoding: SampleEncoding,
    pub frame_duration_us: u32,
}

impl Default for AudioFormat {
    fn default() -> Self {
        Self {
            sample_rate: NORMALIZED_SAMPLE_RATE,
            channels: 1,
            encoding: SampleEncoding::PcmS16le,
            frame_duration_us: 100_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioFrame {
    pub session_id: Uuid,
    pub epoch: u64,
    pub sequence: u64,
    pub source_id: String,
    pub format: AudioFormat,
    pub payload: Vec<u8>,
    pub source_pts_ns: Option<i64>,
    pub capture_mono_ns: i64,
    pub ingress_mono_ns: i64,
    pub duration_us: u32,
    pub discontinuity: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceInfo {
    pub source_id: String,
    pub kind: String,
    pub name: String,
    pub pid: Option<u32>,
    pub bundle_id: Option<String>,
    pub can_mute: bool,
    pub can_exclude_sink: bool,
    pub is_running: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceConfig {
    Mock {
        frames: Option<u32>,
    },
    File {
        path: String,
        realtime: bool,
    },
    MacosProcess {
        pids: Vec<u32>,
        mute_original: bool,
    },
    MacosSystem {
        exclude_pids: Vec<u32>,
        mute_original: bool,
    },
    MacosMicrophone,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self::Mock { frames: Some(40) }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum TranslatorConfig {
    #[default]
    Mock,
    HySimulmt {
        direction: String,
        context: Option<u32>,
        n_predict: Option<u32>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutputConfig {
    #[default]
    Mock,
    MacosSystem {
        voice: Option<String>,
        /// Approximate words per minute for the resident system synthesizer.
        /// Higher values shorten each segment and reduce playback backlog.
        speech_rate: Option<u32>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LatencyMode {
    LowLatency,
    Balanced,
    Complete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverflowBehavior {
    BlockProducer,
    DropOldestAudio,
    DropNewestAudio,
    SkipStaleSpeech,
    FailSession,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuePolicy {
    pub max_frames: usize,
    pub max_buffered_duration_us: u32,
    pub max_model_backlog_us: u32,
    pub overflow_behavior: OverflowBehavior,
}

impl Default for QueuePolicy {
    fn default() -> Self {
        Self {
            max_frames: 64,
            max_buffered_duration_us: 6_400_000,
            max_model_backlog_us: 10_000_000,
            overflow_behavior: OverflowBehavior::DropNewestAudio,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    pub source: SourceConfig,
    pub translator: TranslatorConfig,
    pub output: OutputConfig,
    pub latency_mode: LatencyMode,
    pub queue: QueuePolicy,
    pub mute_original: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            source: SourceConfig::default(),
            translator: TranslatorConfig::default(),
            output: OutputConfig::default(),
            latency_mode: LatencyMode::Balanced,
            queue: QueuePolicy::default(),
            mute_original: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TranslationEventType {
    Append,
    Revise,
    Final,
    Reset,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranslationEvent {
    pub session_id: Uuid,
    pub epoch: u64,
    #[serde(default)]
    pub source_generation: u64,
    pub segment_id: String,
    pub revision: u64,
    pub event_type: TranslationEventType,
    pub source_text: String,
    pub translated_text: String,
    pub source_start_ns: Option<i64>,
    pub source_end_ns: Option<i64>,
    pub generation_started_ns: i64,
    pub generation_finished_ns: i64,
    pub generation_duration_us: u32,
    pub speakable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubtitleCue {
    pub cue_id: String,
    pub revision: u64,
    pub source_text: String,
    pub translated_text: String,
    pub source_start_ns: i64,
    pub source_end_ns: i64,
    pub is_final: bool,
    #[serde(default)]
    pub segment_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlaybackEventType {
    Queued,
    Started,
    Finished,
    Cancelled,
    Expired,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaybackEvent {
    pub event_type: PlaybackEventType,
    pub segment_id: String,
    pub token: u64,
    pub queue_duration_us: u64,
    pub actual_start_mono_ns: Option<i64>,
    #[serde(default)]
    pub source_end_ns: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionMetrics {
    pub captured_frames: u64,
    pub captured_duration_us: u64,
    pub sent_frames: u64,
    pub sent_duration_us: u64,
    pub completed_duration_us: u64,
    pub dropped_frames: u64,
    pub dropped_duration_us: u64,
    pub model_backlog_us: u64,
    pub context_position: u32,
    pub tts_queue_depth: u32,
    pub translations: u64,
    pub playback_started: u64,
    pub playback_finished: u64,
    pub playback_cancelled: u64,
    #[serde(default)]
    pub playback_pending: u64,
    pub output_latency_us: u64,
    pub max_output_latency_us: u64,
    pub playback_lag_us: u64,
    pub max_playback_lag_us: u64,
    pub context_rotations: u64,
    pub model_restarts: u64,
    pub source_resets: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum SessionEventKind {
    State {
        state: SessionState,
        message: Option<String>,
    },
    Translation(TranslationEvent),
    Subtitle(SubtitleCue),
    HistoryCleared,
    Playback(PlaybackEvent),
    Metrics(SessionMetrics),
    Warning {
        code: String,
        message: String,
    },
    Error {
        code: String,
        message: String,
        fatal: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionEvent {
    pub api_version: String,
    pub session_id: Uuid,
    pub epoch: u64,
    pub emitted_mono_ns: i64,
    pub kind: SessionEventKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSnapshot {
    pub api_version: String,
    pub session_id: Uuid,
    pub epoch: u64,
    pub state: SessionState,
    pub title: String,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub ended_at_ms: Option<i64>,
    pub config: PipelineConfig,
    pub metrics: SessionMetrics,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub code: String,
    pub component_id: String,
    pub message: String,
    pub retryable: bool,
    pub fatal: bool,
}
