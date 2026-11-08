use crate::error::{CoreError, Result};
use simulmt_api::{AudioFrame, TranslationEvent};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunContext {
    pub session_id: Uuid,
    pub run_id: Uuid,
    pub epoch: u64,
    pub source_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunPhase {
    Idle,
    Candidate,
    Active,
    Draining,
}

#[derive(Debug)]
pub enum AudioRejection {
    Stale,
    Contract(CoreError),
}

#[derive(Debug)]
pub enum TranslationRejection {
    Stale,
    Contract(CoreError),
}

struct RunState {
    phase: RunPhase,
    context: Option<RunContext>,
    next_source_generation: u64,
    captured_duration_us: u64,
    generation_offsets_us: HashMap<(u64, u64), u64>,
    segments: HashSet<(u64, String)>,
    latest_segment_start_us: Option<u64>,
}

/// Owns capture-run identity, source generations and the cumulative media clock.
/// Lifecycle resources stay in Session; all decisions about which data may write
/// to a run, and where that data belongs on its timeline, are made here.
pub struct CaptureRun {
    session_id: Uuid,
    epoch: AtomicU64,
    state: Mutex<RunState>,
}

impl CaptureRun {
    pub fn new(session_id: Uuid, captured_duration_us: u64) -> Self {
        Self {
            session_id,
            epoch: AtomicU64::new(1),
            state: Mutex::new(RunState {
                phase: RunPhase::Idle,
                context: None,
                next_source_generation: 0,
                captured_duration_us,
                generation_offsets_us: HashMap::new(),
                segments: HashSet::new(),
                latest_segment_start_us: None,
            }),
        }
    }

    pub fn current_epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    pub async fn current_context(&self) -> Option<RunContext> {
        self.state.lock().await.context
    }

    pub async fn captured_duration_us(&self) -> u64 {
        self.state.lock().await.captured_duration_us
    }

    pub async fn begin(&self) -> RunContext {
        let epoch = self.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        let mut state = self.state.lock().await;
        state.next_source_generation = state.next_source_generation.saturating_add(1);
        let context = RunContext {
            session_id: self.session_id,
            run_id: Uuid::new_v4(),
            epoch,
            source_generation: state.next_source_generation,
        };
        let offset = state.captured_duration_us;
        state.phase = RunPhase::Candidate;
        state.context = Some(context);
        state.generation_offsets_us.clear();
        state
            .generation_offsets_us
            .insert((epoch, context.source_generation), offset);
        state.segments.clear();
        state.latest_segment_start_us = None;
        context
    }

    pub async fn activate(&self, context: RunContext) -> Result<()> {
        let mut state = self.state.lock().await;
        if state.phase != RunPhase::Candidate || state.context != Some(context) {
            return Err(CoreError::InvalidState(
                "capture run candidate is no longer current".into(),
            ));
        }
        state.phase = RunPhase::Active;
        Ok(())
    }

    pub async fn reserve_generation(&self) -> u64 {
        let mut state = self.state.lock().await;
        state.next_source_generation = state.next_source_generation.saturating_add(1);
        state.next_source_generation
    }

    pub async fn reset_source(&self, epoch: u64) -> Result<RunContext> {
        let mut state = self.state.lock().await;
        let Some(mut context) = state.context else {
            return Err(CoreError::InvalidState("no capture run to reset".into()));
        };
        if context.epoch != epoch || !matches!(state.phase, RunPhase::Active | RunPhase::Draining) {
            return Err(CoreError::InvalidState(
                "capture run is no longer current".into(),
            ));
        }
        state.next_source_generation = state.next_source_generation.saturating_add(1);
        context.source_generation = state.next_source_generation;
        let offset = state.captured_duration_us;
        state.context = Some(context);
        state.generation_offsets_us.clear();
        state
            .generation_offsets_us
            .insert((epoch, context.source_generation), offset);
        Ok(context)
    }

    pub async fn begin_drain(&self, epoch: u64) -> bool {
        let mut state = self.state.lock().await;
        if state.phase != RunPhase::Active
            || state.context.is_none_or(|context| context.epoch != epoch)
        {
            return false;
        }
        state.phase = RunPhase::Draining;
        true
    }

    fn clear_run(state: &mut RunState) {
        state.phase = RunPhase::Idle;
        state.context = None;
        state.generation_offsets_us.clear();
        state.segments.clear();
        state.latest_segment_start_us = None;
    }

    pub async fn close(&self, epoch: u64) -> bool {
        let mut state = self.state.lock().await;
        if state.context.is_none_or(|context| context.epoch != epoch) {
            return false;
        }
        Self::clear_run(&mut state);
        drop(state);
        self.epoch.fetch_add(1, Ordering::AcqRel);
        true
    }

    pub async fn invalidate(&self) {
        let mut state = self.state.lock().await;
        Self::clear_run(&mut state);
        drop(state);
        self.epoch.fetch_add(1, Ordering::AcqRel);
    }

    pub async fn context_is_current(&self, context: RunContext) -> bool {
        let state = self.state.lock().await;
        matches!(state.phase, RunPhase::Active | RunPhase::Draining)
            && state.context.is_some_and(|current| {
                current.run_id == context.run_id && current.epoch == context.epoch
            })
    }

    pub async fn source_reset_is_current(&self, epoch: u64, source_generation: u64) -> bool {
        let state = self.state.lock().await;
        matches!(state.phase, RunPhase::Active | RunPhase::Draining)
            && state.context.is_some_and(|context| {
                context.epoch == epoch && context.source_generation == source_generation
            })
    }

    /// Accepts a frame and advances the authoritative captured-media cursor.
    /// Returns the projected cumulative duration for Session metrics.
    pub async fn accept_audio(
        &self,
        frame: &AudioFrame,
    ) -> std::result::Result<u64, AudioRejection> {
        let mut state = self.state.lock().await;
        if state.phase != RunPhase::Active {
            return Err(AudioRejection::Stale);
        }
        let Some(context) = state.context else {
            return Err(AudioRejection::Stale);
        };
        if frame.epoch < context.epoch {
            return Err(AudioRejection::Stale);
        }
        if frame.epoch > context.epoch {
            return Err(AudioRejection::Contract(CoreError::InvalidConfig(format!(
                "audio frame uses future epoch {} instead of {}",
                frame.epoch, context.epoch
            ))));
        }
        if frame.session_id != self.session_id {
            return Err(AudioRejection::Contract(CoreError::InvalidConfig(format!(
                "audio frame belongs to session {} instead of {}",
                frame.session_id, self.session_id
            ))));
        }
        state.captured_duration_us = state
            .captured_duration_us
            .saturating_add(frame.duration_us as u64);
        Ok(state.captured_duration_us)
    }

    pub fn validate_translation(event: &TranslationEvent) -> Result<()> {
        let (Some(start_ns), Some(end_ns)) = (event.source_start_ns, event.source_end_ns) else {
            return Err(CoreError::Translator(format!(
                "segment {} is missing source timing",
                event.segment_id
            )));
        };
        if start_ns < 0 || end_ns < start_ns {
            return Err(CoreError::Translator(format!(
                "segment {} has invalid source timing [{start_ns}, {end_ns})",
                event.segment_id
            )));
        }
        Ok(())
    }

    pub async fn accept_translation(
        &self,
        mut event: TranslationEvent,
    ) -> std::result::Result<TranslationEvent, TranslationRejection> {
        let mut state = self.state.lock().await;
        let Some(context) = state.context else {
            return Err(TranslationRejection::Stale);
        };
        if !matches!(state.phase, RunPhase::Active | RunPhase::Draining) {
            return Err(TranslationRejection::Stale);
        }
        if event.epoch < context.epoch {
            return Err(TranslationRejection::Stale);
        }
        if event.epoch > context.epoch {
            return Err(TranslationRejection::Contract(CoreError::Translator(
                format!(
                    "segment {} uses future epoch {} instead of {}",
                    event.segment_id, event.epoch, context.epoch
                ),
            )));
        }
        if event.session_id != self.session_id {
            return Err(TranslationRejection::Contract(CoreError::Translator(
                format!(
                    "segment {} belongs to session {} instead of {}",
                    event.segment_id, event.session_id, self.session_id
                ),
            )));
        }
        if event.source_generation < context.source_generation {
            return Err(TranslationRejection::Stale);
        }
        if event.source_generation != context.source_generation {
            return Err(TranslationRejection::Contract(CoreError::Translator(
                format!(
                    "segment {} uses unregistered source generation {}",
                    event.segment_id, event.source_generation
                ),
            )));
        }
        Self::validate_translation(&event).map_err(TranslationRejection::Contract)?;
        let Some(offset_us) = state
            .generation_offsets_us
            .get(&(event.epoch, event.source_generation))
            .copied()
        else {
            return Err(TranslationRejection::Contract(CoreError::Translator(
                "source generation has no registered timeline origin".into(),
            )));
        };
        let local_id = event.segment_id.clone();
        let segment_key = (event.source_generation, local_id.clone());
        let start_ns = event.source_start_ns.unwrap_or_default();
        let end_ns = event.source_end_ns.unwrap_or_default();
        let offset_ns = offset_us
            .checked_mul(1_000)
            .filter(|offset| *offset <= i64::MAX as u64)
            .ok_or_else(|| {
                TranslationRejection::Contract(CoreError::Translator(
                    "source timeline offset exceeds the API range".into(),
                ))
            })? as i64;
        let absolute_start_ns = start_ns.checked_add(offset_ns).ok_or_else(|| {
            TranslationRejection::Contract(CoreError::Translator(
                "source segment start exceeds the API range".into(),
            ))
        })?;
        let absolute_end_ns = end_ns.checked_add(offset_ns).ok_or_else(|| {
            TranslationRejection::Contract(CoreError::Translator(
                "source segment end exceeds the API range".into(),
            ))
        })?;
        let absolute_start_us = absolute_start_ns as u64 / 1_000;
        if !state.segments.contains(&segment_key)
            && state
                .latest_segment_start_us
                .is_some_and(|latest| absolute_start_us < latest)
        {
            return Err(TranslationRejection::Contract(CoreError::Translator(
                format!("segment {local_id} moves backwards on the source timeline"),
            )));
        }
        state.segments.insert(segment_key);
        state.latest_segment_start_us = Some(
            state
                .latest_segment_start_us
                .unwrap_or(0)
                .max(absolute_start_us),
        );
        event.segment_id = format!(
            "run-{}-source-{}-{local_id}",
            context.run_id, event.source_generation
        );
        event.source_start_ns = Some(absolute_start_ns);
        event.source_end_ns = Some(absolute_end_ns);
        Ok(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use simulmt_api::{AudioFormat, TranslationEventType};

    fn audio(session_id: Uuid, epoch: u64, duration_us: u32) -> AudioFrame {
        AudioFrame {
            session_id,
            epoch,
            sequence: 0,
            source_id: "test".into(),
            format: AudioFormat::default(),
            payload: Vec::new(),
            source_pts_ns: None,
            capture_mono_ns: 0,
            ingress_mono_ns: 0,
            duration_us,
            discontinuity: false,
        }
    }

    fn translation(context: RunContext, source_generation: u64) -> TranslationEvent {
        TranslationEvent {
            session_id: context.session_id,
            epoch: context.epoch,
            source_generation,
            segment_id: "local-0".into(),
            revision: 0,
            event_type: TranslationEventType::Append,
            source_text: "source".into(),
            translated_text: "target".into(),
            source_start_ns: Some(0),
            source_end_ns: Some(100_000_000),
            generation_started_ns: 0,
            generation_finished_ns: 0,
            generation_duration_us: 0,
            speakable: false,
        }
    }

    #[tokio::test]
    async fn future_epoch_is_a_contract_error_instead_of_stale_data() {
        let session_id = Uuid::new_v4();
        let timeline = CaptureRun::new(session_id, 0);
        let context = timeline.begin().await;
        timeline.activate(context).await.unwrap();
        assert!(matches!(
            timeline
                .accept_audio(&audio(session_id, context.epoch + 1, 100_000))
                .await,
            Err(AudioRejection::Contract(_))
        ));
        let mut event = translation(context, context.source_generation);
        event.epoch = context.epoch + 1;
        assert!(matches!(
            timeline.accept_translation(event).await,
            Err(TranslationRejection::Contract(_))
        ));
    }

    #[tokio::test]
    async fn wrong_session_audio_in_current_epoch_is_a_contract_error() {
        let timeline = CaptureRun::new(Uuid::new_v4(), 0);
        let context = timeline.begin().await;
        timeline.activate(context).await.unwrap();
        let event = audio(Uuid::new_v4(), context.epoch, 100_000);
        assert!(matches!(
            timeline.accept_audio(&event).await,
            Err(AudioRejection::Contract(_))
        ));
    }

    #[tokio::test]
    async fn wrong_session_in_current_epoch_is_a_contract_error() {
        let timeline = CaptureRun::new(Uuid::new_v4(), 0);
        let context = timeline.begin().await;
        timeline.activate(context).await.unwrap();
        let mut event = translation(context, context.source_generation);
        event.session_id = Uuid::new_v4();
        assert!(matches!(
            timeline.accept_translation(event).await,
            Err(TranslationRejection::Contract(_))
        ));
    }

    #[tokio::test]
    async fn source_reset_binds_live_edge_and_rejects_old_generation() {
        let session_id = Uuid::new_v4();
        let timeline = CaptureRun::new(session_id, 0);
        let initial = timeline.begin().await;
        timeline.activate(initial).await.unwrap();
        timeline
            .accept_audio(&audio(session_id, initial.epoch, 2_000_000))
            .await
            .unwrap();
        let reset = timeline.reset_source(initial.epoch).await.unwrap();

        let accepted = timeline
            .accept_translation(translation(reset, reset.source_generation))
            .await
            .unwrap();
        assert_eq!(accepted.source_start_ns, Some(2_000_000_000));
        assert!(matches!(
            timeline
                .accept_translation(translation(reset, initial.source_generation))
                .await,
            Err(TranslationRejection::Stale)
        ));
    }
}
