use simulmt_api::{SubtitleCue, TranslationEvent};

/// Groups low-latency translation deltas into readable cues without delaying TTS.
#[derive(Default)]
pub struct SubtitleAssembler {
    current: Option<SubtitleCue>,
}

impl SubtitleAssembler {
    pub fn push(&mut self, event: &TranslationEvent) -> SubtitleCue {
        if self.current.as_ref().is_some_and(|cue| cue.is_final) {
            self.current = None;
        }
        if self.current.is_none() {
            self.current = Some(SubtitleCue {
                cue_id: uuid::Uuid::new_v4().to_string(),
                revision: 0,
                source_text: String::new(),
                translated_text: String::new(),
                source_start_ns: event.source_start_ns.unwrap_or(0),
                source_end_ns: event.source_end_ns.or(event.source_start_ns).unwrap_or(0),
                is_final: false,
                segment_ids: Vec::new(),
            });
        }
        let cue = self.current.as_mut().expect("subtitle cue initialized");
        cue.revision += 1;
        cue.segment_ids.push(event.segment_id.clone());
        append_delta(&mut cue.source_text, &event.source_text);
        append_delta(&mut cue.translated_text, &event.translated_text);
        if let Some(end) = event.source_end_ns {
            cue.source_end_ns = cue.source_end_ns.max(end);
        }
        let duration_us = (cue.source_end_ns - cue.source_start_ns).max(0) / 1_000;
        let word_count = cue.source_text.split_whitespace().count();
        cue.is_final = ends_sentence(&cue.source_text)
            || ends_sentence(&cue.translated_text)
            || duration_us >= 6_400_000
            || word_count >= 28
            || cue.source_text.chars().count() >= 150
            || cue.translated_text.chars().count() >= 90;
        cue.clone()
    }

    pub fn flush(&mut self) -> Option<SubtitleCue> {
        let cue = self.current.as_mut()?;
        if cue.is_final {
            return None;
        }
        cue.is_final = true;
        cue.revision += 1;
        Some(cue.clone())
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

fn ends_sentence(text: &str) -> bool {
    let text = text.trim_end_matches(|character| {
        matches!(character, '"' | '\'' | '”' | '’' | '）' | ')' | ']' | '}')
    });
    text.trim_end()
        .chars()
        .last()
        .is_some_and(|character| matches!(character, '.' | '!' | '?' | '。' | '！' | '？'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use simulmt_api::TranslationEventType;
    use uuid::Uuid;

    fn event(index: u64, source: &str, target: &str) -> TranslationEvent {
        TranslationEvent {
            session_id: Uuid::nil(),
            epoch: 1,
            source_generation: 0,
            segment_id: format!("segment-{index}"),
            revision: 0,
            event_type: TranslationEventType::Append,
            source_text: source.into(),
            translated_text: target.into(),
            source_start_ns: Some((index * 800_000_000) as i64),
            source_end_ns: Some(((index + 1) * 800_000_000) as i64),
            generation_started_ns: 0,
            generation_finished_ns: 0,
            generation_duration_us: 0,
            speakable: true,
        }
    }

    #[test]
    fn combines_model_deltas_into_a_readable_sentence() {
        let mut assembler = SubtitleAssembler::default();
        let first = assembler.push(&event(0, "they both", "它们都"));
        let second = assembler.push(&event(1, "have their own", "有各自的"));
        assert_eq!(first.cue_id, second.cue_id);
        assert_eq!(second.source_text, "they both have their own");
        assert!(!second.is_final);
        let third = assembler.push(&event(2, "environment.", "环境。"));
        assert!(third.is_final);
        let fourth = assembler.push(&event(3, "Next sentence", "下一句"));
        assert_ne!(third.cue_id, fourth.cue_id);
    }

    #[test]
    fn flush_finishes_an_unterminated_sentence() {
        let mut assembler = SubtitleAssembler::default();
        assembler.push(&event(0, "a complete thought", "完整的想法"));
        let finished = assembler.flush().expect("cue");
        assert!(finished.is_final);
        assert!(assembler.flush().is_none());
    }

    #[test]
    fn flush_does_not_revise_an_already_final_sentence() {
        let mut assembler = SubtitleAssembler::default();
        let finished = assembler.push(&event(0, "complete.", "完整。"));
        assert!(finished.is_final);
        assert!(assembler.flush().is_none());
    }
}
