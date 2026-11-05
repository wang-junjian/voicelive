mod subtitle;

pub use subtitle::SubtitleAssembler;

use simulmt_api::{
    AudioFrame, MODEL_BLOCK_US, NORMALIZED_SAMPLE_RATE, OverflowBehavior, QueuePolicy,
};
use std::collections::VecDeque;

const MODEL_BLOCK_SAMPLES: usize =
    (MODEL_BLOCK_US as u64 * NORMALIZED_SAMPLE_RATE as u64 / 1_000_000) as usize;

#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    #[error("audio format must be 16 kHz mono PCM S16LE")]
    UnsupportedFormat,
    #[error("audio payload duration does not match its format")]
    InvalidDuration,
}

/// Converts small transport frames into the model's required 800 ms blocks.
///
/// A discontinuity first emits any buffered audio as an unpadded partial block, then starts a
/// new block. This keeps PCM from opposite sides of the discontinuity out of the same block.
#[derive(Default)]
pub struct AudioChunker {
    buffered: Vec<u8>,
    buffered_samples: usize,
    buffered_start: Option<AudioFrame>,
    next_sequence: u64,
}

impl AudioChunker {
    pub fn push(&mut self, frame: &AudioFrame) -> Result<Vec<AudioFrame>, PolicyError> {
        validate_normalized(frame)?;
        let mut result = Vec::new();
        if frame.discontinuity {
            result.extend(self.emit_buffered());
        }

        let mut consumed_samples = 0;
        let frame_samples = frame.payload.len() / 2;
        while consumed_samples < frame_samples {
            if self.buffered_samples == 0 {
                self.buffered_start = Some(block_start(frame, consumed_samples));
            }
            let samples_to_copy =
                (MODEL_BLOCK_SAMPLES - self.buffered_samples).min(frame_samples - consumed_samples);
            let byte_start = consumed_samples * 2;
            let byte_end = byte_start + samples_to_copy * 2;
            self.buffered
                .extend_from_slice(&frame.payload[byte_start..byte_end]);
            self.buffered_samples += samples_to_copy;
            consumed_samples += samples_to_copy;

            if self.buffered_samples == MODEL_BLOCK_SAMPLES {
                result.extend(self.emit_buffered());
            }
        }
        Ok(result)
    }

    /// Emits buffered audio as an unpadded final partial block, or `None` if there is no audio.
    ///
    /// `AudioFrame` has no separate valid-duration or end-of-input field. The returned payload
    /// therefore contains only valid PCM and `duration_us` is its real duration. A model adapter
    /// that requires 800 ms input must add padding itself, retain this valid duration, and treat
    /// the source's `Ended` event as end-of-input. Calling `finish` again returns `None` until new
    /// audio is pushed.
    pub fn finish(&mut self) -> Option<AudioFrame> {
        self.emit_buffered()
    }

    pub fn buffered_duration_us(&self) -> u32 {
        duration_for_samples(self.buffered_samples)
    }

    fn emit_buffered(&mut self) -> Option<AudioFrame> {
        if self.buffered_samples == 0 {
            return None;
        }
        let mut block = self
            .buffered_start
            .take()
            .expect("buffered audio has a block start");
        block.sequence = self.next_sequence;
        block.payload = std::mem::take(&mut self.buffered);
        block.duration_us = duration_for_samples(self.buffered_samples);
        block.format.frame_duration_us = block.duration_us;
        self.buffered_samples = 0;
        self.next_sequence += 1;
        Some(block)
    }
}

fn block_start(frame: &AudioFrame, offset_samples: usize) -> AudioFrame {
    let offset_ns = timestamp_offset_ns(offset_samples);
    AudioFrame {
        session_id: frame.session_id,
        epoch: frame.epoch,
        sequence: 0,
        source_id: frame.source_id.clone(),
        format: frame.format.clone(),
        payload: Vec::new(),
        source_pts_ns: frame
            .source_pts_ns
            .map(|timestamp| timestamp.saturating_add(offset_ns)),
        capture_mono_ns: frame.capture_mono_ns.saturating_add(offset_ns),
        ingress_mono_ns: frame.ingress_mono_ns.saturating_add(offset_ns),
        duration_us: frame.duration_us,
        discontinuity: frame.discontinuity,
    }
}

fn timestamp_offset_ns(samples: usize) -> i64 {
    let offset_ns = samples as u128 * 1_000_000_000 / NORMALIZED_SAMPLE_RATE as u128;
    offset_ns.min(i64::MAX as u128) as i64
}

fn duration_for_samples(samples: usize) -> u32 {
    ((samples as u64 * 1_000_000 + NORMALIZED_SAMPLE_RATE as u64 / 2)
        / NORMALIZED_SAMPLE_RATE as u64) as u32
}

pub fn validate_normalized(frame: &AudioFrame) -> Result<(), PolicyError> {
    if frame.format.sample_rate != NORMALIZED_SAMPLE_RATE
        || frame.format.channels != 1
        || frame.format.encoding != simulmt_api::SampleEncoding::PcmS16le
    {
        return Err(PolicyError::UnsupportedFormat);
    }
    if !frame.payload.len().is_multiple_of(2) {
        return Err(PolicyError::InvalidDuration);
    }
    let actual_samples = (frame.payload.len() / 2) as u64;
    let expected_samples =
        (u64::from(frame.duration_us) * u64::from(NORMALIZED_SAMPLE_RATE) + 500_000) / 1_000_000;
    if actual_samples != expected_samples {
        return Err(PolicyError::InvalidDuration);
    }
    Ok(())
}

/// The outcome of pushing into a [`BoundedQueue`].
///
/// Variants containing an item return either the evicted item or the unqueued input item. In
/// particular, `WouldBlock` and `Fail` leave the queue unchanged and return ownership of the input.
#[derive(Debug, PartialEq, Eq)]
pub enum PushResult<T> {
    Enqueued,
    DroppedOldest(T),
    DroppedNewest(T),
    WouldBlock(T),
    Fail(T),
}

pub struct BoundedQueue<T> {
    policy: QueuePolicy,
    items: VecDeque<T>,
}

impl<T> BoundedQueue<T> {
    pub fn new(policy: QueuePolicy) -> Self {
        Self {
            policy,
            items: VecDeque::new(),
        }
    }

    /// Pushes without ever exceeding `max_frames`.
    ///
    /// With a zero-sized drop-oldest queue there is no old item to evict, so the new item is
    /// reported as `DroppedNewest` instead.
    pub fn push(&mut self, item: T) -> PushResult<T> {
        if self.items.len() < self.policy.max_frames {
            self.items.push_back(item);
            return PushResult::Enqueued;
        }
        match self.policy.overflow_behavior {
            OverflowBehavior::DropOldestAudio | OverflowBehavior::SkipStaleSpeech => {
                if let Some(old) = self.items.pop_front() {
                    self.items.push_back(item);
                    PushResult::DroppedOldest(old)
                } else {
                    PushResult::DroppedNewest(item)
                }
            }
            OverflowBehavior::DropNewestAudio => PushResult::DroppedNewest(item),
            OverflowBehavior::BlockProducer => PushResult::WouldBlock(item),
            OverflowBehavior::FailSession => PushResult::Fail(item),
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        self.items.pop_front()
    }
    pub fn len(&self) -> usize {
        self.items.len()
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use simulmt_api::{AudioFormat, SampleEncoding};
    use uuid::Uuid;

    fn frame(sequence: u64, duration_us: u32) -> AudioFrame {
        let samples = ((u64::from(duration_us) * u64::from(NORMALIZED_SAMPLE_RATE) + 500_000)
            / 1_000_000) as usize;
        let mut frame = frame_with_samples(sequence, samples);
        frame.duration_us = duration_us;
        frame.format.frame_duration_us = duration_us;
        frame
    }

    fn frame_with_samples(sequence: u64, samples: usize) -> AudioFrame {
        let duration_us = duration_for_samples(samples);
        AudioFrame {
            session_id: Uuid::nil(),
            epoch: 1,
            sequence,
            source_id: "test".into(),
            format: AudioFormat {
                sample_rate: NORMALIZED_SAMPLE_RATE,
                channels: 1,
                encoding: SampleEncoding::PcmS16le,
                frame_duration_us: duration_us,
            },
            payload: vec![0; samples * 2],
            source_pts_ns: None,
            capture_mono_ns: 0,
            ingress_mono_ns: 0,
            duration_us,
            discontinuity: false,
        }
    }

    fn queue_policy(overflow_behavior: OverflowBehavior) -> QueuePolicy {
        QueuePolicy {
            max_frames: 2,
            overflow_behavior,
            ..QueuePolicy::default()
        }
    }

    #[test]
    fn aligns_transport_frames_to_model_blocks() {
        let mut chunker = AudioChunker::default();
        let mut output = Vec::new();
        for i in 0..8 {
            output.extend(chunker.push(&frame(i, 100_000)).unwrap());
        }
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].payload.len(), 25_600);
        assert_eq!(output[0].duration_us, 800_000);
        assert_eq!(output[0].format.frame_duration_us, 800_000);
        assert_eq!(chunker.buffered_duration_us(), 0);
    }

    #[test]
    fn arbitrary_frame_boundaries_are_split_by_pcm_samples() {
        let mut chunker = AudioChunker::default();
        let mut first = frame_with_samples(0, 1);
        first.payload.copy_from_slice(&[1, 0]);
        assert!(chunker.push(&first).unwrap().is_empty());
        assert_eq!(chunker.buffered_duration_us(), 63);

        let mut second = frame_with_samples(1, MODEL_BLOCK_SAMPLES - 1);
        second.payload.fill(2);
        let blocks = chunker.push(&second).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].payload.len(), MODEL_BLOCK_SAMPLES * 2);
        assert_eq!(&blocks[0].payload[..2], &[1, 0]);
        assert!(blocks[0].payload[2..].iter().all(|byte| *byte == 2));
        assert_eq!(blocks[0].duration_us, MODEL_BLOCK_US);
        assert_eq!(chunker.buffered_duration_us(), 0);

        let blocks = chunker
            .push(&frame_with_samples(2, MODEL_BLOCK_SAMPLES * 2 + 1))
            .unwrap();
        assert_eq!(blocks.len(), 2);
        assert!(
            blocks
                .iter()
                .all(|block| block.duration_us == MODEL_BLOCK_US)
        );
        let tail = chunker.finish().expect("one-sample tail");
        assert_eq!(tail.payload.len(), 2);
        assert_eq!(tail.duration_us, 63);
    }

    #[test]
    fn discontinuity_emits_separate_unpadded_blocks() {
        let mut chunker = AudioChunker::default();
        let mut before = frame_with_samples(0, 4_800);
        before.payload.fill(1);
        assert!(chunker.push(&before).unwrap().is_empty());

        let mut after = frame_with_samples(1, 1_600);
        after.payload.fill(2);
        after.discontinuity = true;
        let blocks = chunker.push(&after).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].sequence, 0);
        assert_eq!(blocks[0].payload.len(), 9_600);
        assert!(blocks[0].payload.iter().all(|byte| *byte == 1));
        assert_eq!(blocks[0].duration_us, 300_000);
        assert!(!blocks[0].discontinuity);
        assert_eq!(chunker.buffered_duration_us(), 100_000);

        let tail = chunker.finish().expect("post-discontinuity tail");
        assert_eq!(tail.sequence, 1);
        assert_eq!(tail.payload.len(), 3_200);
        assert!(tail.payload.iter().all(|byte| *byte == 2));
        assert_eq!(tail.duration_us, 100_000);
        assert!(tail.discontinuity);
    }

    #[test]
    fn finish_handles_empty_tail_and_repeated_calls() {
        let mut chunker = AudioChunker::default();
        assert!(chunker.finish().is_none());
        assert!(chunker.finish().is_none());

        chunker.push(&frame(0, 300_000)).unwrap();
        let tail = chunker.finish().expect("partial tail");
        assert_eq!(tail.payload.len(), 9_600);
        assert_eq!(tail.duration_us, 300_000);
        assert_eq!(tail.format.frame_duration_us, 300_000);
        assert_eq!(chunker.buffered_duration_us(), 0);
        assert!(chunker.finish().is_none());
        assert!(chunker.finish().is_none());
    }

    #[test]
    fn blocks_use_the_first_frames_timestamps_and_continuous_sequences() {
        let mut chunker = AudioChunker::default();
        let mut output = Vec::new();
        for index in 0..16 {
            let mut input = frame(index, 100_000);
            input.source_pts_ns = Some(1_000 + index as i64 * 100_000_000);
            input.capture_mono_ns = 2_000 + index as i64 * 100_000_000;
            input.ingress_mono_ns = 3_000 + index as i64 * 100_000_000;
            output.extend(chunker.push(&input).unwrap());
        }
        assert_eq!(output.len(), 2);
        for (index, block) in output.iter().enumerate() {
            let frame_index = index * 8;
            assert_eq!(block.sequence, index as u64);
            assert_eq!(
                block.source_pts_ns,
                Some(1_000 + frame_index as i64 * 100_000_000)
            );
            assert_eq!(
                block.capture_mono_ns,
                2_000 + frame_index as i64 * 100_000_000
            );
            assert_eq!(
                block.ingress_mono_ns,
                3_000 + frame_index as i64 * 100_000_000
            );
        }
    }

    #[test]
    fn block_starting_inside_a_frame_offsets_its_timestamps() {
        let mut chunker = AudioChunker::default();
        chunker.push(&frame(0, 100_000)).unwrap();

        let mut long_frame = frame(1, 800_000);
        long_frame.source_pts_ns = Some(10_000_000_000);
        long_frame.capture_mono_ns = 20_000_000_000;
        long_frame.ingress_mono_ns = 30_000_000_000;
        assert_eq!(chunker.push(&long_frame).unwrap().len(), 1);

        let blocks = chunker.push(&frame(2, 700_000)).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].source_pts_ns, Some(10_700_000_000));
        assert_eq!(blocks[0].capture_mono_ns, 20_700_000_000);
        assert_eq!(blocks[0].ingress_mono_ns, 30_700_000_000);
    }

    #[test]
    fn validation_accepts_odd_sample_counts_and_rejects_invalid_pcm() {
        for (samples, durations) in [(1, [62, 63]), (3, [187, 188])] {
            for duration_us in durations {
                let mut input = frame_with_samples(0, samples);
                input.duration_us = duration_us;
                input.format.frame_duration_us = duration_us;
                assert!(validate_normalized(&input).is_ok());
            }
        }

        let mut odd_bytes = frame_with_samples(0, 1);
        odd_bytes.payload.push(0);
        assert!(matches!(
            validate_normalized(&odd_bytes),
            Err(PolicyError::InvalidDuration)
        ));

        let mut wrong_sample_count = frame_with_samples(0, 2);
        wrong_sample_count.duration_us = 63;
        assert!(matches!(
            validate_normalized(&wrong_sample_count),
            Err(PolicyError::InvalidDuration)
        ));
    }

    #[test]
    fn every_overflow_policy_stays_bounded() {
        let behaviors = [
            OverflowBehavior::BlockProducer,
            OverflowBehavior::DropOldestAudio,
            OverflowBehavior::DropNewestAudio,
            OverflowBehavior::SkipStaleSpeech,
            OverflowBehavior::FailSession,
        ];
        for behavior in behaviors {
            let mut queue = BoundedQueue::new(queue_policy(behavior));
            assert_eq!(queue.push(1), PushResult::Enqueued);
            assert_eq!(queue.push(2), PushResult::Enqueued);
            let result = queue.push(3);
            match behavior {
                OverflowBehavior::DropOldestAudio | OverflowBehavior::SkipStaleSpeech => {
                    assert_eq!(result, PushResult::DroppedOldest(1));
                }
                OverflowBehavior::DropNewestAudio => {
                    assert_eq!(result, PushResult::DroppedNewest(3));
                }
                OverflowBehavior::BlockProducer => {
                    assert_eq!(result, PushResult::WouldBlock(3));
                }
                OverflowBehavior::FailSession => {
                    assert_eq!(result, PushResult::Fail(3));
                }
            }
            assert_eq!(queue.len(), 2);
            match behavior {
                OverflowBehavior::DropOldestAudio | OverflowBehavior::SkipStaleSpeech => {
                    assert_eq!(queue.pop(), Some(2));
                    assert_eq!(queue.pop(), Some(3));
                }
                _ => {
                    assert_eq!(queue.pop(), Some(1));
                    assert_eq!(queue.pop(), Some(2));
                }
            }
            assert!(queue.is_empty());
        }
    }

    #[test]
    fn zero_capacity_queue_never_enqueues() {
        let behaviors = [
            OverflowBehavior::BlockProducer,
            OverflowBehavior::DropOldestAudio,
            OverflowBehavior::DropNewestAudio,
            OverflowBehavior::SkipStaleSpeech,
            OverflowBehavior::FailSession,
        ];
        for behavior in behaviors {
            let mut policy = queue_policy(behavior);
            policy.max_frames = 0;
            let mut queue = BoundedQueue::new(policy);
            let result = queue.push(1);
            match behavior {
                OverflowBehavior::BlockProducer => {
                    assert_eq!(result, PushResult::WouldBlock(1));
                }
                OverflowBehavior::FailSession => {
                    assert_eq!(result, PushResult::Fail(1));
                }
                _ => assert_eq!(result, PushResult::DroppedNewest(1)),
            }
            assert_eq!(queue.len(), 0);
            assert!(queue.is_empty());
        }
    }
}
