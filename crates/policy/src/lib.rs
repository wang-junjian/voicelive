mod subtitle;

pub use subtitle::SubtitleAssembler;

use simulmt_api::{AudioFormat, AudioFrame, OverflowBehavior, QueuePolicy};
use std::collections::VecDeque;

#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    #[error("audio format must be 16 kHz mono PCM S16LE")]
    UnsupportedFormat,
    #[error("audio payload duration does not match its format")]
    InvalidDuration,
}

/// Converts small transport frames into the model's required 800 ms blocks.
pub struct AudioChunker {
    buffered: Vec<u8>,
    buffered_us: u32,
    next_sequence: u64,
}

impl Default for AudioChunker {
    fn default() -> Self {
        Self {
            buffered: Vec::new(),
            buffered_us: 0,
            next_sequence: 0,
        }
    }
}

impl AudioChunker {
    pub fn push(&mut self, frame: &AudioFrame) -> Result<Vec<AudioFrame>, PolicyError> {
        validate_normalized(frame)?;
        self.buffered.extend_from_slice(&frame.payload);
        self.buffered_us += frame.duration_us;
        let mut result = Vec::new();
        while self.buffered_us >= 800_000 {
            let payload: Vec<u8> = self.buffered.drain(..25_600).collect();
            self.buffered_us -= 800_000;
            result.push(AudioFrame {
                session_id: frame.session_id,
                epoch: frame.epoch,
                sequence: self.next_sequence,
                source_id: frame.source_id.clone(),
                format: AudioFormat {
                    frame_duration_us: 800_000,
                    ..frame.format.clone()
                },
                payload,
                source_pts_ns: frame.source_pts_ns,
                capture_mono_ns: frame.capture_mono_ns,
                ingress_mono_ns: frame.ingress_mono_ns,
                duration_us: 800_000,
                discontinuity: frame.discontinuity,
            });
            self.next_sequence += 1;
        }
        Ok(result)
    }

    pub fn buffered_duration_us(&self) -> u32 {
        self.buffered_us
    }
}

pub fn validate_normalized(frame: &AudioFrame) -> Result<(), PolicyError> {
    if frame.format.sample_rate != 16_000
        || frame.format.channels != 1
        || frame.format.encoding != simulmt_api::SampleEncoding::PcmS16le
    {
        return Err(PolicyError::UnsupportedFormat);
    }
    let expected = frame.duration_us as usize * 32 / 1_000;
    if frame.payload.len() != expected {
        return Err(PolicyError::InvalidDuration);
    }
    Ok(())
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

    pub fn push(&mut self, item: T) -> Option<T> {
        if self.items.len() < self.policy.max_frames {
            self.items.push_back(item);
            return None;
        }
        match self.policy.overflow_behavior {
            OverflowBehavior::DropOldestAudio | OverflowBehavior::SkipStaleSpeech => {
                let old = self.items.pop_front();
                self.items.push_back(item);
                old
            }
            OverflowBehavior::DropNewestAudio => Some(item),
            OverflowBehavior::BlockProducer | OverflowBehavior::FailSession => {
                self.items.push_back(item);
                None
            }
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
        AudioFrame {
            session_id: Uuid::nil(),
            epoch: 1,
            sequence,
            source_id: "test".into(),
            format: AudioFormat {
                sample_rate: 16_000,
                channels: 1,
                encoding: SampleEncoding::PcmS16le,
                frame_duration_us: duration_us,
            },
            payload: vec![0; duration_us as usize * 32 / 1_000],
            source_pts_ns: None,
            capture_mono_ns: 0,
            ingress_mono_ns: 0,
            duration_us,
            discontinuity: false,
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
        assert_eq!(chunker.buffered_duration_us(), 0);
    }

    #[test]
    fn bounded_queue_never_grows_without_limit() {
        let mut queue = BoundedQueue::new(QueuePolicy {
            max_frames: 2,
            overflow_behavior: OverflowBehavior::DropOldestAudio,
            ..QueuePolicy::default()
        });
        assert_eq!(queue.push(1), None);
        assert_eq!(queue.push(2), None);
        assert_eq!(queue.push(3), Some(1));
        assert_eq!(queue.pop(), Some(2));
        assert_eq!(queue.pop(), Some(3));
    }
}
