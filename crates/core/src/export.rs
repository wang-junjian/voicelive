use crate::error::{CoreError, Result};
use crate::store::SpeechSegment;
use simulmt_api::SubtitleCue;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

const COPY_BUFFER_BYTES: usize = 64 * 1024;
const CANONICAL_WAV_HEADER_BYTES: usize = 44;
const MINIMUM_CUE_DURATION_NS: i64 = 400_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubtitleExportFormat {
    Srt,
    Vtt,
}

/// Renders one immutable history snapshot without consulting live session state.
pub(crate) fn render_subtitles(cues: &[SubtitleCue], format: SubtitleExportFormat) -> String {
    let vtt = format == SubtitleExportFormat::Vtt;
    let mut output = if vtt {
        "WEBVTT\n\n".to_string()
    } else {
        String::new()
    };
    for (index, cue) in cues.iter().enumerate() {
        if !vtt {
            output.push_str(&(index + 1).to_string());
            output.push('\n');
        }
        output.push_str(&subtitle_time(cue.source_start_ns, vtt));
        output.push_str(" --> ");
        output.push_str(&subtitle_time(
            cue.source_end_ns
                .max(cue.source_start_ns.saturating_add(MINIMUM_CUE_DURATION_NS)),
            vtt,
        ));
        output.push('\n');
        push_subtitle_text(&mut output, &cue.source_text, vtt);
        output.push('\n');
        push_subtitle_text(&mut output, &cue.translated_text, vtt);
        output.push_str("\n\n");
    }
    output
}

/// WebVTT defines text entities; SubRip does not, so SRT text stays verbatim
/// apart from newline normalization and whitespace on otherwise empty lines.
fn push_subtitle_text(output: &mut String, text: &str, vtt: bool) {
    let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
    for (index, line) in normalized.split('\n').enumerate() {
        if index > 0 {
            output.push('\n');
        }
        if line.is_empty() {
            // A physically empty line terminates an SRT/WebVTT cue. A single
            // space preserves an empty source/translation without corrupting
            // the following cue boundary.
            output.push(' ');
            continue;
        }
        if !vtt {
            output.push_str(line);
            continue;
        }
        for character in line.chars() {
            match character {
                '&' => output.push_str("&amp;"),
                '<' => output.push_str("&lt;"),
                '>' => output.push_str("&gt;"),
                _ => output.push(character),
            }
        }
    }
}

fn subtitle_time(ns: i64, vtt: bool) -> String {
    let total_ms = (ns.max(0) / 1_000_000) as u64;
    let hours = total_ms / 3_600_000;
    let minutes = total_ms / 60_000 % 60;
    let seconds = total_ms / 1_000 % 60;
    let millis = total_ms % 1_000;
    let separator = if vtt { '.' } else { ',' };
    format!("{hours:02}:{minutes:02}:{seconds:02}{separator}{millis:03}")
}

/// Builds a WAV from one immutable speech-manifest snapshot.
///
/// Missing, failed, pending, malformed, truncated, or concurrently modified
/// segments are explicit errors. PCM is copied through a fixed-size buffer;
/// no complete input or output file is held in memory. Every invocation writes
/// a distinct temporary file and atomically publishes a distinct result.
pub(crate) async fn export_wav(
    session_id: Uuid,
    segments: Vec<SpeechSegment>,
    cache_dir: PathBuf,
) -> Result<PathBuf> {
    if segments.is_empty() {
        return Err(CoreError::InvalidConfig(
            "no synthesized translation audio is available yet".into(),
        ));
    }
    let incomplete = segments
        .iter()
        .filter(|segment| segment.status != "ready")
        .map(|segment| format!("{} ({})", segment.token, segment.status))
        .collect::<Vec<_>>();
    if !incomplete.is_empty() {
        return Err(CoreError::Output(format!(
            "translation audio is incomplete: {}",
            incomplete.join(", ")
        )));
    }
    tokio::task::spawn_blocking(move || export_wav_sync(session_id, &segments, &cache_dir))
        .await
        .map_err(|error| CoreError::Output(format!("WAV export task failed: {error}")))?
}

fn export_wav_sync(
    session_id: Uuid,
    segments: &[SpeechSegment],
    cache_dir: &Path,
) -> Result<PathBuf> {
    std::fs::create_dir_all(cache_dir)?;
    let export_id = Uuid::new_v4();
    let temporary = cache_dir.join(format!("translation-{session_id}-{export_id}.tmp"));
    let destination = cache_dir.join(format!("translation-{session_id}-{export_id}.wav"));
    let result = write_concatenated_wav(segments, &temporary);
    if let Err(error) = result {
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    if let Err(error) = std::fs::rename(&temporary, &destination) {
        let _ = std::fs::remove_file(&temporary);
        return Err(error.into());
    }
    Ok(destination)
}

fn write_concatenated_wav(segments: &[SpeechSegment], temporary: &Path) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temporary)?;
    let mut writer = BufWriter::new(file);
    writer.write_all(&[0_u8; CANONICAL_WAV_HEADER_BYTES])?;

    let mut expected_spec = None;
    let mut total_data_bytes = 0_u64;
    for segment in segments {
        let before = std::fs::metadata(&segment.path).map_err(|error| {
            CoreError::Output(format!(
                "cannot inspect speech token {} at {}: {error}",
                segment.token,
                segment.path.display()
            ))
        })?;
        let file = File::open(&segment.path).map_err(|error| {
            CoreError::Output(format!(
                "cannot read speech token {} at {}: {error}",
                segment.token,
                segment.path.display()
            ))
        })?;
        let reader = hound::WavReader::new(BufReader::new(file)).map_err(|error| {
            CoreError::Output(format!(
                "invalid WAV in speech token {}: {error}",
                segment.token
            ))
        })?;
        let spec = reader.spec();
        validate_pcm16(spec, segment.token)?;
        if let Some(expected) = expected_spec {
            if spec != expected {
                return Err(CoreError::Output(format!(
                    "inconsistent synthesized WAV format at token {}",
                    segment.token
                )));
            }
        } else {
            expected_spec = Some(spec);
        }

        let segment_bytes = u64::from(reader.len()) * 2;
        total_data_bytes = total_data_bytes
            .checked_add(segment_bytes)
            .ok_or_else(|| CoreError::Output("combined translation WAV is too large".into()))?;
        if total_data_bytes > u64::from(u32::MAX - 36) {
            return Err(CoreError::Output(
                "combined translation WAV exceeds the RIFF 4 GiB limit".into(),
            ));
        }
        let mut data = reader.into_inner();
        copy_exact(&mut data, &mut writer, segment_bytes, segment.token)?;
        let after = std::fs::metadata(&segment.path).map_err(|error| {
            CoreError::Output(format!(
                "cannot verify speech token {} after reading: {error}",
                segment.token
            ))
        })?;
        if before.len() != after.len() || before.modified().ok() != after.modified().ok() {
            return Err(CoreError::Output(format!(
                "speech token {} changed while it was being exported",
                segment.token
            )));
        }
    }

    let spec = expected_spec.ok_or_else(|| {
        CoreError::InvalidConfig("no synthesized translation audio is available yet".into())
    })?;
    writer.flush()?;
    writer.seek(SeekFrom::Start(0))?;
    write_canonical_header(&mut writer, spec, total_data_bytes as u32)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

fn validate_pcm16(spec: hound::WavSpec, token: u64) -> Result<()> {
    if spec.sample_format != hound::SampleFormat::Int
        || spec.bits_per_sample != 16
        || spec.channels == 0
        || spec.sample_rate == 0
    {
        return Err(CoreError::Output(format!(
            "speech token {token} is not PCM16 WAV"
        )));
    }
    spec.channels.checked_mul(2).ok_or_else(|| {
        CoreError::Output(format!("speech token {token} has an invalid channel count"))
    })?;
    u64::from(spec.sample_rate)
        .checked_mul(u64::from(spec.channels) * 2)
        .filter(|byte_rate| *byte_rate <= u64::from(u32::MAX))
        .ok_or_else(|| {
            CoreError::Output(format!("speech token {token} has an invalid sample rate"))
        })?;
    Ok(())
}

fn copy_exact(
    reader: &mut impl Read,
    writer: &mut impl Write,
    mut remaining: u64,
    token: u64,
) -> Result<()> {
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    while remaining > 0 {
        let requested = remaining.min(buffer.len() as u64) as usize;
        let read = match reader.read(&mut buffer[..requested]) {
            Ok(0) => {
                return Err(CoreError::Output(format!(
                    "speech token {token} is truncated"
                )));
            }
            Ok(read) => read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => {
                return Err(CoreError::Output(format!(
                    "cannot read PCM in speech token {token}: {error}"
                )));
            }
        };
        writer.write_all(&buffer[..read]).map_err(|error| {
            CoreError::Output(format!("cannot write exported translation WAV: {error}"))
        })?;
        remaining -= read as u64;
    }
    Ok(())
}

fn write_canonical_header(
    writer: &mut impl Write,
    spec: hound::WavSpec,
    data_bytes: u32,
) -> Result<()> {
    let channels = spec.channels;
    let block_align = channels * 2;
    let byte_rate = spec.sample_rate * u32::from(block_align);
    let mut header = [0_u8; CANONICAL_WAV_HEADER_BYTES];
    header[0..4].copy_from_slice(b"RIFF");
    header[4..8].copy_from_slice(&(36 + data_bytes).to_le_bytes());
    header[8..12].copy_from_slice(b"WAVE");
    header[12..16].copy_from_slice(b"fmt ");
    header[16..20].copy_from_slice(&16_u32.to_le_bytes());
    header[20..22].copy_from_slice(&1_u16.to_le_bytes());
    header[22..24].copy_from_slice(&channels.to_le_bytes());
    header[24..28].copy_from_slice(&spec.sample_rate.to_le_bytes());
    header[28..32].copy_from_slice(&byte_rate.to_le_bytes());
    header[32..34].copy_from_slice(&block_align.to_le_bytes());
    header[34..36].copy_from_slice(&16_u16.to_le_bytes());
    header[36..40].copy_from_slice(b"data");
    header[40..44].copy_from_slice(&data_bytes.to_le_bytes());
    writer.write_all(&header)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segment(token: u64, path: PathBuf, status: &str) -> SpeechSegment {
        SpeechSegment {
            token,
            segment_id: format!("segment-{token}"),
            path,
            status: status.into(),
            source_end_ns: None,
        }
    }

    fn write_wav(path: &Path, samples: &[i16]) {
        write_wav_with_spec(
            path,
            samples,
            hound::WavSpec {
                channels: 1,
                sample_rate: 24_000,
                bits_per_sample: 16,
                sample_format: hound::SampleFormat::Int,
            },
        );
    }

    fn write_wav_with_spec(path: &Path, samples: &[i16], spec: hound::WavSpec) {
        let mut writer = hound::WavWriter::create(path, spec).unwrap();
        for sample in samples {
            writer.write_sample(*sample).unwrap();
        }
        writer.finalize().unwrap();
    }

    #[test]
    fn subtitle_exports_escape_vtt_and_keep_srt_text() {
        let cues = vec![SubtitleCue {
            cue_id: "cue-1".into(),
            revision: 7,
            source_text: "Tom & <speaker> --> 原声\r\n第二行".into(),
            translated_text: "A&B\n\nC".into(),
            source_start_ns: 3_661_007_000_000,
            source_end_ns: 3_662_009_000_000,
            is_final: true,
            segment_ids: Vec::new(),
        }];
        let vtt = render_subtitles(&cues, SubtitleExportFormat::Vtt);
        assert!(vtt.starts_with("WEBVTT\n\n01:01:01.007 --> 01:01:02.009\n"));
        assert!(vtt.contains("Tom &amp; &lt;speaker&gt; --&gt; 原声\n第二行"));
        assert!(vtt.contains("A&amp;B\n \nC"));
        assert!(!vtt.contains('\r'));

        let srt = render_subtitles(&cues, SubtitleExportFormat::Srt);
        assert!(srt.starts_with("1\n01:01:01,007 --> 01:01:02,009\n"));
        assert!(srt.contains("Tom & <speaker> --> 原声\n第二行"));
        assert!(srt.contains("A&B\n \nC"));
    }

    #[test]
    fn subtitle_export_enforces_minimum_duration_without_overflow() {
        let cues = vec![SubtitleCue {
            cue_id: "cue-1".into(),
            revision: 1,
            source_text: String::new(),
            translated_text: String::new(),
            source_start_ns: i64::MAX,
            source_end_ns: i64::MAX,
            is_final: false,
            segment_ids: Vec::new(),
        }];
        let srt = render_subtitles(&cues, SubtitleExportFormat::Srt);
        assert!(srt.contains(" \n \n\n"));
    }

    #[tokio::test]
    async fn manifest_export_streams_token_holes_and_is_concurrency_safe() {
        let root = tempfile::tempdir().unwrap();
        let first = root.path().join("speech-1.wav");
        let third = root.path().join("speech-3.wav");
        let first_samples = (0..100_000)
            .map(|value| (value % 32_000) as i16)
            .collect::<Vec<_>>();
        let third_samples = vec![-7_i16; 70_000];
        write_wav(&first, &first_samples);
        write_wav(&third, &third_samples);
        let manifest = vec![segment(1, first, "ready"), segment(3, third, "ready")];
        let (left, right) = tokio::join!(
            export_wav(Uuid::new_v4(), manifest.clone(), root.path().to_path_buf()),
            export_wav(Uuid::new_v4(), manifest, root.path().to_path_buf())
        );
        let left = left.unwrap();
        let right = right.unwrap();
        assert_ne!(left, right);
        assert_eq!(
            std::fs::read(&left).unwrap(),
            std::fs::read(&right).unwrap()
        );
        let mut reader = hound::WavReader::open(left).unwrap();
        let actual = reader
            .samples::<i16>()
            .map(|sample| sample.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(actual.len(), first_samples.len() + third_samples.len());
        assert_eq!(actual[..first_samples.len()], first_samples);
        assert_eq!(actual[first_samples.len()..], third_samples);
    }

    #[tokio::test]
    async fn strict_export_rejects_missing_or_incomplete_segments() {
        let root = tempfile::tempdir().unwrap();
        let pending = segment(1, root.path().join("pending.wav"), "pending");
        let error = export_wav(Uuid::new_v4(), vec![pending], root.path().to_path_buf())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("incomplete"));

        let missing = segment(2, root.path().join("missing.wav"), "ready");
        let error = export_wav(Uuid::new_v4(), vec![missing], root.path().to_path_buf())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("token 2"));
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn strict_export_rejects_truncated_and_inconsistent_wav() {
        let root = tempfile::tempdir().unwrap();
        let valid = root.path().join("valid.wav");
        write_wav(&valid, &[1, 2, 3]);
        let truncated = root.path().join("truncated.wav");
        let bytes = std::fs::read(&valid).unwrap();
        std::fs::write(&truncated, &bytes[..bytes.len() - 2]).unwrap();
        let error = export_wav(
            Uuid::new_v4(),
            vec![segment(1, truncated, "ready")],
            root.path().to_path_buf(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("truncated"), "{error}");

        let incompatible = root.path().join("incompatible.wav");
        write_wav_with_spec(
            &incompatible,
            &[4, 5],
            hound::WavSpec {
                channels: 1,
                sample_rate: 16_000,
                bits_per_sample: 16,
                sample_format: hound::SampleFormat::Int,
            },
        );
        let error = export_wav(
            Uuid::new_v4(),
            vec![
                segment(1, valid, "ready"),
                segment(2, incompatible, "ready"),
            ],
            root.path().to_path_buf(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("inconsistent"), "{error}");
        assert!(std::fs::read_dir(root.path()).unwrap().all(|entry| {
            entry
                .unwrap()
                .path()
                .extension()
                .and_then(|value| value.to_str())
                != Some("tmp")
        }));
    }
}
