use crate::error::{CoreError, Result};
use crate::store::SpeechSegment;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Builds a WAV from one immutable speech-manifest snapshot.
///
/// Missing, failed, pending, or malformed segments are explicit errors. Every
/// invocation writes a distinct temporary file and atomically publishes a
/// distinct result, so concurrent exports cannot overwrite each other.
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
    let first = hound::WavReader::open(&segments[0].path).map_err(|error| {
        CoreError::Output(format!(
            "cannot read speech token {}: {error}",
            segments[0].token
        ))
    })?;
    let spec = first.spec();
    drop(first);
    let mut writer = hound::WavWriter::create(temporary, spec)
        .map_err(|error| CoreError::Output(error.to_string()))?;
    for segment in segments {
        let mut reader = hound::WavReader::open(&segment.path).map_err(|error| {
            CoreError::Output(format!(
                "cannot read speech token {} at {}: {error}",
                segment.token,
                segment.path.display()
            ))
        })?;
        if reader.spec() != spec {
            return Err(CoreError::Output(format!(
                "inconsistent synthesized WAV format at token {}",
                segment.token
            )));
        }
        for sample in reader.samples::<i16>() {
            writer
                .write_sample(sample.map_err(|error| {
                    CoreError::Output(format!(
                        "invalid PCM in speech token {}: {error}",
                        segment.token
                    ))
                })?)
                .map_err(|error| CoreError::Output(error.to_string()))?;
        }
    }
    writer
        .finalize()
        .map_err(|error| CoreError::Output(error.to_string()))?;
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
        let mut writer = hound::WavWriter::create(
            path,
            hound::WavSpec {
                channels: 1,
                sample_rate: 24_000,
                bits_per_sample: 16,
                sample_format: hound::SampleFormat::Int,
            },
        )
        .unwrap();
        for sample in samples {
            writer.write_sample(*sample).unwrap();
        }
        writer.finalize().unwrap();
    }

    #[tokio::test]
    async fn manifest_export_spans_token_holes_and_is_concurrency_safe() {
        let root = tempfile::tempdir().unwrap();
        let first = root.path().join("speech-1.wav");
        let third = root.path().join("speech-3.wav");
        write_wav(&first, &[1, 2]);
        write_wav(&third, &[3, 4]);
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
        assert_eq!(
            reader
                .samples::<i16>()
                .map(|sample| sample.unwrap())
                .collect::<Vec<_>>(),
            [1, 2, 3, 4]
        );
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
}
