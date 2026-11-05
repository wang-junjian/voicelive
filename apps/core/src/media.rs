use crate::auth::security_error;
use axum::body::Body;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use futures_util::StreamExt;
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

const DEFAULT_MAX_INPUT_BYTES: u64 = 1024 * 1024 * 1024;
const DEFAULT_MAX_OUTPUT_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const DEFAULT_DECODE_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_DISK_RESERVE_BYTES: u64 = 512 * 1024 * 1024;
const DECODER_STDERR_LIMIT: u64 = 64 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct MediaLimits {
    pub max_input_bytes: u64,
    pub max_output_bytes: u64,
    pub decode_timeout: Duration,
    pub disk_reserve_bytes: u64,
    pub concurrency: usize,
}

impl MediaLimits {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            max_input_bytes: environment_u64("SIMULMT_MEDIA_MAX_BYTES", DEFAULT_MAX_INPUT_BYTES)?,
            max_output_bytes: environment_u64(
                "SIMULMT_MEDIA_MAX_OUTPUT_BYTES",
                DEFAULT_MAX_OUTPUT_BYTES,
            )?,
            decode_timeout: Duration::from_secs(environment_u64(
                "SIMULMT_MEDIA_DECODE_TIMEOUT_SECS",
                DEFAULT_DECODE_TIMEOUT.as_secs(),
            )?),
            disk_reserve_bytes: environment_u64(
                "SIMULMT_MEDIA_DISK_RESERVE_BYTES",
                DEFAULT_DISK_RESERVE_BYTES,
            )?,
            concurrency: usize::try_from(environment_u64("SIMULMT_MEDIA_CONCURRENCY", 2)?)
                .map_err(|_| "SIMULMT_MEDIA_CONCURRENCY is too large".to_string())?,
        })
        .and_then(|limits| {
            if limits.max_input_bytes == 0
                || limits.max_output_bytes == 0
                || limits.decode_timeout.is_zero()
                || limits.concurrency == 0
            {
                Err("media limits and concurrency must be greater than zero".into())
            } else {
                Ok(limits)
            }
        })
    }
}

fn environment_u64(name: &str, default: u64) -> Result<u64, String> {
    std::env::var(name).map_or(Ok(default), |value| {
        value
            .parse::<u64>()
            .map_err(|_| format!("{name} must be an unsigned integer"))
    })
}

#[derive(Serialize)]
pub(crate) struct ImportedMedia {
    pub name: String,
    pub path: String,
}

struct MediaArtifacts {
    input: PathBuf,
    temporary_output: PathBuf,
    output: PathBuf,
    published: bool,
}

impl Drop for MediaArtifacts {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.input);
        let _ = std::fs::remove_file(&self.temporary_output);
        if !self.published {
            let _ = std::fs::remove_file(&self.output);
        }
    }
}

pub(crate) async fn import(
    runtime_dir: &Path,
    requested_name: Option<String>,
    headers: HeaderMap,
    body: Body,
    limits: MediaLimits,
) -> Result<ImportedMedia, Response> {
    if headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > limits.max_input_bytes)
    {
        return Err(media_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "media_too_large",
            "the selected media exceeds the configured upload limit",
        ));
    }
    let name = requested_name.unwrap_or_else(|| "local-media.wav".into());
    let extension = Path::new(&name)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("wav")
        .to_ascii_lowercase();
    let supported = ["wav", "mp3", "m4a", "aac", "flac", "mp4", "mov", "m4v"];
    if !supported.contains(&extension.as_str()) {
        return Err(media_error(
            StatusCode::BAD_REQUEST,
            "unsupported_media",
            "unsupported media format; use WAV, MP3, M4A, AAC, FLAC, MP4, MOV or M4V",
        ));
    }

    let directory = runtime_dir.join("media");
    tokio::fs::create_dir_all(&directory)
        .await
        .map_err(io_error)?;
    ensure_disk_space(&directory, limits.disk_reserve_bytes, 0).map_err(|error| *error)?;
    let stem = Uuid::new_v4().to_string();
    let mut artifacts = MediaArtifacts {
        input: directory.join(format!("{stem}-input.{extension}")),
        temporary_output: directory.join(format!("{stem}.decoding")),
        output: directory.join(format!("{stem}.wav")),
        published: false,
    };
    let input_bytes = stream_upload(body, &artifacts.input, limits.max_input_bytes).await?;
    if input_bytes == 0 {
        return Err(media_error(
            StatusCode::BAD_REQUEST,
            "empty_media",
            "the selected media file is empty",
        ));
    }
    ensure_disk_space(&directory, limits.disk_reserve_bytes, input_bytes)
        .map_err(|error| *error)?;
    decode(&artifacts, &limits).await?;
    validate_decoded(&artifacts.temporary_output, limits.max_output_bytes).await?;
    tokio::fs::rename(&artifacts.temporary_output, &artifacts.output)
        .await
        .map_err(io_error)?;
    artifacts.published = true;
    Ok(ImportedMedia {
        name,
        path: artifacts.output.to_string_lossy().into_owned(),
    })
}

async fn stream_upload(body: Body, path: &Path, maximum: u64) -> Result<u64, Response> {
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
        .map_err(io_error)?;
    let mut stream = body.into_data_stream();
    let mut total = 0_u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            media_error(
                StatusCode::BAD_REQUEST,
                "upload_interrupted",
                &format!("media upload was interrupted: {error}"),
            )
        })?;
        total = total.checked_add(chunk.len() as u64).ok_or_else(|| {
            media_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "media_too_large",
                "the selected media exceeds the configured upload limit",
            )
        })?;
        if total > maximum {
            return Err(media_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "media_too_large",
                "the selected media exceeds the configured upload limit",
            ));
        }
        file.write_all(&chunk).await.map_err(io_error)?;
    }
    file.flush().await.map_err(io_error)?;
    file.sync_all().await.map_err(io_error)?;
    Ok(total)
}

async fn decode(artifacts: &MediaArtifacts, limits: &MediaLimits) -> Result<(), Response> {
    let mut child = tokio::process::Command::new("/usr/bin/afconvert")
        .arg("-f")
        .arg("WAVE")
        .arg("-d")
        .arg("LEI16@16000")
        .arg("-c")
        .arg("1")
        .arg(&artifacts.input)
        .arg(&artifacts.temporary_output)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| {
            media_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "decoder_unavailable",
                &format!("failed to start media decoder: {error}"),
            )
        })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        media_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "decoder_unavailable",
            "media decoder stderr is unavailable",
        )
    })?;
    let stderr_task = tokio::spawn(async move {
        let mut buffer = Vec::new();
        let result = stderr
            .take(DECODER_STDERR_LIMIT)
            .read_to_end(&mut buffer)
            .await;
        (result, buffer)
    });
    let deadline = Instant::now() + limits.decode_timeout;
    let status = loop {
        if let Some(status) = child.try_wait().map_err(io_error)? {
            break status;
        }
        if Instant::now() >= deadline {
            terminate_decoder(&mut child).await;
            return Err(media_error(
                StatusCode::GATEWAY_TIMEOUT,
                "media_decode_timeout",
                "media decoding exceeded the configured time limit",
            ));
        }
        if tokio::fs::metadata(&artifacts.temporary_output)
            .await
            .is_ok_and(|metadata| metadata.len() > limits.max_output_bytes)
        {
            terminate_decoder(&mut child).await;
            return Err(media_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "decoded_media_too_large",
                "decoded audio exceeds the configured output limit",
            ));
        }
        if let Err(error) = ensure_disk_space(
            artifacts.output.parent().unwrap_or_else(|| Path::new(".")),
            limits.disk_reserve_bytes,
            0,
        ) {
            terminate_decoder(&mut child).await;
            return Err(*error);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let (read_result, stderr) = stderr_task.await.map_err(|error| {
        media_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "decoder_failure",
            &format!("media decoder task failed: {error}"),
        )
    })?;
    read_result.map_err(io_error)?;
    if !status.success() {
        return Err(media_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "media_decode_failed",
            &format!(
                "cannot decode this media file: {}",
                String::from_utf8_lossy(&stderr).trim()
            ),
        ));
    }
    Ok(())
}

async fn terminate_decoder(child: &mut tokio::process::Child) {
    let _ = child.kill().await;
    let _ = child.wait().await;
}

async fn validate_decoded(path: &Path, maximum: u64) -> Result<(), Response> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let metadata = std::fs::metadata(&path).map_err(io_error)?;
        if metadata.len() == 0 || metadata.len() > maximum {
            return Err(media_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_decoded_media",
                "the decoder produced empty or oversized audio",
            ));
        }
        let reader = hound::WavReader::open(&path).map_err(|error| {
            media_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_decoded_media",
                &format!("the decoder produced invalid WAV audio: {error}"),
            )
        })?;
        let spec = reader.spec();
        if spec.sample_rate != 16_000
            || spec.channels != 1
            || spec.bits_per_sample != 16
            || spec.sample_format != hound::SampleFormat::Int
            || reader.duration() == 0
        {
            return Err(media_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_decoded_media",
                "decoded audio must be non-empty 16 kHz mono PCM16 WAV",
            ));
        }
        Ok(())
    })
    .await
    .map_err(|error| {
        media_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "decoder_failure",
            &format!("decoded media validation task failed: {error}"),
        )
    })?
}

fn ensure_disk_space(path: &Path, reserve: u64, additional: u64) -> Result<(), Box<Response>> {
    let required = reserve.checked_add(additional).ok_or_else(|| {
        Box::new(media_error(
            StatusCode::INSUFFICIENT_STORAGE,
            "insufficient_storage",
            "media storage requirement overflows the local disk budget",
        ))
    })?;
    if available_space(path)? < required {
        return Err(Box::new(media_error(
            StatusCode::INSUFFICIENT_STORAGE,
            "insufficient_storage",
            "insufficient free disk space for a safe local media import",
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn available_space(path: &Path) -> Result<u64, Box<Response>> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        Box::new(media_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "storage_unavailable",
            "the media storage path is invalid",
        ))
    })?;
    // SAFETY: statvfs initializes the provided struct on success; path is a
    // valid NUL-terminated string and the pointer remains valid for the call.
    let mut statistics = unsafe { std::mem::zeroed::<libc::statvfs>() };
    // SAFETY: both pointers are valid and statistics is exclusively borrowed.
    if unsafe { libc::statvfs(path.as_ptr(), &mut statistics) } != 0 {
        return Err(Box::new(io_error(std::io::Error::last_os_error())));
    }
    Ok(u64::from(statistics.f_bavail).saturating_mul(statistics.f_frsize))
}

#[cfg(not(unix))]
fn available_space(_path: &Path) -> Result<u64, Box<Response>> {
    Ok(u64::MAX)
}

fn io_error(error: std::io::Error) -> Response {
    media_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "media_storage_failure",
        &format!("local media operation failed: {error}"),
    )
}

fn media_error(status: StatusCode, code: &str, message: &str) -> Response {
    security_error(status, code, message)
}
