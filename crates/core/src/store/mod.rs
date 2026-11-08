use crate::error::{CoreError, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use simulmt_api::{OutputConfig, PipelineConfig, SubtitleCue};
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SessionMetadata {
    pub title: String,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub ended_at_ms: Option<i64>,
    #[serde(default)]
    pub audio_duration_us: u64,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct StoredSession {
    pub id: Uuid,
    pub config: PipelineConfig,
    pub metadata: SessionMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpeechSegment {
    pub token: u64,
    pub segment_id: String,
    pub path: PathBuf,
    pub status: String,
    pub source_end_ns: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExportSnapshot {
    pub cues: Vec<SubtitleCue>,
    pub speech_segments: Vec<SpeechSegment>,
}

/// A single SQLite connection, used only from short blocking tasks.
///
/// Async callers never hold the connection mutex across an await. WAL permits
/// external read-only tools to inspect a consistent snapshot while Core runs.
pub(crate) struct Store {
    connection: Arc<Mutex<Connection>>,
}

impl Store {
    pub fn open(runtime_dir: &Path) -> Result<Arc<Self>> {
        fs::create_dir_all(runtime_dir)?;
        let connection = Connection::open(runtime_dir.join("voicelive.db"))
            .map_err(|error| CoreError::Storage(error.to_string()))?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(|error| CoreError::Storage(error.to_string()))?;
        connection
            .execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA foreign_keys = ON;
                 PRAGMA synchronous = FULL;
                 CREATE TABLE IF NOT EXISTS schema_migrations (
                     version INTEGER PRIMARY KEY,
                     applied_at_ms INTEGER NOT NULL
                 );
                 INSERT OR IGNORE INTO schema_migrations(version, applied_at_ms)
                     VALUES (1, unixepoch('subsec') * 1000);
                 CREATE TABLE IF NOT EXISTS sessions (
                     session_id TEXT PRIMARY KEY,
                     config_json TEXT NOT NULL,
                     title TEXT NOT NULL,
                     created_at_ms INTEGER NOT NULL,
                     started_at_ms INTEGER,
                     ended_at_ms INTEGER,
                     audio_duration_us INTEGER NOT NULL DEFAULT 0
                 );
                 CREATE TABLE IF NOT EXISTS cues (
                     session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
                     cue_id TEXT NOT NULL,
                     ordinal INTEGER NOT NULL,
                     latest_revision INTEGER NOT NULL,
                     legacy_ambiguous INTEGER NOT NULL DEFAULT 0,
                     PRIMARY KEY(session_id, cue_id)
                 );
                 CREATE TABLE IF NOT EXISTS cue_revisions (
                     session_id TEXT NOT NULL,
                     cue_id TEXT NOT NULL,
                     revision INTEGER NOT NULL,
                     source_text TEXT NOT NULL,
                     translated_text TEXT NOT NULL,
                     source_start_ns INTEGER NOT NULL,
                     source_end_ns INTEGER NOT NULL,
                     is_final INTEGER NOT NULL,
                     PRIMARY KEY(session_id, cue_id, revision),
                     FOREIGN KEY(session_id, cue_id)
                         REFERENCES cues(session_id, cue_id) ON DELETE CASCADE
                 );
                 CREATE TABLE IF NOT EXISTS cue_segments (
                     session_id TEXT NOT NULL,
                     cue_id TEXT NOT NULL,
                     revision INTEGER NOT NULL,
                     position INTEGER NOT NULL,
                     segment_id TEXT NOT NULL,
                     PRIMARY KEY(session_id, cue_id, revision, position),
                     FOREIGN KEY(session_id, cue_id, revision)
                         REFERENCES cue_revisions(session_id, cue_id, revision) ON DELETE CASCADE
                 );
                 CREATE TABLE IF NOT EXISTS speech_segments (
                     session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
                     token INTEGER NOT NULL,
                     segment_id TEXT NOT NULL,
                     path TEXT NOT NULL,
                     status TEXT NOT NULL,
                     source_end_ns INTEGER,
                     PRIMARY KEY(session_id, token)
                 );
                 CREATE TABLE IF NOT EXISTS migration_ledger (
                     source_path TEXT NOT NULL,
                     item_key TEXT NOT NULL,
                     source_hash TEXT NOT NULL,
                     status TEXT NOT NULL,
                     detail TEXT,
                     PRIMARY KEY(source_path, item_key)
                 );",
            )
            .map_err(|error| CoreError::Storage(error.to_string()))?;
        Ok(Arc::new(Self {
            connection: Arc::new(Mutex::new(connection)),
        }))
    }

    async fn run<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        let connection = Arc::clone(&self.connection);
        tokio::task::spawn_blocking(move || {
            let mut connection = connection
                .lock()
                .map_err(|_| CoreError::Storage("database connection was poisoned".into()))?;
            operation(&mut connection)
        })
        .await
        .map_err(|error| CoreError::Storage(format!("database task failed: {error}")))?
    }

    pub async fn save_session(&self, stored: StoredSession) -> Result<()> {
        self.run(move |connection| {
            upsert_session(connection, &stored, true)?;
            Ok(())
        })
        .await
    }

    pub async fn load_sessions(&self) -> Result<Vec<StoredSession>> {
        self.run(|connection| {
            let mut statement = connection.prepare(
                "SELECT session_id, config_json, title, created_at_ms, started_at_ms,
                        ended_at_ms, audio_duration_us
                 FROM sessions ORDER BY created_at_ms, session_id",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            })?;
            let mut sessions = Vec::new();
            for row in rows {
                let (id, config, title, created_at_ms, started_at_ms, ended_at_ms, duration) = row?;
                sessions.push(StoredSession {
                    id: Uuid::parse_str(&id)
                        .map_err(|error| CoreError::Storage(error.to_string()))?,
                    config: parse_stored_config(&config)?,
                    metadata: SessionMetadata {
                        title,
                        created_at_ms,
                        started_at_ms,
                        ended_at_ms,
                        audio_duration_us: duration.max(0) as u64,
                    },
                });
            }
            Ok(sessions)
        })
        .await
    }

    pub async fn delete_session(&self, id: Uuid) -> Result<()> {
        self.run(move |connection| {
            connection.execute(
                "DELETE FROM sessions WHERE session_id = ?1",
                params![id.to_string()],
            )?;
            Ok(())
        })
        .await
    }

    /// Returns false for an idempotent replay or a stale revision.
    pub async fn record_cue(&self, session_id: Uuid, cue: SubtitleCue) -> Result<bool> {
        self.run(move |connection| {
            let transaction = connection.transaction()?;
            let inserted = insert_cue(&transaction, session_id, &cue, false)?;
            transaction.commit()?;
            Ok(inserted)
        })
        .await
    }

    pub async fn history(&self, session_id: Uuid) -> Result<Vec<SubtitleCue>> {
        self.run(move |connection| history_sync(connection, session_id))
            .await
    }

    /// Captures cue revisions and speech rows under one SQLite read snapshot.
    /// Later writes cannot alter the owned vectors returned to an export.
    pub async fn export_snapshot(&self, session_id: Uuid) -> Result<ExportSnapshot> {
        self.run(move |connection| {
            let transaction = connection.transaction()?;
            let snapshot = ExportSnapshot {
                cues: history_sync(&transaction, session_id)?,
                speech_segments: speech_segments_sync(&transaction, session_id)?,
            };
            transaction.commit()?;
            Ok(snapshot)
        })
        .await
    }

    pub async fn clear_history(&self, session_id: Uuid) -> Result<()> {
        self.run(move |connection| {
            connection.execute(
                "DELETE FROM cues WHERE session_id = ?1",
                params![session_id.to_string()],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn playback_status(&self, session_id: Uuid, token: u64) -> Result<Option<String>> {
        self.run(move |connection| {
            connection
                .query_row(
                    "SELECT status FROM speech_segments WHERE session_id = ?1 AND token = ?2",
                    params![session_id.to_string(), token as i64],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(Into::into)
        })
        .await
    }

    pub async fn persist_playback(&self, session_id: Uuid, segment: SpeechSegment) -> Result<()> {
        self.run(move |connection| upsert_speech_segment(connection, session_id, segment))
            .await
    }

    pub async fn highest_speech_token(&self, session_id: Uuid) -> Result<u64> {
        self.run(move |connection| {
            let maximum = connection.query_row(
                "SELECT COALESCE(MAX(token), 0) FROM speech_segments WHERE session_id = ?1",
                params![session_id.to_string()],
                |row| row.get::<_, i64>(0),
            )?;
            Ok(maximum.max(0) as u64)
        })
        .await
    }

    pub async fn persist_speech_recovery(
        &self,
        session_id: Uuid,
        segments: Vec<SpeechSegment>,
    ) -> Result<()> {
        self.run(move |connection| {
            let transaction = connection.transaction()?;
            for segment in segments {
                upsert_speech_segment(&transaction, session_id, segment)?;
            }
            transaction.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn speech_segments(&self, session_id: Uuid) -> Result<Vec<SpeechSegment>> {
        self.run(move |connection| speech_segments_sync(connection, session_id))
            .await
    }

    pub async fn migrate_legacy(&self, runtime_dir: PathBuf) -> Result<()> {
        self.run(move |connection| {
            let transaction = connection.transaction()?;
            migrate_legacy_sync(&transaction, &runtime_dir)?;
            transaction.commit()?;
            Ok(())
        })
        .await
    }
}

fn upsert_speech_segment(
    connection: &Connection,
    session_id: Uuid,
    segment: SpeechSegment,
) -> Result<()> {
    connection.execute(
        "INSERT INTO speech_segments(
             session_id, token, segment_id, path, status, source_end_ns
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(session_id, token) DO UPDATE SET
             segment_id = excluded.segment_id,
             path = excluded.path,
             status = excluded.status,
             source_end_ns = excluded.source_end_ns",
        params![
            session_id.to_string(),
            segment.token as i64,
            segment.segment_id,
            segment.path.to_string_lossy(),
            segment.status,
            segment.source_end_ns,
        ],
    )?;
    Ok(())
}

fn strip_legacy_deployment_fields(value: &mut serde_json::Value) {
    let config = match value.get_mut("config") {
        Some(config) => config,
        None => value,
    };
    for component in ["translator", "output"] {
        if let Some(object) = config
            .get_mut(component)
            .and_then(|item| item.as_object_mut())
        {
            for field in ["executable", "model", "player_path", "cache_dir"] {
                object.remove(field);
            }
        }
    }
}

fn parse_stored_config(contents: &str) -> Result<PipelineConfig> {
    let mut value: serde_json::Value =
        serde_json::from_str(contents).map_err(|error| CoreError::Storage(error.to_string()))?;
    strip_legacy_deployment_fields(&mut value);
    serde_json::from_value(value).map_err(|error| CoreError::Storage(error.to_string()))
}

fn parse_stored_session(contents: &str) -> Result<StoredSession> {
    let mut value: serde_json::Value =
        serde_json::from_str(contents).map_err(|error| CoreError::Storage(error.to_string()))?;
    strip_legacy_deployment_fields(&mut value);
    serde_json::from_value(value).map_err(|error| CoreError::Storage(error.to_string()))
}

fn upsert_session(connection: &Connection, stored: &StoredSession, update: bool) -> Result<()> {
    let config = serde_json::to_string(&stored.config)
        .map_err(|error| CoreError::Storage(error.to_string()))?;
    let conflict = if update {
        "ON CONFLICT(session_id) DO UPDATE SET
             config_json = excluded.config_json,
             title = excluded.title,
             created_at_ms = excluded.created_at_ms,
             started_at_ms = excluded.started_at_ms,
             ended_at_ms = excluded.ended_at_ms,
             audio_duration_us = excluded.audio_duration_us"
    } else {
        "ON CONFLICT(session_id) DO NOTHING"
    };
    connection.execute(
        &format!(
            "INSERT INTO sessions(
                 session_id, config_json, title, created_at_ms, started_at_ms, ended_at_ms,
                 audio_duration_us
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) {conflict}"
        ),
        params![
            stored.id.to_string(),
            config,
            stored.metadata.title,
            stored.metadata.created_at_ms,
            stored.metadata.started_at_ms,
            stored.metadata.ended_at_ms,
            stored.metadata.audio_duration_us as i64,
        ],
    )?;
    Ok(())
}

fn insert_cue(
    connection: &Connection,
    session_id: Uuid,
    cue: &SubtitleCue,
    legacy_ambiguous: bool,
) -> Result<bool> {
    let existing = connection
        .query_row(
            "SELECT ordinal, latest_revision FROM cues WHERE session_id = ?1 AND cue_id = ?2",
            params![session_id.to_string(), cue.cue_id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;
    if existing.is_some_and(|(_, revision)| revision >= cue.revision as i64) {
        return Ok(false);
    }
    let ordinal = match existing {
        Some((ordinal, _)) => ordinal,
        None => connection.query_row(
            "SELECT COALESCE(MAX(ordinal), -1) + 1 FROM cues WHERE session_id = ?1",
            params![session_id.to_string()],
            |row| row.get(0),
        )?,
    };
    connection.execute(
        "INSERT INTO cues(session_id, cue_id, ordinal, latest_revision, legacy_ambiguous)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(session_id, cue_id) DO UPDATE SET
             latest_revision = excluded.latest_revision,
             legacy_ambiguous = excluded.legacy_ambiguous",
        params![
            session_id.to_string(),
            cue.cue_id,
            ordinal,
            cue.revision as i64,
            legacy_ambiguous,
        ],
    )?;
    connection.execute(
        "INSERT INTO cue_revisions(
             session_id, cue_id, revision, source_text, translated_text,
             source_start_ns, source_end_ns, is_final
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            session_id.to_string(),
            cue.cue_id,
            cue.revision as i64,
            cue.source_text,
            cue.translated_text,
            cue.source_start_ns,
            cue.source_end_ns,
            cue.is_final,
        ],
    )?;
    for (position, segment_id) in cue.segment_ids.iter().enumerate() {
        connection.execute(
            "INSERT INTO cue_segments(session_id, cue_id, revision, position, segment_id)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                session_id.to_string(),
                cue.cue_id,
                cue.revision as i64,
                position as i64,
                segment_id,
            ],
        )?;
    }
    Ok(true)
}

fn history_sync(connection: &Connection, session_id: Uuid) -> Result<Vec<SubtitleCue>> {
    let mut statement = connection.prepare(
        "SELECT c.cue_id, r.revision, r.source_text, r.translated_text,
                r.source_start_ns, r.source_end_ns, r.is_final
         FROM cues c
         JOIN cue_revisions r
           ON r.session_id = c.session_id AND r.cue_id = c.cue_id
          AND r.revision = c.latest_revision
         WHERE c.session_id = ?1 ORDER BY c.ordinal",
    )?;
    let rows = statement.query_map(params![session_id.to_string()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, bool>(6)?,
        ))
    })?;
    let mut cues = Vec::new();
    for row in rows {
        let (cue_id, revision, source_text, translated_text, start, end, is_final) = row?;
        let mut segments = connection.prepare(
            "SELECT segment_id FROM cue_segments
             WHERE session_id = ?1 AND cue_id = ?2 AND revision = ?3 ORDER BY position",
        )?;
        let segment_rows = segments
            .query_map(params![session_id.to_string(), cue_id, revision], |row| {
                row.get::<_, String>(0)
            })?;
        let mut segment_ids = Vec::new();
        for segment in segment_rows {
            segment_ids.push(segment?);
        }
        cues.push(SubtitleCue {
            cue_id,
            revision: revision.max(0) as u64,
            source_text,
            translated_text,
            source_start_ns: start,
            source_end_ns: end,
            is_final,
            segment_ids,
        });
    }
    Ok(cues)
}

fn speech_segments_sync(connection: &Connection, session_id: Uuid) -> Result<Vec<SpeechSegment>> {
    let mut statement = connection.prepare(
        "SELECT token, segment_id, path, status, source_end_ns
         FROM speech_segments WHERE session_id = ?1 ORDER BY token",
    )?;
    let rows = statement.query_map(params![session_id.to_string()], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<i64>>(4)?,
        ))
    })?;
    let mut segments = Vec::new();
    for row in rows {
        let (token, segment_id, path, status, source_end_ns) = row?;
        segments.push(SpeechSegment {
            token: token.max(0) as u64,
            segment_id,
            path: PathBuf::from(path),
            status,
            source_end_ns,
        });
    }
    Ok(segments)
}

#[derive(Clone)]
struct LegacyCursor {
    imported_id: String,
    occurrence: u64,
    cue: SubtitleCue,
}

fn migrate_legacy_sync(connection: &Connection, runtime_dir: &Path) -> Result<()> {
    let directory = runtime_dir.join("sessions");
    let mut manifests = match fs::read_dir(directory) {
        Ok(entries) => entries
            .filter_map(std::result::Result::ok)
            .map(|entry| entry.path())
            .collect::<Vec<_>>(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    manifests.sort();
    for manifest in manifests {
        if manifest.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let stored = match fs::read_to_string(&manifest)
            .map_err(CoreError::from)
            .and_then(|contents| parse_stored_session(&contents))
        {
            Ok(stored) => stored,
            Err(error) => {
                tracing::warn!(path = %manifest.display(), "ignored invalid legacy session: {error}");
                continue;
            }
        };
        upsert_session(connection, &stored, false)?;
        migrate_subtitles(connection, runtime_dir, stored.id)?;
        migrate_speech(connection, &stored, runtime_dir)?;
    }
    Ok(())
}

fn migrate_subtitles(connection: &Connection, runtime_dir: &Path, session_id: Uuid) -> Result<()> {
    let path = runtime_dir
        .join("subtitles")
        .join(format!("{session_id}.jsonl"));
    let contents = match fs::read(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let source_path = path.to_string_lossy().into_owned();
    let text = String::from_utf8_lossy(&contents);
    let mut cursors = HashMap::<String, LegacyCursor>::new();
    for (index, line) in text.lines().enumerate() {
        let line_number = index + 1;
        if line.trim().is_empty() {
            continue;
        }
        let item_key = format!("cue:{line_number}");
        let line_hash = hex_digest(line.as_bytes());
        let already_applied =
            migration_already_applied(connection, &source_path, &item_key, &line_hash)?;
        let parsed = serde_json::from_str::<SubtitleCue>(line);
        let mut cue = match parsed {
            Ok(cue) => cue,
            Err(error) => {
                record_migration(
                    connection,
                    &source_path,
                    &item_key,
                    &line_hash,
                    "invalid",
                    Some(format!("line {line_number}: {error}")),
                )?;
                continue;
            }
        };
        let legacy_id = cue.cue_id.clone();
        let is_uuid = Uuid::parse_str(&legacy_id).is_ok();
        let mut ambiguous = false;
        let imported_id = match cursors.get(&legacy_id) {
            None if is_uuid => legacy_id.clone(),
            None => stable_legacy_cue_id(session_id, line_number, 0),
            Some(previous)
                if (cue.revision <= previous.cue.revision && cue != previous.cue)
                    || cue.source_start_ns < previous.cue.source_start_ns =>
            {
                ambiguous = true;
                stable_legacy_cue_id(session_id, line_number, previous.occurrence + 1)
            }
            Some(previous) => previous.imported_id.clone(),
        };
        let occurrence = cursors.get(&legacy_id).map_or(0, |previous| {
            if imported_id == previous.imported_id {
                previous.occurrence
            } else {
                previous.occurrence + 1
            }
        });
        let duplicate = cursors
            .get(&legacy_id)
            .is_some_and(|previous| previous.cue == cue);
        if duplicate {
            if !already_applied {
                record_migration(
                    connection,
                    &source_path,
                    &item_key,
                    &line_hash,
                    "duplicate",
                    None,
                )?;
            }
            continue;
        }
        let cursor_cue = cue.clone();
        if !already_applied {
            cue.cue_id = imported_id.clone();
            insert_cue(connection, session_id, &cue, ambiguous || !is_uuid)?;
            record_migration(
                connection,
                &source_path,
                &item_key,
                &line_hash,
                "applied",
                None,
            )?;
        }
        cursors.insert(
            legacy_id,
            LegacyCursor {
                imported_id,
                occurrence,
                cue: cursor_cue,
            },
        );
    }
    Ok(())
}

fn stable_legacy_cue_id(session_id: Uuid, line_number: usize, occurrence: u64) -> String {
    Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!("voicelive-legacy-cue:{session_id}:{line_number}:{occurrence}").as_bytes(),
    )
    .to_string()
}

fn migrate_speech(
    connection: &Connection,
    stored: &StoredSession,
    runtime_dir: &Path,
) -> Result<()> {
    if !matches!(stored.config.output, OutputConfig::MacosSystem { .. }) {
        return Ok(());
    }
    let directory = runtime_dir.join("cache");
    let mut paths = match fs::read_dir(&directory) {
        Ok(entries) => entries
            .filter_map(std::result::Result::ok)
            .map(|entry| entry.path())
            .collect::<Vec<_>>(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    paths.sort();
    let prefix = format!("speech-{}-", stored.id);
    for path in paths {
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        let Some(token) = file_name
            .strip_prefix(&prefix)
            .and_then(|value| value.strip_suffix(".wav"))
            .and_then(|value| value.parse::<u64>().ok())
        else {
            continue;
        };
        let source_path = path.to_string_lossy().into_owned();
        let source_hash = hash_file(&path)?;
        if migration_already_applied(connection, &source_path, "speech", &source_hash)? {
            continue;
        }
        let validation = hound::WavReader::open(&path).map(|reader| reader.spec());
        let (status, detail) = match validation {
            Ok(_) => ("ready", None),
            Err(error) => ("failed", Some(error.to_string())),
        };
        connection.execute(
            "INSERT INTO speech_segments(session_id, token, segment_id, path, status, source_end_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, NULL)
             ON CONFLICT(session_id, token) DO NOTHING",
            params![
                stored.id.to_string(),
                token as i64,
                format!("legacy-{token}"),
                source_path,
                status,
            ],
        )?;
        record_migration(
            connection,
            &source_path,
            "speech",
            &source_hash,
            status,
            detail,
        )?;
    }
    Ok(())
}

fn migration_already_applied(
    connection: &Connection,
    source_path: &str,
    item_key: &str,
    source_hash: &str,
) -> Result<bool> {
    let existing = connection
        .query_row(
            "SELECT source_hash, status FROM migration_ledger
             WHERE source_path = ?1 AND item_key = ?2",
            params![source_path, item_key],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    match existing {
        None => Ok(false),
        Some((hash, _)) if hash == source_hash => Ok(true),
        Some((_, status)) if status == "invalid" => Ok(false),
        Some(_) => Err(CoreError::Storage(format!(
            "legacy source changed after migration: {source_path} ({item_key})"
        ))),
    }
}

fn record_migration(
    connection: &Connection,
    source_path: &str,
    item_key: &str,
    source_hash: &str,
    status: &str,
    detail: Option<String>,
) -> Result<()> {
    connection.execute(
        "INSERT INTO migration_ledger(source_path, item_key, source_hash, status, detail)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(source_path, item_key) DO UPDATE SET
             source_hash = excluded.source_hash,
             status = excluded.status,
             detail = excluded.detail",
        params![source_path, item_key, source_hash, status, detail],
    )?;
    Ok(())
}

fn hex_digest(contents: &[u8]) -> String {
    format!("{:x}", Sha256::digest(contents))
}

fn hash_file(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cue(id: &str, revision: u64, start: i64, text: &str) -> SubtitleCue {
        SubtitleCue {
            cue_id: id.into(),
            revision,
            source_text: text.into(),
            translated_text: format!("译文：{text}"),
            source_start_ns: start,
            source_end_ns: start + 800_000_000,
            is_final: revision > 1,
            segment_ids: vec![format!("segment-{revision}")],
        }
    }

    fn write_wav(path: &Path) {
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
    async fn legacy_migration_is_loss_tolerant_deterministic_and_reentrant() {
        let root = tempfile::tempdir().unwrap();
        let runtime = root.path();
        let session_id = Uuid::new_v4();
        let cache = runtime.join("cache");
        fs::create_dir_all(runtime.join("sessions")).unwrap();
        fs::create_dir_all(runtime.join("subtitles")).unwrap();
        fs::create_dir_all(&cache).unwrap();
        let stored = StoredSession {
            id: session_id,
            config: PipelineConfig {
                output: OutputConfig::MacosSystem {
                    voice: None,
                    speech_rate: None,
                },
                ..PipelineConfig::default()
            },
            metadata: SessionMetadata {
                title: "legacy".into(),
                created_at_ms: 42,
                started_at_ms: None,
                ended_at_ms: None,
                audio_duration_us: 1_600_000,
            },
        };
        let manifest = runtime.join("sessions").join(format!("{session_id}.json"));
        let history = runtime
            .join("subtitles")
            .join(format!("{session_id}.jsonl"));
        let mut legacy_manifest = serde_json::to_value(&stored).unwrap();
        for (component, fields) in [
            ("translator", ["executable", "model"]),
            ("output", ["player_path", "cache_dir"]),
        ] {
            for field in fields {
                legacy_manifest["config"][component][field] =
                    serde_json::Value::String("/legacy/privileged/path".into());
            }
        }
        let original_manifest = serde_json::to_vec_pretty(&legacy_manifest).unwrap();
        fs::write(&manifest, &original_manifest).unwrap();
        let lines = [
            serde_json::to_string(&cue("cue-0", 1, 0, "first draft")).unwrap(),
            serde_json::to_string(&cue("cue-0", 2, 0, "first final")).unwrap(),
            "{not-json".into(),
            serde_json::to_string(&cue("cue-0", 1, 1_000_000_000, "second final")).unwrap(),
            serde_json::to_string(&cue("cue-0", 1, 1_000_000_000, "second final")).unwrap(),
        ];
        let original_history = format!("{}\n", lines.join("\n"));
        fs::write(&history, &original_history).unwrap();
        write_wav(&cache.join(format!("speech-{session_id}-1.wav")));
        write_wav(&cache.join(format!("speech-{session_id}-3.wav")));
        fs::write(cache.join(format!("speech-{session_id}-4.wav")), b"RIFF").unwrap();

        let store = Store::open(runtime).unwrap();
        store.migrate_legacy(runtime.to_path_buf()).await.unwrap();
        let restored = store.load_sessions().await.unwrap();
        assert_eq!(restored.len(), 1);
        assert!(
            !serde_json::to_string(&restored[0].config)
                .unwrap()
                .contains("/legacy/privileged/path")
        );
        let first = store.history(session_id).await.unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(first[0].source_text, "first final");
        assert_eq!(first[1].source_text, "second final");
        assert_ne!(first[0].cue_id, first[1].cue_id);
        assert!(first.iter().all(|cue| Uuid::parse_str(&cue.cue_id).is_ok()));
        let speech = store.speech_segments(session_id).await.unwrap();
        assert_eq!(
            speech.iter().map(|item| item.token).collect::<Vec<_>>(),
            [1, 3, 4]
        );
        assert_eq!(
            speech
                .iter()
                .map(|item| item.status.as_str())
                .collect::<Vec<_>>(),
            ["ready", "ready", "failed"]
        );

        store.migrate_legacy(runtime.to_path_buf()).await.unwrap();
        assert_eq!(store.history(session_id).await.unwrap(), first);
        assert_eq!(store.speech_segments(session_id).await.unwrap(), speech);
        assert_eq!(fs::read(&manifest).unwrap(), original_manifest);
        assert_eq!(fs::read_to_string(&history).unwrap(), original_history);

        let appended = format!(
            "{original_history}{}\n",
            serde_json::to_string(&cue(
                "cue-0",
                2,
                1_000_000_000,
                "second revised after append"
            ))
            .unwrap()
        );
        fs::write(&history, &appended).unwrap();
        store.migrate_legacy(runtime.to_path_buf()).await.unwrap();
        let after_append = store.history(session_id).await.unwrap();
        assert_eq!(after_append.len(), 2);
        assert_eq!(after_append[1].cue_id, first[1].cue_id);
        assert_eq!(after_append[1].source_text, "second revised after append");
        assert_eq!(after_append[1].revision, 2);

        fs::write(
            &history,
            appended.replacen("first draft", "modified prefix", 1),
        )
        .unwrap();
        assert!(store.migrate_legacy(runtime.to_path_buf()).await.is_err());
        assert_eq!(store.history(session_id).await.unwrap(), after_append);
    }

    #[tokio::test]
    async fn export_snapshot_keeps_original_cue_revisions_and_speech_manifest() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path()).unwrap();
        let id = Uuid::new_v4();
        store
            .save_session(StoredSession {
                id,
                config: PipelineConfig::default(),
                metadata: SessionMetadata {
                    title: "snapshot".into(),
                    created_at_ms: 1,
                    started_at_ms: None,
                    ended_at_ms: None,
                    audio_duration_us: 0,
                },
            })
            .await
            .unwrap();
        store
            .record_cue(id, cue("cue", 1, 0, "snapshot draft"))
            .await
            .unwrap();
        store
            .persist_playback(
                id,
                SpeechSegment {
                    token: 1,
                    segment_id: "segment-1".into(),
                    path: root.path().join(format!("speech-{id}-1.wav")),
                    status: "ready".into(),
                    source_end_ns: Some(800_000_000),
                },
            )
            .await
            .unwrap();

        let snapshot = store.export_snapshot(id).await.unwrap();
        store
            .record_cue(id, cue("cue", 2, 0, "later revision"))
            .await
            .unwrap();
        store
            .persist_playback(
                id,
                SpeechSegment {
                    token: 2,
                    segment_id: "segment-2".into(),
                    path: root.path().join(format!("speech-{id}-2.wav")),
                    status: "pending".into(),
                    source_end_ns: Some(1_600_000_000),
                },
            )
            .await
            .unwrap();

        assert_eq!(snapshot.cues[0].revision, 1);
        assert_eq!(snapshot.cues[0].source_text, "snapshot draft");
        assert_eq!(snapshot.speech_segments.len(), 1);
        assert_eq!(snapshot.speech_segments[0].status, "ready");
        assert_eq!(store.history(id).await.unwrap()[0].revision, 2);
        assert_eq!(store.speech_segments(id).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn stale_cue_revisions_never_overwrite_current_history() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path()).unwrap();
        let id = Uuid::new_v4();
        store
            .save_session(StoredSession {
                id,
                config: PipelineConfig::default(),
                metadata: SessionMetadata {
                    title: "test".into(),
                    created_at_ms: 1,
                    started_at_ms: None,
                    ended_at_ms: None,
                    audio_duration_us: 0,
                },
            })
            .await
            .unwrap();
        assert!(
            store
                .record_cue(id, cue("cue", 1, 0, "draft"))
                .await
                .unwrap()
        );
        assert!(
            store
                .record_cue(id, cue("cue", 2, 0, "final"))
                .await
                .unwrap()
        );
        assert!(
            !store
                .record_cue(id, cue("cue", 1, 0, "stale"))
                .await
                .unwrap()
        );
        let history = store.history(id).await.unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].source_text, "final");
        assert_eq!(history[0].revision, 2);
    }
}
