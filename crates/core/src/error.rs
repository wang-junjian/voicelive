#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("invalid state: {0}")]
    InvalidState(String),
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("unsafe audio routing: {0}")]
    RoutingUnsafe(String),
    #[error("source error: {0}")]
    Source(String),
    #[error("translator error: {0}")]
    Translator(String),
    #[error("output error: {0}")]
    Output(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("session not found: {0}")]
    SessionNotFound(uuid::Uuid),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("policy error: {0}")]
    Policy(#[from] simulmt_policy::PolicyError),
}

pub type Result<T> = std::result::Result<T, CoreError>;

impl From<rusqlite::Error> for CoreError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Storage(error.to_string())
    }
}
