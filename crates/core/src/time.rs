use std::sync::OnceLock;
use std::time::Instant;

static EPOCH: OnceLock<Instant> = OnceLock::new();

pub fn monotonic_ns() -> i64 {
    EPOCH.get_or_init(Instant::now).elapsed().as_nanos() as i64
}
