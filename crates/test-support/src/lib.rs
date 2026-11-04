use std::time::{SystemTime, UNIX_EPOCH};

pub fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn silent_pcm(duration_us: u32) -> Vec<u8> {
    vec![0; duration_us as usize * 32 / 1_000]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pcm_size_matches_duration() {
        assert_eq!(silent_pcm(100_000).len(), 3_200);
        assert_eq!(silent_pcm(800_000).len(), 25_600);
    }
}
