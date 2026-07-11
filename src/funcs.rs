use crate::models::*;

pub fn project_dir() -> directories::ProjectDirs {
    directories::ProjectDirs::from("com", "tongsima", "jamc").expect("Failed to find project directories")
}

pub fn unix_timestamp() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_millis() as i64
}

pub fn double_time_clamped(duration: std::time::Duration) -> std::time::Duration {
    std::cmp::min(duration * 2, MAX_RETRY_DELAY)
}