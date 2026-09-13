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

pub fn get_download_time(byte_count: u64, download_speed_mbps: f64) -> f64 {
    byte_count as f64 / (download_speed_mbps / 8.0 * 1e6)
}

pub fn spawn_handle_err<F: std::future::Future<Output = Result<Resolution>> + Send + 'static>(future: F, context: &'static str, res_id: ResolveID) {
    tokio::spawn(await_handle_err(future, context, res_id));
}

pub fn spawn_or_err<F: std::future::Future<Output = Result<()>> + Send + 'static>(future: F, context: &'static str, res_id: ResolveID) {
    // The key difference here is that the future also takes in the resolve id so we must only resolve on fail
    tokio::spawn(async move {
        if let Err(e) = future.await {
            eprintln!("[ERR] {}: {}", context, e);
            ResolveStore::fail(res_id, e);
        }
    });
}

pub async fn await_handle_err(future: impl std::future::Future<Output = Result<Resolution>>, context: &str, res_id: ResolveID) {
    let res = future.await;
    if let Err(e) = res { 
        eprintln!("[ERR] {}: {}", context, e); 
        ResolveStore::fail(res_id, e);
    }
    else if let Ok(res) = res {
        ResolveStore::resolve(res_id, res);
    }
}

pub fn with_jitter(duration: std::time::Duration) -> std::time::Duration {
    let jitter = rand::random::<f64>() * JITTER_VARIANCE + (1.0 - (JITTER_VARIANCE/ 2.0));
    duration.mul_f64(jitter)
}

pub async fn wait_with_jitter(duration: std::time::Duration) { // 0.8x - 1.2x
    tokio::time::sleep(with_jitter(duration)).await;
}
