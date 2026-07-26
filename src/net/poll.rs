// A polling task loop that repeatedly calls POLL on NetActor based on the current application state.
use crate::{models::*, net::NetMessage};
use std::time::{Duration, Instant};

pub struct PollTask {
    last_poll_time: Instant,
    current_state: AppState,
    // recv: tokio::sync::mpsc::Receiver<()>, 
}

impl PollTask {

    pub fn new() {
        let pt = Self {
            last_poll_time: Instant::now(),
            current_state: AppState::get(),
        };
        tokio::spawn(pt.run());
    }

    pub async fn run(mut self) {
        let mut duration_todo: Option<Duration> = None;
        loop {
            let change = tokio::select!(
                res = AppState::await_change() => Some(res),
                _ = wait_with_jitter(duration_todo.unwrap_or(self.duration_based_on_state())) => None,
            );
            if change.is_none() {
                self.last_poll_time = Instant::now();
                Senders::net(NetMessage { cred_id: u64::MAX, action: super::NetAction::POLL }).await;
            }
            else if let Some(new) = change {
                let old_duration = self.duration_based_on_state();
                self.current_state = new;
                let new_duration = self.duration_based_on_state();
                let instant_total = self.last_poll_time + old_duration;
                let duration_left = instant_total - Instant::now();
                if duration_left < new_duration { duration_todo = Some(duration_left); }
                else { duration_todo = None; }
            }
        }
    }

    pub fn duration_based_on_state(&self) -> Duration {
        match self.current_state {
            AppState::AFK => Duration::from_mins(2),
            AppState::INACTIVE => Duration::from_mins(1),
            AppState::OUTOFFOCUS => Duration::from_secs(20),
            AppState::FOCUSED => Duration::from_secs(10),
            AppState::ACTIVE => Duration::from_secs(5),
        }
    }
}


