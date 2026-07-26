// Mutex-protected stores for the application state (slower to access than globals cause mutex)
use std::sync::Mutex;

use crate::srv;

static ID_STORE: Mutex<IDStore> = Mutex::new(IDStore { next_cmd_id: 0, next_s_id: 0 });

// Only used while the program is running and is completely unrelated to the sql db
pub struct IDStore { 
    next_cmd_id: u64,
    next_s_id: u64,
}

impl IDStore {
    pub fn cmd_id() -> u64 {
        let mut store = ID_STORE.lock().unwrap();
        store.next_cmd_id += 1;
        store.next_cmd_id
    }

    pub fn s_id() -> u64 {
        let mut store = ID_STORE.lock().unwrap();
        store.next_s_id += 1;
        store.next_s_id
    }
}

static CREDENTIAL_STORE: Mutex<std::sync::LazyLock<CredentialStore>> = Mutex::new(std::sync::LazyLock::new(|| CredentialStore::new()));

pub struct CredentialStore {
    hs : Vec<super::Credentials>,
}

impl CredentialStore {
    fn new() -> Self {
        Self { hs: Vec::new(), }
    }
    pub fn insert(creds: super::Credentials) -> u64 {
        let mut store = CREDENTIAL_STORE.lock().unwrap();
        store.hs.push(creds);
        store.hs.len() as u64 - 1
    }
    pub fn get(id: u64) -> super::Credentials {
        if id == u64::MAX { panic!("CredentialStore::get called on u64::MAX"); }
        CREDENTIAL_STORE.lock().unwrap().hs.get(id as usize).cloned().unwrap()
    }
}

static APP_STATE_STORE: std::sync::LazyLock<AppStateStore> = std::sync::LazyLock::new(|| AppStateStore::init());

struct AppStateStore {
    recv: tokio::sync::watch::Receiver<AppState>,
    sender: tokio::sync::watch::Sender<AppState>,
}

impl AppStateStore {
    fn init() -> AppStateStore {
        let (send, recv) = tokio::sync::watch::channel(AppState::ACTIVE);
        AppStateStore {
            recv: recv,
            sender: send,
        }
    }
}

#[derive(Debug, Clone, Copy, Ord, PartialOrd, PartialEq, Eq)]
pub enum AppState {
    AFK, // Idle for > 30 minutes
    INACTIVE, // Idle for 10 - 30 minutes
    OUTOFFOCUS, // Idle for 5 - 10 minutes and app isn't focused
    FOCUSED, // Idle for 5 - 10 minutes and app is focused
    ACTIVE,  // Idle for < 5 minutes (app focused or unfocused doesn't matter)
}

impl AppState { // kinda braindead but good enough
    pub fn set(state: AppState) {
        APP_STATE_STORE.sender.send(state);
    }
    
    pub fn get() -> AppState {
        APP_STATE_STORE.recv.borrow().clone()
    }
    
    pub async fn await_change() -> Self {
        let current = Self::get();
        APP_STATE_STORE.recv.clone().wait_for(|state| state != &current).await.unwrap().clone()
    }
}


