// Mutex-protected stores for the application state (slower to access than globals cause mutex)
use std::{panic, sync::Mutex};

use crate::{models::{AppState::{ACTIVE, FOCUSED, INACTIVE}, Senders}, srv::{self}};
use crate::db::types::{SQLKey, SQLObj};
use anyhow::Result;
use futures::lock;

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
    hm : std::collections::HashMap<super::CredentialID, super::Credentials>,
    next_cred_id: super::CredentialID,
}

impl CredentialStore {
    fn new() -> Self {
        Self { hm: std::collections::HashMap::new(), next_cred_id: 0 }
    }
    pub async fn insert(creds: super::Credentials) -> u64 {
        let mut store = CREDENTIAL_STORE.lock().unwrap();
        let id = store.next_cred_id;
        store.next_cred_id += 1;
        drop(store);
        use srv::*;
        let res_id = ResolveStore::make();
        let (local_part, domain) = creds.login.split_once('@').unwrap_or((&creds.login, ""));
        
        Senders::srv(
            SrvMessage { 
                action: SrvAction::SYNCACCOUNT { acc: db::AccountSQL {
                    id: Some(db::KeyWrapper(db::AccountKey::LOCALPARTDOMAIN(local_part.into(), domain.into()))),
                    create_time: None,
                    update_time: None,
                    local_part: Some(local_part.into()),
                    domain: Some(domain.into()),
                    fetch_server: Some(creds.fetch_server.clone()),
                    push_server: Some(creds.push_server.clone()),
                }},
                resolve: res_id,
            }
        ).await;
        
        let resolution = ResolveStore::receive(res_id).await.unwrap();
        let account_sql = match resolution {
            Resolution::AccountSQL(account_sql) => account_sql,
            Resolution::Nothing => panic!("No resolution found for ResolveID: {}", res_id),
            _ => panic!("Unexpected resolution: {:?}", resolution),
        };
        
        let mut store = CREDENTIAL_STORE.lock().unwrap();
        store.hm.insert(id, creds);
        id
    }
    pub fn get(id: u64) -> super::Credentials {
        if id == u64::MAX { panic!("CredentialStore::get called on u64::MAX"); }
        CREDENTIAL_STORE.lock().unwrap().hm.get(&id).cloned().unwrap()
    }
    // pub fn get_account_id(id: u64) -> srv::db::SqliteID {
    //     if id == u64::MAX { panic!("CredentialStore::get called on u64::MAX"); }
    //     CREDENTIAL_STORE.lock().unwrap().hm.get(&id).cloned().unwrap().1
    // }
    // pub fn invalidate_account_id(id: srv::db::SqliteID) {
    //     // I honestly don't really like the way I structured this but I can't think of a better way right now.
    //     for (cred_id, (_, acc_id)) in CREDENTIAL_STORE.lock().unwrap().hm.iter() {
    //         if *acc_id == id {
    //             CREDENTIAL_STORE.lock().unwrap().hm.remove(&cred_id);
    //             break;
    //         }
    //     }
    // }
}

static APP_STATE_STORE: std::sync::LazyLock<AppStateStore> = std::sync::LazyLock::new(|| AppStateStore::init());

pub struct AppStateStore {
    recv: tokio::sync::watch::Receiver<AppState>,
    sender: tokio::sync::watch::Sender<AppState>,
    last_change_time: Mutex<std::time::Instant>,
}

impl AppStateStore {
    pub fn init() -> AppStateStore {
        let (send, recv) = tokio::sync::watch::channel(AppState::ACTIVE);
        AppStateStore {
            recv: recv,
            sender: send,
            last_change_time: Mutex::new(std::time::Instant::now()),
        }
    }

    pub fn set(state: AppState) {
        APP_STATE_STORE.sender.send(state);
    }
    
    pub fn get() -> AppState {
        APP_STATE_STORE.recv.borrow().clone()
    }

    pub fn update() {
        let ch_time = APP_STATE_STORE.last_change_time.lock().unwrap();
        let duration = std::time::Instant::now().duration_since( *ch_time );
        if duration < std::time::Duration::from_secs(60) {
            APP_STATE_STORE.sender.send(ACTIVE);
        }
        else if duration < std::time::Duration::from_secs(300) {
            APP_STATE_STORE.sender.send(FOCUSED);
        }
        else if duration < std::time::Duration::from_secs(1800) {
            APP_STATE_STORE.sender.send(INACTIVE);
        }
        else {
            APP_STATE_STORE.sender.send(AppState::AFK); // Not sure why the compiler wants me to qualify this one 
        }
    }

    pub fn ping() {
        *APP_STATE_STORE.last_change_time.lock().unwrap() = std::time::Instant::now();
        Self::set(ACTIVE);
        // Self::update(); // Possible race condition?
    }
    
    pub async fn await_change() -> AppState {
        let current = Self::get();
        let mut recv = APP_STATE_STORE.recv.clone();
        recv.wait_for(|state| state != &current).await.unwrap().clone()
    }
}

#[derive(Debug, Clone, Copy, Ord, PartialOrd, PartialEq, Eq)]
pub enum AppState {
    AFK, // Idle for > 30 minutes
    INACTIVE, // Idle for 10 - 30 minutes
    FOCUSED, // Idle for 1 - 10 minutes and app is focused
    ACTIVE,  // Idle for < 1 minutes (app focused or unfocused doesn't matter)
}

pub type Resolve = anyhow::Result<Resolution>;

#[derive(Debug)]
pub enum Resolution {
    Nothing, // Mbappe Special
    MailboxSQL(crate::db::MailboxSQL),
    MessageAndPartSQL(Vec<crate::db::MessageSQL>, Vec<crate::db::MessagePartSQL>),
    MessageSQL(crate::db::MessageSQL),
    MessagePartSQL(crate::db::MessagePartSQL),
    AccountSQL(crate::db::AccountSQL),
    Search(std::collections::HashSet<u32>),
    Status(Vec<(crate::net::fetch::imap::ImapSessionId, crate::Status)>),
}

pub type Resolver = Option<tokio::sync::oneshot::Sender<anyhow::Result<Resolution>>>;
pub type Resolvee = Option<tokio::sync::oneshot::Receiver<anyhow::Result<Resolution>>>;
pub type ResolveID = u64;

pub struct ResolveStore {
    resolve_count: ResolveID,
    hashmap: std::collections::HashMap<ResolveID, (Resolvee, Resolver)>,
}

pub const NULL_RESOLVE_ID: ResolveID = u64::MAX;

impl ResolveStore {
    pub fn new() -> Self {
        Self {
            resolve_count: 0,
            hashmap: std::collections::HashMap::new(),
        }
    }

    pub fn make() -> ResolveID { // Mutex lock 
        let mut store = RESOLVE_STORE.lock().unwrap();
        let id = store.resolve_count;
        let (rx, tx) = tokio::sync::oneshot::channel::<Resolve>();
        store.hashmap.insert(id, (Some(tx), Some(rx)));
        store.resolve_count += 1;
        id
    }

    pub fn delete_if_resolved(id: ResolveID) { // Mutex lock
        let mut store = RESOLVE_STORE.lock().unwrap();
        if let Some((None, None)) = store.hashmap.get(&id) { store.hashmap.remove(&id); }
    }

    pub fn resolve(id: ResolveID, resolution: Resolution) {
        if id == NULL_RESOLVE_ID { return }
        { // Mutex lock
            let mut lock = RESOLVE_STORE.lock().unwrap();
            let mut resolve = lock.hashmap.get_mut(&id);
            if resolve.is_none() { panic!("ResolveID: {} not found", id) }
            resolve.unwrap().1.take().unwrap().send(Resolve::Ok(resolution)).unwrap();
        }
        Self::delete_if_resolved(id);
    }

    pub fn fail(id: ResolveID, error: anyhow::Error) {
        if id == NULL_RESOLVE_ID { return }
        { // Mutex lock
            let mut lock = RESOLVE_STORE.lock().unwrap();
            let mut resolve = lock.hashmap.get_mut(&id);
            if resolve.is_none() { panic!("ResolveID: {} not found", id) }
            
            resolve.unwrap().1.take().unwrap().send(Resolve::Err(error)).unwrap();
        }
        Self::delete_if_resolved(id);
    }

    pub async fn receive(id: ResolveID) -> Resolve {
        let mut rx: Resolvee;
        { // Mutex lock
            let mut lock = RESOLVE_STORE.lock().unwrap();
            let mut resolve = lock.hashmap.get_mut(&id);
            if resolve.is_none() { panic!("ResolveID: {} not found", id) }
            
            rx = resolve.unwrap().0.take();
        }
        Self::delete_if_resolved(id);
        rx.unwrap().await.expect("Ima be honest to ya I have no idea how this would even error")
    }
}

static RESOLVE_STORE: Mutex<std::sync::LazyLock<ResolveStore>> = Mutex::new(std::sync::LazyLock::new(|| ResolveStore::new()));