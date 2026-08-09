use crate::net::fetch::imap::{ImapSessionCommandType::LISTFETCH, *};
use crate::models::*;
use std::sync::Arc;
use imap_proto::parser::core::nstring_utf8;
use tokio::sync::Mutex as TMutex;
use futures::stream::{OrElse, Stream, StreamExt, Then};

pub mod fetch;
pub mod push;
pub mod structure;
pub mod poll;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionUpdate {
    STARTED(ImapSessionId),
    CMDSUCCESS(ImapSessionId, ActionId), // action id
    CMDSUCCESSRETRY(ImapSessionId, ActionId), // action id
    CMDFAILURETRYAGAIN(ImapSessionId, ActionId, bool), // action Id, do we still own the connection? 
    CMDFAILUREUNRECOVERABLE(ImapSessionId, ActionId, bool), // action Id, do we still own the connection? 
    SESSIONABORT(ImapSessionId), // Session lost network connection and could not continue 
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetAction {
    /* External Use */
    // ECHO { cred_id: CredentialID, }, // -> Ok
    SEND { cred_id: CredentialID, }, // -> Ok
    LISTFETCH { cred_id: CredentialID, }, // -> Ok
    // PREFETCH(CredentialID),
    // FETCH(CredentialID),
    STATUS { cred_id: CredentialID, }, // -> Ok
    SUGGEST { cred_id: CredentialID, mb: MailboxName }, // -> Ok
    POLL, // -> None
    /* External Use */
    /* From ImapSession */
    IMAPUPDATE { cred_id: CredentialID, update: SessionUpdate }, // -> Ok or The resolution type of the retried action
    /* From ImapSession */
    SHUTDOWN, // -> None
}

#[derive(Debug)]
pub struct NetMessage {
    pub action: NetAction,
    pub resolve: ResolveID, // Incase you want to hook onto whether the action succeeded or failed (and the result)
}

pub struct NetActor {
    inbox: tokio::sync::mpsc::Receiver<NetMessage>,
    managers: std::collections::HashMap<CredentialID, Arc<TMutex<Option<ImapManager>>>>,  
}


impl NetActor {
    pub async fn new(inbox: tokio::sync::mpsc::Receiver<NetMessage>) -> Self {
        poll::PollTask::new(); // Start the poll task
        Self { inbox, managers: std::collections::HashMap::new() }
    }
    
    pub async fn run(&mut self) {
        use NetAction::*;
        println!("Starting net actor");
        while let Some(msg) = self.inbox.recv().await {
            if !matches!(
                &msg.action, 
                IMAPUPDATE{ cred_id: _, update: SessionUpdate::CMDSUCCESS(_, _) } |
                IMAPUPDATE{ cred_id: _, update: SessionUpdate::CMDSUCCESSRETRY(_, _) }
            ) {
                println!("Doing action: {:?}", msg.action);
            }
            let res_id = msg.resolve;
            
            match msg.action {
                // ECHO { cred_id } => { tokio::spawn(push::smtp::send_echo_email(cred_id)); }
                SEND { cred_id } => { tokio::spawn(push::smtp::send_test_email(cred_id)); }
                LISTFETCH { cred_id } => { tokio::spawn(Self::run_list_fetch(self.get_manager_arc(cred_id).await, cred_id, res_id)); }
                STATUS { cred_id } => { tokio::spawn(Self::run_status(self.get_manager_arc(cred_id).await, cred_id, res_id)); }
                IMAPUPDATE { cred_id, update } => { tokio::spawn(Self::run_imap_update(self.get_manager_arc(cred_id).await, cred_id, update, res_id)); }
                SUGGEST { cred_id, mb } => { tokio::spawn(Self::run_suggest(self.get_manager_arc(cred_id).await, cred_id, mb, res_id)); }
                POLL => { self.run_poll(res_id); }
                SHUTDOWN => { break; }
                // _ => { println!("Unknown action: {:?}", msg.action) }
            }
        }
        println!("Ending net actor");
    }

    pub async fn get_manager_arc(&mut self, c: CredentialID) -> Arc<TMutex<Option<ImapManager>>> {
        // This function serves as returning the "Box" in which the manager will reside. 
        // It allows us to bypass the lengthy ImapManager::new() creation process (And any future lengthy imapmanager methods) while allowing other commands to reach other managers.
        if !self.managers.contains_key(&c) { self.managers.insert(c, Arc::new(TMutex::new(None))); }
        self.managers.get(&c).unwrap().clone()
    }

    async fn get_manager_mutex<'a>(manager_ref: &'a Arc<TMutex<Option<ImapManager>>>, c: CredentialID) -> tokio::sync::MutexGuard<'a, Option<ImapManager>> {
        // This will yield the current thread if the manager is still being used by a previous action.
        // Also returning the MutexGuard allows us to call methods on the manager without the lock locking.
        let mut manager_mutex = manager_ref.lock().await;
        if manager_mutex.is_none() { *manager_mutex = Some(ImapManager::new(c).await.unwrap()); } 
        manager_mutex
    }

    pub async fn run_send(cred_id: CredentialID, resolve: ResolveID) {
        push::smtp::send_test_email(cred_id).await;
        ResolveStore::resolve(resolve, Resolution::Nothing)
    }

    pub async fn run_list_fetch(manager_ref: Arc<TMutex<Option<ImapManager>>>, c: CredentialID, resolve: ResolveID) {
        use fetch::imap::FetchType::*;
        Self::get_manager_mutex(&manager_ref, c).await
            .as_mut().unwrap().call_session(LISTFETCH("INBOX".into(), SeqRange::all()), resolve).await;
    }

    pub async fn run_imap_update(manager_arc: Arc<TMutex<Option<ImapManager>>>, c: CredentialID, session_update: SessionUpdate, res_id: ResolveID) {
        use SessionUpdate::*;
        let isid = match &session_update {
            STARTED(isid) => isid,
            CMDSUCCESS(isid, _) => isid,
            CMDSUCCESSRETRY(isid, _) => isid,
            CMDFAILURETRYAGAIN(isid, _, _) => isid,
            CMDFAILUREUNRECOVERABLE(isid, _, _) => isid,
            SESSIONABORT(isid) => isid,
        };
        Self::get_manager_mutex(&manager_arc, c).await
            .as_mut().unwrap().rcv_session_update(session_update, res_id).await;
    }

    pub async fn run_status(manager_arc: Arc<TMutex<Option<ImapManager>>>, c: CredentialID, res_id: ResolveID) {
        let manager = Self::get_manager_mutex(&manager_arc, c).await;
        let status = manager.as_ref().unwrap().status();
        for (id, status) in &status {
            println!("{:?} -> {:?}", id.s_id, status);
        }
        ResolveStore::resolve(res_id, Resolution::Status(status));
    }

    pub async fn run_suggest(manager_arc: Arc<TMutex<Option<ImapManager>>>, c: CredentialID, mb: MailboxName, res_id: ResolveID) {
        Self::get_manager_mutex(&manager_arc, c).await
            .as_mut().unwrap().handle_suggest(mb, res_id).await;
    }

    pub fn run_poll(&mut self, res_id: ResolveID) {
        let managers = self.managers.clone();
        for (cred_id, manager_arc) in managers.iter() {
            let manager_arc = manager_arc.clone();
            let cred_id = *cred_id;
            tokio::spawn(async move {
                let mut manager = Self::get_manager_mutex(&manager_arc, cred_id).await;
                manager.as_mut().unwrap().handle_poll().await;
            });
        }
        ResolveStore::resolve(res_id, Resolution::Nothing);
    }
}
