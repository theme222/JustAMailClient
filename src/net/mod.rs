use crate::net::fetch::imap::{ImapSessionCommandType::LISTFETCH, *};
use crate::models::*;
use std::sync::Arc;
use tokio::sync::Mutex as TMutex;
use futures::stream::{Stream, StreamExt};

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
    ECHO,
    SEND,
    LISTFETCH,
    // PREFETCH(CredentialID),
    // FETCH(CredentialID),
    STATUS,
    SUGGEST(MailboxName), // Give a hint to what the next action's domain will be. (This will *sometimes* call SELECT. Based on if there is an available session.)
    POLL, // A general call to poll for new mail.
    /* External Use */
    /* From ImapSession */
    IMAPUPDATE(SessionUpdate),
    /* From ImapSession */
    SHUTDOWN,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetMessage {
    pub cred_id: CredentialID, // on actions that don't use it the value will always be u64::MAX
    pub action: NetAction,
}

pub struct NetActor {
    inbox: tokio::sync::mpsc::Receiver<NetMessage>,
    managers: std::collections::HashMap<CredentialID, Arc<TMutex<Option<ImapManager>>>>,  
}

// pub async fn retry_network<T, Func, Fut>(action: Func) -> Result<T>
// where 
//     Func: Fn() -> Fut,
//     Fut: std::future::Future<Output = Result<T>> + Send + 'static
// {
//     for _ in 0..RETRIES {
//         let result = action().await; 
//         match result {
//             Ok(out) => { return Ok(out) }
//             Err(e) => { println!("Failed! {:?} Retrying...", e); }
//         }
//         tokio::time::sleep(std::time::Duration::from_millis(500)).await;
//     }
//     panic!("Failed to execute network action after {} retries", RETRIES)
// }

impl NetActor {
    pub async fn new(inbox: tokio::sync::mpsc::Receiver<NetMessage>) -> Self {
        poll::PollTask::new(); // Start the poll task
        Self { inbox, managers: std::collections::HashMap::new() }
    }
    
    pub async fn run(&mut self) {
        use NetAction::*;
        println!("Starting net actor");
        while let Some(msg) = self.inbox.recv().await {
            println!("Doing action: {:?}", msg.action);
            let c = msg.cred_id; 
            
            match msg.action {
                ECHO => { tokio::spawn(push::smtp::send_echo_email(c)); }
                SEND => { tokio::spawn(push::smtp::send_test_email(c)); }
                LISTFETCH => { tokio::spawn(Self::run_list_fetch(self.get_manager_arc(c).await, c)); }
                STATUS => { Self::run_status(self.get_manager_arc(c).await, c).await; }
                IMAPUPDATE(session_update) => { Self::run_imap_update(self.get_manager_arc(c).await, c, session_update).await; }
                SUGGEST(mb) => { Self::run_suggest(self.get_manager_arc(c).await, c, mb).await; }
                POLL => {/* TODO Fill this bad boy in */},
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
        // Also returning the MutexGuard allows us to call methods on the manager without the lock unlocking.
        let mut manager_mutex = manager_ref.lock().await;
        if manager_mutex.is_none() { *manager_mutex = Some(ImapManager::new(c).await.unwrap()); } 
        manager_mutex
    }

    pub async fn run_list_fetch(manager_ref: Arc<TMutex<Option<ImapManager>>>, c: CredentialID) {
        use fetch::imap::FetchType::*;
        Self::get_manager_mutex(&manager_ref, c).await
            .as_mut().unwrap().call_session(LISTFETCH("INBOX".into(), SeqRange::all())).await;
    }

    pub async fn run_imap_update(manager_arc: Arc<TMutex<Option<ImapManager>>>, c: CredentialID, session_update: SessionUpdate) {
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
            .as_mut().unwrap().rcv_session_update(session_update).await;
    }

    pub async fn run_status(manager_arc: Arc<TMutex<Option<ImapManager>>>, c: CredentialID) {
        let manager = Self::get_manager_mutex(&manager_arc, c).await;
        let status = manager.as_ref().unwrap().status();
        for (id, status) in status {
            println!("{:?} -> {:?}", id.s_id, status);
        }
    }

    pub async fn run_suggest(manager_arc: Arc<TMutex<Option<ImapManager>>>, c: CredentialID, mb: MailboxName) {
        Self::get_manager_mutex(&manager_arc, c).await
            .as_mut().unwrap().handle_suggest(mb).await;
    }
}
