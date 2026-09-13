use crate::models::*;
use std::sync::Arc;
use imap_proto::parser::core::nstring_utf8;
use tokio::sync::Mutex as TMutex;
use futures::stream::{OrElse, Stream, StreamExt, Then};

pub mod fetch;
pub mod push;
pub mod structure;
pub mod poll;

use fetch::imap::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionUpdate {
    STARTED(ImapSessionId),
    CMDSUCCESS(ImapSessionId, ActionId), // action id
    CMDSUCCESSRETRY(ImapSessionId, ActionId), // action id
    CMDFAILURETRYAGAIN(ImapSessionId, ActionId, bool), // action Id, do we still own the connection? 
    CMDFAILUREUNRECOVERABLE(ImapSessionId, ActionId, bool), // action Id, do we still own the connection? 
    SESSIONABORT(ImapSessionId), // Session lost network connection and could not continue 
}

#[derive(Debug, Clone)]
pub enum NetAction {
    /* External Use */
    // ECHO { cred_id: CredentialID, }, // -> Ok
    SEND { cred_id: CredentialID, }, // -> Ok
    SELECT { cred_id: CredentialID, mb: MailboxName }, // -> Ok
    LISTFETCH { cred_id: CredentialID, seq_range: SeqRange, }, // -> Ok
    // PREFETCH(CredentialID),
    // FETCH(CredentialID),
    STATUS { cred_id: CredentialID, }, // -> Ok
    SUGGEST { cred_id: CredentialID, mb: MailboxName }, // -> Ok
    STOP { cred_id: CredentialID }, // -> None
    START { cred_id: CredentialID }, // -> None
    POLL, // -> None
    /* External Use */
    /* From ImapSession */
    IMAPUPDATE { cred_id: CredentialID, update: SessionUpdate }, // -> Ok or The resolution type of the retried action
    /* From ImapSession */
    SHUTDOWN, // -> None
}

impl NetAction {
    fn cred_id(&self) -> Option<CredentialID> {
        use NetAction::*;
        match self {
            SEND { cred_id, .. } => Some(*cred_id),
            LISTFETCH { cred_id, .. } => Some(*cred_id),
            STATUS { cred_id, .. } => Some(*cred_id),
            SUGGEST { cred_id, .. } => Some(*cred_id),
            STOP { cred_id, .. } => Some(*cred_id),
            START { cred_id } => Some(*cred_id),
            IMAPUPDATE { cred_id, .. } => Some(*cred_id),
            SELECT { cred_id, mb } => Some(*cred_id),
            POLL => None,
            SHUTDOWN => None,
        }
    }
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
            let cred_id = msg.action.cred_id();
            let manager_arc = if cred_id.is_some() { Some(self.get_manager_arc(cred_id.unwrap()).await) } else { None };
            
            match msg.action {
                // ECHO { cred_id } => { tokio::spawn(push::smtp::send_echo_email(cred_id)); }
                SEND { cred_id } => { tokio::spawn(push::smtp::send_test_email(cred_id)); }
                LISTFETCH { cred_id, seq_range } => { spawn_or_err(Self::run_list_fetch(manager_arc.unwrap(), cred_id, seq_range, res_id), "LISTFETCH", res_id); }
                STATUS { cred_id } => { spawn_or_err(Self::run_status(manager_arc.unwrap(), cred_id, res_id), "STATUS", res_id); }
                IMAPUPDATE { cred_id, update } => { spawn_or_err(Self::run_imap_update(manager_arc.unwrap(), cred_id, update, res_id), "IMAPUPDATE", res_id); }
                SUGGEST { cred_id, mb } => { spawn_or_err(Self::run_suggest(manager_arc.unwrap(), cred_id, mb, res_id), "SUGGEST", res_id); }
                SELECT { cred_id, mb } => { spawn_or_err(Self::run_select(manager_arc.unwrap(), cred_id, mb, res_id), "SELECT", res_id); }
                STOP { cred_id } => { spawn_or_err(Self::run_stop(manager_arc.unwrap(), cred_id, res_id), "STOP", res_id); }
                START { cred_id } => { spawn_or_err(Self::run_start(manager_arc.unwrap(), cred_id, res_id), "START", res_id); }
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

    async fn get_manager_mutex<'a>(manager_ref: &'a Arc<TMutex<Option<ImapManager>>>, c: CredentialID) -> Result<tokio::sync::MutexGuard<'a, Option<ImapManager>>> {
        // This will yield the current thread if the manager is still being used by a previous action.
        // Also returning the MutexGuard allows us to call methods on the manager without the lock locking.
        let mut manager_mutex = manager_ref.lock().await;
        // if manager_mutex.is_none() { *manager_mutex = Some(ImapManager::new(c).await.unwrap()); } 
        if manager_mutex.is_none() { return Err(anyhow::anyhow!("Manager not found")); }
        Ok(manager_mutex)
    }

    pub async fn run_send(cred_id: CredentialID, resolve: ResolveID) {
        push::smtp::send_test_email(cred_id).await;
        ResolveStore::resolve(resolve, Resolution::Nothing)
    }

    pub async fn run_list_fetch(manager_arc: Arc<TMutex<Option<ImapManager>>>, c: CredentialID, seq_range: SeqRange, resolve: ResolveID) -> Result<()> {
        use fetch::imap::types::FetchType::*;
        Self::get_manager_mutex(&manager_arc, c).await?
            .as_mut().unwrap().call_session(LISTFETCH("INBOX".into(), seq_range), resolve).await;
        Ok(())
    }

    pub async fn run_select(manager_arc: Arc<TMutex<Option<ImapManager>>>, c: CredentialID, mb: MailboxName, resolve: ResolveID) -> Result<()> {
        Self::get_manager_mutex(&manager_arc, c).await?
            .as_mut().unwrap().call_session(SELECT(mb), resolve).await;
        Ok(())
    }

    pub async fn run_start(manager_arc: Arc<TMutex<Option<ImapManager>>>, c: CredentialID, resolve: ResolveID) -> Result<()> {
        let mut manager_mutex = manager_arc.lock().await;
        let new_manager = ImapManager::new(c).await?;
        *manager_mutex = Some(new_manager);
        Ok(())
    }

    pub async fn run_imap_update(manager_arc: Arc<TMutex<Option<ImapManager>>>, c: CredentialID, session_update: SessionUpdate, res_id: ResolveID) -> Result<()> {
        use SessionUpdate::*;
        let isid = match &session_update {
            STARTED(isid) => isid,
            CMDSUCCESS(isid, _) => isid,
            CMDSUCCESSRETRY(isid, _) => isid,
            CMDFAILURETRYAGAIN(isid, _, _) => isid,
            CMDFAILUREUNRECOVERABLE(isid, _, _) => isid,
            SESSIONABORT(isid) => isid,
        };
        let mut manager = Self::get_manager_mutex(&manager_arc, c).await?;
        manager.as_mut().unwrap().rcv_session_update(session_update, res_id).await;
        Ok(())
    }

    pub async fn run_status(manager_arc: Arc<TMutex<Option<ImapManager>>>, c: CredentialID, res_id: ResolveID) -> Result<()> {
        let manager = Self::get_manager_mutex(&manager_arc, c).await?;
        let status = manager.as_ref().unwrap().status();
        for (id, status) in &status {
            println!("{:?} -> {:?}", id.s_id, status);
        }
        ResolveStore::resolve(res_id, Resolution::Status(status));
        Ok(())
    }

    pub async fn run_suggest(manager_arc: Arc<TMutex<Option<ImapManager>>>, c: CredentialID, mb: MailboxName, res_id: ResolveID) -> Result<()> {
        Self::get_manager_mutex(&manager_arc, c).await?
            .as_mut().unwrap().handle_suggest(mb, res_id).await;
        Ok(())
    }

    pub fn run_poll(&mut self, res_id: ResolveID) {
        let managers = self.managers.clone();
        for (cred_id, manager_arc) in managers.iter() {
            let manager_arc = manager_arc.clone();
            let cred_id = *cred_id;
            tokio::spawn(async move {
                if let Some(mut manager) = Self::get_manager_mutex(&manager_arc, cred_id).await.ok() {
                    manager.as_mut().unwrap().handle_poll().await;
                }
            });
        }
        ResolveStore::resolve(res_id, Resolution::Nothing);
    }

    pub async fn run_sync(manager_arc: Arc<TMutex<Option<ImapManager>>>, c: CredentialID, mb: MailboxName, current_uids: std::collections::HashSet<u32>, mod_seq: u64, res_id: ResolveID) -> Result<()> {
        let mut manager_mutex = Self::get_manager_mutex(&manager_arc, c).await?;
        let mut manager = manager_mutex.as_mut().unwrap();
        
        
        if manager.capabilities.able_to(Capability::QRESYNC) {
            // TODO
        }
        else {
            let search_resolve = ResolveStore::make();
            manager.call_session(SEARCH(mb, vec![], SeqRange::all(true)), search_resolve).await;
            let hs = if let Resolution::Search(hs) = ResolveStore::receive(search_resolve).await? { hs } else { panic!("Expected Search resolution") };
            let missing_uids = hs.difference(&current_uids).map(|uid| *uid).collect::<Vec<u32>>();
            
            
        }
        
        Ok(())
    }

    pub async fn run_stop(manager_arc: Arc<TMutex<Option<ImapManager>>>, c: CredentialID, res_id: ResolveID) -> Result<()> {
        let mut manager = Self::get_manager_mutex(&manager_arc, c).await?;
        manager.take(); // Force drop the manager and thus stops all sessions.
        ResolveStore::resolve(res_id, Resolution::Nothing);
        Ok(())
    }
}
