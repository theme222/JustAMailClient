use crate::net::fetch::imap::{ImapSessionCommandType::LISTFETCH, *};
use crate::models::*;
use futures::stream::{Stream, StreamExt};

pub mod fetch;
pub mod push;
pub mod structure;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionUpdate {
    STARTED(ImapSessionId),
    CMDSUCCESS(ImapSessionId, u64), // action id
    CMDFAILURETRYAGAIN(ImapSessionId, u64), // action Id
    CMDFAILUREUNRECOVERABLE(ImapSessionId, u64), // action Id
    SESSIONABORT(ImapSessionId), // Session lost network connection and could not continue 
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetAction {
    /* External Use */
    ECHO(Credentials),
    SEND(Credentials),
    LISTFETCH(Credentials),
    PREFETCH(Credentials),
    FETCH(Credentials),
    STATUS(Credentials),
    /* External Use */
    /* From ImapSession */
    IMAPUPDATE(SessionUpdate),
    /* From ImapSession */
    SHUTDOWN,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetMessage {
    pub action: NetAction,
}

pub struct NetActor {
    inbox: tokio::sync::mpsc::Receiver<NetMessage>,
    managers: std::collections::HashMap<Credentials, ImapManager>, 
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
        Self { inbox, managers: std::collections::HashMap::new() }
    }
    
    pub async fn run(&mut self) {
        use NetAction::*;
        println!("Starting net actor");
        while let Some(msg) = self.inbox.recv().await {
            println!("Doing action: {:?}", msg.action);
            
            match msg.action {
                ECHO(c) => { tokio::spawn(push::smtp::send_echo_email(c)); }
                SEND(c) => { tokio::spawn(push::smtp::send_test_email(c)); }
                LISTFETCH(c) => { self.run_list_fetch(c).await; /* Can't run tokio::spawn */ }
                STATUS(c) => { self.run_status(c).await; }
                SHUTDOWN => { break; }
                IMAPUPDATE(session_update) => { self.run_imap_update(session_update).await; }
                _ => { println!("Unknown action: {:?}", msg.action) }
            }
        }
        println!("Ending net actor");
    }

    pub async fn get_manager(&mut self, c: Credentials) -> &mut ImapManager {
        if !self.managers.contains_key(&c) {
            let manager = ImapManager::new(c.clone()).await.unwrap();
            self.managers.insert(c.clone(), manager);
        }
        self.managers.get_mut(&c).unwrap()
    }

    pub async fn run_list_fetch(&mut self, c: Credentials) {
        use fetch::imap::FetchType::*;
        let manager = self.get_manager(c.clone()).await;
        manager.call_session(LISTFETCH("INBOX".into(), SeqRange::all())).await;
    }

    pub async fn run_imap_update(&mut self, session_update: SessionUpdate) {
        use SessionUpdate::*;
        let isid = match &session_update {
            STARTED(isid) => isid,
            CMDSUCCESS(isid, _) => isid,
            CMDFAILURETRYAGAIN(isid, _) => isid,
            CMDFAILUREUNRECOVERABLE(isid, _) => isid,
            SESSIONABORT(isid) => isid,
        };
        let manager = self.get_manager(isid.m_id.clone()).await;
        manager.rcv_session_update(session_update).await;
    }

    pub async fn run_status(&mut self, c: Credentials) {
        let status = self.get_manager(c).await.status();
        for (id, status) in status {
            println!("{:?} -> {:?}", id.s_id, status);
        }
    }
}
