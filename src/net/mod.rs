use crate::net::fetch::imap::*;
use crate::models::*;
use futures::stream::{Stream, StreamExt};

pub mod fetch;
pub mod push;
pub mod structure;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetAction {
    SENDECHO(Credentials),
    SENDEMAIL(Credentials),
    FETCHEMAIL(Credentials),
    SHUTDOWN,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetMessage {
    pub action: NetAction,
}

pub struct NetActor {
    inbox: tokio::sync::mpsc::Receiver<NetMessage>,
    senders: Senders,
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
    pub async fn new(inbox: tokio::sync::mpsc::Receiver<NetMessage>, senders: Senders) -> Self {
        Self { inbox, senders, managers: std::collections::HashMap::new() }
    }
    
    pub async fn run(&mut self) {
        use NetAction::*;
        use fetch::imap::FetchType::*;
        
        println!("Starting net actor");
        while let Some(msg) = self.inbox.recv().await {
            println!("Doing action: {:?}", msg.action);
            
            match msg.action {
                SENDECHO(c) => { push::smtp::send_echo_email(&c).await.unwrap(); }
                SENDEMAIL(c) => { push::smtp::send_test_email(&c).await.unwrap(); }
                FETCHEMAIL(c) => {
                    
                    if !self.managers.contains_key(&c) {
                        let manager = ImapManager::new(c.clone(), self.senders.clone()).await.unwrap();
                        self.managers.insert(c.clone(), manager);
                    }
                    
                    let manager = self.managers.get_mut(&c).unwrap();
                    
                    let session = manager.get_session(fetch::imap::ImapSessionType::Puller("INBOX".into())).await;
                    let mut stream = session.fetch_stream(&SeqRange::all(), &vec![
                        UID,
                        BODYSTRUCTURE,
                        ENVELOPE,
                        FLAGS,
                        INTERNALDATE,
                        RFC822SIZE,
                        BODYPEEKSECTION("TEXT".into(), format!("0.{}", MAX_PREVIEW_SIZE)),
                    ]).await.unwrap();
                    
                    let mut results: Vec<async_imap::types::Fetch> = Vec::new();
                    
                    use crate::srv::*;
                    use crate::srv::SrvAction::*;
                
                    while let Some(mail_result) = stream.next().await {
                        match mail_result {
                            Err(e) => { eprintln!("Error while parsing fetch stream: {:?}", e); }
                            Ok(mail) => { self.senders.srv_sender.send(SrvMessage {action: SYNCEMAIL(mail)}).await.unwrap(); }
                        }
                    }
              
                }
                SHUTDOWN => { break; }
            }
        }
        println!("Ending net actor");
    }
}
