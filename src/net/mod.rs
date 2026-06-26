use crate::net::fetch::imap::*;
use crate::models::*;

pub mod fetch;
pub mod push;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetAction {
    SendEcho(Credentials),
    SendEmail(Credentials),
    FetchEmail(Credentials),
    Shutdown,
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

async fn get_or_create_manager<'a>(managers: &'a mut std::collections::HashMap<Credentials, ImapManager>, credentials: &Credentials) -> Result<&'a mut ImapManager> {
    if !managers.contains_key(credentials) {
        let manager = ImapManager::new(credentials).await?;
        managers.insert(credentials.clone(), manager);
    }
    Ok(managers.get_mut(&credentials).unwrap())
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
                SendEcho(c) => { push::smtp::send_echo_email(&c).await.unwrap(); }
                SendEmail(c) => { push::smtp::send_test_email(&c).await.unwrap(); }
                FetchEmail(c) => {
                    let manager = get_or_create_manager(&mut self.managers, &c).await.unwrap();
                    let session = manager.get_session(fetch::imap::ImapSessionType::Puller("INBOX".into())).await;
                    let mut stream = session.fetch_stream(&SeqRange::last(), &vec![
                        UID,
                        BODYSTRUCTURE,
                        ENVELOPE,
                        FLAGS,
                        INTERNALDATE,
                        RFC822SIZE,
                        BODYPEEKSECTION("TEXT", "8192")
                    ]).await.unwrap();
                    println!("{:?}", ImapSession::parse_fetch_stream_all(&mut stream).await.unwrap());
                }
                Shutdown => { break; }
            }
        }
        println!("Ending net actor");
    }
}
