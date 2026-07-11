
pub use anyhow::{anyhow, Result, Error, Context};
use crate::gui::GUIMessage;
use crate::net::NetMessage;
use crate::srv::SrvMessage;

pub use crate::consts::*;
pub use crate::funcs::*;
pub use globals::*;

#[allow(static_mut_refs)]
pub mod globals { 
    // Will absolutely crash if not initialized before use
    // These values must be set ONCE at the start of the program (before parallelizations occur) and stay constant
    // Like yes ik I should do Arc Mutex whatever but ehhhhhhh can't be bothered
     
    static mut SENDERS: Option<Senders> = None;
    
    #[derive(Clone, Debug)]
    pub struct Senders { // Global values after initialization
        pub net_sender: tokio::sync::mpsc::Sender<super::NetMessage>,
        pub srv_sender: tokio::sync::mpsc::Sender<super::SrvMessage>,
        pub gui_sender:  tokio::sync::mpsc::Sender<super::GUIMessage>,
    }
    #[allow(static_mut_refs)]

    impl Senders {
        pub fn get() -> Self { unsafe { SENDERS.clone().expect("SENDERS has not been initialized") } }
        pub fn set(senders: Self) { 
            unsafe { 
                if SENDERS.is_some() { panic!("SENDERS is already initialized, cannot overwrite"); }
                SENDERS = Some(senders);
            };
        }

        pub async fn net(msg: super::NetMessage) { 
            if let Err(e) = Self::get().net_sender.send(msg).await {
                eprintln!("Failed to send NET message: {:?}", e);
            }
        }
        pub async fn srv(msg: super::SrvMessage) { 
            if let Err(e) = Self::get().srv_sender.send(msg).await {
                eprintln!("Failed to send SRV message: {:?}", e);
            }
        }
        pub async fn gui(msg: super::GUIMessage) { 
            if let Err(e) = Self::get().gui_sender.send(msg).await {
                eprintln!("Failed to send GUI message: {:?}", e);
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Credentials {
    pub login: String,
    pub secret: String,
    pub fetch_server: String,
    pub push_server: String,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("login", &self.login)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mail {
    pub id: String,
    pub subject: String,
    pub from: String,
    pub to: String,
    pub date: String,
    pub body: String,
}

// General status
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Status {
    ALIVE,
    BUSY,
    CONNECTING,
    FAILED,
}
