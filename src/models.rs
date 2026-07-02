pub use anyhow::{anyhow, Result, Error, Context};
use crate::ui::UIMessage;
use crate::net::NetMessage;
use crate::srv::SrvMessage;

pub use crate::consts::*;

#[derive(Clone, Debug)]
pub struct Senders { // Global values after initialization
    pub net_sender: tokio::sync::mpsc::Sender<NetMessage>,
    pub srv_sender: tokio::sync::mpsc::Sender<SrvMessage>,
    pub ui_sender:  tokio::sync::mpsc::Sender<UIMessage>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Credentials {
    pub login: String,
    pub secret: String,
    pub fetch_server: String,
    pub push_server: String,
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

pub fn project_dir() -> directories::ProjectDirs {
    directories::ProjectDirs::from("com", "tongsima", "jamc").expect("Failed to find project directories")
}

pub fn unix_timestamp() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_millis() as i64
}