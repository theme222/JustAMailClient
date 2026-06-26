pub mod db;
use db::sql;

use crate::models::*;

pub enum SrvMessage {
    SaveEmail,
}


pub struct SrvActor {
    db_pool: sqlx::SqlitePool,
    inbox: tokio::sync::mpsc::Receiver<SrvMessage>,
    senders: Senders,
}

impl SrvActor {
    pub async fn new(inbox: tokio::sync::mpsc::Receiver<SrvMessage>, senders: Senders) -> Result<Self> {
        let db_pool = sql::initialize_database().await?;
        Ok( Self { db_pool, inbox, senders, } )
    }

    pub async fn run(&mut self) {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(200)).await;
        }
    }
}