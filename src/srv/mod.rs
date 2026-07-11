pub mod db;
use db::sql;
use crate::net;

use crate::{models::*, net::fetch::imap::MailFlag};

#[derive(Debug)]
pub enum SrvAction {
    SYNCLISTEMAIL(async_imap::types::Fetch),
    SYNCMAILBOX(net::fetch::imap::Mailbox),
    LISTEMAILS,
    IDLENEWEMAIL,
    IDLEEXPUNGED,
    SHUTDOWN,
}

pub struct SrvMessage {
    pub action: SrvAction,
}


pub struct SrvActor {
    db_pool: sqlx::SqlitePool,
    inbox: tokio::sync::mpsc::Receiver<SrvMessage>,
}

impl SrvActor {
    pub async fn new(inbox: tokio::sync::mpsc::Receiver<SrvMessage>) -> Self {
        let db_pool = sql::initialize_database().await.unwrap();
        
        // ensure at least one account exists for now
        Self { db_pool, inbox }
    }

    pub async fn run(&mut self) {

        println!("Starting srv actor");
        use SrvAction::*;
        
        while let Some(msg) = self.inbox.recv().await {
            // println!("Doing action: {:?}", msg.action);
            
            match msg.action {
                SYNCLISTEMAIL(mail) => { 
                    tokio::spawn(SrvActor::run_sync_email(mail, self.db_pool.clone())); 
                }
                SYNCMAILBOX(mb) => { 
                    tokio::spawn(SrvActor::run_sync_mailbox(mb, self.db_pool.clone()));
                }
                LISTEMAILS => { 
                    tokio::spawn(SrvActor::run_list_emails(self.db_pool.clone())); 
                }
                SHUTDOWN => { break; }
                _ => { println!("Action not implemented: {:?}", msg.action)}
            }
        }

        println!("Ending srv actor");
    }

    pub async fn run_sync_mailbox(mb: net::fetch::imap::Mailbox, db_pool: sqlx::SqlitePool) { // Sync flags and uid_validity
        let res = sqlx::query!("
            SELECT uid_validity 
            FROM mailboxes 
            WHERE name = ?
            AND account_id = 1
            ", mb.name
        ).fetch_all(&db_pool).await.unwrap();
        
        if res.len() == 0 {
            sqlx::query!("
                INSERT INTO mailboxes (account_id, name, uid_validity, highest_modseq, flags, ty)
                VALUES (
                    ?,  -- account_id
                    ?,  -- name
                    ?,  -- uid_validity
                    ?,  -- highest_modseq
                    jsonb(?),  -- flags
                    ?   -- ty
                )
                ", 1, mb.name, mb.uid_validity.unwrap_or_default(), mb.highest_modseq.unwrap_or_default() as i64, serde_json::to_string(&mb.flags).unwrap(), ""
            ).execute(&db_pool).await.unwrap();
        }
        else {
            // TODO: If uid_validity is different, invalidate all messages' uids 
            sqlx::query!("
                UPDATE mailboxes
                SET uid_validity = ?, flags = jsonb(?)
                WHERE name = ? AND account_id = 1
                ", mb.uid_validity.unwrap_or_default(), serde_json::to_string(&mb.flags).unwrap(), mb.name
            ).execute(&db_pool).await.unwrap();
        }
        
        
    }
    
    pub async fn run_sync_email(mail: async_imap::types::Fetch, db_pool: sqlx::SqlitePool) {
        println!("Saving email {}", mail.message);
        sql::insert_or_update_message(&db_pool, mail.into()).await.unwrap();
    }

    pub async fn run_list_emails(db_pool: sqlx::Pool<sqlx::Sqlite>) {
        // let messages = sqlx::query("
        //     SELECT bodystructure
        //     FROM messages
        //     LIMIT 2
        //     ")
        //     .fetch_all(&db_pool)
        //     .await
        //     .unwrap();
        
        // for message in messages {
        //     println!("{:?}", message);
        // }
        db::sql::select_messages(&db_pool).await.unwrap();
    }
}