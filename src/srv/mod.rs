pub mod db;
use db::sql;
use crate::net;

use crate::{models::*, net::fetch::imap::MailFlag};

#[derive(Debug)]
pub enum SrvAction {
    SYNCEMAIL(async_imap::types::Fetch),
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
    senders: Senders,
}

impl SrvActor {
    pub async fn new(inbox: tokio::sync::mpsc::Receiver<SrvMessage>, senders: Senders) -> Self {
        let db_pool = sql::initialize_database().await.unwrap();
        
        // ensure at least one account exists for now
        Self { db_pool, inbox, senders, }
    }

    pub async fn run(&mut self) {

        println!("Starting srv actor");
        use SrvAction::*;
        
        while let Some(msg) = self.inbox.recv().await {
            // println!("Doing action: {:?}", msg.action);
            
            match msg.action {
                SYNCEMAIL(mail) => { self.run_sync_email(mail).await; }
                SYNCMAILBOX(mb) => { self.run_sync_mailbox(mb).await; }
                LISTEMAILS => { self.run_list_emails().await; }
                SHUTDOWN => { break; }
                _ => { println!("Action not implemented: {:?}", msg.action)}
            }
        }

        println!("Ending srv actor");
    }

    pub async fn run_sync_mailbox(&mut self, mb: net::fetch::imap::Mailbox) { // Sync flags and uid_validity
        let res = sqlx::query!("
            SELECT uid_validity 
            FROM mailboxes 
            WHERE name = ?
            AND account_id = 1
            ", mb.name
        )
            .fetch_all(&self.db_pool)
            .await
            .unwrap();
        
        if res.len() == 0 {
            sqlx::query!("
                INSERT INTO mailboxes (account_id, name, uid_validity, flags, type)
                VALUES (
                    ?,  -- account_id
                    ?,  -- name
                    ?,  -- uid_validity
                    jsonb(?),  -- flags
                    ?   -- type
                )
                ", 1, mb.name, mb.uid_validity.unwrap_or_default(), serde_json::to_string(&mb.flags).unwrap(), ""
            );
        }
        else {
            // TODO: If uid_validity is different, invalidate all messages' uids 
            sqlx::query!("
                UPDATE mailboxes
                SET uid_validity = ?, flags = jsonb(?)
                WHERE name = ? AND account_id = 1
                ", mb.uid_validity.unwrap_or_default(), serde_json::to_string(&mb.flags).unwrap(), mb.name
            );
        }
        
        
    }
    
    pub async fn run_sync_email(&mut self, mail: async_imap::types::Fetch) {
        println!("Saving email {}", mail.message);
        let account_id = 1;
        let ty = "";
        let flags = mail
            .flags()
            .collect::<Vec<_>>()
            .into_iter()
            .map(|f| f
            .into())
            .collect::<Vec<MailFlag>>();
        let flags = serde_json::to_string(&flags).unwrap();
        let size = mail.size.unwrap();
        let internal_date = mail.internal_date().unwrap().timestamp_millis();                    
        let bodystructure: net::structure::MailBodyStructure = mail.bodystructure().unwrap().into();
        let bodystructure = serde_json::to_string(&bodystructure).unwrap();
        let imap_uid = mail.uid.unwrap();
        let body_bytes = mail.text().unwrap_or_default();
        let body_preview = net::structure::get_preview_from_partial(&body_bytes, mail.bodystructure().unwrap());
        let body_raw = if size > MAX_PREVIEW_SIZE { None } else { Some(body_bytes) };
        
        sqlx::query!("
            INSERT INTO messages (account_id, type, last_sync_time, flags, size, internal_date, bodystructure, imap_uid, body_preview, body_raw) 
            VALUES (
                ?, -- account_id
                ?, -- type
                ?, -- last_sync_time
                jsonb(?), -- flags 
                ?, -- size 
                ?, -- internal_date
                jsonb(?), -- bodystructure
                ?,  -- imap_uid
                ?,  -- body_preview
                ?  -- body_raw
            )", 
            account_id, ty, unix_timestamp(), flags, size, internal_date, bodystructure, imap_uid, body_preview, body_raw
        )
        .execute(&self.db_pool)
        .await
        .unwrap();
    }

    pub async fn run_list_emails(&self) {
        let messages = sqlx::query!("SELECT * FROM messages")
            .fetch_all(&self.db_pool)
            .await
            .unwrap();
        
        for message in messages {
            println!("{:?}", message);
        }
    }
}