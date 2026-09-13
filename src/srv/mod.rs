pub mod db;

use std::fs::TryLockError::Error;

use db::sql;
use crate::net;

use crate::models::*;
use db::types::*;


#[derive(Debug)]
pub enum SrvAction { // TODO: Consider for query and remove commands, make incoming arguments a list to prevent repeated calls to the database.
    LISTEMAILS,
    SHUTDOWN,
    SYNCEMAIL { msg: MessageSQL, },
    SYNCEMAILSECTION { part: MessagePartSQL, },
    SYNCMAILBOX { mb: MailboxSQL, },
    SYNCACCOUNT { acc: AccountSQL },
    QUERYACCOUNT { acc_k: AccountKey, },
    QUERYMAILBOX { mb_k: MailboxKey, },
    QUERYMESSAGE { msg_k: MessageKey, },
    QUERYMESSAGEPART { part_k: MessagePartKey, },
    RMACCOUNT { acc_k: AccountKey, },
    RMMAILBOX { mb_k: MailboxKey, },
    RMMESSAGE { msg_k: MessageKey, },
    RMMESSAGEPART { part_k: MessagePartKey, },
}

pub struct SrvMessage {
    pub action: SrvAction,
    pub resolve: ResolveID, // Incase you want to hook onto whether the action succeeded or failed
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

    pub async fn run(&mut self) { // Techincally it is plausible to have a race condition here. 

        println!("Starting srv actor");
        use SrvAction::*;
        
        while let Some(msg) = self.inbox.recv().await {
            
            // Writing actions use await_handle_err to prevent race conditions (like having a mail part pointing to a mail that doesn't exist in the database yet.)
            let res_id = msg.resolve;
            match msg.action { 
                LISTEMAILS => spawn_handle_err(SrvActor::run_list_emails(self.db_pool.clone()), "Listing emails", res_id),
                SHUTDOWN => break,
                SYNCEMAIL { msg } => await_handle_err(SrvActor::run_sync_email(msg, self.db_pool.clone()), "Syncing list email", res_id).await,
                SYNCEMAILSECTION { part } => await_handle_err(SrvActor::run_sync_email_section(part, self.db_pool.clone()), "Syncing email section", res_id).await,
                SYNCMAILBOX { mb } => await_handle_err(SrvActor::run_sync_mailbox(mb, self.db_pool.clone()), "Syncing mailbox", res_id).await,
                SYNCACCOUNT { acc } => await_handle_err(SrvActor::run_sync_account(acc, self.db_pool.clone()), "Syncing account", res_id).await,
                QUERYACCOUNT { acc_k } => spawn_handle_err(SrvActor::run_query_account(self.db_pool.clone(), msg.resolve, acc_k), "Querying account", res_id),
                QUERYMAILBOX { mb_k } => todo!(),
                QUERYMESSAGE { msg_k } => todo!(),
                QUERYMESSAGEPART { part_k } => todo!(),
                RMACCOUNT { acc_k } => todo!(),
                RMMAILBOX { mb_k } => todo!(),
                RMMESSAGE { msg_k } => await_handle_err(SrvActor::run_rm_message(self.db_pool.clone(), msg.resolve, msg_k), "Removing message", res_id).await,
                RMMESSAGEPART { part_k } => todo!(),
            }
        }

        println!("Ending srv actor");
    }

    pub async fn run_sync_account(mut acc: AccountSQL, db_pool: sqlx::SqlitePool) -> Result<Resolution> {
        if acc.get_key().is_none() { return Err(anyhow::anyhow!("AccountSQL must have a key to be synced")); }
        println!("Syncing account {:?}", acc.id.as_ref().unwrap().0);
        
        let mut db_tx = db_pool.begin().await?;
        acc.resolve_foreign(&mut db_tx).await;
        acc.upsert(&mut db_tx).await?;
        let acc_after = acc.find(&mut db_tx).await?.ok_or(anyhow::anyhow!("Failed to find account after upsert"))?;
        db_tx.commit().await?;
        Ok(Resolution::AccountSQL(acc_after))
    }

    pub async fn run_sync_mailbox(mut mb: MailboxSQL, db_pool: sqlx::SqlitePool) -> Result<Resolution> { // Sync flags and uid_validity
        // TODOTODO: Consider whether or not it is needed to figure out the before value of the mailbox before continuing with the syncing.
        if mb.get_key().is_none() { return Err(anyhow::anyhow!("MailboxSQL must have a key to be synced")); }
        println!("Syncing mailbox {:?}", mb.id.as_ref().unwrap().0);
        
        let mut db_tx = db_pool.begin().await?;
        mb.resolve_foreign(&mut db_tx).await;
        mb.upsert(&mut db_tx).await?;
        let mb_after: MailboxSQL = mb.find(&mut db_tx).await?.ok_or(anyhow::anyhow!("Failed to find mailbox after upsert"))?;
        db_tx.commit().await?;
        Ok(Resolution::MailboxSQL(mb_after))
    }
    
    pub async fn run_sync_email(mut mail: MessageSQL, db_pool: sqlx::SqlitePool) -> Result<Resolution> {
        if mail.get_key().is_none() { return Err(anyhow::anyhow!("MessageSQL must have a key to be synced")); }
        println!("Saving email {:?}", mail.id.as_ref().unwrap().0);

        let mut db_tx = db_pool.begin().await?;
        mail.resolve_foreign(&mut db_tx).await;
        mail.upsert(&mut db_tx).await?;
        let msg_after: MessageSQL = mail.find(&mut db_tx).await?.ok_or(anyhow::anyhow!("Failed to find message after upsert"))?;
        db_tx.commit().await?;
        Ok(Resolution::MessageSQL(msg_after))
    }
    
    pub async fn run_sync_email_section(mut part: MessagePartSQL, db_pool: sqlx::SqlitePool) -> Result<Resolution> {
        if part.get_key().is_none() { return Err(anyhow::anyhow!("MessagePartSQL must have a key to be synced")); }
        println!("Saving email section {:?}", part.id.as_ref().unwrap().0);
        use imap_proto::types::{SectionPath::*, MessageSection};
        
        let mut db_tx = db_pool.begin().await?;
        part.resolve_foreign(&mut db_tx).await;
        part.upsert(&mut db_tx).await?;
        let part_after = part.find(&mut db_tx).await?.ok_or(anyhow::anyhow!("Failed to find message part after upsert"))?;
        db_tx.commit().await?;
        Ok(Resolution::MessagePartSQL(part_after))
    }

    pub async fn run_list_emails(db_pool: sqlx::Pool<sqlx::Sqlite>) -> Result<Resolution> {
        println!("Listing emails...");
        let mut db_tx = db_pool.begin().await?;
        db::sql::select_messages(&mut db_tx).await?;
        db_tx.commit().await?;
        Ok(Resolution::Nothing)
    }

    pub async fn run_query_account(db_pool: sqlx::Pool<sqlx::Sqlite>, res_id: ResolveID, acc_key: AccountKey) -> Result<Resolution> {
        let mut db_tx = db_pool.begin().await?;
        let account_sql = AccountSQL::from_key(&acc_key, &mut db_tx).await?;
        db_tx.commit().await?;
        println!("Query account: {:?}", account_sql);
        if account_sql.is_none() { ResolveStore::resolve(res_id, Resolution::Nothing); }
        else { ResolveStore::resolve(res_id, Resolution::AccountSQL(account_sql.unwrap())); }
        Ok(Resolution::Nothing)
    }
    
    pub async fn run_rm_message(db_pool: sqlx::Pool<sqlx::Sqlite>, res_id: ResolveID, msg_key: MessageKey) -> Result<Resolution> {
        println!("Removing message: {:?}", msg_key);
        let mut db_tx = db_pool.begin().await?;
        MessageSQL::new(msg_key).rm(&mut db_tx).await?;
        db_tx.commit().await?;
        Ok(Resolution::Nothing)
    }
}