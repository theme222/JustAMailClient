pub mod db;
use std::fs::TryLockError::Error;

use db::sql;
use crate::net;

use crate::models::*;
use db::types::*;


#[derive(Debug)]
pub enum SrvAction {
    SYNCEMAIL{
        msg: MessageSQL,
    },
    SYNCEMAILSECTION{
        part: MessagePartSQL,
    },
    SYNCMAILBOX{
        mb: MailboxSQL,
    },
    SYNCACCOUNT { acc: AccountSQL },
    LISTEMAILS,
    // IDLENEWEMAIL,
    // IDLEEXPUNGED,
    SHUTDOWN,
    QUERYACCOUNT { acc: AccountSQL, },
    QUERYMAILBOX { mb: MailboxSQL, },
    QUERYMESSAGE { msg: MessageSQL, },
    QUERYMESSAGEPART { part: MessagePartSQL, },
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
                SYNCEMAIL { msg } => { // Writing action 
                    await_handle_err(SrvActor::run_sync_email(msg, self.db_pool.clone()), "Syncing list email", res_id).await;
                },
                SYNCEMAILSECTION { part } => { // Writing action
                    await_handle_err(SrvActor::run_sync_email_section(part, self.db_pool.clone()), "Syncing email section", res_id).await;
                }
                SYNCMAILBOX { mb } => { // Writing action
                    await_handle_err(SrvActor::run_sync_mailbox(mb, self.db_pool.clone()), "Syncing mailbox", res_id).await;
                }
                SYNCACCOUNT { acc } => { // Writing action
                    await_handle_err(SrvActor::run_sync_account(acc, self.db_pool.clone()), "Syncing account", res_id).await;
                },
                LISTEMAILS => { // Reading action
                    spawn_handle_err(SrvActor::run_list_emails(self.db_pool.clone()), "Listing emails", res_id);
                }
                SHUTDOWN => { break; }
                // _ => { println!("Action not implemented: {:?}", msg.action)}
                QUERYACCOUNT { acc } => {
                    spawn_handle_err(SrvActor::run_query_account(self.db_pool.clone(), msg.resolve, acc), "Querying account", res_id);
                },
                QUERYMAILBOX { mb } => todo!(),
                QUERYMESSAGE { msg } => todo!(),
                QUERYMESSAGEPART { part } => todo!(),
            }
        }

        println!("Ending srv actor");
    }

    pub async fn run_sync_account(acc: AccountSQL, db_pool: sqlx::SqlitePool) -> Result<Resolution> {
        let mut db_tx = db_pool.begin().await?;
        acc.upsert(&mut db_tx).await?;
        let acc_after = acc.find(&mut db_tx).await?;
        db_tx.commit().await?;
        Ok(Resolution::AccountSQL(acc_after.ok_or(anyhow::anyhow!("Failed to find account after upsert"))?))
    }

    pub async fn run_sync_mailbox(mb: MailboxSQL, db_pool: sqlx::SqlitePool) -> Result<Resolution> { // Sync flags and uid_validity
        let mut db_tx = db_pool.begin().await?;
        mb.upsert(&mut db_tx).await?;
        let mb_after: Option<MailboxSQL> = mb.find(&mut db_tx).await?;
        db_tx.commit().await?;
        Ok(Resolution::MailboxSQL(mb_after.ok_or(anyhow::anyhow!("Failed to find mailbox after upsert"))?))
    }
    
    pub async fn run_sync_email(mail: MessageSQL, db_pool: sqlx::SqlitePool) -> Result<Resolution> {
        println!("Saving email {:?}", mail.id);

        let mut db_tx = db_pool.begin().await?;
        mail.upsert(&mut db_tx).await?;
        let msg_after: Option<MessageSQL> = mail.find(&mut db_tx).await?;
        db_tx.commit().await?;
        Ok(Resolution::MessageSQL( msg_after.ok_or(anyhow::anyhow!("Failed to find message after upsert"))? ))
    }
    
    pub async fn run_sync_email_section(part: MessagePartSQL, db_pool: sqlx::SqlitePool) -> Result<Resolution> {
        println!("Saving email section {:?}", part.id);
        use imap_proto::types::{SectionPath::*, MessageSection};
        
        let mut db_tx = db_pool.begin().await?;
        part.upsert(&mut db_tx).await?;
        let part_after = part.find(&mut db_tx).await?;
        db_tx.commit().await?;
        Ok(Resolution::MessagePartSQL(part_after.ok_or(anyhow::anyhow!("Failed to find message part after upsert"))?))
    }

    pub async fn run_list_emails(db_pool: sqlx::Pool<sqlx::Sqlite>) -> Result<Resolution> {
        println!("Listing emails...");
        let mut db_tx = db_pool.begin().await?;
        db::sql::select_messages(&mut db_tx).await?;
        db_tx.commit().await?;
        Ok(Resolution::Nothing)
    }

    pub async fn run_query_account(db_pool: sqlx::Pool<sqlx::Sqlite>, res_id: ResolveID, account_sql: AccountSQL) -> Result<Resolution> {
        let mut db_tx = db_pool.begin().await?;
        let account_sql = account_sql.find(&mut db_tx).await?;
        db_tx.commit().await?;
        println!("Query account: {:?}", account_sql);
        if account_sql.is_none() { ResolveStore::resolve(res_id, Resolution::Nothing); }
        else { ResolveStore::resolve(res_id, Resolution::AccountSQL(account_sql.unwrap())); }
        Ok(Resolution::Nothing)
    }
}