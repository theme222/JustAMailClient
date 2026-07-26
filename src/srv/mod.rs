pub mod db;
use db::sql;
use crate::net;

use crate::models::*;

#[derive(Debug)]
pub enum SrvAction {
    SYNCLISTEMAIL(CredentialID, MailboxName, async_imap::types::Fetch),
    SYNCFULLEMAIL(CredentialID, MailboxName, async_imap::types::Fetch),
    SYNCEMAILSECTION(CredentialID, MailboxName, Vec<MailBodyStructure>, async_imap::types::Fetch),
    SYNCMAILBOX(CredentialID, Mailbox),
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

    pub async fn run(&mut self) { // Techincally it is plausible to have a race condition here. 

        println!("Starting srv actor");
        use SrvAction::*;
        
        while let Some(msg) = self.inbox.recv().await {
            // println!("Doing action: {:?}", msg.action);
            
            match msg.action { // Maybe consider removing the tokio::spawn for writing actions.
                SYNCFULLEMAIL(cred_id, mb, mail) => { // Writing action
                    await_handle_err(SrvActor::run_sync_full_email(cred_id, mb, mail, self.db_pool.clone()), "Syncing full email").await;
                },
                SYNCLISTEMAIL(cred_id, mb, mail) => { // Writing action 
                    await_handle_err(SrvActor::run_sync_list_email(cred_id, mb, mail, self.db_pool.clone()), "Syncing list email").await;
                },
                SYNCEMAILSECTION(cred_id, mb, bs, mail) => { // Writing action
                    await_handle_err(SrvActor::run_sync_email_section(cred_id, mb, bs, mail, self.db_pool.clone()), "Syncing email section").await;
                }
                SYNCMAILBOX(cred_id, mb) => { // Writing action
                    await_handle_err(SrvActor::run_sync_mailbox(cred_id, mb, self.db_pool.clone()), "Syncing mailbox").await;
                }
                LISTEMAILS => { // Reading action
                    spawn_handle_err(SrvActor::run_list_emails(self.db_pool.clone()), "Listing emails");
                }
                SHUTDOWN => { break; }
                _ => { println!("Action not implemented: {:?}", msg.action)}
            }
        }

        println!("Ending srv actor");
    }

    pub async fn run_sync_mailbox(cred_id: CredentialID, mb: Mailbox, db_pool: sqlx::SqlitePool) -> Result<()> { // Sync flags and uid_validity
        let mut db_tx = db_pool.begin().await?;
        sql::upd_mailbox(&mut db_tx, mb, cred_id).await?;
        db_tx.commit().await?;
        Ok(())
    }
    
    pub async fn run_sync_list_email(cred_id: CredentialID, mb: MailboxName, mail: async_imap::types::Fetch, db_pool: sqlx::SqlitePool) -> Result<()> {
        println!("Saving email {}", mail.message);

        let mut db_tx = db_pool.begin().await?;
        let mut mail: Message = mail.into();
        // Invalidate body_raw for large mails  
        mail.body_raw = if mail.size > MAX_LISTFETCH_SIZE as i64 { None } else { mail.body_raw };
        sql::upd_messages(&mut db_tx, mail).await?;
        db_tx.commit().await?;
        Ok(())
    }
    
    pub async fn run_sync_full_email(cred_id: CredentialID, mb: MailboxName, mail: async_imap::types::Fetch, db_pool: sqlx::SqlitePool) -> Result<()> {
        println!("Saving email {}", mail.message);

        let mut db_tx = db_pool.begin().await?;
        sql::upd_messages(&mut db_tx, mail.into()).await?;
        db_tx.commit().await?;
        Ok(())
    }

    pub async fn run_sync_email_section(cred_id: CredentialID, mb: MailboxName, bs: Vec<MailBodyStructure>, mail: async_imap::types::Fetch, db_pool: sqlx::SqlitePool) -> Result<()> {
        println!("Saving email section");
        use imap_proto::types::{SectionPath::*, MessageSection};
        
        let message_parts = bs
            .clone()
            .iter()
            .map(|bs| 
                (
                    bs.part_spec_str(),
                    mail.section(&Part(bs.part_spec().clone(), None))
                )
            )
            .filter(|(_, section)| section.is_some())
            .map(|(part_spec, section)| (part_spec, section.unwrap().to_vec()))
            .collect::<Vec<_>>();

        let mut db_tx = db_pool.begin().await?;
        sql::upd_message_parts(&mut db_tx, mail.into(), message_parts).await?;
        db_tx.commit().await?;
        Ok(())
    }

    pub async fn run_list_emails(db_pool: sqlx::Pool<sqlx::Sqlite>) -> Result<()> {
        println!("Listing emails...");
        let mut db_tx = db_pool.begin().await?;
        db::sql::select_messages(&mut db_tx).await?;
        db_tx.commit().await?;
        Ok(())
    }
}