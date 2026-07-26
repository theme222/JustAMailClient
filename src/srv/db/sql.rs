use std::str::FromStr;

use async_native_tls::AcceptError;

use crate::models::*;
use crate::*;

pub type DBTx<'a> = sqlx::Transaction<'a, sqlx::Sqlite>;

pub async fn initialize_database() -> Result<sqlx::Pool<sqlx::Sqlite>> {
    let project_dir = project_dir();
    let data_dir = project_dir.data_local_dir();
    let db_path = data_dir.join("mail.db");
    let db_url = format!("sqlite://{}", db_path.display());

    let options = sqlx::sqlite::SqliteConnectOptions::from_str(&db_url)?
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal) 
        .busy_timeout(std::time::Duration::from_secs(5));

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await
        .context("Failed to connect to SQLite database")?;

    println!("Database connected at {:?}", db_path);

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .context("Failed to execute database migrations")?;
    
    println!("Database schema is up to date!");
    
    Ok(pool)
}

pub async fn select_messages(db_tx: &mut DBTx<'_>) -> Result<()> {
    let records: Vec<(i64, sqlx::types::Json<MailBodyStructure>)> = sqlx::query_as(
            r#"
            SELECT 
            id,  
            json(bodystructure)
            FROM messages
            LIMIT 5
            "#
        )
        .fetch_all(&mut **db_tx)
        .await?;
    
    for record in records {
        println!("Email ID: {}", record.0);
        println!("Bodystructure: {:#?}", record.1.0);
        println!("---------------------------------------------------");
    }

    Ok(())
}

impl CredentialStore {
    pub async fn get_account_id(id: u64, db_tx: &mut DBTx<'_>) -> Result<i64> { // Gets the account id (db id) associated with the given credential id
        let creds = Self::get(id);
        select_account_by_creds(db_tx, creds).await.map(|r| r.unwrap().id)
    }
}


pub async fn select_account_by_creds(db_tx: &mut DBTx<'_>, creds: Credentials) -> Result<Option<Account>> {
    let Credentials { login, secret, fetch_server, push_server, auth_method, encryption_method } = creds;
    let local_part = login.split('@').next().unwrap_or("");
    let domain = login.split('@').last().unwrap_or("");
    Ok(sqlx::query_as!(
        Account,
        "
        SELECT *
        FROM accounts
        WHERE local_part = ?
        AND domain = ?
        ",
        local_part, domain
    ).fetch_optional(&mut **db_tx).await?)
}


pub async fn get_id_by_message(db_tx: &mut DBTx<'_>, message: Message) -> Result<Option<i64>>
{
    Ok(sqlx::query!(
        "
        SELECT id
        FROM messages
        WHERE imap_uid = ?
        AND account_id = ?
        ",
        message.imap_uid.expect("imap_uid is required"),
        message.account_id
    ).fetch_optional(&mut **db_tx).await?.map(|r| r.id))
}

pub async fn upd_messages(db_tx: &mut DBTx<'_>, msg: Message) -> Result<()> {
    if msg.imap_uid.is_none() { panic!("imap_uid is required"); }
    
    let Message { 
        id, 
        account_id,
        last_sync_time,
        last_query_time,
        flags,
        size,
        internal_date,
        bodystructure,
        imap_uid,
        rfc_message_id,
        env_date,
        env_subject,
        env_from,
        env_reply_to,
        env_to,
        env_cc,
        env_bcc,
        env_in_reply_to,
        header_raw,
        body_preview,
        modseq,
        body_raw, 
    } = msg.clone();

    let flags = serde_json::to_string(&flags).unwrap(); 
    let _bodystructure = serde_json::to_string(&bodystructure).unwrap();
    let env_from = serde_json::to_string(&env_from).unwrap();
    let env_reply_to = serde_json::to_string(&env_reply_to).unwrap();
    let env_to = serde_json::to_string(&env_to).unwrap();
    let env_cc = serde_json::to_string(&env_cc).unwrap();
    let env_bcc = serde_json::to_string(&env_bcc).unwrap();

    // Upsert new message into messages
    sqlx::query!("
        INSERT INTO messages (
            account_id, 
            last_sync_time, 
            flags, 
            size, 
            internal_date, 
            bodystructure, 
            imap_uid, 
            modseq,
            rfc_message_id,
            env_date,
            env_subject,
            env_from,
            env_reply_to,
            env_to,
            env_cc,
            env_bcc,
            env_in_reply_to,
            header_raw,
            body_preview
        ) 
        VALUES (
            ?, -- account_id
            ?, -- last_sync_time
            jsonb(?), -- flags 
            ?, -- size 
            ?, -- internal_date
            jsonb(?), -- bodystructure
            ?,  -- imap_uid
            ?,  -- modseq
            ?,  -- rfc_message_id
            ?,  -- env_date
            ?,  -- env_subject
            jsonb(?),  -- env_from
            jsonb(?),  -- env_reply_to
            jsonb(?),  -- env_to
            jsonb(?),  -- env_cc
            jsonb(?),  -- env_bcc
            ?,  -- env_in_reply_to
            ?,  -- header_raw
            ?  -- body_preview
        )
        ON CONFLICT(account_id, imap_uid) 
        DO UPDATE SET
        last_sync_time = EXCLUDED.last_sync_time,
        flags = EXCLUDED.flags,
        modseq = EXCLUDED.modseq,
        header_raw = COALESCE(EXCLUDED.header_raw, messages.header_raw)
        ", 
        account_id, 
        unix_timestamp(), 
        flags, 
        size, 
        internal_date, 
        _bodystructure, 
        imap_uid, 
        modseq,
        rfc_message_id, 
        env_date, 
        env_subject, 
        env_from, 
        env_reply_to, 
        env_to, 
        env_cc, 
        env_bcc,
        env_in_reply_to, 
        header_raw, 
        body_preview 
    ).execute(&mut **db_tx).await?; 
    
    // Get the new message id
    let message_id = Some(sqlx::query!("
        SELECT id 
        FROM messages
        WHERE imap_uid = ? 
        AND account_id = ?
    ", imap_uid.unwrap(), account_id)
    .fetch_one(&mut **db_tx).await?.id);
    
    // Insert into mailboxes_messages
    sqlx::query!("
        INSERT OR IGNORE INTO mailboxes_messages (mailbox_id, message_id)
        VALUES (?, ?)
    ", 1, message_id)
    .execute(&mut **db_tx).await?;
    
    if body_raw.is_none() { return Ok(()); }
    // We are expecting that in this section right here body_raw is the entire email

    let body_raw = body_raw.unwrap();
    let body = mailparse::parse_mail(&body_raw)?;
    let mut parts = body.parts();
    let mut mbs_iter = bodystructure.into_iter();

    let mut dfs_traverse = parts.zip(mbs_iter);
    let mut parts: Vec<(String, Vec<u8>)> = Vec::new();
    
    // Parse through all parts and add the leaf nodes (parts with no subparts) to the db
    while let Some((mailparse_part, mailbodystructure)) = dfs_traverse.next() {
        if !mailparse_part.subparts.is_empty() { continue; }
        parts.push((mailbodystructure.part_spec_str(), mailparse_part.raw_bytes.to_vec()));
    }

    upd_message_parts(db_tx, msg, parts).await?;
    Ok(())
}

pub async fn upd_message_parts(db_tx: &mut DBTx<'_>, message: Message, parts: Vec<(String, Vec<u8>)>) -> Result<()> {
    let imap_uid = message.imap_uid.expect("rfc_message_id is required");
    
    for (part_spec, data) in parts {
        // Insert or ignore into message_parts
        sqlx::query!("
            INSERT OR IGNORE INTO message_parts (message_id, part_spec, data)
            VALUES (
            ( -- message_id
                SELECT id 
                FROM messages 
                WHERE account_id = ? 
                AND imap_uid = ?
            )
            , ?, ?)
        ", message.account_id, imap_uid, part_spec, data)
        .execute(&mut **db_tx).await?;
    }
    
    Ok(())
}

pub async fn upd_mailbox(db_tx: &mut DBTx<'_>, mb: Mailbox, cred_id: CredentialID) -> Result<()> {
    
    // TODO: If uid_validity is different, invalidate all messages' uids 
    sqlx::query!("
        INSERT INTO mailboxes (account_id, name, mail_count, recent, unseen, uid_validity, highest_modseq, flags)
        VALUES (
            ?,  -- account_id
            ?,  -- name
            ?,  -- mail_count
            ?,  -- recent
            ?,  -- unseen
            ?,  -- uid_validity 
            ?,  -- highest_modseq
            jsonb(?)  -- flags
        )
        ON CONFLICT (account_id, name)
        DO UPDATE SET 
        mail_count = EXCLUDED.mail_count,
        recent = EXCLUDED.recent,
        unseen = EXCLUDED.unseen,
        uid_validity = EXCLUDED.uid_validity,
        highest_modseq = EXCLUDED.highest_modseq,
        flags = EXCLUDED.flags
        ", 
        CredentialStore::get_account_id(cred_id, db_tx).await?,
        mb.name, 
        mb.exists,
        mb.recent,
        mb.unseen,
        mb.uid_validity, 
        mb.highest_modseq.map(|v| v as i64), 
        serde_json::to_string(&mb.attrs).unwrap(),
    ).execute(&mut **db_tx).await?;
    
    Ok(())
}