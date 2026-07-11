use std::str::FromStr;

use async_native_tls::AcceptError;

use crate::models::*;
use crate::net::structure::MailBodyStructure;
use crate::*;
use super::types;

pub async fn initialize_database() -> Result<sqlx::Pool<sqlx::Sqlite>> {
    let project_dir = project_dir();
    let data_dir = project_dir.data_local_dir();
    let db_path = data_dir.join("mail.db");
    let db_url = format!("sqlite://{}", db_path.display());

    let options = sqlx::sqlite::SqliteConnectOptions::from_str(&db_url)?
        .create_if_missing(true);

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

pub async fn select_messages(pool: &sqlx::Pool<sqlx::Sqlite>) -> Result<()> {
    let records: Vec<(i64, sqlx::types::Json<MailBodyStructure>)> = sqlx::query_as(
            r#"
            SELECT 
            id,  
            json(bodystructure)
            FROM messages
            LIMIT 5
            "#
        )
        .fetch_all(pool)
        .await?;
    
    for record in records {
        println!("Email ID: {}", record.0);
        println!("Bodystructure: {:#?}", record.1.0);
        println!("---------------------------------------------------");
    }

    Ok(())
}

pub async fn insert_or_update_message(db_pool: &sqlx::Pool<sqlx::Sqlite>, msg: types::Message) -> Result<()> {
    if msg.imap_uid.is_none() { panic!("imap_uid is required"); }

    let types::Message { 
        id, 
        account_id,
        ty,
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
    } = msg;

    let flags = serde_json::to_string(&flags).unwrap(); 
    let _bodystructure = serde_json::to_string(&bodystructure).unwrap();
    let env_from = serde_json::to_string(&env_from).unwrap();
    let env_reply_to = serde_json::to_string(&env_reply_to).unwrap();
    let env_to = serde_json::to_string(&env_to).unwrap();
    let env_cc = serde_json::to_string(&env_cc).unwrap();
    let env_bcc = serde_json::to_string(&env_bcc).unwrap();
    
    // Check if message already exists
    let mut message_id = sqlx::query!("
        SELECT id
        FROM messages
        WHERE imap_uid = ? 
        AND account_id = ?
    ", imap_uid.unwrap(), account_id) // TODO: Add uid_validity check
    .fetch_optional(db_pool).await?.map(|r| r.id);

    if message_id.is_none() {
        // Insert new message into messages
        sqlx::query!("
            INSERT INTO messages (
                account_id, 
                ty, 
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
                ?, -- ty
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
            )", 
            account_id, 
            ty, 
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
        ).execute(db_pool).await?; 
        
        // Get the new message id
        message_id = Some(sqlx::query!("
            SELECT id 
            FROM messages
            WHERE imap_uid = ?
        ", msg.imap_uid.unwrap())
        .fetch_one(db_pool).await?.id);
        
        // Insert into mailboxes_messages
        sqlx::query!("
            INSERT INTO mailboxes_messages (mailbox_id, message_id)
            VALUES (?, ?)
        ", 1, message_id)
        .execute(db_pool).await?;
    }
    else if let Some(message_id) = message_id {
        sqlx::query!("
            UPDATE messages
            SET flags = jsonb(?),
                last_sync_time = ?,
                modseq = ?
            WHERE id = ?
        ", flags, unix_timestamp(), modseq, message_id)
        .execute(db_pool).await?;
    }
    
    if body_raw.is_none() { return Ok(()); }
    // We are expecting that in this section right here body_raw is the entire email or at least parts that we know fully exist.
    
    // Query existing parts to avoid duplicates
    let existing_parts = sqlx::query!("
        SELECT part_spec
        FROM message_parts
        WHERE message_id = ?
    ", message_id)
    .fetch_all(db_pool)
    .await?
    .into_iter()
    .map(|r| r.part_spec)
    .collect::<std::collections::HashSet<String>>(); // Realistically this is not a good idea. But I can't be bothered to do a full test setup
     
    let body_raw = body_raw.unwrap();
    let body = mailparse::parse_mail(&body_raw)?;
    let mut parts = body.parts();
    let mut mbs_iter = bodystructure.into_iter();

    let mut dfs_traverse = parts.zip(mbs_iter);
    
    // Parse through all parts and add the leaf nodes (parts with no subparts) to the db
    while let Some((mailparse_part, mailbodystructure)) = dfs_traverse.next() {

        if !mailparse_part.subparts.is_empty() { continue; }
        if existing_parts.contains(&mailbodystructure.get_part_spec_str()) { continue; }
        
        // Insert into message_parts
        sqlx::query!("
            INSERT INTO message_parts (message_id, part_spec, data)
            VALUES (?, ?, ?)
        ", message_id, mailbodystructure.get_part_spec_str(), mailparse_part.raw_bytes)
        .execute(db_pool).await?;
        
    }
    Ok(())
}