use sqlx::{AssertSqlSafe, Row, Sqlite, sqlite::SqliteQueryResult};

use crate::models::*;

pub type DBTx<'a> = sqlx::Transaction<'a, sqlx::Sqlite>;
pub type SqliteID = i64;

pub trait SQLObj: Sized + for<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow> + Send + Unpin + Default {
    type Key: SQLKey;
    const DB_TABLE: &'static str = Self::Key::DB_TABLE;

    fn get_key(&self) -> &Option<KeyWrapper<Self::Key>>;
    fn get_key_mut(&mut self) -> &mut Option<KeyWrapper<Self::Key>>;
    async fn resolve_foreign(&mut self, db_tx: &mut DBTx<'_>); // Resolves foreign keys
    async fn upsert(&self, db_tx: &mut DBTx<'_>) -> Result<SqliteQueryResult, sqlx::Error>;
    
    fn new(k: Self::Key) -> Self {
        let mut obj = Self::default();
        *obj.get_key_mut() = Some(KeyWrapper(k));
        obj
    }
    async fn resolve_keys(&mut self, db_tx: &mut DBTx<'_>) { // Resolves primary and foreign keys
        self.resolve_foreign(db_tx).await;
        self.resolve_primary(db_tx).await;
    }
    async fn resolve_primary(&mut self, db_tx: &mut DBTx<'_>) { // Resolves the primary key
        if self.get_key().is_none() { return; }
        let key = self.get_key().clone().unwrap().0;
        let resolved = key.resolve(db_tx).await;
        *self.get_key_mut() = resolved.and_then(|k| Some(KeyWrapper(k)));
    } 
    async fn find(&self, db_tx: &mut DBTx<'_>) -> Result<Option<Self>, sqlx::Error> {
        let key = self.get_key();
        if key.is_none() { return Ok(None); }
        let mut qb: sqlx::QueryBuilder<Sqlite> = sqlx::QueryBuilder::new(format!("SELECT * FROM {} WHERE ", Self::DB_TABLE));
        
        let key = key.clone().unwrap().0.resolve(db_tx).await.unwrap(); // Resolve the key into a standard form
        key.condition(&mut qb);
        
        qb.build_query_as::<Self>()
            .fetch_optional(&mut **db_tx)
            .await
    }
    async fn from_key(key: &Self::Key, db_tx: &mut DBTx<'_>) -> Result<Option<Self>, sqlx::Error> { // Get the object from the database by key
        let mut qb: sqlx::QueryBuilder<Sqlite> = sqlx::QueryBuilder::new(format!("SELECT * FROM {} WHERE ", Self::DB_TABLE));
        let mut obj = Self::new(key.clone());
        obj.resolve_primary(db_tx).await;
        if obj.get_key().is_none() { return Ok(None); }
        obj.get_key().as_ref().unwrap().0.condition(&mut qb);
        qb.build_query_as::<Self>()
            .fetch_optional(&mut **db_tx)
            .await
    }
    async fn rm(&self, db_tx: &mut DBTx<'_>) -> Result<SqliteQueryResult, sqlx::Error> {
        let mut qb: sqlx::QueryBuilder<Sqlite> = sqlx::QueryBuilder::new(format!("DELETE FROM {} WHERE ", Self::DB_TABLE));
        
        let key = self.get_key().clone().expect("Key must exist to remove from database").0.resolve(db_tx).await.unwrap(); // Resolve the key into a standard form
        key.condition(&mut qb);
        
        qb.build().execute(&mut **db_tx).await
    }
}

#[derive(Debug, Clone)]
pub struct KeyWrapper<T>(pub T); // Why? uhhhhhhhhhhhhhh

pub trait SQLKey: Clone + std::fmt::Debug {
    const DB_TABLE: &'static str;
    
    fn condition(&self, qb: &mut sqlx::QueryBuilder<Sqlite>); // Add the condition used to check for this key to the qb
    fn from_id(id: SqliteID) -> Self;
    fn as_id(&self) -> Option<SqliteID>;
    async fn resolve_inner(&self, db_tx: &mut DBTx<'_>) -> Option<Self>; // Resolve all the key's dependants
    
    async fn resolve(&self, db_tx: &mut DBTx<'_>) -> Option<Self> { // Resolve key into standard form
        if let Some(id) = self.as_id() { return Some(self.clone()); }
        let res = self.resolve_inner(db_tx).await?;
        let mut qb: sqlx::QueryBuilder<Sqlite> = sqlx::QueryBuilder::new(format!("SELECT id FROM {} WHERE ", Self::DB_TABLE));
        res.condition(&mut qb);
        let id = qb.build()
            .fetch_optional(&mut **db_tx)
            .await
            .unwrap_or_default();
        id.map(|id| Self::from_id(id.get("id")))
    }
}

impl<T: SQLKey> sqlx::Type<Sqlite> for KeyWrapper<T> {
    fn type_info() -> sqlx::sqlite::SqliteTypeInfo {
        <SqliteID as sqlx::Type<Sqlite>>::type_info()
    }
}
impl<'r, T: SQLKey> sqlx::Decode<'r, Sqlite> for KeyWrapper<T> {
    fn decode(value: sqlx::sqlite::SqliteValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let id = <SqliteID as sqlx::Decode<Sqlite>>::decode(value)?;
        Ok(Self(T::from_id(id)))
    }
}
impl<'q, T: SQLKey> sqlx::Encode<'q, Sqlite> for KeyWrapper<T> {
    fn encode_by_ref(
        &self,
        buf: &mut <Sqlite as sqlx::Database>::ArgumentBuffer,
    ) -> std::prelude::v1::Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <Option<SqliteID> as sqlx::Encode<'q, Sqlite>>::encode_by_ref(&self.0.as_id(), buf)
    }
}

#[derive(Debug, Clone,)]
pub enum MessageKey {
    ID(SqliteID),
    ACCIDIMAPUID(AccountKey, i64)
}

impl SQLKey for MessageKey {
    const DB_TABLE: &'static str = "messages";
    fn from_id(id: SqliteID) -> Self { MessageKey::ID(id) }
    fn as_id(&self) -> Option<SqliteID> {
        match self {
            MessageKey::ID(id) => Some(*id),
            _ => None,
        }
    }
    fn condition(&self, qb: &mut sqlx::QueryBuilder<Sqlite>) {
        match self {
            MessageKey::ID(id) => {
                qb.push("id = ");
                qb.push_bind(id);
            },
            MessageKey::ACCIDIMAPUID(acc_id, imap_uid) => {
                qb.push("account_id = ");
                qb.push_bind(acc_id.as_id().expect("Account ID is unresolved"));
                qb.push(" AND imap_uid = ");
                qb.push_bind(imap_uid);
            },
        }
    }
    async fn resolve_inner(&self, db_tx: &mut DBTx<'_>) -> Option<Self> {
        match self {
            MessageKey::ID(id) => Some(self.clone()),
            MessageKey::ACCIDIMAPUID(acc_id, imap_uid) => {
                let acc_id = acc_id.resolve(db_tx).await?;
                Some(MessageKey::ACCIDIMAPUID(acc_id, *imap_uid))
            }
        }
    }
}

#[derive(Debug, Clone, Default, sqlx::FromRow)]
pub struct MessageSQL {
    pub id: Option<KeyWrapper<MessageKey>>,
    pub create_time: Option<i64>,
    pub update_time: Option<i64>,
    pub account_id: Option<KeyWrapper<AccountKey>>, 
    pub flags: Option<JSONB>, 
    pub size: Option<i64>,
    pub internal_date: Option<i64>, 
    pub bodystructure: Option<JSONB>, // Jsonb 
    pub imap_uid: Option<i64>, 
    pub modseq: Option<i64>,
    pub rfc_message_id: Option<String>, 
    pub env_date: Option<String>, 
    pub env_subject: Option<String>, 
    pub env_from: Option<JSONB>, 
    pub env_reply_to: Option<JSONB>, 
    pub env_to: Option<JSONB>,
    pub env_cc: Option<JSONB>,
    pub env_bcc: Option<JSONB>,
    pub env_in_reply_to: Option<String>, 
    pub header_raw: Option<Vec<u8>>, 
    pub body_preview: Option<String>, 
}

impl SQLObj for MessageSQL {
    type Key = MessageKey;
    
    fn get_key(&self) -> &Option<KeyWrapper<MessageKey>> { &self.id }
    fn get_key_mut(&mut self) -> &mut Option<KeyWrapper<MessageKey>> { &mut self.id }
    async fn resolve_foreign(&mut self, db_tx: &mut DBTx<'_>) {
        if self.account_id.is_none() { return; }
        let key = self.account_id.as_ref().unwrap().0.clone();
        let resolved = key.resolve(db_tx).await.expect("Failed to resolve AccountKey");
        self.account_id = Some(KeyWrapper(resolved));
    }
    async fn upsert(&self, db_tx: &mut DBTx<'_>) -> Result<SqliteQueryResult, sqlx::Error> {
        // We do not update the id field, create_time field. Since those must stay constant after creation.
        // Everything else is merely a suggestion to be constant 
        const C: &str = r#"
            update_time = EXCLUDED.update_time,
            account_id = COALESCE(EXCLUDED.account_id, account_id),
            flags = COALESCE(EXCLUDED.flags, flags),
            size = COALESCE(EXCLUDED.size, size),
            internal_date = COALESCE(EXCLUDED.internal_date, internal_date),
            bodystructure = COALESCE(EXCLUDED.bodystructure, bodystructure),
            imap_uid = COALESCE(EXCLUDED.imap_uid, imap_uid),
            modseq = COALESCE(EXCLUDED.modseq, modseq),
            rfc_message_id = COALESCE(EXCLUDED.rfc_message_id, rfc_message_id),
            env_date = COALESCE(EXCLUDED.env_date, env_date),
            env_subject = COALESCE(EXCLUDED.env_subject, env_subject),
            env_from = COALESCE(EXCLUDED.env_from, env_from),
            env_reply_to = COALESCE(EXCLUDED.env_reply_to, env_reply_to),
            env_to = COALESCE(EXCLUDED.env_to, env_to),
            env_cc = COALESCE(EXCLUDED.env_cc, env_cc),
            env_bcc = COALESCE(EXCLUDED.env_bcc, env_bcc),
            env_in_reply_to = COALESCE(EXCLUDED.env_in_reply_to, env_in_reply_to),
            header_raw = COALESCE(EXCLUDED.header_raw, header_raw),
            body_preview = COALESCE(EXCLUDED.body_preview, body_preview)
        "#;

        const Q: &str = constcat::concat!(r#"
            INSERT INTO messages (
                id,
                create_time,
                update_time,
                account_id,
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
            ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
            "#,
            C,
            r#"
            ON CONFLICT (account_id, imap_uid) DO UPDATE SET
            "#,
            C 
        );
        
        let t = unix_timestamp();
        sqlx::query(Q)
            .bind(&self.id)
            .bind(&t) // create_time
            .bind(&t) // update_time
            .bind(&self.account_id)
            .bind(&self.flags)
            .bind(&self.size)
            .bind(&self.internal_date)
            .bind(&self.bodystructure)
            .bind(&self.imap_uid)
            .bind(&self.modseq)
            .bind(&self.rfc_message_id)
            .bind(&self.env_date)
            .bind(&self.env_subject)
            .bind(&self.env_from)
            .bind(&self.env_reply_to)
            .bind(&self.env_to)
            .bind(&self.env_cc)
            .bind(&self.env_bcc)
            .bind(&self.env_in_reply_to)
            .bind(&self.header_raw) 
            .bind(&self.body_preview)
            .execute(&mut **db_tx)
            .await
    }
}

#[derive(Debug, Clone)]
pub enum AccountKey {
    ID(SqliteID),
    LOCALPARTDOMAIN(String, String)
}

impl SQLKey for AccountKey {
    const DB_TABLE: &'static str = "accounts";

    fn condition(&self, qb: &mut sqlx::QueryBuilder<Sqlite>) {
        match self {
            AccountKey::ID(id) => {
                qb.push("id = ");
                qb.push_bind(id);
            },
            AccountKey::LOCALPARTDOMAIN(local_part, domain) => {
                qb.push("local_part = ");
                qb.push_bind(local_part);
                qb.push(" AND domain = ");
                qb.push_bind(domain);
            },
        }
    }
    fn from_id(id: SqliteID) -> Self { AccountKey::ID(id) }
    fn as_id(&self) -> Option<SqliteID> {
        match self {
            AccountKey::ID(id) => Some(*id),
            _ => None,
        }
    }
    async fn resolve_inner(&self, db_tx: &mut DBTx<'_>) -> Option<Self> { Some(self.clone()) }
}

#[derive(Debug, Clone, Default, sqlx::FromRow)]
pub struct AccountSQL {
    pub id: Option<KeyWrapper<AccountKey>>,
    pub create_time: Option<i64>,
    pub update_time: Option<i64>,
    pub local_part: Option<String>,
    pub domain: Option<String>,
    pub fetch_server: Option<String>,
    pub push_server: Option<String>,
}

impl SQLObj for AccountSQL {
    type Key = AccountKey;

    fn get_key(&self) -> &Option<KeyWrapper<AccountKey>> { &self.id }
    fn get_key_mut(&mut self) -> &mut Option<KeyWrapper<AccountKey>> { &mut self.id }
    async fn resolve_foreign(&mut self, db_tx: &mut DBTx<'_>) { /* No foreign keys */ }
    async fn upsert(&self, db_tx: &mut DBTx<'_>) -> Result<SqliteQueryResult, sqlx::Error> {
        const C: &str = r#"
            update_time = EXCLUDED.update_time,
            local_part = COALESCE(EXCLUDED.local_part, local_part),
            domain = COALESCE(EXCLUDED.domain, domain),
            fetch_server = COALESCE(EXCLUDED.fetch_server, fetch_server),
            push_server = COALESCE(EXCLUDED.push_server, push_server)
        "#;
        const Q: &str = constcat::concat!(r#"
            INSERT INTO accounts (
                id, 
                create_time,
                update_time,
                local_part,
                domain,
                fetch_server,
                push_server
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
            "#,
            C,
            r#"
            ON CONFLICT(local_part, domain) DO UPDATE SET
            "#, 
            C,
        );

        let t = unix_timestamp();
        sqlx::query(Q)
            .bind(&self.id) 
            .bind(&t)
            .bind(&t)
            .bind(&self.local_part)
            .bind(&self.domain)
            .bind(&self.fetch_server)
            .bind(&self.push_server)
            .execute(&mut **db_tx)
            .await
    }
}

#[derive(Clone, Debug)]
pub enum MailboxKey {
    ID(SqliteID),
    ACCIDNAME(AccountKey, MailboxName)
}

impl SQLKey for MailboxKey {
    const DB_TABLE: &'static str = "mailboxes";

    fn condition(&self, qb: &mut sqlx::QueryBuilder<Sqlite>) {
        match self {
            MailboxKey::ID(id) => {
                qb.push("id = ");
                qb.push_bind(id);
            },
            MailboxKey::ACCIDNAME(acc, name) => {
                qb.push("account_id = ");
                qb.push_bind(&acc.as_id().expect("Account ID is unresolved"));
                qb.push(" AND name = ");
                qb.push_bind(name);
            }
        }
    }
    fn from_id(id: SqliteID) -> Self {
        MailboxKey::ID(id)
    }
    fn as_id(&self) -> Option<SqliteID> {
        match self {
            MailboxKey::ID(id) => Some(*id),
            MailboxKey::ACCIDNAME(_, _) => None,
        }
    }
    async fn resolve_inner(&self, db_tx: &mut DBTx<'_>) -> Option<Self> {
        match self {
            MailboxKey::ID(_) => Some(self.clone()),
            MailboxKey::ACCIDNAME(account_key, mb_name) => {
                let account_key = account_key.resolve(db_tx).await?;
                Some(MailboxKey::ACCIDNAME(account_key, mb_name.clone()))
            },
        }
        
    }
}

#[derive(Debug, Clone, Default, sqlx::FromRow)]
pub struct MailboxSQL {
    pub id: Option<KeyWrapper<MailboxKey>>,
    pub create_time: Option<i64>,
    pub update_time: Option<i64>,
    pub account_id: Option<KeyWrapper<AccountKey>>,
    pub name: Option<String>,
    pub mail_count: Option<i64>,
    pub recent: Option<i64>,
    pub unseen: Option<i64>,
    pub attrs: Option<JSONB>,
    pub highest_modseq: Option<i64>,
    pub uid_next: Option<i64>,
    pub uid_validity: Option<i64>,
}

impl SQLObj for MailboxSQL {
    type Key = MailboxKey;

    fn get_key(&self) -> &Option<KeyWrapper<MailboxKey>> { &self.id }
    fn get_key_mut(&mut self) -> &mut Option<KeyWrapper<MailboxKey>> { &mut self.id }
    async fn resolve_foreign(&mut self, db_tx: &mut DBTx<'_>) {
        if self.account_id.is_none() { return; }
        let account_id = self.account_id.as_ref().unwrap().0.clone();
        let resolved = account_id.resolve(db_tx).await.expect("Failed to resolve AccountKey");
        self.account_id = Some(KeyWrapper(resolved));
    }
    async fn upsert(&self, db_tx: &mut DBTx<'_>) -> Result<SqliteQueryResult, sqlx::Error> {
        const C: &str = r#"
            update_time = EXCLUDED.update_time,
            account_id = COALESCE(EXCLUDED.account_id, account_id),
            name = COALESCE(EXCLUDED.name, name),
            mail_count = COALESCE(EXCLUDED.mail_count, mail_count),
            recent = COALESCE(EXCLUDED.recent, recent),
            unseen = COALESCE(EXCLUDED.unseen, unseen),
            attrs = COALESCE(EXCLUDED.attrs, attrs),
            highest_modseq = COALESCE(EXCLUDED.highest_modseq, highest_modseq),
            uid_next = COALESCE(EXCLUDED.uid_next, uid_next),
            uid_validity = COALESCE(EXCLUDED.uid_validity, uid_validity)
        "#;
        const Q: &str = constcat::concat!(r#"
            INSERT INTO mailboxes (
                id,
                create_time,
                update_time,
                account_id,
                name,
                mail_count,
                recent,
                unseen,
                attrs,
                highest_modseq,
                uid_next,
                uid_validity
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
            "#,
            C,
            r#"
            ON CONFLICT (account_id, name) DO UPDATE SET
            "#,
            C
        );
        let t = unix_timestamp();
        sqlx::query(Q)
            .bind(&self.id)
            .bind(&t)
            .bind(&t)
            .bind(&self.account_id)
            .bind(&self.name)
            .bind(&self.mail_count)
            .bind(&self.recent)
            .bind(&self.unseen)
            .bind(&self.attrs)
            .bind(&self.highest_modseq)
            .bind(&self.uid_next)
            .bind(&self.uid_validity)
            .execute(&mut **db_tx)
            .await
    }
}

#[derive(Debug, Clone)]
pub enum MessagePartKey {
    ID(SqliteID),
    MSGIDPARTSPEC(MessageKey, String)
}

impl SQLKey for MessagePartKey {
    const DB_TABLE: &'static str = "message_parts";
    fn condition(&self, qb: &mut sqlx::QueryBuilder<Sqlite>) {
        match self {
            MessagePartKey::ID(id) => {
                qb.push("id = ");
                qb.push_bind(id);
            }
            MessagePartKey::MSGIDPARTSPEC(msg_key, part_spec) => {
                qb.push("message_id = ");
                qb.push_bind(msg_key.as_id().expect("MessageKey is unresolved"));
                qb.push(" AND part_spec = ");
                qb.push_bind(part_spec);
            }
        }
    }
    fn from_id(id: SqliteID) -> Self { MessagePartKey::ID(id) }
    fn as_id(&self) -> Option<SqliteID> {
        match self {
            MessagePartKey::ID(id) => Some(*id),
            MessagePartKey::MSGIDPARTSPEC(msg_key, _) => msg_key.as_id(),
        }
    }
    async fn resolve_inner(&self, db_tx: &mut DBTx<'_>) -> Option<Self> {
        match self {
            MessagePartKey::ID(id) => Some(self.clone()),
            MessagePartKey::MSGIDPARTSPEC(msg_key, part_spec) => {
                let msg_key = msg_key.resolve(db_tx).await?;
                Some(MessagePartKey::MSGIDPARTSPEC(msg_key, part_spec.clone()))
            },
        }
    }
}

#[derive(Debug, Clone, Default, sqlx::FromRow)]
pub struct MessagePartSQL {
    pub id: Option<KeyWrapper<MessagePartKey>>,
    pub create_time: Option<i64>,
    pub update_time: Option<i64>,
    pub message_id: Option<KeyWrapper<MessageKey>>,
    pub part_spec: Option<String>,
    pub data: Option<Vec<u8>>
}

impl SQLObj for MessagePartSQL {
    type Key = MessagePartKey;

    fn get_key(&self) -> &Option<KeyWrapper<MessagePartKey>> { &self.id }
    fn get_key_mut(&mut self) -> &mut Option<KeyWrapper<MessagePartKey>> { &mut self.id }
    async fn resolve_foreign(&mut self, db_tx: &mut DBTx<'_>) {
        if self.message_id.is_none() { return; }
        let message_id = self.message_id.as_ref().unwrap().0.clone();
        let resolved = message_id.resolve(db_tx).await.expect("Failed to resolve MessageID");
        self.message_id = Some(KeyWrapper(resolved));
    }
    async fn upsert(&self, db_tx: &mut DBTx<'_>) -> Result<SqliteQueryResult, sqlx::Error> {
        const C: &str = r#"
            update_time = EXCLUDED.update_time,
            message_id = COALESCE(EXCLUDED.message_id, message_id),
            part_spec = COALESCE(EXCLUDED.part_spec, part_spec),
            data = COALESCE(EXCLUDED.data, data)
        "#;
        const Q: &str = constcat::concat!(r#"
            INSERT INTO message_parts (
                id,
                create_time,
                update_time,
                message_id, 
                part_spec,
                data
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
            "#,
            C,
            r#"
            ON CONFLICT (message_id, part_spec) DO UPDATE SET
            "#,
            C
        );
        let t = unix_timestamp();
        sqlx::query(Q)
            .bind(&self.id)
            .bind(&t)
            .bind(&t)
            .bind(&self.message_id)
            .bind(&self.part_spec)
            .bind(&self.data)
            .execute(&mut **db_tx)
            .await
    }
    
}