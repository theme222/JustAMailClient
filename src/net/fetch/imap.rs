use std::error::Error;
use std::fmt;

use async_imap::extensions::idle::IdleResponse::{ManualInterrupt, NewData, Timeout};
use async_imap::types::{Seq, UnsolicitedResponse};
use async_native_tls::TlsStream;
use futures::{Stream, StreamExt, TryStreamExt};
use imap_proto::Response::MailboxData;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use crate::models::Status::FAILED;
use crate::net::fetch::imap::ResponseCode::{LIMIT, UNAVAILABLE};
use crate::srv::SrvAction::SYNCMAILBOX;
use crate::*;
use crate::{models::*, srv::SrvMessage};
use bitflags::bitflags;

use async_imap::error::Error as AsyncImapError;
use async_imap::error::Result as AsyncImapResult;

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum ResponseCode {
    ALERT,
    ALREADYEXISTS,
    APPENDUID,
    AUTHENTICATIONFAILED,
    AUTHORIZATIONFAILED,
    BADCHARSET,
    CANNOT,
    CAPABILITY,
    CLIENTBUG,
    CLOSED,
    CONTACTADMIN,
    COPYUID,
    CORRUPTION,
    EXPIRED,
    EXPUNGEISSUED,
    HASCHILDREN,
    INUSE,
    LIMIT,
    NONEXISTENT,
    NOPERM,
    OVERQUOTA,
    PARSE,
    PERMANENTFLAGS,
    PRIVACYREQUIRED,
    READONLY,
    READWRITE,
    SERVERBUG,
    TRYCREATE,
    UIDNEXT,
    UIDNOTSTICKY,
    UIDVALIDITY,
    UNAVAILABLE,
    UNKNOWNCTE,
    UNKNOWN,
}

impl From<&str> for ResponseCode {
    fn from(value: &str) -> Self {
        use ResponseCode::*;

        // 1. Extract the potential token out of raw IMAP strings (e.g., "* NO [AUTHENTICATIONFAILED] ...")
        // This strips brackets or splits words so we can do an exact match.
        let token = value
            .trim_matches(|c| c == '[' || c == ']' || c == '*' || c == ' ')
            .split_whitespace()
            .next()
            .unwrap_or("");

        // 2. Look it up instantly in the compile-time perfect hash map
        static MAP: phf::Map<&'static str, ResponseCode> = phf::phf_map! {
            "ALERT" => ALERT,
            "ALREADYEXISTS" => ALREADYEXISTS,
            "APPENDUID" => APPENDUID,
            "AUTHENTICATIONFAILED" => AUTHENTICATIONFAILED,
            "AUTHORIZATIONFAILED" => AUTHORIZATIONFAILED,
            "BADCHARSET" => BADCHARSET,
            "CANNOT" => CANNOT,
            "CAPABILITY" => CAPABILITY,
            "CLIENTBUG" => CLIENTBUG,
            "CLOSED" => CLOSED,
            "CONTACTADMIN" => CONTACTADMIN,
            "COPYUID" => COPYUID,
            "CORRUPTION" => CORRUPTION,
            "EXPIRED" => EXPIRED,
            "EXPUNGEISSUED" => EXPUNGEISSUED,
            "HASCHILDREN" => HASCHILDREN,
            "INUSE" => INUSE,
            "LIMIT" => LIMIT,
            "NONEXISTENT" => NONEXISTENT,
            "NOPERM" => NOPERM,
            "OVERQUOTA" => OVERQUOTA,
            "PARSE" => PARSE,
            "PERMANENTFLAGS" => PERMANENTFLAGS,
            "PRIVACYREQUIRED" => PRIVACYREQUIRED,
            "READONLY" => READONLY,
            "READWRITE" => READWRITE,
            "SERVERBUG" => SERVERBUG,
            "TRYCREATE" => TRYCREATE,
            "UIDNEXT" => UIDNEXT,
            "UIDNOTSTICKY" => UIDNOTSTICKY,
            "UIDVALIDITY" => UIDVALIDITY,
            "UNAVAILABLE" => UNAVAILABLE,
            "UNKNOWNCTE" => UNKNOWNCTE,
        };

        MAP.get(token).cloned().unwrap_or(UNKNOWN)
    }
}

pub fn address_to_string(adr: &imap_proto::types::Address) -> Option<String> {
    let name = adr
        .name
        .as_deref()
        .and_then(|s| rfc2047_decoder::decode(s.to_owned()).ok())
        .unwrap_or_default();
    let local_part = adr
        .mailbox
        .as_deref()
        .and_then(|s| rfc2047_decoder::decode(s.to_owned()).ok())
        .unwrap_or_default();
    let domain = adr
        .host
        .as_deref()
        .and_then(|s| rfc2047_decoder::decode(s.to_owned()).ok())
        .unwrap_or_default();

    if local_part.is_empty() || domain.is_empty() {
        None
    } else if name.is_empty() {
        Some(format!("{}@{}", local_part, domain))
    } else {
        Some(format!("{} <{}@{}>", name, local_part, domain))
    }
}

// type Mailbox = async_imap::types::Mailbox;
type StreamResult<'a, T> =
    std::pin::Pin<Box<dyn Stream<Item = Result<T, async_imap::error::Error>> + 'a + Send>>;

#[derive(Debug, Clone)]
pub enum SeqRange {
    // Zero Indexed. Negative values count from the end of the mailbox
    Range { start: i32, end: i32 },
    Single(i32),
    Combo { vec: Vec<SeqRange> },
}

impl SeqRange {
    pub fn first() -> Self {
        Self::Range { start: 0, end: 0 }
    }

    pub fn last() -> Self {
        Self::Range { start: -1, end: -1 }
    }

    pub fn all() -> Self {
        Self::Range { start: 0, end: -1 }
    }

    pub fn sequence_set_str(&self, mailbox_size: u32) -> String {
        match self {
            SeqRange::Range { start, end } => {
                let start = *start;
                let end = *end;
                let size = mailbox_size as i32;
                let start = if start < 0 {
                    size + start + 1
                } else {
                    start + 1
                };
                let start = start.clamp(1, size);
                let end = if end < 0 { size + end + 1 } else { end + 1 };
                let end = end.clamp(1, size);
                format!("{}:{}", start, end)
            }
            SeqRange::Single(val) => val.to_string(),
            SeqRange::Combo { vec } => vec
                .iter()
                .map(|r| r.sequence_set_str(mailbox_size))
                .collect::<Vec<_>>()
                .join(","),
        }
    }

    pub fn get_total_items(&self, mailbox_size: u32) -> u32 {
        match self {
            SeqRange::Range { start, end } => {
                let size = mailbox_size as i32;
                let start = if *start < 0 {
                    size + *start + 1
                } else {
                    *start + 1
                };
                let start = start.clamp(1, size);
                let end = if *end < 0 { size + *end + 1 } else { *end + 1 };
                let end = end.clamp(1, size);
                (end - start + 1) as u32
            }
            SeqRange::Single(_) => 1,
            SeqRange::Combo { vec } => vec.iter().map(|r| r.get_total_items(mailbox_size)).sum(),
        }
    }
}


impl<'a> From<async_imap::types::Flag<'a>> for MailFlag {
    fn from(flag: async_imap::types::Flag<'a>) -> Self {
        match flag {
            async_imap::types::Flag::Seen => MailFlag::SEEN,
            async_imap::types::Flag::Answered => MailFlag::ANSWERED,
            async_imap::types::Flag::Flagged => MailFlag::FLAGGED,
            async_imap::types::Flag::Deleted => MailFlag::DELETED,
            async_imap::types::Flag::Draft => MailFlag::DRAFT,
            async_imap::types::Flag::Recent => MailFlag::RECENT,
            async_imap::types::Flag::MayCreate => MailFlag::MAYCREATE,
            async_imap::types::Flag::Custom(custom) => {
                MailFlag::CUSTOM(custom.clone().into_owned())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchType {
    // Ignoring types that are equivalent / older with no use case
    UID,
    BODYSTRUCTURE,
    ENVELOPE,
    FLAGS,
    INTERNALDATE,
    RFC822SIZE,
    BODYPEEKSECTION(String, String),
    BODYSECTION(String, String),
}

impl FetchType {
    pub fn fetch_string(fetch_type: &Vec<FetchType>) -> String {
        let mut result_str = String::new();

        for ft in fetch_type {
            if !result_str.is_empty() {
                result_str.push_str(" ");
            }

            match ft {
                FetchType::UID => result_str.push_str("UID"),
                FetchType::BODYSTRUCTURE => result_str.push_str("BODYSTRUCTURE"),
                FetchType::ENVELOPE => result_str.push_str("ENVELOPE"),
                FetchType::FLAGS => result_str.push_str("FLAGS"),
                FetchType::INTERNALDATE => result_str.push_str("INTERNALDATE"),
                FetchType::RFC822SIZE => result_str.push_str("RFC822.SIZE"),
                FetchType::BODYPEEKSECTION(section, partial) => {
                    if partial.len() == 0 {
                        result_str.push_str(&format!("BODY.PEEK[{}]", section))
                    } else {
                        result_str.push_str(&format!("BODY.PEEK[{}]<{}>", section, partial))
                    }
                }
                FetchType::BODYSECTION(section, partial) => {
                    if partial.len() == 0 {
                        result_str.push_str(&format!("BODY[{}]", section))
                    } else {
                        result_str.push_str(&format!("BODY[{}]<{}>", section, partial))
                    }
                }
            }
        }

        if fetch_type.len() > 1 {
            result_str = format!("({})", result_str);
        }
        return result_str;
    }
}

pub struct EmailAccount {
    pub is_init: bool,
    pub mailboxes: Vec<async_imap::types::Mailbox>,
}

#[derive(Debug, Clone)]
pub struct Mailbox { // Intermediary type
    pub name: String,
    pub exists: u32,
    pub recent: u32,
    pub unseen: Option<u32>,
    pub uid_next: Option<u32>,
    pub uid_validity: Option<u32>,
    pub highest_modseq: Option<u64>,
}

impl From<&async_imap::types::Mailbox> for Mailbox {
    fn from(mailbox: &async_imap::types::Mailbox) -> Self {
        Mailbox {
            name: String::new(),
            exists: mailbox.exists,
            recent: mailbox.recent,
            unseen: mailbox.unseen,
            uid_next: mailbox.uid_next,
            uid_validity: mailbox.uid_validity,
            highest_modseq: mailbox.highest_modseq,
        }
    }
}

impl From<&Mailbox> for db::MailboxSQL {
    fn from(value: &Mailbox) -> Self {
        db::MailboxSQL {
            id: None,
            create_time: None,
            update_time: None,
            account_id: None,
            name: Some(value.name.clone()),
            attrs: None,
            mail_count: Some(value.exists as i64),
            recent: Some(value.recent as i64),
            unseen: value.unseen.map(|v| v as i64),
            uid_next: value.uid_next.map(|v| v as i64),
            uid_validity: value.uid_validity.map(|v| v as i64),
            highest_modseq: value.highest_modseq.map(|v| v as i64),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Message { // Intermediary type
    pub flags: Vec<MailFlag>, // It is impossible to distinguish whether the original fetch query contained "FLAGS" as part of the request and thus we can't determine None vs empty
    pub size: Option<u32>,
    pub internal_date: Option<i64>, 
    pub bodystructure: Option<MailBodyStructure>, // Jsonb 
    pub imap_uid: Option<u32>, 
    pub modseq: Option<u64>,
    pub rfc_message_id: Option<String>, 
    pub env_date: Option<String>, 
    pub env_subject: Option<String>, 
    pub env_from: Option<Vec<String>>, 
    pub env_reply_to: Option<Vec<String>>, 
    pub env_to: Option<Vec<String>>,
    pub env_cc: Option<Vec<String>>,
    pub env_bcc: Option<Vec<String>>,
    pub env_in_reply_to: Option<String>, 
    pub header_raw: Option<Vec<u8>>, 
    pub body_preview: Option<String>,
}

impl From<&async_imap::types::Fetch> for Message {
    fn from(mail: &async_imap::types::Fetch) -> Self {
        let mut msg = Message::default(); 
        let internal_date = mail.internal_date().map(|d| d.timestamp_millis());
        let body_bytes = mail.text().unwrap_or_default();
        let body_raw = Some(body_bytes.to_vec());
        let env = mail.envelope();

        msg.internal_date = mail.internal_date().map(|d| d.timestamp_millis());
        msg.bodystructure = mail.bodystructure().map(|b| b.into());
        msg.imap_uid = mail.uid;
        msg.body_preview = msg.bodystructure.as_ref().and_then(|bs| structure::get_preview_from_partial(body_bytes));
        msg.size = mail.size;
        msg.flags = mail
            .flags()
            .collect::<Vec<_>>()
            .into_iter()
            .map(|f| f.into())
            .collect::<Vec<MailFlag>>();
        msg.modseq = mail.modseq;
        
        let deref_then_decode = |o: &Option<std::borrow::Cow<[u8]>>| 
            o.as_deref().and_then(
                |m| rfc2047_decoder::decode(m).ok()
            );
        
        msg.rfc_message_id = env.and_then(|e| deref_then_decode(&e.message_id));
        msg.env_date = env.and_then(|e| deref_then_decode(&e.date));
        msg.env_subject = env.and_then(|e| deref_then_decode(&e.subject));
        msg.env_in_reply_to = env.and_then(|e| deref_then_decode(&e.in_reply_to));
        
        let deref_then_decode_address = |o: &Option<Vec<imap_proto::types::Address>>| 
            o.as_deref().map(
                |v| v.into_iter()
                    .map(|f| address_to_string(&f))
                    .flatten()
                    .collect::<Vec<String>>()
            );
        
        msg.env_from = env.and_then(|e| deref_then_decode_address(&e.from));
        msg.env_reply_to = env.and_then(|e| deref_then_decode_address(&e.reply_to));
        msg.env_to = env.and_then(|e| deref_then_decode_address(&e.to));
        msg.env_cc = env.and_then(|e| deref_then_decode_address(&e.cc));
        msg.env_bcc = env.and_then(|e| deref_then_decode_address(&e.bcc));

        msg
    }
}

impl From<&Message> for db::MessageSQL {
    fn from(from_msg: &Message) -> Self {
        let mut msg = db::MessageSQL::default();

        msg.flags = serde_sqlite_jsonb::to_vec(&from_msg.flags).ok();
        msg.size = from_msg.size.map(|s| s as i64);
        msg.internal_date = from_msg.internal_date;
        msg.bodystructure = from_msg.bodystructure.as_ref().and_then(|bs| serde_sqlite_jsonb::to_vec(bs).ok());
        msg.modseq = from_msg.modseq.map(|s| s as i64);
        msg.imap_uid = from_msg.imap_uid.map(|u| u as i64);
        msg.rfc_message_id = from_msg.rfc_message_id.clone();
        msg.env_date = from_msg.env_date.clone();
        msg.env_subject = from_msg.env_subject.clone();
        msg.env_from = from_msg.env_from.as_ref().and_then(|x| serde_sqlite_jsonb::to_vec(x).ok());
        msg.env_reply_to = from_msg.env_reply_to.as_ref().and_then(|x| serde_sqlite_jsonb::to_vec(x).ok());
        msg.env_to = from_msg.env_to.as_ref().and_then(|x| serde_sqlite_jsonb::to_vec(x).ok());
        msg.env_cc = from_msg.env_cc.as_ref().and_then(|x| serde_sqlite_jsonb::to_vec(x).ok());
        msg.env_bcc = from_msg.env_bcc.as_ref().and_then(|x| serde_sqlite_jsonb::to_vec(x).ok());
        msg.env_in_reply_to = from_msg.env_in_reply_to.clone();
        msg.body_preview = from_msg.body_preview.clone();

        msg
    }
}

#[derive(Debug, Clone)]
pub enum ImapSessionCommandType {
    SELECT(MailboxName), // -> MailboxSQL
    LISTFETCH(MailboxName, SeqRange), // -> MessageSQL
    SECTIONFETCH(MailboxName, SeqRange, Vec<MailBodyStructure>), // -> MessageSQL 
    FULLFETCH(MailboxName, SeqRange, Option<u64> /* msg size */), // -> MessageSQL
    NOOPON(MailboxName), // -> Nothing
    IDLE(MailboxName), // -> Doesn't resolve with success 
    /* Internal use */
    NOOP, // -> Nothing
    /* Internal use */
    SHUTDOWN, // -> Doesn't resolve
    // TODO: insert more here
}

impl ImapSessionCommandType {
    pub fn get_required_mailbox(&self) -> Option<&MailboxName> {
        match self {
            ImapSessionCommandType::LISTFETCH(mailbox, _) => Some(mailbox),
            ImapSessionCommandType::IDLE(mailbox) => Some(mailbox),
            ImapSessionCommandType::SECTIONFETCH(mailbox, _, _) => Some(mailbox),
            ImapSessionCommandType::FULLFETCH(mailbox, _, _) => Some(mailbox),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ImapSessionCommand {
    pub id: u64,
    pub ty: ImapSessionCommandType,
    pub resolve: ResolveID,
}


impl ImapSessionCommand {
    pub fn weight(&self) -> f64 {
        use ImapSessionCommandType::*;

        // Get the expected weight (in units of multiples of 1 rtt)
        match &self.ty {
            LISTFETCH(_, range) => {
                get_download_time(
                    range.get_total_items(MAILBOX_INBOX_ASSUMED_SIZE) as u64
                        * MAX_LISTFETCH_SIZE as u64,
                    ASSUMED_DOWNLOAD_SPEED as f64,
                ) / ASSUMED_LATENCY as f64
            } // Educated guess.
            IDLE(_) => NETSOCK_REFRESH_INTERVAL.as_secs_f64() / ASSUMED_LATENCY as f64 * 1000.0,
            NOOP => 1.0,
            NOOPON(_) => 1.0,
            SHUTDOWN => 1.0,
            SELECT(_) => 1.0,
            SECTIONFETCH(_, range, vec_bs) => {
                let size = vec_bs.iter().fold(0, |acc, bs| acc + bs.get_total_size()) as u64;
                get_download_time(
                    range.get_total_items(MAILBOX_INBOX_ASSUMED_SIZE) as u64 * size,
                    ASSUMED_DOWNLOAD_SPEED as f64,
                ) / ASSUMED_LATENCY as f64
            }
            FULLFETCH(_, range, size) => {
                let size = size.unwrap_or(MAX_PREFETCH_STRAT1_SIZE as u64);
                get_download_time(
                    range.get_total_items(MAILBOX_INBOX_ASSUMED_SIZE) as u64 * size,
                    ASSUMED_DOWNLOAD_SPEED as f64,
                ) / ASSUMED_LATENCY as f64
            }
        }
    }

    pub fn get_mailbox_domain(&self) -> Option<&str> {
        // Get the mailbox the command works on
        use ImapSessionCommandType::*;

        match &self.ty {
            LISTFETCH(mb, _) => Some(mb),
            IDLE(mb) => Some(mb),
            NOOP => None,
            NOOPON(mb) => Some(mb),
            SHUTDOWN => None,
            SELECT(mb) => Some(mb),
            SECTIONFETCH(mb, _, _) => Some(mb),
            FULLFETCH(mb, _, _) => Some(mb),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub struct ImapSessionId {
    pub s_id: u64,
    pub m_id: CredentialID,
}

type Aborter = tokio::sync::mpsc::Receiver<()>;
type ImapSessionResult<T> = std::result::Result<T, ImapSessionError>;

pub struct ImapSession {
    pub id: ImapSessionId,
    pub net: Option<async_imap::Session<Compat<TlsStream<TcpStream>>>>,
    pub current_mailbox: Option<Mailbox>,
    pub receiver: tokio::sync::mpsc::Receiver<ImapSessionCommand>,
    pub abort: Aborter, // Instantly kill the current command and shutdown the session
}

pub type ImapSessionSuccess = Attempt;

#[derive(Debug)]
pub enum ImapSessionError {
    ASYNCIMAPERROR(AsyncImapError),    // Async imap / imap proto lib errors
    TLSERROR(async_native_tls::Error), // Encryption errors
    AUTHENTICATIONERROR,               // Invalid auth arguments
    ABORTED,                           // The current command was aborted with abort.send(())
    TIMEOUT,                           // The current command timed out
    INVALIDRESPONSE(String),           // When the server wants to be naughty and sends us some bs
}

impl std::fmt::Display for ImapSessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use ImapSessionError::*;
        write!(f,
            "ImapSessionError: {}",
            match &self {
                ASYNCIMAPERROR(err) =>  format!("AsyncImapError {err}"),
                TLSERROR(error) => format!("TLSError {error}"),
                ABORTED => format!("Session aborted"),
                TIMEOUT => format!("Session timed out"),
                INVALIDRESPONSE(err) => format!("Received invalid response {err}"),
                AUTHENTICATIONERROR => format!("Authentication error"),
            }
        )
    }
}

impl Error for ImapSessionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None // TODO: uhhhhhhhhhhhhhhh
    }
}

impl From<std::io::Error> for ImapSessionError {
    fn from(e: std::io::Error) -> Self {
        let a: AsyncImapError = e.into();
        a.into()
    }
}

impl From<AsyncImapError> for ImapSessionError {
    fn from(e: AsyncImapError) -> Self {
        ImapSessionError::ASYNCIMAPERROR(e)
    }
}

impl From<async_native_tls::Error> for ImapSessionError {
    fn from(e: async_native_tls::Error) -> Self {
        ImapSessionError::TLSERROR(e)
    }
}

impl ImapSession {
    // Imap session itself doesn't handle retries. The retry logic is handled by the manager.

    async fn get_client(
        cred: CredentialID,
    ) -> std::result::Result<(async_imap::Session<Compat<TlsStream<TcpStream>>>, Option<async_imap::types::Capabilities>), ImapSessionError>
    {
        let cred = CredentialStore::get(cred);

        // Assert right now because I have no other support for anything else
        assert!(cred.auth_method == AuthMethod::LOGIN);
        assert!(cred.encryption_method == EncryptionMethod::SSLTLS);

        let imap_addr = (cred.fetch_server.clone(), cred.fetch_port);
        let tcp_stream = TcpStream::connect(&imap_addr).await?;
        let tls = async_native_tls::TlsConnector::new();
        let tls_stream = tls
            .connect(cred.fetch_server.clone(), tcp_stream)
            .await?
            .compat();

        let mut client = async_imap::Client::new(tls_stream);
        Ok(client
            .login_with_capabilities(&cred.login, &cred.secret)
            .await
            .map_err(|e| e.0)?)
    }

    pub async fn new(
        id: ImapSessionId,
    ) -> ImapSessionResult<
        (
            tokio::sync::mpsc::Sender<ImapSessionCommand>,
            tokio::sync::mpsc::Sender<()>,
        )
    > {
        let (sender, receiver) = tokio::sync::mpsc::channel::<ImapSessionCommand>(100);
        let (abort_sender, abort_recv) = tokio::sync::mpsc::channel::<()>(5);
        let session = ImapSession {
            id: id.clone(),
            net: Some(Self::get_client(id.m_id).await?.0),
            current_mailbox: None,
            receiver: receiver,
            abort: abort_recv,
        };
        tokio::spawn(session.run());
        Ok((sender, abort_sender))
    }

    pub async fn run(mut self) {
        use FetchType::*;
        use ImapSessionCommandType::*;
        use NetAction::*;
        use SessionUpdate::*;

        Senders::net(NetMessage {
            action: IMAPUPDATE {
                cred_id: self.id.m_id,
                update: STARTED(self.id.clone()),
            },
            resolve: NULL_RESOLVE_ID,
        })
        .await;

        while let Some(command) = tokio::select!(
            command = self.receiver.recv() => command,
            command = wait_with_jitter(NETSOCK_REFRESH_INTERVAL) => Some(ImapSessionCommand { id: u64::MAX, ty: NOOP, resolve: NULL_RESOLVE_ID }),
            command = self.abort.recv() => Some(ImapSessionCommand { id: u64::MAX, ty: ImapSessionCommandType::SHUTDOWN, resolve: NULL_RESOLVE_ID }),
        ) {
            println!("Imap Session running: {:?}", command);
            
            let res_id = command.resolve;
            // We will only resolve if the command succeeds or has an unrecoverable failure (so not retries)
            let res = match &command.ty {
                NOOP => self.noop(res_id).await,
                IDLE(mb) => self.idle(mb.into(), res_id).await,
                SELECT(mb) => self.select(res_id, &mb, true).await,
                NOOPON(mb) => self.noopon(res_id, &mb).await,
                ImapSessionCommandType::SHUTDOWN => {
                    ResolveStore::resolve(res_id, Resolution::Nothing);
                    break;
                },
                ImapSessionCommandType::LISTFETCH(mb, seq_range) => {
                    self.fetch(
                        res_id,
                        mb.into(),
                        &seq_range,
                        &vec![
                            UID,
                            BODYSTRUCTURE,
                            ENVELOPE,
                            FLAGS,
                            INTERNALDATE,
                            RFC822SIZE,
                            BODYPEEKSECTION("TEXT".into(), format!("0.{}", MAX_LISTFETCH_SIZE)),
                        ],
                        command.ty.clone(),
                    )
                    .await
                }
                SECTIONFETCH(mb, seq_range, mail_body_structures) => {
                    let fetch_types: Vec<FetchType> = vec![
                        UID,
                        BODYSTRUCTURE,
                        ENVELOPE,
                        FLAGS,
                        INTERNALDATE,
                        RFC822SIZE,
                        BODYPEEKSECTION("HEADER".into(), "".into()),
                    ];

                    let fetch_body_sections: Vec<FetchType> = mail_body_structures
                        .iter()
                        .map(|mb| BODYPEEKSECTION(mb.part_spec_str(), "".into()))
                        .collect();

                    self.fetch(
                        res_id,
                        mb.into(),
                        &seq_range,
                        &fetch_types.into_iter().chain(fetch_body_sections).collect(),
                        command.ty.clone(),
                    )
                    .await
                }
                FULLFETCH(mb, seq_range, _) => {
                    self.fetch(
                        res_id,
                        mb.into(),
                        &seq_range,
                        &vec![
                            UID,
                            BODYSTRUCTURE,
                            ENVELOPE,
                            FLAGS,
                            INTERNALDATE,
                            RFC822SIZE,
                            BODYPEEKSECTION("HEADER".into(), "".into()),
                            BODYPEEKSECTION("TEXT".into(), "".into()),
                        ],
                        command.ty.clone(),
                    )
                    .await
                }
                LISTFETCH(_, seq_range) => todo!(),
            };
            

            if let Ok(success) = res {
                if command.id == u64::MAX { }
                else if success == ImapSessionSuccess::ONCE {
                    Senders::net(NetMessage {
                        action: IMAPUPDATE {
                            update: CMDSUCCESS(self.id.clone(), command.id),
                            cred_id: self.id.m_id
                        },
                        resolve: NULL_RESOLVE_ID // Do not inherit cause we will have already resolved it
                    })
                    .await;
                }
                else if success == ImapSessionSuccess::REPEAT {
                    Senders::net(NetMessage {
                        action: IMAPUPDATE {
                            update: CMDSUCCESSRETRY(self.id.clone(), command.id),
                            cred_id: self.id.m_id
                        },
                        resolve: res_id // Inherit resolve id
                    })
                    .await;
                }
            }
            else if let Err(err) = res {
                let net_exists = self.net.is_some();
                eprintln!("Err while executing command {:?}: {:?}", command.ty, err);
                use AsyncImapError::*;
                use ImapSessionError::*;
                let session_update = match &err {
                    ASYNCIMAPERROR(Io(error)) => {
                        CMDFAILURETRYAGAIN(self.id.clone(), command.id, net_exists)
                    }
                    ASYNCIMAPERROR(Bad(_)) => {
                        CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id, net_exists)
                    }
                    ASYNCIMAPERROR(No(_)) => {
                        CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id, net_exists)
                    }
                    ASYNCIMAPERROR(ConnectionLost) => {
                        CMDFAILURETRYAGAIN(self.id.clone(), command.id, net_exists)
                    }
                    ASYNCIMAPERROR(Parse(parse_error)) => {
                        CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id, net_exists)
                    }
                    ASYNCIMAPERROR(Validate(validate_error)) => {
                        CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id, net_exists)
                    }
                    ASYNCIMAPERROR(Append) => {
                        CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id, net_exists)
                    }
                    ASYNCIMAPERROR(_) => {
                        CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id, net_exists)
                    }
                    TIMEOUT => CMDFAILURETRYAGAIN(self.id.clone(), command.id, net_exists),
                    INVALIDRESPONSE(_) => {
                        CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id, net_exists)
                    }
                    ABORTED => {
                        self.net = None;
                        CMDFAILURETRYAGAIN(self.id.clone(), command.id, net_exists)
                    }
                    TLSERROR(error) => unreachable!(), // Probably idk
                    AUTHENTICATIONERROR => unreachable!(), // I better hope so
                };

                if let CMDFAILURETRYAGAIN(_, _, _) = session_update {
                    Senders::net(NetMessage {
                        action: IMAPUPDATE {
                            update: session_update,
                            cred_id: self.id.m_id,
                        },
                        resolve: res_id, // Inherit res_id
                    }).await;
                }
                else if let CMDFAILUREUNRECOVERABLE(_, _, _) = session_update {
                    Senders::net(NetMessage {
                        action: IMAPUPDATE {
                            update: session_update,
                            cred_id: self.id.m_id,
                        },
                        resolve: NULL_RESOLVE_ID, // We will resolve this right now
                    }).await;
                    ResolveStore::fail(res_id, err.into());
                }


                // Check to see if we still own the network
                if self.net.is_none() {
                    break;
                }
            }

            while !self.net.as_mut().unwrap().unsolicited_responses.is_empty() {
                let Ok(us) = self.net.as_mut().unwrap().unsolicited_responses.recv().await else { break; };
                println!("Received unsolicted response {:?}", us);
            }

        }

        self.net.take(); // force drop the network either way
        // This notifies the shutdown of the session (usually from aborts or the sender of the session gets dropped)
        Senders::net(NetMessage {
            action: IMAPUPDATE {
                update: SESSIONABORT(self.id.clone()),
                cred_id: self.id.m_id,
            },
            resolve: NULL_RESOLVE_ID
        })
        .await;
    }

    fn get_net(&mut self) -> &mut async_imap::Session<Compat<TlsStream<TcpStream>>> {
        self.net.as_mut().unwrap()
    }

    fn take_net(&mut self) -> async_imap::Session<Compat<TlsStream<TcpStream>>> {
        self.net.take().unwrap()
    }

    fn return_net(&mut self, net: async_imap::Session<Compat<TlsStream<TcpStream>>>) {
        self.net = Some(net);
    }

    fn net_abort(
        &mut self,
    ) -> (
        &mut async_imap::Session<Compat<TlsStream<TcpStream>>>,
        &mut tokio::sync::mpsc::Receiver<()>,
    ) {
        // If its stupid but it works, is it really stupid?
        let net = self.net.as_mut().unwrap();
        let abort = &mut self.abort;
        (net, abort)
    }

    pub async fn call_with_abort<F: Future<Output = AsyncImapResult<T>>, T>(
        abort: &mut tokio::sync::mpsc::Receiver<()>,
        future: F,
    ) -> ImapSessionResult<T> {
        let val = tokio::select! {
            _ = abort.recv() => { return Err(ImapSessionError::ABORTED); }
            _ = tokio::time::sleep(NETSOCK_TIMEOUT) => { return Err(ImapSessionError::TIMEOUT); }
            res = future => { res? }
        };
        Ok(val)
    }

    // This section onwards will contain wrappers to common imap protocol operations. 
    // If the function name starts with an _ it will return the direct result from the call.
    // Otherwise, it will simply return ImapSessionSuccess (This implies the result was already sent through channels or dropped)

    // NOOP: Do nothing and it won't fail (sometimes)
    pub async fn noop(&mut self, res_id: ResolveID) -> ImapSessionResult<ImapSessionSuccess> {
        let (network, abort) = self.net_abort();
        let res = Self::call_with_abort(abort, network.noop()).await;
        // let a = network.unsolicited_responses.try_recv();
        // println!("{:?}", a);
        ResolveStore::resolve(res_id, Resolution::Nothing);
        Ok(ImapSessionSuccess::ONCE)
    }

    pub async fn noopon(&mut self, res_id: ResolveID, mb_name: &str) -> ImapSessionResult<ImapSessionSuccess> {
        self.select(res_id, mb_name, false).await?;
        self.noop(res_id).await
    }

    // SELECT: Select a mailbox on the server.
    pub async fn select<'a>(self: &'a mut Self, res_id: ResolveID, mb_name: &str, force: bool) -> ImapSessionResult<ImapSessionSuccess> {
        if let Some(curr_mb) = &self.current_mailbox {
            if curr_mb.name == mb_name && !force {
                ResolveStore::resolve(res_id, Resolution::Nothing);
                return Ok(ImapSessionSuccess::ONCE);
            }
        }
        let (network, abort) = self.net_abort();
        let mailbox = Self::call_with_abort(abort, network.select_condstore(mb_name)).await?;
        let mut mailbox: Mailbox = (&mailbox).into();
        mailbox.name = mb_name.into();
        let mut mailbox_sql: db::MailboxSQL = (&mailbox).into();  
        
        let acc_key: db::AccountKey = CredentialStore::get(self.id.m_id).into();
        mailbox_sql.id = Some(db::KeyWrapper(
            db::MailboxKey::ACCIDNAME( acc_key.clone(), mailbox_sql.name.clone().unwrap() )
        ));
        mailbox_sql.account_id = Some(db::KeyWrapper(acc_key));

        self.current_mailbox = Some(mailbox);
        self.current_mailbox
            .as_mut()
            .map(|mut mb| mb.name = mb_name.into());
        // println!("Selected mailbox: {:?}", self.current_mailbox);
        Senders::srv(SrvMessage {
            action: srv::SrvAction::SYNCMAILBOX {
                mb: mailbox_sql.clone(),
            },
            resolve: NULL_RESOLVE_ID
        })
        .await;
        ResolveStore::resolve(res_id, Resolution::MailboxSQL(mailbox_sql));
        Ok(ImapSessionSuccess::ONCE)
    }

    // FETCH: Fetch a stream of mails from the server.
    pub async fn fetch(
        self: &mut Self,
        res_id: ResolveID,
        mb: MailboxName,
        ss: &SeqRange,
        fetch_types: &Vec<FetchType>,
        from: ImapSessionCommandType,
    ) -> ImapSessionResult<ImapSessionSuccess> {
        self.select(NULL_RESOLVE_ID, &mb, true).await?;
        let creds = self.id.m_id;
        let fetch_query = FetchType::fetch_string(fetch_types);
        let sss = ss.sequence_set_str(self.current_mailbox.as_ref().unwrap().exists);
        println!("Fetching: {} {}", sss, fetch_query);

        let (network, abort) = self.net_abort();
        let mut stream = Self::call_with_abort(abort, network.fetch(&sss, fetch_query)).await?; // The result is bounded to the network. If it errors, returning the network socket is impossible.

        let mut err: Option<AsyncImapError> = None;
        let mut msgs: Vec<db::MessageSQL> = Vec::new();
        let mut msg_parts_sqls: Vec<db::MessagePartSQL> = Vec::new();

        while let Some(mail_result) = {
            let res = tokio::select! {
                _ = abort.recv() => { Err(ImapSessionError::ABORTED) }
                _ = tokio::time::sleep(NETSOCK_TIMEOUT) => { Err(ImapSessionError::TIMEOUT) }
                res = stream.next() => { Ok(res) }
            };

            if let Err(e) = res {
                Some(Err(e))
            } else if let Ok(None) = res {
                None
            } else if let Ok(Some(Err(e))) = res {
                Some(Err(e.into()))
            } else if let Ok(Some(Ok(r))) = res {
                Some(Ok(r))
            } else {
                unreachable!()
            }
        } {
            if let Err(e) = mail_result {
                eprintln!("{:?}", e);
                use ImapSessionError::*;
                match e {
                    ASYNCIMAPERROR(e) => {
                        err = Some(e);
                        continue;
                    }
                    ABORTED | TIMEOUT => return Err(e),
                    _ => unreachable!(),
                }
            } 
            use SrvAction::*;
            use ImapSessionCommandType::*;
            use db::*;
            
            let mail = mail_result.unwrap();
            let message = Message::from(&mail);
            
            let mut msg_sql = db::MessageSQL::from(&message);
            let acc_key: db::AccountKey = CredentialStore::get(creds).into();
            let message_key = KeyWrapper( MessageKey::ACCIDIMAPUID( acc_key.clone(), msg_sql.imap_uid.unwrap() ) );
            msg_sql.id = Some(message_key.clone()); // Explicitly set the id because MessageSQL::from() simply doesn't have enough information to infer this
            msg_sql.account_id = Some(db::KeyWrapper( acc_key ));
            
            let mut msg_parts_sql: Vec<db::MessagePartSQL> = Vec::new();
            
            'match_from: {
                match &from {
                    LISTFETCH(_, seq_range) | FULLFETCH(_, seq_range, _) => {
                        if matches!(from, LISTFETCH(_, _)) && msg_sql.size.unwrap() > MAX_LISTFETCH_SIZE as i64 { break 'match_from; }
                        let body_opt = mail.text().and_then(|txt| mailparse::parse_mail(txt).ok());
                        if body_opt.is_none() || message.bodystructure.is_none() { break 'match_from; }
                        let body = body_opt.unwrap();
                        let bodystructure = message.bodystructure.as_ref().unwrap();
                        let mut parts = body.parts();
                        let mut dfs_traverse = parts.zip(bodystructure.clone().into_iter());
                        while let Some((mailparse_part, mailbodystructure)) = dfs_traverse.next() {
                            if !mailparse_part.subparts.is_empty() { continue; }
                            msg_parts_sql.push(
                                db::MessagePartSQL {
                                    id: Some(KeyWrapper(MessagePartKey::MSGIDPARTSPEC(message_key.clone().0, mailbodystructure.part_spec_str()))),
                                    create_time: None,
                                    update_time: None,
                                    message_id: Some(message_key.clone()),
                                    part_spec: Some(mailbodystructure.part_spec_str()),
                                    data: Some(mailparse_part.raw_bytes.to_vec()),
                                }
                            );
                        }
                    },
                    SECTIONFETCH(_, seq_range, mail_body_structures) => {
                        let mail_body_structures = mail_body_structures.clone();
                        let message_parts = mail_body_structures
                            .iter()
                            .map(|bs| 
                                db::MessagePartSQL {
                                    id: Some(KeyWrapper(MessagePartKey::MSGIDPARTSPEC(message_key.clone().0, bs.part_spec_str()))),
                                    create_time: None,
                                    update_time: None,
                                    message_id: Some(message_key.clone()),
                                    part_spec: Some(bs.part_spec_str()),
                                    data: mail.section(&imap_proto::SectionPath::Part(bs.part_spec().clone(), None)).map(Vec::from),
                                }
                            )
                            .filter(|mps| mps.data.is_some());
                        msg_parts_sql.extend(message_parts);
                    },

                    _ => unreachable!()
                }
            }

            
            Senders::srv(SrvMessage {
                action: SrvAction::SYNCEMAIL { msg: msg_sql.clone() },
                resolve: NULL_RESOLVE_ID
            }).await;

            for mps in &msg_parts_sql {
                Senders::srv(SrvMessage {
                    action: SrvAction::SYNCEMAILSECTION { part: mps.clone() },
                    resolve: NULL_RESOLVE_ID,
                }).await;
            }
            
            msgs.push(msg_sql);
            msg_parts_sqls.extend(msg_parts_sql);

        }

        ResolveStore::resolve(res_id, Resolution::MessageAndPartSQL(msgs, msg_parts_sqls));
        drop(stream); // stream bounded to the network so we gotta drop it first
        if let Some(e) = err {
            return Err(ImapSessionError::ASYNCIMAPERROR(e));
        } // due to limitations, we simply just return the last one found
        Ok(ImapSessionSuccess::ONCE)
    }

    // DELETE: Mark mails as deleted and expunge them.
    pub async fn delete<'a>(
        self: &'a mut Self,
        mb: MailboxName,
        ss: &SeqRange,
    ) -> ImapSessionResult<ImapSessionSuccess> {
        self.store(mb, ss, '+', &vec![MailFlag::DELETED]).await?;
        let (network, abort) = self.net_abort();
        Self::call_with_abort(abort, network.expunge()).await?;
        Ok(ImapSessionSuccess::ONCE)
    }

    // STORE: Update flags of a mail.
    pub async fn store<'a>(
        self: &'a mut Self,
        mb: MailboxName,
        ss: &SeqRange,
        store_type: char,
        flags: &Vec<MailFlag>,
    ) -> ImapSessionResult<ImapSessionSuccess> {
        if store_type != '+' && store_type != '-' {
            panic!("store_type must be '+' or '-'")
        }

        self.select(NULL_RESOLVE_ID, &mb, false).await?;
        let flag_string = MailFlag::flag_string(flags);
        let sss = ss.sequence_set_str(self.current_mailbox.as_ref().unwrap().exists);
        let (network, abort) = self.net_abort();
        let res = Self::call_with_abort(
            abort,
            network.store(
                sss,
                format!(
                    "{}FLAGS.SILENT {}", // Silent to avoid server echoing back the updated flags cause like why tho that kinda useless
                    store_type, flag_string
                ),
            ),
        )
        .await?;

        Ok(ImapSessionSuccess::ONCE)
    }

    // // APPEND: Append a mail to the mailbox.
    // pub async fn append<'a>(
    //     self: &'a mut Self,
    //     folder: &str,
    //     flags: &Vec<MailFlag>,
    //     date: Option<&str>,
    //     body: String,
    // ) -> ImapSessionResult<()> {
    //     let flag_string = MailFlag::flag_string(flags);
    //     let flags_arg = if flags.len() == 0 { None } else { Some(flag_string.as_str()) };
    //     let date_args = if date.is_none() { None } else { Some(date.unwrap()) };
    //     self.get_net()
    //         .append(folder, flags_arg, date_args, body.as_bytes())
    //         .await?;
    //     Ok(())
    // }

    // IDLE: Wait for new mails to arrive.
    pub async fn idle<'a>(&mut self, mb: MailboxName, res_id: ResolveID) -> ImapSessionResult<ImapSessionSuccess> {
        self.select(NULL_RESOLVE_ID, &mb, false).await?;

        // From this point onwards if it fails the network connection will be unrecoverable and has to be remade.

        let mut handle = self.take_net().idle();
        let init_res = handle.init().await?;
        let (
            idle_wait_future,
            this_guy_has_to_have_a_name_otherwise_it_will_get_dropped_and_the_future_will_return_with_a_manual_interrupt_every_single_time,
        ) = handle.wait_with_timeout(with_jitter(NETSOCK_REFRESH_INTERVAL));
        let res = Self::call_with_abort(&mut self.abort, idle_wait_future).await?;
        println!("idle result: {:?}", res);
        let res = match &res {
            ManualInterrupt => unreachable!(),
            Timeout =>  Ok(ImapSessionSuccess::REPEAT),
            NewData(response_data) => {
                use imap_proto::Response::*;
                // match response_data.borrow_dependent() {
                //     MailboxData(mailbox_datum) => todo!(),
                //     Expunge(seqnum) => todo!(),
                //     Fetch(seqnum, attribute_values) => todo!(),
                //     _ => return Err(ImapSessionErrors::INVALIDRESPONSE(format!("{:?}", response_data)))
                // }
                Ok(ImapSessionSuccess::REPEAT)
            }
        };

        let network = Self::call_with_abort(&mut self.abort, handle.done()).await?;
        self.return_net(network);
        res
    }

    // CAPABILITIES: Get the server's capabilities
    pub async fn _capabilities(&mut self) -> ImapSessionResult<CapabilitiesList> {
        let (net, abort) = self.net_abort();
        let capabilities = Self::call_with_abort(abort, net.capabilities()).await?;
        Ok(capabilities.into())
    }

    // LIST: Get the list of mailboxes
    pub async fn _list(&mut self) -> ImapSessionResult<impl Stream<Item = AsyncImapResult<async_imap::types::Name>>> {
        let (net, abort) = self.net_abort();
        let list = Self::call_with_abort(abort, net.list(None, Some("*"))).await?;
        Ok(list)
    }
    

    // pub async fn parse_fetch_stream_all<'a>(stream: &mut StreamResult<'a, async_imap::types::Fetch>) {
    //     let mut results: Vec<async_imap::types::Fetch> = Vec::new();

    //     while let Some(mail_result) = stream.next().await {
    //         if let Err(e) = mail_result {
    //             eprintln!("Error while parsing fetch stream: {:?}", e);
    //             continue
    //         }
    //         else if let Ok(mail) = mail_result {
    //             results.push(mail);

    //         }
    //     }
    // }
}

#[derive(Clone, Debug)]
pub struct ImapSessionState {
    // Since during creation of ImapSession we do not actually own it we must keep track of its state based on what we send and what the responses are.
    pub to_session: Option<tokio::sync::mpsc::Sender<ImapSessionCommand>>,
    pub to_abort: Option<tokio::sync::mpsc::Sender<()>>,
    pub pending_cmds: std::collections::VecDeque<ImapSessionCommand>, // With the current implementations this actually doesn't need to be a VecDeque haha
    pub last_known_mailbox: Option<MailboxName>,
    running: bool,
    failed: bool,
}

pub fn imap_connection_retry_logic<T>(res: &ImapSessionResult<T>) -> Attempt {
    use ImapSessionError::*;
    use async_imap::error::Error::*;
    match res {
        Ok(_) => Attempt::ONCE,
        Err(ASYNCIMAPERROR(Io(_)) | ASYNCIMAPERROR(ConnectionLost)) => Attempt::REPEAT,
        _ => Attempt::ONCE,
    }
}

impl ImapSessionState {
    pub async fn get_imap_session(
        id: ImapSessionId,
    ) -> Option<(
        tokio::sync::mpsc::Sender<ImapSessionCommand>,
        tokio::sync::mpsc::Sender<()>,
    )> {
        use ImapSessionError::*;
        use async_imap::error::Error::*;
        let mut current_retry_delay = INITIAL_RETRY_DELAY;
        loop {
            let res = ImapSession::new(id.clone()).await;
            if let Err(e) = &res { eprintln!("{e}"); }
            if imap_connection_retry_logic(&res) == Attempt::ONCE { return res.ok(); }
            wait_with_jitter(current_retry_delay).await;
            current_retry_delay = double_time_clamped(current_retry_delay);
        }
        return None;
    }

    pub fn new(id: ImapSessionId) -> Self {
        Self {
            to_session: None,
            to_abort: None,
            pending_cmds: std::collections::VecDeque::new(),
            last_known_mailbox: None,
            running: false,
            failed: false,
        }
    }

    pub async fn connect(&mut self, id: ImapSessionId) -> bool {
        // Do not run this if we are already connencted
        let res = Self::get_imap_session(id).await;
        let success = res.is_some();
        self.running = false;
        self.failed = false;
        if let Some((to_session, to_abort)) = res {
            self.to_session = Some(to_session);
            self.to_abort = Some(to_abort);

            // Send any pending commands back to the session
            for cmd in self.pending_cmds.iter() {
                self.to_session
                    .as_ref()
                    .unwrap()
                    .send(cmd.clone())
                    .await
                    .unwrap();
            }
        }
        success
    }

    pub async fn add_command(&mut self, cmd: ImapSessionCommand) {
        let next_known_mailbox = cmd.get_mailbox_domain();
        if next_known_mailbox.is_some() {
            self.last_known_mailbox = next_known_mailbox.map(|mb| mb.to_string());
        }
        self.pending_cmds.push_back(cmd.clone());
        if let Some(sender) = self.to_session.as_ref() {
            sender.send(cmd).await.unwrap();
        }
    }

    pub fn rm_command(&mut self, id: u64) -> Option<ImapSessionCommand> {
        // id here either comes from the ImapSession calling itself or the ImapManager
        // where the imap session will always pick a command id of u64::MAX
        // This means that the id must be in the front of the queue or not a valid id that we send
        self.pending_cmds.pop_front_if(|cmd| cmd.id == id)
    }

    pub fn sum_weights(&self) -> f64 {
        self.pending_cmds.iter().map(|cmd| cmd.weight()).sum()
    }

    pub fn status(&self) -> Status {
        if self.failed {
            Status::FAILED
        } else if !self.running {
            Status::CONNECTING
        } else if self.pending_cmds.is_empty() {
            Status::ALIVE
        } else {
            Status::BUSY
        }
    }
}

#[repr(u64)]
#[derive(Debug, Clone, PartialEq, Eq, Copy, num_derive::FromPrimitive)]
pub enum Capability {
    IMAP4rev1 = 1 << 0,
    IMAP4rev2 = 1 << 1,
    JMAPACCESS = 1 << 2, // Its like seeing the light but I can't reach it
    STARTTLS = 1 << 3,
    LOGINDISABLED = 1 << 4,
    AUTHPLAIN = 1 << 5,
    AUTHLOGIN = 1 << 6,
    AUTHOAUTH2 = 1 << 7, // matches AUTH=XOAUTH2 and AUTH=OAUTHBEARER
    SASLIR = 1 << 8, // matches SASL-IR
    ID = 1 << 9,
    IDLE = 1 << 10,
    CONDSTORE = 1 << 11,
    QRESYNC = 1 << 12,
    COMPRESSDEFLATE = 1 << 13, // matches COMPRESS=DEFLATE
    UIDPLUS = 1 << 14,
    ENABLE = 1 << 15,
    MOVE = 1 << 16,
    SPECIALUSE = 1 << 17, // matches SPECIAL-USE
    BINARY = 1 << 18,
    LITERALPLUS = 1 << 19, // matches LITERAL+
    LITERALMINUS = 1 << 20, // matches LITERAL-
    UTF8ACCEPT = 1 << 21, // matches UTF8=ACCEPT
    UTF8ONLY = 1 << 22, // matches UTF8=ONLY
    NAMESPACE = 1 << 23,
    XGMEXT1 = 1 << 62, // matches X-GM-EXT-1
    XAPPLEPUSHSERVICE = 1 << 63,
}

macro_rules! caps {
    ( $($capenum:ident::$cap:ident),* ) => {
        {
            use crate::net::fetch::imap::Capability;
            let mut cap_list = CapabilitiesList::new();
            $( cap_list.set(
                 Capability::$cap 
            );)*
            cap_list
        }
    };
    ( $($cap:ident),* ) => {
        {
            use crate::net::fetch::imap::Capability;
            let mut cap_list = CapabilitiesList::new();
            $( cap_list.set(
                 $cap 
            );)*
            cap_list
        }
    };
}

impl Capability {
    pub const fn implied(self) -> CapabilitiesList {   
        // Doesn't return itself
        match self {
            Capability::IMAP4rev2 => {
                // RFC 9051: IMAP4rev2 obsoleted IMAP4rev1 by baking all of 
                // these formerly optional extensions directly into the base protocol.
                caps!(
                    Capability::IMAP4rev1,
                    Capability::SASLIR,
                    Capability::IDLE,
                    Capability::CONDSTORE,
                    Capability::QRESYNC,
                    Capability::UIDPLUS,
                    Capability::ENABLE,
                    Capability::MOVE,
                    Capability::SPECIALUSE,
                    Capability::LITERALPLUS,
                    Capability::LITERALMINUS,
                    Capability::UTF8ACCEPT,
                    Capability::NAMESPACE
                )
            }
            Capability::QRESYNC => {
                // RFC 7162: Quick Resync relies on modification sequences (MODSEQ),
                // which means it inherently requires CONDSTORE. Both require ENABLE.
                caps!(
                    Capability::CONDSTORE,
                    Capability::ENABLE
                )
            }
            Capability::CONDSTORE => {
                // RFC 7162: CONDSTORE requires the ENABLE command to be activated.
                caps!(
                    Capability::ENABLE
                )
            }
            Capability::UTF8ONLY => {
                // RFC 9755: A server strictly enforcing UTF-8 inherently accepts it.
                caps!(
                    Capability::UTF8ACCEPT,
                    Capability::ENABLE
                )
            }
            Capability::UTF8ACCEPT => {
                // RFC 9755: UTF8=ACCEPT requires the ENABLE command to turn it on.
                caps!(
                    Capability::ENABLE
                )
            }
            Capability::LITERALMINUS => {
                // RFC 7888: LITERAL- is a strict upgrade that implies LITERAL+.
                caps!(
                    Capability::LITERALPLUS
                )
            }
            cap => CapabilitiesList::new()
        }
    }
    pub const fn from_bits(bits: u64) -> Option<Self> {
        
        None
    }
}

pub struct CapabilitiesList {
    contains: u64,
    implies: u64
}


impl CapabilitiesList {
    
    pub const fn new() -> Self {
        CapabilitiesList {
            contains: 0,
            implies: 0,
        }
    }

    pub const fn union(&mut self, other: CapabilitiesList) {
        self.contains |= other.contains;
        self.implies |= other.implies;
    }
    
    // pub const fn has(&self, cap: Capability) -> bool {
    //     self.contains & cap as u64 != 0
    // }

    pub const fn able_to(&self, cap: Capability) -> bool {
        self.implies & cap as u64 != 0
    }

    pub const fn capable_to(&self, caps: CapabilitiesList) -> bool {
        self.implies & caps.implies == caps.implies
    }
    
    pub const fn set(&mut self, cap: Capability) {
        self.contains |= cap as u64;
        self.implies |= cap.implied().implies | cap as u64;
    }
    
    pub fn to_vec(&self) -> Vec<Capability> {
        let mut vec = Vec::new();
        for i in 0..64 {
            if self.contains & (1 << i) != 0 {
                use num_traits::FromPrimitive;
                vec.push(Capability::from_u64(1 << i).unwrap());
            }
        }
        vec
    }
}

impl std::fmt::Debug for CapabilitiesList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let vec = self.to_vec();
        vec.fmt(f)
    }
}

impl From<async_imap::types::Capabilities> for CapabilitiesList {
    fn from(async_imap_caps: async_imap::types::Capabilities) -> Self {
        let mut cap = CapabilitiesList::new();
        for cap_name in async_imap_caps.iter() {
            match &cap_name {
                async_imap::types::Capability::Imap4rev1 => cap.set(Capability::IMAP4rev1),
                async_imap::types::Capability::Auth(s) => {
                    match s.as_str() {
                        "PLAIN" => cap.set(Capability::AUTHPLAIN),
                        "LOGIN" => cap.set(Capability::AUTHLOGIN),
                        "OAUTHBEARER" => cap.set(Capability::AUTHOAUTH2),
                        _ => eprintln!("Unknown auth capability: {}", s)
                    }
                },
                async_imap::types::Capability::Atom(s) => {
                    match s.as_str() {
                        "IMAP4rev2" => cap.set(Capability::IMAP4rev2),
                        "JMAPACCESS" => cap.set(Capability::JMAPACCESS),
                        "STARTTLS" => cap.set(Capability::STARTTLS),
                        "LOGINDISABLED" => cap.set(Capability::LOGINDISABLED),
                        "SASL-IR" => cap.set(Capability::SASLIR),
                        "ID" => cap.set(Capability::ID),
                        "IDLE" => cap.set(Capability::IDLE),
                        "CONDSTORE" => cap.set(Capability::CONDSTORE),
                        "QRESYNC" => cap.set(Capability::QRESYNC),
                        "COMPRESS=DEFLATE" => cap.set(Capability::COMPRESSDEFLATE),
                        "UIDPLUS" => cap.set(Capability::UIDPLUS),
                        "ENABLE" => cap.set(Capability::ENABLE),
                        "MOVE" => cap.set(Capability::MOVE),
                        "SPECIAL-USE" => cap.set(Capability::SPECIALUSE),
                        "BINARY" => cap.set(Capability::BINARY),
                        "LITERAL+" => cap.set(Capability::LITERALPLUS),
                        "LITERAL-" => cap.set(Capability::LITERALMINUS),
                        "UTF8=ACCEPT" => cap.set(Capability::UTF8ACCEPT),
                        "UTF8=ONLY" => cap.set(Capability::UTF8ONLY),
                        "NAMESPACE" => cap.set(Capability::NAMESPACE),
                        "X-GM-EXT-1" => cap.set(Capability::XGMEXT1),
                        "XAPPLEPUSHSERVICE" | "X-APPLE-PUSH-SERVICE" => cap.set(Capability::XAPPLEPUSHSERVICE),
                        "XOAUTH2" => cap.set(Capability::AUTHOAUTH2),
                        _ => eprintln!("Unknown capability: {}", s)
                    }
                },
            }
        }
        cap
    }
}

const MINIMUM_CAPS_ALLOWED: CapabilitiesList = caps!( Capability::IMAP4rev1 );
const MINIMUM_CAPS_IDLE: CapabilitiesList = caps!( Capability::IMAP4rev1, Capability::IDLE );
static IDLE_IS_BROKEN: [Service; 2] = [Service::YAHOO, Service::AOL]; // Yahoo why you do this :(

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollingStrategy {
    IDLE,
    NOOPSPAM, // I am naming it this because disrepsectfully stfu
}

type PollStrategyMap = std::collections::HashMap<MailboxName, PollingStrategy>;
pub struct ImapManager {
    // One manager per account
    pub id: CredentialID,
    pub imap_session_states: std::collections::HashMap<ImapSessionId, ImapSessionState>, // Key is id
    pub capabilities: CapabilitiesList,
    pub poll_strategy: PollStrategyMap,
}

impl ImapManager {

    // Evaluates the current state of the 
    fn evaluate_poll_strategy(&mut self) -> Result<()> { // If this fails we can't continue
        let active_sessions = self.get_active_session_count();
        let mailbox_names = self.poll_strategy.keys().cloned().collect::<Vec<_>>();
        let creds = CredentialStore::get(self.id);
        self.poll_strategy.values_mut().for_each(|s| { *s = PollingStrategy::NOOPSPAM; });
        if !self.capabilities.capable_to(MINIMUM_CAPS_ALLOWED) {
            return Err(ImapSessionError::INVALIDRESPONSE("Server does not support minimum required capabilities".to_string()).into());
        } else if active_sessions == 0 {
            return Err(anyhow::anyhow!("Failed to create any IMAP sessions"));
        } else if active_sessions >= 2 && self.capabilities.capable_to(MINIMUM_CAPS_IDLE) && !IDLE_IS_BROKEN.contains(&creds.service) {
            self.poll_strategy.insert("INBOX".into(), PollingStrategy::IDLE);
        }
        Ok(())
    }

    pub async fn new(m_id: CredentialID) -> Result<Self> {
        let (cap_list, poll_strat_map) = Self::probe_server(m_id).await?;
        let mut manager = Self {
            id: m_id,
            imap_session_states: std::collections::HashMap::new(),
            capabilities: cap_list,
            poll_strategy: poll_strat_map,
        };

        manager.create_session_states(m_id, MAX_IMAP_SESSIONS).await;
        manager.evaluate_poll_strategy()?;
        println!("{:?}", manager.capabilities);

        for (mb, poll_strat) in manager.poll_strategy.clone().into_iter() {
            if poll_strat != PollingStrategy::IDLE { continue; }
            manager
                .call_session(ImapSessionCommandType::IDLE(mb), NULL_RESOLVE_ID)
                .await;
        }
        Ok(manager)
    }

    pub async fn probe_server(m_id: CredentialID) -> ImapSessionResult<(CapabilitiesList, PollStrategyMap)> {
        let (sender, receiver) = tokio::sync::mpsc::channel::<ImapSessionCommand>(100);
        let (abort_sender, abort_recv) = tokio::sync::mpsc::channel::<()>(5);
        let mut client: Option<async_imap::Session<Compat<TlsStream<TcpStream>>>> = None;
        let mut cap_option: Option<async_imap::types::Capabilities> = None;
        let mut poll_strat_map: PollStrategyMap = std::collections::HashMap::new();
        
        loop {
            let res = ImapSession::get_client(m_id).await;
            if let Err(e) = &res { eprintln!("{e}"); }
            if imap_connection_retry_logic(&res) == Attempt::ONCE { 
                let (a, b) = res?; 
                client = Some(a);
                cap_option = b;
                break;
            }
        }
        
        let mut session = ImapSession {
            id: ImapSessionId { s_id: IDStore::s_id(), m_id },
            net: client,
            current_mailbox: None,
            receiver: receiver,
            abort: abort_recv,
        };

        let cap_list = if cap_option.is_none() {
            session._capabilities().await?
        } else {
            cap_option.unwrap().into()
        };

        let mut stream = session._list().await?;
        while let Some(res) = stream.next().await {
            let Ok(name) = res else { continue };
            let attrs = name.attributes();
            let mb_name = name.name();
            println!("{:?} {}", attrs, mb_name);

            poll_strat_map.insert(mb_name.into(), PollingStrategy::NOOPSPAM);
            let acc_id: db::AccountKey = CredentialStore::get(m_id).into();
            let mailbox_sql = db::MailboxSQL {
                id: Some(db::KeyWrapper(db::MailboxKey::ACCIDNAME(
                    acc_id.clone(),
                    mb_name.to_string(),
                ))), // Personally I like the way I structured it.
                create_time: None,
                update_time: None,
                account_id: Some(db::KeyWrapper(acc_id.clone())),
                name: Some(mb_name.into()),
                mail_count: None,
                recent: None,
                unseen: None,
                attrs: Some(
                    serde_sqlite_jsonb::to_vec(
                        &attrs.iter().map(MailboxAttr::from).collect::<Vec<_>>()
                    ).unwrap()
                ),
                highest_modseq: None,
                uid_next: None,
                uid_validity: None,
            };
            Senders::srv(SrvMessage { action: SYNCMAILBOX { mb: mailbox_sql }, resolve: NULL_RESOLVE_ID }).await;
        }

        Ok((cap_list, poll_strat_map))
    }

    pub fn get_active_session_count(&self) -> usize {
        self.imap_session_states.iter().fold(
            0,
            |acc, (_, s)| {
                if s.to_session.is_some() { acc + 1 } else { acc }
            },
        )
    }

    pub fn status(&self) -> Vec<(ImapSessionId, Status)> {
        self.imap_session_states
            .clone()
            .into_iter()
            .map(|(id, s)| (id, s.status()))
            .collect()
    }

    pub async fn rcv_session_update(&mut self, upd: SessionUpdate, res_id: ResolveID) {
        use SessionUpdate::*;
        match upd {
            STARTED(isid) => {
                self.imap_session_states.get_mut(&isid).unwrap().running = true;
            }
            CMDSUCCESS(isid, cmdid) => {
                self.imap_session_states
                    .get_mut(&isid)
                    .unwrap()
                    .rm_command(cmdid);
            }
            CMDSUCCESSRETRY(isid, cmdid) => {
                let state = self.imap_session_states.get_mut(&isid).unwrap();
                let cmd = state.rm_command(cmdid).unwrap();
                self.call_session(cmd.ty, res_id).await;
            }
            SESSIONABORT(isid) => {
                // let state = self.imap_session_states.get_mut(&isid).unwrap();
                // state.running = false;
            }
            CMDFAILURETRYAGAIN(isid, cmdid, net_exists) => {
                let state = self.imap_session_states.get_mut(&isid).unwrap();

                if !net_exists { // Set failure
                    state.running = false;
                    state.failed = true;
                }

                let cmd = state.rm_command(cmdid).unwrap(); // rm previous command
                self.call_session(cmd.ty, res_id).await; // call it again (on hopefully one that didn't fail)

                if !net_exists { // Try to reconnect
                    let state = self.imap_session_states.get_mut(&isid).unwrap(); // Hello future me. This line is required. Sincerely, me.
                    state.connect(isid).await;
                }
            }
            CMDFAILUREUNRECOVERABLE(isid, cmdid, net_exists) => {
                println!("Received unrecoverable failure for command {:?}", cmdid);
                let state = self.imap_session_states.get_mut(&isid).unwrap();

                if !net_exists {
                    state.running = false;
                    state.failed = true;
                    state.connect(isid).await;
                }
            }
        }
    }

    pub async fn create_session_states(&mut self, m_id: CredentialID, count: usize) {
        // Try to create as many imap sessions per manager as possible
        use AsyncImapError::*;
        use ImapSessionError::*;
        use ResponseCode::*;

        for i in 0..count {
            let isid = ImapSessionId {
                s_id: IDStore::s_id(),
                m_id: m_id,
            };
            let mut state = ImapSessionState::new(isid.clone());
            if !state.connect(isid.clone()).await {
                break;
            }
            self.imap_session_states.insert(isid, state);
        }
    }

    pub async fn call_session(&mut self, cmd: ImapSessionCommandType, resolve: ResolveID) {
        // Find the most available session to send the command to
        fn weight_by_diff_mailbox(iss: &ImapSessionState, cmd: &ImapSessionCommandType) -> f64 {
            (!cmd.get_required_mailbox().is_none()
                && iss.last_known_mailbox != cmd.get_required_mailbox().cloned()) as i64
                as f64
        }

        let lowest_weight_state = self
            .imap_session_states
            .values_mut()
            .into_iter()
            .filter(|state| !state.failed)
            .min_by(|a, b| {
                (a.sum_weights() + weight_by_diff_mailbox(a, &cmd))
                    .total_cmp(&(b.sum_weights() + weight_by_diff_mailbox(b, &cmd)))
            })
            .expect("No available sessions");

        lowest_weight_state
            .add_command(ImapSessionCommand {
                id: IDStore::cmd_id(),
                ty: cmd,
                resolve
            })
            .await;
    }

    pub async fn handle_suggest(&mut self, mb: MailboxName, res_id: ResolveID) {
        let available_session = self
            .imap_session_states
            .iter_mut()
            .filter(|(_, s)| s.pending_cmds.is_empty() && s.running && !s.failed)
            .next();
        if let Some((_, state)) = available_session {
            state
                .add_command(ImapSessionCommand {
                    id: IDStore::cmd_id(),
                    ty: ImapSessionCommandType::SELECT(mb),
                    resolve: res_id,
                })
                .await;
        }
    }

    pub async fn handle_poll(&mut self) {
        println!("Polling strategy: {:?}", self.poll_strategy);
        for (mb, poll_strat) in self.poll_strategy.clone().into_iter() {
            if poll_strat == PollingStrategy::IDLE {continue;}
            self.call_session(ImapSessionCommandType::NOOPON(mb), NULL_RESOLVE_ID).await;
        }
        // self.call_session(ImapSessionCommandType::NOOPON("Draft".into()), NULL_RESOLVE_ID).await;
    }

    pub async fn shutdown(&mut self, res_id: ResolveID) {
        for (_, state) in self.imap_session_states.iter_mut() {
            state
                .add_command(ImapSessionCommand {
                    id: IDStore::cmd_id(),
                    ty: ImapSessionCommandType::SHUTDOWN,
                    resolve: res_id,
                })
                .await;
        }
    }
}
