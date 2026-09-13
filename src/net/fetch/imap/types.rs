use std::fmt;

use crate::models;
use crate::*;

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

// type Mailbox = async_imap::types::Mailbox;
type StreamResult<'a, T> =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<T, async_imap::error::Error>> + 'a + Send>>;
    
#[derive(Debug, Clone)]
pub enum Sequence {
    // Zero Indexed. Negative values count from the end of the mailbox
    Empty,
    Range { start: i32, end: i32 },
    Single(i32),
    Combo { vec: Vec<Sequence> },
}

impl fmt::Display for Sequence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Sequence::Empty => write!(f, ""),
            Sequence::Range { start, end } => {
                let start = *start;
                let end = *end;
                if start == -1 { write!(f, "*:{}", end) }
                else if end == -1 { write!(f, "{}:*", start) }
                else { write!(f, "{}:{}", start, end) }
            }
            Sequence::Single(val) => write!(f, "{}", val),
            Sequence::Combo { vec } => vec
                .iter()
                .map(|r| r.to_string())
                .collect::<Vec<_>>()
                .join(",")
                .fmt(f),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SeqRange {
    pub is_uid: bool,
    pub range: Sequence,
}

impl SeqRange {
    
    pub fn first(is_uid: bool) -> Self {
        Self { is_uid, range: Sequence::Single(0), }
    }

    pub fn single(is_uid: bool, val: i32) -> Self {
        Self { is_uid, range: Sequence::Single(val), }
    }

    // pub fn last(is_uid: bool) -> Self {
    //     Self { is_uid, range: Sequence::Single(-1), }
    // }

    pub fn all(is_uid: bool) -> Self {
        Self { is_uid, range: Sequence::Range { start: 0, end: -1 }, }
    }
    
    pub fn empty(is_uid: bool) -> Self {
        Self { is_uid, range: Sequence::Empty, }
    }

    pub fn from_vec(is_uid: bool, vec: Vec<u32>) -> Self {
        // Run compression so that it can be represented as minimal seq ranges

        if vec.is_empty() { return Self::empty(false); }
        if vec.len() == 1 { return Self::single(false, vec[0] as i32); }
        let mut current_combo: Vec<Sequence> = Vec::new();

        let mut current_start: Option<i32> = None;
        let mut current_end: Option<i32> = None;
        for val in vec.iter() { // Sometimes in life, you gotta admit that writing it imparatively is just simply easier
            let val = *val as i32;
            if current_start.is_none() {
                current_start = Some(val);
                current_end = Some(val);
            } else if current_end.unwrap() == val - 1 {
                current_end = Some(val);
            } else {
                current_combo.push(
                    if current_start == current_end { Sequence::Single(current_start.unwrap()) }
                    else { Sequence::Range { start: current_start.unwrap(), end: current_end.unwrap() } }
                );
                current_start = None;
                current_end = None;
            }
        }
        
        if current_start.is_some() && current_end.is_some() {
            current_combo.push(
                if current_start == current_end { Sequence::Single(current_start.unwrap()) }
                else { Sequence::Range { start: current_start.unwrap(), end: current_end.unwrap() } }
            );
        }
        
        SeqRange {is_uid, range: Sequence::Combo { vec: current_combo }}
    }
}


impl fmt::Display for SeqRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.range)
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
    CHANGEDSINCE(u32),
    UNCHANGEDSINCE(u32),
    VANISHED
}

impl FetchType {
    pub fn fetch_string(fetch_type: &Vec<FetchType>) -> String {
        let mut result_str = String::new();
        let mut query_str: Vec<String> = Vec::new();
        let mut qresync_str: Vec<String> = Vec::new();

        for ft in fetch_type {
            match ft {
                FetchType::UID => query_str.push("UID".into()),
                FetchType::BODYSTRUCTURE => query_str.push("BODYSTRUCTURE".into()),
                FetchType::ENVELOPE => query_str.push("ENVELOPE".into()),
                FetchType::FLAGS => query_str.push("FLAGS".into()),
                FetchType::INTERNALDATE => query_str.push("INTERNALDATE".into()),
                FetchType::RFC822SIZE => query_str.push("RFC822.SIZE".into()),
                FetchType::BODYPEEKSECTION(section, partial) => {
                    if partial.len() == 0 {
                        query_str.push(format!("BODY.PEEK[{}]", section))
                    } else {
                        query_str.push(format!("BODY.PEEK[{}]<{}>", section, partial))
                    }
                }
                FetchType::BODYSECTION(section, partial) => {
                    if partial.len() == 0 {
                        query_str.push(format!("BODY[{}]", section))
                    } else {
                        query_str.push(format!("BODY[{}]<{}>", section, partial))
                    }
                }
                FetchType::CHANGEDSINCE(uid) => qresync_str.push(format!("CHANGEDSINCE {}", uid)),
                FetchType::UNCHANGEDSINCE(uid) => qresync_str.push(format!("UNCHANGEDSINCE {}", uid)),
                FetchType::VANISHED => qresync_str.push("VANISHED".into()),
            }
        }

        if query_str.len() > 1 {
            result_str = format!("({})", query_str.join(" "));
        }
        if qresync_str.len() > 0 {
            result_str = format!("{} ({})", result_str, qresync_str.join(" "));
        }
        return result_str;
    }
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
        
        let address_to_string = |adr: &imap_proto::types::Address| -> Option<String> {
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
        };

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
    SELECT(MailboxName), // (force) -> MailboxSQL
    LISTFETCH(MailboxName, SeqRange), // -> MessageSQL
    SECTIONFETCH(MailboxName, SeqRange, Vec<MailBodyStructure>), // -> MessageSQL 
    FULLFETCH(MailboxName, SeqRange, Option<u64> /* msg size */), // -> MessageSQL
    NOOPON(MailboxName), // -> Nothing
    IDLE(MailboxName), // -> Doesn't resolve with success 
    SEARCH(MailboxName, Vec<SearchQuery>, SeqRange), // -> Search
    /* Internal use */
    NOOP, // -> Nothing
    /* Internal use */
    SHUTDOWN, // -> Doesn't resolve
    // TODO: insert more here
}

pub use ImapSessionCommandType::*;

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

        // Get the expected weight (in units of multiples of 1 rtt) ignoring the initial latency
        match &self.ty {
            LISTFETCH(_, range) => {
                get_download_time(
                    MAILBOX_INBOX_ASSUMED_SIZE as u64 / 3 
                        * MAX_LISTFETCH_SIZE as u64,
                    ASSUMED_DOWNLOAD_SPEED as f64,
                ) / ASSUMED_LATENCY as f64
            } // Educated guess.
            IDLE(_) => NETSOCK_REFRESH_INTERVAL.as_secs_f64() / ASSUMED_LATENCY as f64 * 1000.0,
            NOOP => 0.0,
            NOOPON(_) => 0.0,
            SHUTDOWN => 0.0,
            SEARCH(_, _, range) => {
                get_download_time(
                    10 * MAILBOX_INBOX_ASSUMED_SIZE as u64,
                    ASSUMED_DOWNLOAD_SPEED as f64,
                ) / ASSUMED_LATENCY as f64
            }
            SELECT(_) => 0.0,
            SECTIONFETCH(_, range, vec_bs) => {
                let size = vec_bs.iter().fold(0, |acc, bs| acc + bs.get_total_size()) as u64;
                get_download_time(
                    MAILBOX_INBOX_ASSUMED_SIZE as u64 / 200 * size,
                    ASSUMED_DOWNLOAD_SPEED as f64,
                ) / ASSUMED_LATENCY as f64
            }
            FULLFETCH(_, range, size) => {
                let size = size.unwrap_or(MAX_PREFETCH_STRAT1_SIZE as u64);
                get_download_time(
                    MAILBOX_INBOX_ASSUMED_SIZE as u64 / 100 * size,
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
            SEARCH(mb, _, _) => Some(mb),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub struct ImapSessionId {
    pub s_id: u64,
    pub m_id: CredentialID,
}

pub type Aborter = tokio::sync::mpsc::Receiver<()>;
pub type ImapSessionResult<T> = std::result::Result<T, ImapSessionError>;

pub type ImapSessionSuccess = Attempt;

#[derive(Debug)]
pub enum ImapSessionError {
    ASYNCIMAPERROR(async_imap::error::Error),    // Async imap / imap proto lib errors
    TLSERROR(async_native_tls::Error), // Encryption errors
    AUTHENTICATIONERROR,               // Invalid auth arguments
    ABORTED,                           // The current command was aborted with abort.send(())
    TIMEOUT,                           // The current command timed out
    INVALIDRESPONSE(String),           // When the server wants to be naughty and sends us some bs
}

pub use ImapSessionError::*;

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

impl std::error::Error for ImapSessionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None // TODO: uhhhhhhhhhhhhhhh
    }
}

impl From<std::io::Error> for ImapSessionError {
    fn from(e: std::io::Error) -> Self {
        let a: async_imap::error::Error = e.into();
        a.into()
    }
}

impl From<async_imap::error::Error> for ImapSessionError {
    fn from(e: async_imap::error::Error) -> Self {
        ImapSessionError::ASYNCIMAPERROR(e)
    }
}

impl From<async_native_tls::Error> for ImapSessionError {
    fn from(e: async_native_tls::Error) -> Self {
        ImapSessionError::TLSERROR(e)
    }
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollingStrategy {
    IDLE,
    SELECTSPAM, // I am naming it this because disrepsectfully stfu
    NOOPSPAM, // UNUSED BECAUSE IT DOESNT UPDATE FAST ENOUGH
}

pub type PollStrategyMap = std::collections::HashMap<MailboxName, PollingStrategy>;

// TODO: Convert date strings to Date Objects and handle the deserialization
#[derive(Debug, Clone)]
pub enum SearchQuery { // Only contains Imap4rev1 stuff for now.
    /* Doesn't include the sequence set */
    ALL,
    ANSWERED,
    BCC(String),
    BEFORE(String /* Date */),
    BODY(String),
    CC(String),
    DELETED,
    DRAFT,
    FLAGGED,
    FROM(String),
    HEADER(String /* Field name */ , String),
    KEYWORD(String /* Flag */),
    LARGER(u32),
    NEW,
    NOT(Box<SearchQuery>),
    OLD,
    ON(String /* Date */),
    OR(Box<SearchQuery>, Box<SearchQuery>),
    RECENT,
    SEEN,
    SENTBEFORE(String /* Date */),
    SENTON(String /* Date */),
    SENTSINCE(String /* Date */),
    SINCE(String /* Date */),
    SMALLER(u32),
    SUBJECT(String),
    TEXT(String),
    TO(String),
    UNANSWERED,
    UNDELETED,
    UNDRAFT,
    UNFLAGGED,
    UNKEYWORD(String /* Flag */),
    UNSEEN,
}

impl SearchQuery {
    pub fn to_query_str(vec: &Vec<SearchQuery>) -> String {
        let query = vec.clone().iter().map(|q| q.to_string()).reduce(|a,b| format!("{} {}", a, b));
        query.unwrap_or_default()
    }
}

impl fmt::Display for SearchQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SearchQuery::ALL => write!(f, "ALL"),
            SearchQuery::ANSWERED => write!(f, "ANSWERED"),
            SearchQuery::BCC(str) => write!(f, "BCC {}", str),
            SearchQuery::BEFORE(str) => write!(f, "BEFORE {}", str),
            SearchQuery::BODY(str) => write!(f, "BODY {}", str),
            SearchQuery::CC(str) => write!(f, "CC {}", str),
            SearchQuery::DELETED => write!(f, "DELETED"),
            SearchQuery::DRAFT => write!(f, "DRAFT"),
            SearchQuery::FLAGGED => write!(f, "FLAGGED"),
            SearchQuery::FROM(str) => write!(f, "FROM {}", str),
            SearchQuery::HEADER(str, str1) => write!(f, "HEADER {} {}", str, str1),
            SearchQuery::KEYWORD(str) => write!(f, "KEYWORD {}", str),
            SearchQuery::LARGER(num) => write!(f, "LARGER {}", num),
            SearchQuery::NEW => write!(f, "NEW"),
            SearchQuery::NOT(search_query) => write!(f, "NOT"),
            SearchQuery::OLD => write!(f, "OLD"),
            SearchQuery::ON(str) => write!(f, "ON {}", str),
            SearchQuery::OR(search_query, search_query1) => write!(f, "OR"),
            SearchQuery::RECENT => write!(f, "RECENT"),
            SearchQuery::SEEN => write!(f, "SEEN"),
            SearchQuery::SENTBEFORE(str) => write!(f, "SENTBEFORE {}", str),
            SearchQuery::SENTON(str) => write!(f, "SENTON {}", str),
            SearchQuery::SENTSINCE(str) => write!(f, "SENTSINCE {}", str),
            SearchQuery::SINCE(str) => write!(f, "SINCE {}", str),
            SearchQuery::SMALLER(num) => write!(f, "SMALLER {}", num),
            SearchQuery::SUBJECT(str) => write!(f, "SUBJECT {}", str),
            SearchQuery::TEXT(str) => write!(f, "TEXT {}", str),
            SearchQuery::TO(str) => write!(f, "TO {}", str),
            SearchQuery::UNANSWERED => write!(f, "UNANSWERED"),
            SearchQuery::UNDELETED => write!(f, "UNDELETED"),
            SearchQuery::UNDRAFT => write!(f, "UNDRAFT"),
            SearchQuery::UNFLAGGED => write!(f, "UNFLAGGED"),
            SearchQuery::UNKEYWORD(str) => write!(f, "UNKEYWORD {}", str),
            SearchQuery::UNSEEN => write!(f, "UNSEEN"),
        }
    }
}

#[repr(u64)]
#[derive(Debug, Clone, PartialEq, Eq, Copy, Hash, num_derive::FromPrimitive)]
pub enum Capability {
    IMAP4rev1 = 1 << 0,
    IMAP4rev2 = 1 << 1,
    JMAPACCESS = 1 << 2, // Its like seeing the light but I can't reach it
    STARTTLS = 1 << 3,
    LOGINDISABLED = 1 << 4,
    AUTHPLAIN = 1 << 5,
    AUTHLOGIN = 1 << 6,
    AUTHOAUTH2 = 1 << 7, // matches AUTH=OAUTHBEARER
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

static CAPABILITIES_MAP: std::sync::LazyLock<bimap::BiMap<&'static str, Capability>> =
    std::sync::LazyLock::new(|| {
        let mut map = bimap::BiMap::new();
        map.insert("IMAP4rev1", Capability::IMAP4rev1);
        map.insert("IMAP4rev2", Capability::IMAP4rev2);
        map.insert("JMAPACCESS", Capability::JMAPACCESS);
        map.insert("STARTTLS", Capability::STARTTLS);
        map.insert("LOGINDISABLED", Capability::LOGINDISABLED);
        map.insert("AUTH=PLAIN", Capability::AUTHPLAIN);
        map.insert("AUTH=LOGIN", Capability::AUTHLOGIN);
        map.insert("SASL-IR", Capability::SASLIR);
        map.insert("ID", Capability::ID);
        map.insert("IDLE", Capability::IDLE);
        map.insert("CONDSTORE", Capability::CONDSTORE);
        map.insert("QRESYNC", Capability::QRESYNC);
        map.insert("COMPRESS=DEFLATE", Capability::COMPRESSDEFLATE);
        map.insert("UIDPLUS", Capability::UIDPLUS);
        map.insert("ENABLE", Capability::ENABLE);
        map.insert("MOVE", Capability::MOVE);
        map.insert("SPECIAL-USE", Capability::SPECIALUSE);
        map.insert("BINARY", Capability::BINARY);
        map.insert("LITERAL+", Capability::LITERALPLUS);
        map.insert("LITERAL-", Capability::LITERALMINUS);
        map.insert("UTF8=ACCEPT", Capability::UTF8ACCEPT);
        map.insert("UTF8=ONLY", Capability::UTF8ONLY);
        map.insert("NAMESPACE", Capability::NAMESPACE);
        map.insert("X-GM-EXT-1", Capability::XGMEXT1);
        map.insert("X-APPLE-PUSH-SERVICE", Capability::XAPPLEPUSHSERVICE);
        map
    });

#[macro_export]
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
    pub fn as_str(&self) -> &'static str {
        CAPABILITIES_MAP.get_by_right(self).copied().expect("no string representation for capability")
    }

    pub fn from_str(s: &str) -> Option<Self> {
        CAPABILITIES_MAP
            .get_by_left(s)
            .copied()
    }

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
}

impl fmt::Display for Capability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Clone, Copy)]
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

impl std::fmt::Display for CapabilitiesList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let vec = self.to_vec();
        let strs: Vec<&str> = vec.iter().map(|c| c.as_str()).collect();
        write!(f, "{}", strs.join(" "))
    }
}

impl From<async_imap::types::Capabilities> for CapabilitiesList {
    fn from(async_imap_caps: async_imap::types::Capabilities) -> Self {
        let mut cap = CapabilitiesList::new();
        for cap_name in async_imap_caps.iter() {
            let parsed = match cap_name {
                async_imap::types::Capability::Imap4rev1 => Capability::from_str("IMAP4rev1"),
                async_imap::types::Capability::Auth(s) => Capability::from_str(s.as_str()),
                async_imap::types::Capability::Atom(s) => Capability::from_str(s.as_str()),
            };
            if let Some(c) = parsed {
                cap.set(c);
            } else {
                eprintln!("Unknown capability: {:?}", cap_name);
            }
        }
        cap
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_str_to_capability_mapping() {
        assert_eq!(Capability::from_str("IMAP4rev1"), Some(Capability::IMAP4rev1));
        assert_eq!(Capability::from_str("IMAP4rev2"), Some(Capability::IMAP4rev2));
        assert_eq!(Capability::from_str("JMAPACCESS"), Some(Capability::JMAPACCESS));
        assert_eq!(Capability::from_str("STARTTLS"), Some(Capability::STARTTLS));
        assert_eq!(Capability::from_str("LOGINDISABLED"), Some(Capability::LOGINDISABLED));
        assert_eq!(Capability::from_str("AUTH=PLAIN"), Some(Capability::AUTHPLAIN));
        assert_eq!(Capability::from_str("PLAIN"), Some(Capability::AUTHPLAIN));
        assert_eq!(Capability::from_str("AUTH=LOGIN"), Some(Capability::AUTHLOGIN));
        assert_eq!(Capability::from_str("LOGIN"), Some(Capability::AUTHLOGIN));
        assert_eq!(Capability::from_str("AUTH=XOAUTH2"), Some(Capability::AUTHOAUTH2));
        assert_eq!(Capability::from_str("XOAUTH2"), Some(Capability::AUTHOAUTH2));
        assert_eq!(Capability::from_str("AUTH=OAUTHBEARER"), Some(Capability::AUTHOAUTH2));
        assert_eq!(Capability::from_str("OAUTHBEARER"), Some(Capability::AUTHOAUTH2));
        assert_eq!(Capability::from_str("SASL-IR"), Some(Capability::SASLIR));
        assert_eq!(Capability::from_str("ID"), Some(Capability::ID));
        assert_eq!(Capability::from_str("IDLE"), Some(Capability::IDLE));
        assert_eq!(Capability::from_str("CONDSTORE"), Some(Capability::CONDSTORE));
        assert_eq!(Capability::from_str("QRESYNC"), Some(Capability::QRESYNC));
        assert_eq!(Capability::from_str("COMPRESS=DEFLATE"), Some(Capability::COMPRESSDEFLATE));
        assert_eq!(Capability::from_str("UIDPLUS"), Some(Capability::UIDPLUS));
        assert_eq!(Capability::from_str("ENABLE"), Some(Capability::ENABLE));
        assert_eq!(Capability::from_str("MOVE"), Some(Capability::MOVE));
        assert_eq!(Capability::from_str("SPECIAL-USE"), Some(Capability::SPECIALUSE));
        assert_eq!(Capability::from_str("BINARY"), Some(Capability::BINARY));
        assert_eq!(Capability::from_str("LITERAL+"), Some(Capability::LITERALPLUS));
        assert_eq!(Capability::from_str("LITERAL-"), Some(Capability::LITERALMINUS));
        assert_eq!(Capability::from_str("UTF8=ACCEPT"), Some(Capability::UTF8ACCEPT));
        assert_eq!(Capability::from_str("UTF8=ONLY"), Some(Capability::UTF8ONLY));
        assert_eq!(Capability::from_str("NAMESPACE"), Some(Capability::NAMESPACE));
        assert_eq!(Capability::from_str("X-GM-EXT-1"), Some(Capability::XGMEXT1));
        assert_eq!(Capability::from_str("XAPPLEPUSHSERVICE"), Some(Capability::XAPPLEPUSHSERVICE));
        assert_eq!(Capability::from_str("X-APPLE-PUSH-SERVICE"), Some(Capability::XAPPLEPUSHSERVICE));
    }

    #[test]
    fn test_capability_to_str_mapping() {
        let all_caps = [
            Capability::IMAP4rev1,
            Capability::IMAP4rev2,
            Capability::JMAPACCESS,
            Capability::STARTTLS,
            Capability::LOGINDISABLED,
            Capability::AUTHPLAIN,
            Capability::AUTHLOGIN,
            Capability::AUTHOAUTH2,
            Capability::SASLIR,
            Capability::ID,
            Capability::IDLE,
            Capability::CONDSTORE,
            Capability::QRESYNC,
            Capability::COMPRESSDEFLATE,
            Capability::UIDPLUS,
            Capability::ENABLE,
            Capability::MOVE,
            Capability::SPECIALUSE,
            Capability::BINARY,
            Capability::LITERALPLUS,
            Capability::LITERALMINUS,
            Capability::UTF8ACCEPT,
            Capability::UTF8ONLY,
            Capability::NAMESPACE,
            Capability::XGMEXT1,
            Capability::XAPPLEPUSHSERVICE,
        ];

        for cap in all_caps {
            let str_repr = cap.as_str();
            assert!(!str_repr.is_empty(), "Missing string mapping for {:?}", cap);
            let parsed_cap = Capability::from_str(str_repr);
            assert_eq!(parsed_cap, Some(cap), "Bidirectional mapping failed for {:?} -> {}", cap, str_repr);
            assert_eq!(cap.to_string(), str_repr);
        }
    }

    #[test]
    fn test_capabilities_list_display() {
        let list = caps!(Capability::IMAP4rev1, Capability::IDLE, Capability::ENABLE);
        let s = list.to_string();
        assert!(s.contains("IMAP4rev1"));
        assert!(s.contains("IDLE"));
        assert!(s.contains("ENABLE"));
    }
}
