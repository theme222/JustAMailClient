use std::error::Error;

use async_imap::extensions::idle::IdleResponse::{ManualInterrupt, NewData, Timeout};
use async_native_tls::TlsStream;
use futures::{Stream, StreamExt, TryStreamExt};
use imap_proto::Response::MailboxData;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use crate::models::Status::FAILED;
use crate::net::fetch::imap::ResponseCode::{LIMIT, UNAVAILABLE};
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MailFlag {
    SEEN,
    ANSWERED,
    FLAGGED,
    DELETED,
    DRAFT,
    RECENT,
    MAYCREATE,
    CUSTOM(String),
}

impl MailFlag {
    pub fn flag_string(flags: &Vec<MailFlag>) -> String {
        let mut result_str = String::new();

        for flag in flags {
            if !result_str.is_empty() {
                result_str.push_str(" ");
            }

            match flag {
                MailFlag::SEEN => result_str.push_str("\\SEEN"),
                MailFlag::ANSWERED => result_str.push_str("\\ANSWERED"),
                MailFlag::FLAGGED => result_str.push_str("\\FLAGGED"),
                MailFlag::DELETED => result_str.push_str("\\DELETED"),
                MailFlag::DRAFT => result_str.push_str("\\DRAFT"),
                MailFlag::RECENT => result_str.push_str("\\RECENT"),
                MailFlag::MAYCREATE => result_str.push_str("\\MAYCREATE"),
                MailFlag::CUSTOM(custom) => result_str.push_str(custom),
            }
        }

        format!("({})", result_str)
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

#[derive(Clone)]
pub struct Mailbox {
    pub name: String,
    pub flags: Vec<MailFlag>,
    pub exists: u32,
    pub recent: u32,
    pub unseen: Option<u32>,
    pub permanent_flags: Vec<MailFlag>,
    pub uid_next: Option<u32>,
    pub uid_validity: Option<u32>,
    pub highest_modseq: Option<u64>,
}

impl std::fmt::Debug for Mailbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mailbox")
            .field("name", &self.name)
            .field("exists", &self.exists)
            .field("recent", &self.recent)
            .field("uid_next", &self.uid_next)
            .field("uid_validity", &self.uid_validity)
            .finish()
    }
}

impl PartialEq for Mailbox {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Mailbox {}

impl From<&async_imap::types::Mailbox> for Mailbox {
    fn from(mailbox: &async_imap::types::Mailbox) -> Self {
        Mailbox {
            name: String::new(),
            flags: mailbox
                .flags
                .clone()
                .into_iter()
                .map(MailFlag::from)
                .collect(),
            exists: mailbox.exists,
            recent: mailbox.recent,
            unseen: mailbox.unseen,
            permanent_flags: mailbox
                .permanent_flags
                .clone()
                .into_iter()
                .map(MailFlag::from)
                .collect(),
            uid_next: mailbox.uid_next,
            uid_validity: mailbox.uid_validity,
            highest_modseq: mailbox.highest_modseq,
        }
    }
}

type MailboxName = String;

#[derive(Debug, Clone)]
pub enum ImapSessionCommandType {
    // SELECT(String),
    LISTFETCH(MailboxName, SeqRange),
    // PREFETCHSTRAT1(MailboxName, SeqRange),
    // PREFETCHSTRAT2(MailboxName, SeqRange),
    // FULLFETCH(MailboxName, SeqRange),
    // PARTFETCH(MailboxName, SeqRange), // Probably will be the same interface as PREFETCHSTRAT2
    IDLE(MailboxName),
    NOOP,
    SHUTDOWN,
    // TODO: insert more here
}

impl ImapSessionCommandType {
    pub fn get_required_mailbox(&self) -> Option<&MailboxName> {
        match self {
            ImapSessionCommandType::LISTFETCH(mailbox, _) => Some(mailbox),
            ImapSessionCommandType::IDLE(mailbox) => Some(mailbox),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ImapSessionCommand {
    pub id: u64,
    pub ty: ImapSessionCommandType,
}

impl ImapSessionCommand {
    pub fn weight(&self) -> f64 {
        use ImapSessionCommandType::*;

        // Get the expected weight (in units of multiples of 1 rtt)
        match &self.ty {
            LISTFETCH(_, r) => {
                r.get_total_items(MAILBOX_INBOX_ASSUMED_SIZE) as f64 * MAX_LISTFETCH_SIZE as f64
                    / MAX_PREFETCH_STRAT1_SIZE as f64
            } // Educated guess.
            IDLE(_) => NETSOCK_REFRESH_INTERVAL.as_secs_f64() / ASSUMED_LATENCY as f64 * 1000.0,
            NOOP => 1.0,
            SHUTDOWN => 1.0,
        }
    }

    pub fn get_mailbox_domain(&self) -> Option<&str> {
        // Get the mailbox the command works on
        use ImapSessionCommandType::*;

        match &self.ty {
            LISTFETCH(mb, _) => Some(mb),
            IDLE(mb) => Some(mb),
            NOOP => None,
            SHUTDOWN => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ImapSessionId {
    pub s_id: u64,
    pub m_id: Credentials,
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

#[derive(Debug)]
pub enum ImapSessionError {
    ASYNCIMAPERROR(AsyncImapError), // Async imap / imap proto lib errors
    TLSERROR(async_native_tls::Error), // Encryption errors
    ABORTED,                        // The current command was aborted with abort.send(())
    TIMEOUT,                        // The current command timed out
    INVALIDRESPONSE(String),        // When the server wants to be naughty and sends us some bs
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
        cred: &Credentials,
    ) -> std::result::Result<async_imap::Session<Compat<TlsStream<TcpStream>>>, ImapSessionError>
    {
        let imap_addr = (cred.fetch_server.clone(), 993);
        let tcp_stream = TcpStream::connect(&imap_addr).await?;
        let tls = async_native_tls::TlsConnector::new();
        let tls_stream = tls
            .connect(cred.fetch_server.clone(), tcp_stream)
            .await?
            .compat();

        let client = async_imap::Client::new(tls_stream);
        Ok(client
            .login(&cred.login, &cred.secret)
            .await
            .map_err(|e| e.0)?)
    }

    pub async fn new(
        id: ImapSessionId,
    ) -> std::result::Result<(tokio::sync::mpsc::Sender<ImapSessionCommand>, tokio::sync::mpsc::Sender<()>), ImapSessionError> {
        let (sender, receiver) = tokio::sync::mpsc::channel::<ImapSessionCommand>(100);
        let (abort_sender, abort_recv) = tokio::sync::mpsc::channel::<()>(5);
        let session = ImapSession {
            id: id.clone(),
            net: Some(Self::get_client(&id.m_id).await?),
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
            action: IMAPUPDATE(STARTED(self.id.clone())),
        })
        .await;

        while let Some(command) = tokio::select!(
            command = self.receiver.recv() => command,
            command = tokio::time::sleep(NETSOCK_REFRESH_INTERVAL) => Some(ImapSessionCommand { id: u64::MAX, ty: NOOP }),
            command = self.abort.recv() => Some(ImapSessionCommand { id: u64::MAX, ty: ImapSessionCommandType::SHUTDOWN }),
        ) {
            println!("Imap Session running: {:?}", command);

            let res = match &command.ty {
                NOOP => self.noop().await,
                ImapSessionCommandType::SHUTDOWN => break,
                ImapSessionCommandType::LISTFETCH(mb, seq_range) => {
                    self.fetch(
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
                    )
                    .await
                }
                IDLE(mb) => self.idle(mb.into()).await,
                _ => unimplemented!("{:?}", command.ty),
            };

            if let Ok(_) = res {
                Senders::net(NetMessage {
                    action: IMAPUPDATE(CMDSUCCESS(self.id.clone(), command.id)),
                })
                .await;
                continue;
            }

            if let Err(err) = res {
                eprintln!("Error while executing command {:?}: {:?}", command.ty, err);
                use AsyncImapError::*;
                use ImapSessionError::*;
                let session_update = match &err {
                    ASYNCIMAPERROR(Io(error)) => CMDFAILURETRYAGAIN(self.id.clone(), command.id),
                    ASYNCIMAPERROR(Bad(_)) => CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id),
                    ASYNCIMAPERROR(No(_)) => CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id),
                    ASYNCIMAPERROR(ConnectionLost) => {
                        CMDFAILURETRYAGAIN(self.id.clone(), command.id)
                    }
                    ASYNCIMAPERROR(Parse(parse_error)) => {
                        CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id)
                    }
                    ASYNCIMAPERROR(Validate(validate_error)) => {
                        CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id)
                    }
                    ASYNCIMAPERROR(Append) => CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id),
                    ASYNCIMAPERROR(_) => CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id),
                    TIMEOUT => CMDFAILURETRYAGAIN(self.id.clone(), command.id),
                    INVALIDRESPONSE(_) => CMDFAILUREUNRECOVERABLE(self.id.clone(), command.id),
                    ABORTED => CMDFAILURETRYAGAIN(self.id.clone(), command.id),
                    TLSERROR(error) => unreachable!(), // Probably idk
                };
                Senders::net(NetMessage {
                    action: IMAPUPDATE(session_update),
                })
                .await;

                if let ABORTED = err {
                    break;
                }
            }

            // Check to see if we still own the network
            if self.net.is_none() {
                break;
            }
        }

        self.net.take(); // force drop the network either way
        Senders::net(NetMessage {
            action: IMAPUPDATE(SESSIONABORT(self.id.clone())),
        })
        .await;
    }

    pub fn get_net(&mut self) -> &mut async_imap::Session<Compat<TlsStream<TcpStream>>> {
        self.net.as_mut().unwrap()
    }

    pub fn take_net(&mut self) -> async_imap::Session<Compat<TlsStream<TcpStream>>> {
        self.net.take().unwrap()
    }

    pub fn return_net(&mut self, net: async_imap::Session<Compat<TlsStream<TcpStream>>>) {
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
        Ok(tokio::select! {
            _ = abort.recv() => { return Err(ImapSessionError::ABORTED); }
            _ = tokio::time::sleep(NETSOCK_TIMEOUT) => { return Err(ImapSessionError::TIMEOUT); }
            res = future => { res? }
        })
    }

    // NOOP: Do nothing and it won't fail (sometimes)
    pub async fn noop(&mut self) -> ImapSessionResult<()> {
        let (network, abort) = self.net_abort();
        let res = Self::call_with_abort(abort, network.noop()).await;
        res
    }

    // SELECT: Select a mailbox on the server.
    pub async fn select_mailbox<'a>(
        self: &'a mut Self,
        name: &str,
        force: bool,
    ) -> ImapSessionResult<()> {
        if let Some(curr_mb) = &self.current_mailbox {
            if curr_mb.name == name && !force {
                return Ok(());
            }
        }
        let (network, abort) = self.net_abort();
        let mailbox = Self::call_with_abort(abort, network.select_condstore(name)).await?;

        self.current_mailbox = Some((&mailbox).into());
        self.current_mailbox
            .as_mut()
            .map(|mut mb| mb.name = name.into());
        println!("Selected mailbox: {:?}", self.current_mailbox);
        Senders::srv(SrvMessage {
            action: srv::SrvAction::SYNCMAILBOX(self.current_mailbox.as_ref().unwrap().clone()),
        })
        .await;
        Ok(())
    }

    // FETCH: Fetch a stream of mails from the server.
    pub async fn fetch<'a>(
        self: &'a mut Self,
        mb: MailboxName,
        ss: &SeqRange,
        fetch_types: &Vec<FetchType>,
    ) -> ImapSessionResult<()> {
        self.select_mailbox(&mb, false).await?;
        let fetch_query = FetchType::fetch_string(fetch_types);
        let sss = ss.sequence_set_str(self.current_mailbox.as_ref().unwrap().exists);
        println!("Fetching: {} {}", sss, fetch_query);

        let (network, abort) = self.net_abort();
        let mut stream = Self::call_with_abort(abort, network.fetch(&sss, fetch_query)).await?; // The result is bounded to the network. If it errors, returning the network socket is impossible.

        let mut err: Option<AsyncImapError> = None;
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
            } else if let Ok(mail) = mail_result {
                Senders::srv(SrvMessage {
                    action: SrvAction::SYNCLISTEMAIL(mail),
                })
                .await;
            }
        }

        drop(stream); // stream bounded to the network so we gotta drop it first
        if let Some(e) = err {
            return Err(ImapSessionError::ASYNCIMAPERROR(e));
        } // due to limitations, we simply just return the last one found
        Ok(())
    }

    // DELETE: Mark mails as deleted and expunge them.
    pub async fn delete<'a>(
        self: &'a mut Self,
        mb: MailboxName,
        ss: &SeqRange,
    ) -> ImapSessionResult<()> {
        self.store(mb, ss, '+', &vec![MailFlag::DELETED]).await?;
        let (network, abort) = self.net_abort();
        Self::call_with_abort(abort, network.expunge()).await?;
        Ok(())
    }

    // STORE: Update flags of a mail.
    pub async fn store<'a>(
        self: &'a mut Self,
        mb: MailboxName,
        ss: &SeqRange,
        store_type: char,
        flags: &Vec<MailFlag>,
    ) -> ImapSessionResult<()> {
        if store_type != '+' && store_type != '-' {
            panic!("store_type must be '+' or '-'")
        }

        self.select_mailbox(&mb, false).await?;
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

        Ok(())
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
    pub async fn idle<'a>(&mut self, mb: MailboxName) -> ImapSessionResult<()> {
        self.select_mailbox(&mb, false).await?;

        // From this point onwards if it fails the network connection will be unrecoverable and has to be remade.

        let mut handle = self.take_net().idle();
        let init_res = handle.init().await?;
        let (idle_wait_future, aaa) = handle.wait_with_timeout(NETSOCK_REFRESH_INTERVAL);
        let res = Self::call_with_abort(&mut self.abort, idle_wait_future).await?;
        println!("idle result: {:?}", res);
        let res = match &res {
            ManualInterrupt => unreachable!(),
            Timeout => {
                Err(ImapSessionError::TIMEOUT)
            },
            NewData(response_data) => {
                use imap_proto::Response::*;
                // match response_data.borrow_dependent() {
                //     MailboxData(mailbox_datum) => todo!(),
                //     Expunge(seqnum) => todo!(),
                //     Fetch(seqnum, attribute_values) => todo!(),
                //     _ => return Err(ImapSessionErrors::INVALIDRESPONSE(format!("{:?}", response_data)))
                // }
                Ok(())
            }
        };

        let network = Self::call_with_abort(&mut self.abort, handle.done()).await?;
        self.return_net(network);
        res
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

impl ImapSessionState {
    pub async fn get_imap_session(
        id: ImapSessionId,
    ) -> Option<(tokio::sync::mpsc::Sender<ImapSessionCommand>, tokio::sync::mpsc::Sender<()>)> {
        use ImapSessionError::*;
        use async_imap::error::Error::*;
        let mut current_retry_delay = INITIAL_RETRY_DELAY;
        loop {
            match ImapSession::new(id.clone()).await {
                Ok((sender, abort_sender)) => {
                    return Some((sender, abort_sender));
                }
                Err(ASYNCIMAPERROR(Io(_)) | ASYNCIMAPERROR(ConnectionLost)) => {
                    // Try Again
                    eprintln!("Connection issue...");
                    tokio::time::sleep(current_retry_delay).await;
                    current_retry_delay = double_time_clamped(current_retry_delay);
                    continue;
                }
                Err(ASYNCIMAPERROR(Bad(s)) | ASYNCIMAPERROR(No(s))) => {
                    let res_code: ResponseCode = s.as_str().into();
                    eprintln!("Bad/No response: {}", s);
                    match res_code {
                        UNAVAILABLE => break, // Server is unavailable or we are being rate limited
                        LIMIT => break,
                        _ => break, // Something major broke
                    };
                }
                Err(ASYNCIMAPERROR(Parse(e))) => {
                    eprintln!("Error parsing server response: {}", e);
                    break;
                }
                Err(ASYNCIMAPERROR(Validate(e))) => {
                    eprintln!("Error parsing command inputs: {}", e);
                    break;
                }
                Err(TLSERROR(e)) => {
                    eprintln!("TLS error: {}", e);
                    break;
                }
                e => {
                    eprintln!("Unhandled error: {:?}", e);
                    break;
                }
            }
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
        let res = Self::get_imap_session(id).await;
        let success = res.is_some();
        self.running = false;
        self.failed = false;
        if let Some((to_session, to_abort)) = res {
            self.to_session = Some(to_session);
            self.to_abort = Some(to_abort);
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
        if !self.running {
            Status::CONNECTING
        } else if self.failed {
            Status::FAILED
        } else if self.pending_cmds.is_empty() {
            Status::ALIVE
        } else {
            Status::BUSY
        }
    }
}

pub struct ImapManager {
    // One manager per account
    pub credentials: Credentials,
    pub next_cmd_id: u64,
    pub next_session_id: u64,
    pub imap_session_states: std::collections::HashMap<ImapSessionId, ImapSessionState>, // Key is id
}

impl ImapManager {
    pub async fn new(credentials: Credentials) -> Result<Self> {
        let mut manager = Self {
            credentials: credentials.clone(),
            next_cmd_id: 0,
            next_session_id: 0,
            imap_session_states: std::collections::HashMap::new(),
        };
        manager
            .create_session_states(credentials.clone(), MAX_IMAP_SESSIONS)
            .await;
        let active_session_count = manager.get_active_session_count();
        if active_session_count == 0 {
            return Err(anyhow::anyhow!("Failed to create any IMAP sessions"));
        } else if active_session_count == 1 {
            return Err(anyhow::anyhow!(
                "Imap session states is 1. This strategy is currently unimplemented."
            ));
        }

        manager
            .call_session(ImapSessionCommandType::IDLE("INBOX".into()))
            .await;
        Ok(manager)
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

    pub async fn rcv_session_update(&mut self, upd: SessionUpdate) {
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
            SESSIONABORT(isid) => {
                let state = self.imap_session_states.get_mut(&isid).unwrap();
                state.running = false;
                state.connect(isid.clone()).await; // Attempt a reconnect
            }
            CMDFAILURETRYAGAIN(isid, cmdid) => {
                let state = self.imap_session_states.get_mut(&isid).unwrap();
                state.failed = true;
                let cmd = state.rm_command(cmdid).unwrap();
                self.call_session(cmd.ty).await;
            }
            CMDFAILUREUNRECOVERABLE(isid, cmdid) => {
                let state = self.imap_session_states.get_mut(&isid).unwrap();
                state.failed = true;
                // TODO: Handle it ???
            }
        }
    }

    pub async fn create_session_states(&mut self, credentials: Credentials, count: usize) {
        // Try to create as many imap sessions per manager as possible
        let mut current_retry_delay = INITIAL_RETRY_DELAY;
        use AsyncImapError::*;
        use ImapSessionError::*;
        use ResponseCode::*;

        for i in 0..count {
            let isid = ImapSessionId {
                s_id: self.next_session_id as u64,
                m_id: credentials.clone(),
            };
            let mut state = ImapSessionState::new(isid.clone());
            if !state.connect(isid.clone()).await { break; }
            self.imap_session_states.insert(isid, state);

            self.next_session_id += 1;
        }
    }

    pub async fn call_session(&mut self, cmd: ImapSessionCommandType) {
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
                id: self.next_cmd_id,
                ty: cmd,
            })
            .await;
        self.next_cmd_id += 1;
    }

    pub async fn shutdown(&mut self) {
        for (_, state) in self.imap_session_states.iter_mut() {
            state
                .add_command(ImapSessionCommand {
                    id: self.next_cmd_id,
                    ty: ImapSessionCommandType::SHUTDOWN,
                })
                .await;
            self.next_cmd_id += 1;
        }
    }
}
