use std::error::Error;

use async_imap::extensions::idle::IdleResponse::{ManualInterrupt, NewData, Timeout};
use async_native_tls::TlsStream;
use tokio::{net::TcpStream};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};
use futures::{Stream, StreamExt, TryStreamExt};

use bitflags::bitflags;
use crate::models::*;

// type Mailbox = async_imap::types::Mailbox;
type StreamResult<'a, T> = std::pin::Pin<Box<dyn Stream<Item = Result<T, async_imap::error::Error>> + 'a + Send>>;

pub struct SeqRange { // Zero Indexed. Negative values count from the end of the mailbox
    pub start: i32,
    pub end: i32, 
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
            async_imap::types::Flag::Custom(custom) => MailFlag::CUSTOM(custom.clone().into_owned()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchType<'a> { // Ignoring types that are equivalent / older with no use case
    UID,
    BODYSTRUCTURE,
    ENVELOPE,
    FLAGS,
    INTERNALDATE,
    RFC822SIZE,
    BODYPEEKSECTION(&'a str, &'a str),
    BODYSECTION(&'a str, &'a str),
}

impl<'a> FetchType<'a> {
    
    pub fn fetch_string(fetch_type: &Vec<FetchType<'a>>) -> String {
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
                    if partial.len() == 0 { result_str.push_str(&format!("BODY.PEEK[{}]", section)) }
                    else { result_str.push_str(&format!("BODY.PEEK[{}]<{}>", section, partial)) }
                }
                FetchType::BODYSECTION(section, partial) => {
                    if partial.len() == 0 { result_str.push_str(&format!("BODY[{}]", section)) }
                    else { result_str.push_str(&format!("BODY[{}]<{}>", section, partial)) }
                }
            }
        }
    
        if fetch_type.len() > 1 { result_str = format!("({})", result_str); }
        return result_str
    }

    
}

impl SeqRange {
    
    pub fn first() -> Self {
        Self { start: 0, end: 0 }
    }

    pub fn last() -> Self {
        Self { start: -1, end: -1 }
    }

    pub fn all() -> Self {
        Self { start: 0, end: -1 }
    }

    pub fn sequence_set_str(&self, mailbox: &Mailbox) -> String {
        let size = mailbox.exists;
        let start = if self.start < 0 { (size as i32) + self.start + 1 } else { self.start + 1 };
        let start = start.clamp(1, size as i32);
        let end = if self.end < 0 { (size as i32) + self.end + 1 } else { self.end + 1 };
        let end = end.clamp(1, size as i32);
        format!("{}:{}", start, end)
    }
    
}

pub struct EmailAccount {
    pub is_init: bool,
    pub mailboxes: Vec<async_imap::types::Mailbox>,
}

#[derive(Debug, Clone)]
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

impl PartialEq for Mailbox {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Mailbox {}

impl From<async_imap::types::Mailbox> for Mailbox {
    fn from(mailbox: async_imap::types::Mailbox) -> Self {
        Mailbox {
            name: String::new(),
            flags: mailbox.flags.into_iter().map(MailFlag::from).collect(),
            exists: mailbox.exists,
            recent: mailbox.recent,
            unseen: mailbox.unseen,
            permanent_flags: mailbox.permanent_flags.into_iter().map(MailFlag::from).collect(),
            uid_next: mailbox.uid_next,
            uid_validity: mailbox.uid_validity,
            highest_modseq: mailbox.highest_modseq,
        }
    }
}

pub struct ImapSession {
    pub net_session: Option<async_imap::Session<Compat<TlsStream<TcpStream>>>>, 
    pub current_mailbox: Option<Mailbox>,
    // pub account: EmailAccount,
} 

impl ImapSession {
    
    pub async fn new(cred: &Credentials) -> Result<Self> {
        let imap_addr = (cred.fetch_server.clone(), 993);
        let tcp_stream = TcpStream::connect(&imap_addr).await?;
        let tls = async_native_tls::TlsConnector::new();
        let tls_stream = tls.connect(cred.fetch_server.clone(), tcp_stream).await?.compat();
    
        let client = async_imap::Client::new(tls_stream);
        Ok(ImapSession {
            net_session: Some(client.login(&cred.login, &cred.secret).await.map_err(|e| e.0)?),
            current_mailbox: None,
            // account: EmailAccount {
            //     is_init: false,
            //     mailboxes: Vec::new(),
            // }
        })
    }
    
    pub fn get_net(&mut self) -> &mut async_imap::Session<Compat<TlsStream<TcpStream>>> {
        self.net_session.as_mut().expect("Imap session unavailable")
    }
    
    pub async fn select_mailbox<'a>(self: &'a mut Self, mailbox: &str) -> Result<()> {
        if let Some(curr_mb) = &self.current_mailbox { if curr_mb.name == mailbox { return Ok(()); } }
        let mailbox = self.get_net().select(mailbox).await?;
        self.current_mailbox = Some(mailbox.into());
        Ok(())
    }
    
    pub async fn ensure_mailbox_exists<'a>(self: &'a mut Self) -> Result<()> {
        if self.current_mailbox.is_none() { Self::select_mailbox(self, "INBOX").await?; }
        Ok(())
    }
    
    pub async fn fetch_stream<'a>(self: &'a mut Self, ss: &SeqRange, fetch_types: &Vec<FetchType<'_>>) -> Result<StreamResult<'a, async_imap::types::Fetch>> { 
        self.ensure_mailbox_exists().await?;
        let fetch_query = FetchType::fetch_string(fetch_types);
        let sss = ss.sequence_set_str(self.current_mailbox.as_ref().unwrap());
        let fetch_result = self.get_net().fetch(&sss, fetch_query).await?;
        Ok(Box::pin(fetch_result))
    }
    
    pub async fn delete<'a>(self: &'a mut Self, ss: &SeqRange) -> Result<()> {
        self.store(ss, '+', &vec![MailFlag::DELETED]).await?;
        self.get_net().expunge().await?;
        Ok(())
    }
    
    pub async fn store<'a>(self: &'a mut Self, ss: &SeqRange, store_type: char, flags: &Vec<MailFlag>) -> Result<()> {
        if store_type != '+' && store_type != '-' { anyhow::bail!("store_type must be '+' or '-'"); }
        self.ensure_mailbox_exists().await?;
        let flag_string = MailFlag::flag_string(flags);
        let sss = ss.sequence_set_str(self.current_mailbox.as_ref().unwrap());
        self.get_net().store(
            sss,
            format!(
                "{}FLAGS.SILENT {}",  // Silent to avoid server echoing back the updated flags cause like why tho that kinda useless
                store_type, 
                flag_string
            )
        ).await?.try_collect::<Vec<_>>().await?; // Ensure the stream is fully available (even though nothing is supposed to return lol)
        Ok(())
    }
    
    pub async fn append<'a>(self: &'a mut Self, folder: &str, flags: &Vec<MailFlag>, date: Option<&str>, body: String) -> Result<()> {
        let flag_string = MailFlag::flag_string(flags);
        let flags_arg = if flags.len() == 0 { None } else { Some(flag_string.as_str()) };
        let date_args = if date.is_none() { None } else { Some(date.unwrap()) };
        self.get_net().append(folder, flags_arg, date_args, body.as_bytes()).await?;
        Ok(())
    }
    
    pub async fn idle<'a>(mut self: Self) -> Result<()> {
        Self::ensure_mailbox_exists(&mut self).await?;
    
        // 2. Initialize the IDLE command
        let idle_timeout = std::time::Duration::from_secs(29 * 60);
        let mut handle = self.net_session.take().expect("net_session is None").idle();
        handle.init().await?;
        
        loop {
            let (idle_wait_future, stop_idle) = handle.wait_with_timeout(idle_timeout);
            let idle_response = idle_wait_future.await?; 
            // This is what it returned btw:
            // NewData(ResponseData { raw: 4096, response: MailboxData(Exists(8)) })
            // what they hell am I supposed to do with this shit?
            match &idle_response {
                NewData(x) => { },
                ManualInterrupt => { break; },
                Timeout => { continue; },
            }
            
            println!("IDLE response: {:?}", idle_response);
        }
    
        self.net_session = Some(handle.done().await?);
        Ok(())
    }
    
    pub async fn parse_fetch_stream_all<'a>(stream: &mut StreamResult<'a, async_imap::types::Fetch>) -> Result<Vec<async_imap::types::Fetch>> {
        let mut results: Vec<async_imap::types::Fetch> = Vec::new();
    
        while let Some(mail_result) = stream.next().await {
            if let Err(e) = mail_result {
                eprintln!("Error while parsing fetch stream: {:?}", e);
                continue
            }
            else if let Ok(mail) = mail_result {
                results.push(mail);
            }
        }
    
        Ok(results)
    }

}

pub enum ImapSessionType {
    Idler(&'static str),
    Puller(&'static str),
    Actor(&'static str),
}

pub struct ImapManager { // One manager per account
    pub credentials: Credentials,
    pub idler: Option<ImapSession>,
    pub puller: Option<ImapSession>,
    pub actor: Option<ImapSession>,
}

impl ImapManager {
    
    pub async fn new(credentials: &Credentials) -> Result<Self> { 
        // A very simple model which will be very temporary for now
        let (idler, puller, actor) = tokio::join!(
            ImapSession::new(credentials),
            ImapSession::new(credentials),
            ImapSession::new(credentials),
        );

        Ok(Self {
            credentials: credentials.clone(),
            idler: idler.ok(),
            puller: puller.ok(),
            actor: actor.ok(),
        })
    }

    pub async fn get_session(&mut self, session_type: ImapSessionType) -> &mut ImapSession {
        let (session, mb) = match session_type {
            ImapSessionType::Idler(mb) => (self.idler.as_mut().unwrap(), mb),
            ImapSessionType::Puller(mb) => (self.puller.as_mut().unwrap(), mb),
            ImapSessionType::Actor(mb) => (self.actor.as_mut().unwrap(), mb),
        };

        session.select_mailbox(mb); // Obviously in the future this will probably be more complicated
        session
    }

    pub async fn get_owned_session(&mut self, session_type: ImapSessionType) -> ImapSession {
        let (mut session, mb) = match session_type {
            ImapSessionType::Idler(mb) => (self.idler.take().unwrap(), mb),
            ImapSessionType::Puller(mb) => (self.puller.take().unwrap(), mb),
            ImapSessionType::Actor(mb) => (self.actor.take().unwrap(), mb),
        };

        session.select_mailbox(mb); // Obviously in the future this will probably be more complicated
        session
    }

}
