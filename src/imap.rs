use std::error::Error;

use async_native_tls::TlsStream;
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};
use futures::{Stream, StreamExt, TryStreamExt};

use bitflags::bitflags;
use crate::models::*;

type Mailbox = async_imap::types::Mailbox;
type StreamResult<'a, T> = std::pin::Pin<Box<dyn Stream<Item = Result<T, async_imap::error::Error>> + 'a + Send>>;

#[derive(Clone)]
pub struct ImapCredentials {
    pub username: String,
    pub password: String,
    pub server: String
}

pub struct SeqRange { // Zero Indexed. Negative values count from the end of the mailbox
    pub start: i32,
    pub end: i32, 
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MailFlag<'a> {
    SEEN,
    ANSWERED,
    FLAGGED,
    DELETED,
    DRAFT,
    RECENT,
    CUSTOM(&'a str),
}

impl<'a> MailFlag<'a> {
    pub fn flag_string(flags: &Vec<MailFlag<'a>>) -> String {
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
                MailFlag::CUSTOM(custom) => result_str.push_str(custom),
            }
        }

        format!("({})", result_str)
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

// impl<'a> FetchType<'a> {
//     pub fn all() -> Vec<FetchType<'a>> {
//         vec![
//             FetchType::UID,
//             FetchType::BODYSTRUCTURE,
//             FetchType::ENVELOPE,
//             FetchType::FLAGS,
//             FetchType::INTERNALDATE,
//             FetchType::RFC822SIZE,
//             FetchType::BODYPEEKSECTION("", ""),
//             FetchType::BODYSECTION("", ""),
//         ]
//     }
// }

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

pub struct ImapSession {
    pub net: async_imap::Session<Compat<TlsStream<TcpStream>>>, 
    pub credentials: ImapCredentials,
    pub current_mailbox: Option<async_imap::types::Mailbox>,
    // pub account: EmailAccount,
} 

pub async fn connect_imap(cred: &ImapCredentials) -> Result<ImapSession, DynErr> {
    let imap_addr = (cred.server.clone(), 993);
    let tcp_stream = TcpStream::connect(&imap_addr).await?;
    let tls = async_native_tls::TlsConnector::new();
    let tls_stream = tls.connect(cred.server.clone(), tcp_stream).await?.compat();

    let client = async_imap::Client::new(tls_stream);
    Ok(ImapSession {
        net: client.login(&cred.username, &cred.password).await.map_err(|e| e.0)?,
        credentials: cred.clone(),
        current_mailbox: None,
        // account: EmailAccount {
        //     is_init: false,
        //     mailboxes: Vec::new(),
        // }
    })
}

pub async fn select_mailbox<'a>(session: &'a mut ImapSession, mailbox: &str) -> Result<(), DynErr> {
    let mailbox = session.net.select(mailbox).await?;
    session.current_mailbox = Some(mailbox);
    Ok(())
}

pub async fn ensure_mailbox_exists<'a>(session: &'a mut ImapSession) -> Result<(), DynErr> {
    if session.current_mailbox.is_none() {  select_mailbox(session, "INBOX").await?; }
    Ok(())
}

pub async fn fetch_stream<'a>(session: &'a mut ImapSession, ss: &SeqRange, fetch_types: &Vec<FetchType<'_>>) -> Result<StreamResult<'a, async_imap::types::Fetch>, DynErr> { 
    ensure_mailbox_exists(session);
    let fetch_query = FetchType::fetch_string(fetch_types);
    let fetch_result = session.net.fetch(ss.sequence_set_str(session.current_mailbox.as_ref().unwrap()), fetch_query).await?;
    Ok(Box::pin(fetch_result))
}

pub async fn delete<'a>(session: &'a mut ImapSession, ss: &SeqRange) -> Result<(), DynErr> {
    store(session, ss, StoreType::SET, &vec![MailFlag::DELETED]).await?;
    session.net.expunge().await?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreType {
    SET,
    UNSET,
}

pub async fn store<'a>(session: &'a mut ImapSession, ss: &SeqRange, store_type: StoreType, flags: &Vec<MailFlag<'_>>) -> Result<(), DynErr> {
    ensure_mailbox_exists(session);
    let flag_string = MailFlag::flag_string(flags);
    session.net.store(
        ss.sequence_set_str(session.current_mailbox.as_ref().unwrap()), 
        format!(
            "{}FLAGS.SILENT {}",  // Silent to avoid server echoing back the updated flags cause like why tho that kinda useless
            if store_type == StoreType::SET { "+" } else { "-" }, 
            flag_string
        )
    ).await?.try_collect::<Vec<_>>().await?; // Ensure the stream has been fully available (even though nothing is supposed to return lol)
    Ok(())
}

pub async fn parse_fetch_stream_all<'a>(stream: &mut StreamResult<'a, async_imap::types::Fetch>) -> Result<Vec<async_imap::types::Fetch>, DynErr> {
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