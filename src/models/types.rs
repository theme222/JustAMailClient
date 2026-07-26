#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum Attempt {
    ONCE,
    REPEAT,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AuthMethod {
    LOGIN,
    AUTHPLAIN,
    AUTHLOGIN, // Yes theres a difference.
    AUTHOAUTH2,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EncryptionMethod {
    SSLTLS,
    STARTTLS,
}

pub type CredentialID = u64;
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Credentials {
    pub login: String,
    pub secret: String,
    pub fetch_server: String,
    pub push_server: String,
    pub auth_method: AuthMethod,
    pub encryption_method: EncryptionMethod,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("login", &self.login)
            .finish()
    }
}

// General status
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Status {
    ALIVE,
    BUSY,
    CONNECTING,
    FAILED,
}

pub type MailboxName = String;

// TODO: Make models.rs a directory and do stuff :D
#[derive(Debug, Clone, Default)]
pub struct Message {
    pub id: i64,
    pub account_id: i64, 
    pub last_sync_time: i64,
    pub last_query_time: Option<i64>,
    pub flags: Vec<MailFlag>, 
    pub size: i64,
    pub internal_date: i64, 
    pub bodystructure: MailBodyStructure, // Jsonb 
    pub imap_uid: Option<i64>, 
    pub modseq: Option<i64>,
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
    pub body_preview: String, 
    pub body_raw: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Default)]
pub struct Account {
    pub id: i64,
    pub local_part: String,
    pub domain: String,
    pub fetch_server: String,
    pub push_server: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MailboxAttr {
    NOINFERIORS,
    NOSELECT,
    MARKED,
    UNMARKED,
    ALL,
    ARCHIVE,
    DRAFTS,
    FLAGGED,
    JUNK,
    SENT,
    TRASH,
    CUSTOM(String),
}

impl<'a> From<&'a async_imap::types::NameAttribute<'a>> for MailboxAttr {
    fn from(attr: &'a async_imap::types::NameAttribute) -> Self {
        match attr {
            async_imap::types::NameAttribute::NoInferiors => MailboxAttr::NOINFERIORS,
            async_imap::types::NameAttribute::NoSelect => MailboxAttr::NOSELECT,
            async_imap::types::NameAttribute::Marked => MailboxAttr::MARKED,
            async_imap::types::NameAttribute::Unmarked => MailboxAttr::UNMARKED,
            async_imap::types::NameAttribute::All => MailboxAttr::ALL,
            async_imap::types::NameAttribute::Archive => MailboxAttr::ARCHIVE,
            async_imap::types::NameAttribute::Drafts => MailboxAttr::DRAFTS,
            async_imap::types::NameAttribute::Flagged => MailboxAttr::FLAGGED,
            async_imap::types::NameAttribute::Junk => MailboxAttr::JUNK,
            async_imap::types::NameAttribute::Sent => MailboxAttr::SENT,
            async_imap::types::NameAttribute::Trash => MailboxAttr::TRASH,
            async_imap::types::NameAttribute::Extension(cow) => MailboxAttr::CUSTOM(cow.to_string()),
            _ => todo!("New attribute has not been implemented {:?}", attr),
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


#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, Default)]
pub struct BodyHeaders {
    pub content_type: String,
    pub content_subtype: String,
    pub content_params: std::collections::HashMap<String, String>,
    pub disposition: Option<String>,
    pub disposition_params: std::collections::HashMap<String, String>,
    // pub language: Vec<String>,
    // pub location: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, Default)]
pub struct BodyContent {
    pub id: Option<String>,
    // pub md5: Option<String>,
    // pub description: Option<String>,
    pub transfer_encoding: String,
    pub size_octects: u32,
}

// I'm ignoring BodyExtension for now
// My version of imap_proto::BodyStructure
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum MailBodyStructure { 
    Single {
        headers: BodyHeaders,
        content: BodyContent,
        part_spec: Vec<u32>,
    },
    Multi {
        headers: BodyHeaders,
        parts: Vec<MailBodyStructure>,
        part_spec: Vec<u32>,
    }
}

impl MailBodyStructure {
    pub fn from_imap_proto_rec(value: &imap_proto::BodyStructure, section_id: Vec<u32>) -> Self {
        use imap_proto::BodyStructure::*;
        use MailBodyStructure::*;
        match value {
            Basic { common, other, extension } =>  { 
                Single {  headers: common.into(),  content: other.into(), part_spec: section_id } 
            }
            Text { common, other, lines, extension } => { 
                Single {  headers: common.into(),  content: other.into(), part_spec: section_id } 
            }
            Message { common, other, envelope, body, lines, extension } =>
                { todo!("Uh oh") }
            Multipart { common, bodies, extension } => {
                Multi { 
                    headers: common.into(),
                    parts: bodies
                        .into_iter()
                        .enumerate()
                        .map(|(i, p)| {
                            let mut section_id = section_id.clone();
                            section_id.push((i+1).try_into().unwrap());
                            MailBodyStructure::from_imap_proto_rec(p, section_id)
                        })
                        .collect(),
                    part_spec: section_id,
                } 
            }
        }
    }

    pub fn headers(&self) -> &BodyHeaders {
        match self {
            MailBodyStructure::Single { headers, .. } => headers,
            MailBodyStructure::Multi { headers, .. } => headers,
        }
    }

    pub fn part_spec(&self) -> &Vec<u32> {
        match self {
            MailBodyStructure::Single { part_spec, .. } => part_spec,
            MailBodyStructure::Multi { part_spec, .. } => part_spec,
        }
    }

    pub fn part_spec_str(&self) -> String {
        match self {
            MailBodyStructure::Single { part_spec, .. } => part_spec,
            MailBodyStructure::Multi { part_spec, .. } => part_spec,
        }.iter().map(ToString::to_string).collect::<Vec<_>>().join(".")
    }

    pub fn get_total_size(&self) -> u32 {
        use crate::net::structure::*;
        let mut sum = 0;
        for bs in self.clone().into_iter() {
            if let MailBodyStructure::Single { headers, content, part_spec } = bs {
                sum += content.size_octects;
            }  
        }
        sum
    }

    pub fn is_leaf_node(&self) -> bool {
        matches!(self, MailBodyStructure::Single { .. })
    }
}

impl Default for MailBodyStructure {
    fn default() -> Self {
        MailBodyStructure::Single {
            headers: BodyHeaders::default(),
            content: BodyContent::default(),
            part_spec: vec![],
        }
    }
}

// TODO: I am really sad to say this but you must make sure every field that isn't an important identifier be Optional so that we can do COALESCE to update the fields that we know the new values of
#[derive(Clone)]
pub struct Mailbox {
    pub name: String,
    pub attrs: Vec<MailboxAttr>,
    pub exists: u32,
    pub recent: u32,
    pub unseen: Option<u32>,
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

pub type ActionId = u64;