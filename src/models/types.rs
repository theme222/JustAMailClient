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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Service {
    YAHOO,
    AOL,
    GMAIL,
    OUTLOOK,
    ICLOUD,
    FASTMAIL,
    YANDEX,
    CUSTOM(String)
} 

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServiceConfig {
    pub service: Service,
    pub fetch_server: &'static str,
    pub fetch_port: u16,
    pub push_server: &'static str,
    pub push_port: u16,
    pub auth_method: AuthMethod,
    pub encryption_method: EncryptionMethod,
}

pub type CredentialID = u64;
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Credentials {
    pub service: Service,
    pub login: String,
    pub secret: String,
    pub fetch_server: String,
    pub fetch_port: u16,
    pub push_server: String,
    pub push_port: u16,
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

impl From<Credentials> for crate::srv::db::AccountKey {
    fn from(creds: Credentials) -> Self {
        let (local_part, domain) = creds.login.split_once('@').unwrap_or((&creds.login, ""));
        crate::srv::db::AccountKey::LOCALPARTDOMAIN(local_part.into(), domain.into())
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
pub type JSONString = String;
pub type JSONB = Vec<u8>;

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
        use async_imap::types::NameAttribute::*;
        use MailboxAttr::*;
        match attr {
            NoInferiors => NOINFERIORS,
            NoSelect => NOSELECT,
            Marked => MARKED,
            Unmarked => UNMARKED,
            All => ALL,
            Archive => ARCHIVE,
            Drafts => DRAFTS,
            Flagged => FLAGGED,
            Junk => JUNK,
            Sent => SENT,
            Trash => TRASH,
            Extension(cow) => CUSTOM(cow.to_string()),
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
    pub fn flag_string(flags: &Vec<Self>) -> String {
        let mut result_str = String::new();

        for flag in flags {
            if !result_str.is_empty() {
                result_str.push_str(" ");
            }

            use MailFlag::*;
            match flag {
                SEEN => result_str.push_str("\\SEEN"),
                ANSWERED => result_str.push_str("\\ANSWERED"),
                FLAGGED => result_str.push_str("\\FLAGGED"),
                DELETED => result_str.push_str("\\DELETED"),
                DRAFT => result_str.push_str("\\DRAFT"),
                RECENT => result_str.push_str("\\RECENT"),
                MAYCREATE => result_str.push_str("\\MAYCREATE"),
                CUSTOM(custom) => result_str.push_str(custom),
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

pub type ActionId = u64;