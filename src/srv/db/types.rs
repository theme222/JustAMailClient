use crate::net::fetch::imap::address_to_string;
use crate::*;
use crate::models::*;

#[derive(Debug, Clone, Default)]
pub struct Message {
    pub id: i64,
    pub account_id: i64, 
    pub ty: Option<String>, 
    pub last_sync_time: i64,
    pub last_query_time: Option<i64>,
    pub flags: Vec<fetch::imap::MailFlag>, 
    pub size: i64,
    pub internal_date: i64, 
    pub bodystructure: structure::MailBodyStructure, // Jsonb 
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

impl From<async_imap::types::Fetch> for Message {
    fn from(mail: async_imap::types::Fetch) -> Self {
        
        let mut msg = Message::default();
        let size = mail.size.unwrap() as i64;
        let internal_date = mail.internal_date().unwrap().timestamp_millis();                    
        let body_bytes = mail.text().unwrap_or_default();
        let body_raw = if size > MAX_LISTFETCH_SIZE as i64 { None } else { Some(body_bytes) } .map(|b| b.to_vec());
        let env = mail.envelope().unwrap();
        
        msg.id = 0;
        msg.account_id = 1;
        msg.bodystructure = mail.bodystructure().unwrap().into();
        msg.last_sync_time = unix_timestamp();
        msg.imap_uid = Some(mail.uid.unwrap() as i64);
        msg.body_preview = net::structure::get_preview_from_partial(&body_bytes, mail.bodystructure().unwrap());
        msg.size = size;
        msg.flags = mail.flags().collect::<Vec<_>>().into_iter().map(|f| f.into()).collect::<Vec<fetch::imap::MailFlag>>();
        msg.modseq = mail.modseq.map(|s| s as i64) ;
        msg.rfc_message_id = env.message_id.as_deref().and_then(|m| rfc2047_decoder::decode(&m).ok());
        msg.body_raw = body_raw;
        msg.env_date = env.date.as_deref().and_then(|a| rfc2047_decoder::decode(&a).ok());
        msg.env_subject = env.subject.as_deref().and_then(|s| rfc2047_decoder::decode(&s).ok());
        msg.env_in_reply_to = env.in_reply_to.as_deref().and_then(|i| rfc2047_decoder::decode(&i).ok());
        msg.env_from = env.from.as_deref().map(|v| v.into_iter().map(|f| address_to_string(&f)).flatten().collect::<Vec<_>>());
        msg.env_reply_to = env.reply_to.as_deref().map(|v| v.into_iter().map(|f| address_to_string(&f)).flatten().collect::<Vec<_>>());
        msg.env_to = env.to.as_deref().map(|v| v.into_iter().map(|f| address_to_string(&f)).flatten().collect::<Vec<_>>());
        msg.env_cc = env.cc.as_deref().map(|v| v.into_iter().map(|f| address_to_string(&f)).flatten().collect::<Vec<_>>());
        msg.env_bcc = env.bcc.as_deref().map(|v| v.into_iter().map(|f| address_to_string(&f)).flatten().collect::<Vec<_>>());

        msg
        
    }
}

#[derive(Debug, Clone, Default)]
pub struct Account {
    pub id: i64,
    pub local_part: String,
    pub domain: String,
    pub ty: Option<String>,
    pub fetch_server: String,
    pub push_server: String,
}

#[derive(Debug, Clone, Default)]
pub struct Mailbox {
    pub id: i64,
    pub account_id: i64,
    pub name: String,
    pub uid_validity: i64,
    pub flags: Vec<fetch::imap::MailFlag>,
    pub ty: Option<String>,
}