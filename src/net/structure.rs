use std::borrow::Cow;
use std::collections::*;

pub fn content_encoding_to_string(encoding: &imap_proto::ContentEncoding) -> String {
    use imap_proto::ContentEncoding::*;
    match encoding {
        SevenBit => "7bit".into(),
        EightBit => "8bit".into(),
        Binary => "binary".into(),
        QuotedPrintable => "quoted-printable".into(),
        Base64 => "base64".into(),
        Other(v) => v.clone().into(),
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

impl From<&imap_proto::BodyContentCommon<'_>> for BodyHeaders {
    fn from(value: &imap_proto::BodyContentCommon<'_>) -> Self {
        BodyHeaders {
            content_type: value.ty.ty.to_string(),
            content_subtype: value.ty.subtype.to_string(),
            content_params: HashMap::from(
                value.ty.params
                    .as_ref()
                    .unwrap_or(&Vec::new())
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect::<HashMap<String, String>>()
            ),
            disposition: None,
            disposition_params: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, Default)]
pub struct BodyContent {
    pub id: Option<String>,
    // pub md5: Option<String>,
    // pub description: Option<String>,
    pub transfer_encoding: String,
    pub size_octects: u32,
}

impl From<&imap_proto::BodyContentSinglePart<'_>> for BodyContent {
    fn from(value: &imap_proto::BodyContentSinglePart<'_>) -> Self {
        BodyContent {
            id: value.id.clone().map(|s| s.to_string()),
            transfer_encoding: content_encoding_to_string(&value.transfer_encoding),
            size_octects: value.octets,
        }
    }
}


// I'm ignoring BodyExtension for now
// My version of imap_proto::BodyStructure
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum MailBodyStructure { 
    Single {
        headers: BodyHeaders,
        content: BodyContent
    },
    Multi {
        headers: BodyHeaders,
        parts: Vec<MailBodyStructure>
    }
}

impl<'a> From<&imap_proto::BodyStructure<'a>> for MailBodyStructure {
    fn from(value: &imap_proto::BodyStructure<'a>) -> Self {
        use imap_proto::BodyStructure::*;
        use MailBodyStructure::*;
        match value {
            Basic { common, other, extension } => 
                { Single { headers: common.into(), content: other.into() } }
            Text { common, other, lines, extension } =>
                { Single { headers: common.into(), content: other.into() } }
            Message { common, other, envelope, body, lines, extension } =>
                { unimplemented!("Uh oh") }
            Multipart { common, bodies, extension } =>
                { Multi { headers: common.into(), parts: bodies.into_iter().map(|p| p.into()).collect(), } }
        }
    }
}

pub fn get_mailparse_body_parts<'a>(raw: &'a [u8], bs: &imap_proto::BodyStructure) -> (mailparse::ParsedContentType, String) {
    use imap_proto::BodyStructure::*;
    let (common, csp) = match bs {
        Message { common: _, other: _, envelope: _, body: _, lines: _, extension: _} => { panic!("Can not decode type Message") }
        Multipart {common: _, bodies: _, extension: _} => { panic!("Can not decode type Multipart") }
        Text { common: c, other: csp, extension: _, lines: _} => { (c, csp) }
        Basic { common: c, other: csp, extension: _ } => { (c, csp) }
    };

    let mimetype: String = format!("{}/{}", common.ty.ty, common.ty.subtype);
    let charset: String = common.ty.params
        .as_ref()
        .and_then(|params| params.iter().find(|v| v.0 == "charset"))
        .map(|v| v.clone().1.into())
        .unwrap_or("utf-8".into());
    let mut params: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    let ce: String = content_encoding_to_string(&csp.transfer_encoding);
    if let Some(bs_params) = &common.ty.params {
        for (k, v) in bs_params.iter() {
            params.insert(k.to_string(), v.to_string());
        }
    }


    let pct = mailparse::ParsedContentType {mimetype: mimetype, charset: charset, params};
    (pct, ce) // To the creators of mailparse I hope your pillow is warm tonight
}

pub fn body_as_string(body: &mailparse::body::Body) -> String {
    use mailparse::body::Body::*;
    match body {
        Base64(encoded_body) => encoded_body.get_decoded_as_string(),
        QuotedPrintable(encoded_body) => encoded_body.get_decoded_as_string(),
        SevenBit(text_body) => text_body.get_as_string(),
        EightBit(text_body) => text_body.get_as_string(),
        Binary(binary_body) => binary_body.get_as_string(),
    }.unwrap_or("!! Could not decode this section !!".into())
}

pub fn body_as_bytes(body: &mailparse::body::Body) -> Vec<u8> {
    use mailparse::body::Body::*;
    match body {
        Base64(encoded_body) => encoded_body.get_decoded(),
        QuotedPrintable(encoded_body) => encoded_body.get_decoded(),
        SevenBit(text_body) => Ok(text_body.get_raw().to_vec()),
        EightBit(text_body) => Ok(text_body.get_raw().to_vec()),
        Binary(binary_body) => Ok(binary_body.get_raw().to_vec()),
    }.unwrap_or("!! Could not decode this section !!".into())
}

// pub fn decode_as_bytes(raw: &[u8], bs: &imap_proto::BodyStructure) -> Vec<u8> {
//     let (pct, ce) = get_mailparse_body_parts(raw, bs);
//     let body = mailparse::body::Body::new(&raw, &pct, &Some(ce));
//     use mailparse::body::Body::*;
//     match body {
//         Base64(encoded_body) => encoded_body.get_decoded(),
//         QuotedPrintable(encoded_body) => encoded_body.get_decoded(),
//         SevenBit(text_body) => Ok(text_body.get_raw().to_vec()),
//         EightBit(text_body) => Ok(text_body.get_raw().to_vec()),
//         Binary(binary_body) => Ok(binary_body.get_raw().to_vec()),
//     }.unwrap_or("!! Could not decode this section !!".into())
// }

pub fn get_preview_from_partial(raw_partial: &[u8], bs: &imap_proto::BodyStructure) -> String {
    let parsed = mailparse::parse_mail(raw_partial);  

    if let Err(e) = parsed { return "!! Could not parse this body !!".into(); };
    let parsed = parsed.unwrap();

    let mut result_str = String::new();
    
    use mailparse::body::Body::*;
    for part in parsed.parts() { 
        if part.ctype.mimetype == "text/plain" { // Only care about sections that are text/plain (they usually exist since most html parts have a text/plain fallback from mixed/alternative)
            result_str.push_str(&body_as_string(&part.get_body_encoded()));
        }
    }
    
    result_str
}