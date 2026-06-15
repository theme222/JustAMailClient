mod imap;
mod models;

use models::*;

use crate::imap::FetchType;

#[tokio::main]
async fn main() {
    let res = runner().await;
    if let Err(e) = res { eprintln!("{}", e); }
}

async fn runner() -> Result<(), DynErr> {
    dotenvy::dotenv().ok();
    
    let imap_server = std::env::var("ICLOUD_SERVER").expect("ICLOUD_SERVER not set");
    let login = std::env::var("ICLOUD_EMAIL").expect("ICLOUD_EMAIL not set");
    let password = std::env::var("ICLOUD_PASSWORD").expect("ICLOUD_PASSWORD not set");

    let credentials = imap::ImapCredentials {
        username: login,
        password: password,
        server: imap_server
    };

    let mut client = imap::connect_imap(&credentials).await?;
    imap::select_mailbox(&mut client, "INBOX").await?;
    imap::delete(&mut client, &imap::SeqRange::last()).await?;
    
    
    async_imap::Session::logout(&mut client.net).await.expect("failed to logout");
    Ok(())
}


// let mut res = imap::fetch_stream(&mut client, &imap::SeqRange::last(), 
//     &vec![FetchType::UID, FetchType::BODYSTRUCTURE, FetchType::ENVELOPE, FetchType::FLAGS, FetchType::BODYPEEKSECTION("HEADER", ""), FetchType::BODYPEEKSECTION("TEXT", "")]
// ).await.expect("failed to fetch stream");
// let parsed = imap::parse_fetch_stream_all(&mut res).await.expect("failed to parse fetch stream");

// for mail in &parsed {
//     let parsed_header = 
//         mailparse::parse_headers( // haskell flashbacks
//             mail.header()
//                 .unwrap_or_default())
//                 .map_or(
//                     String::new(), 
//                     |hvec| hvec.0
//                                .into_iter()
//                                .map( 
//                                    |header_str| format!(
//                                        "{}: {}", header_str.get_key(), 
//                                        rfc2047_decoder::decode(header_str.get_value()).unwrap_or_default()
//                                    )
//                 )
//                 .reduce(|acc, header| format!("{}\n{}", acc, header))
//                 .unwrap()
//         );
//     let parsed_body = mailparse::parse_mail(mail.text().unwrap()).unwrap();
//     let mut body_parts = parsed_body.parts();
//     let envelope = mail.envelope().unwrap();
//     // let parsed_bodystructure = mail.bodystructure().map_or(String::new(), |b| b.get_value());
//     println!("Headers\n{}", parsed_header);
//     // println!("Envelope: {:#?}", envelope);
//     // println!("Body of mail: {:?}", parsed_body);
//     for part in body_parts {
//         println!("Part: {:?}", part.ctype);
//     }
//     // println!("Body Structure: {:#?}", mail.bodystructure());
    
// }

// drop(res); // Drop the result stream since it still owns a mutable reference to the session
