mod net;
mod srv;
mod models;
mod init;
mod gui;

use std::{collections::BTreeMap, io::Write};

use models::*;

use init::*;
use net::*;
use srv::*;


#[tokio::main]
async fn main() {
    let res = runner().await;
    // let res = test_decode().await;
    if let Err(e) = res { eprintln!("Error: {}", e); }
}


async fn test_decode() -> Result<()> {
    let raw_data = std::fs::read(std::path::Path::new("sample/partial.eml")).unwrap();
    let pct = mailparse::ParsedContentType {mimetype: "text/plain".into(), charset: "utf-8".into(), params: BTreeMap::new()};
    let parser = mailparse::body::Body::new(&raw_data, &pct, &Some(String::from("quoted-printable")));
    // let parsed_mail = mailparse::parse_mail(&raw_data).unwrap();
    // for (i, part) in decoded.enumerate() {
    //     println!("Part {}:\n{}\n", i, part.get_body().unwrap());
    //     tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    // }
    use mailparse::body::Body::*;
    let result = match parser {
        Base64(val) => { val.get_decoded_as_string() },
        QuotedPrintable(val) => { val.get_decoded_as_string() },
        SevenBit(val) => { val.get_as_string() },
        EightBit(val) => { val.get_as_string() },
        Binary(val) => { val.get_as_string() },
    };

    println!("{}", result.unwrap_or("Unable to decode".into()));
    Ok(())
}

async fn runner() -> Result<()> {
    delete_database_if_exists();
    ensure_project_dir_structure()?;
    dotenvy::dotenv()?;

    let service = "AOL";
    let imap_server_str = format!("{}_IMAP_SERVER", service);
    let smtp_server_str = format!("{}_SMTP_SERVER", service);
    let login_str = format!("{}_EMAIL", service);
    let password_str = format!("{}_PASSWORD", service);
     
    let imap_server = std::env::var(imap_server_str).context("IMAP_SERVER not set")?;
    let smtp_server = std::env::var(smtp_server_str).context("SMTP_SERVER not set")?;
    let login = std::env::var(login_str).context("EMAIL not set")?;
    let password = std::env::var(password_str).context("PASSWORD not set")?;
    
    let creds = Credentials {
        login: login,
        secret: password,
        fetch_server: imap_server,
        push_server: smtp_server,
        auth_method: AuthMethod::LOGIN,
        encryption_method: EncryptionMethod::SSLTLS,
    };

    let cred_id = CredentialStore::insert(creds);

    let (net_sender, net_receiver) = tokio::sync::mpsc::channel::<net::NetMessage>(100);
    let (srv_sender, srv_receiver) = tokio::sync::mpsc::channel::<srv::SrvMessage>(100);
    let (ui_sender, ui_receiver) = tokio::sync::mpsc::channel::<gui::GUIMessage>(100);

    let senders = Senders {
        net_sender: net_sender,
        srv_sender: srv_sender,
        gui_sender: ui_sender,
    }; 
    
    Senders::set(senders);

    let mut net_actor = net::NetActor::new(net_receiver).await;
    let mut srv_actor = srv::SrvActor::new(srv_receiver).await;
    // let ui_actor = ui::UiActor::new(ui_receiver);

    tokio::spawn(async move { net_actor.run().await; });
    tokio::spawn(async move { srv_actor.run().await; });

    // For now lets just treat this as a weird shell like interface (right now we are acting as the gui component)
    loop {
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let input = input.trim();

        match input {
            "send" => { Senders::net(NetMessage {cred_id, action: NetAction::SEND}).await; }
            "echo" => { Senders::net(NetMessage {cred_id, action: NetAction::ECHO}).await; }
            "fetch" => { Senders::net(NetMessage {cred_id, action: NetAction::LISTFETCH}).await; }
            "status" => { Senders::net(NetMessage {cred_id, action: NetAction::STATUS}).await; }
            "list" => { Senders::srv(SrvMessage {action: SrvAction::LISTEMAILS}).await; }
            "help" => { println!("Available commands: send, echo, fetch, status, list, help, exit"); }
            "exit" => { break; }
            _ => { println!("Unknown command: {}", input); }
        }
    }
    
    Senders::net(NetMessage {cred_id, action: NetAction::SHUTDOWN}).await; 
    Senders::srv(SrvMessage {action: SrvAction::SHUTDOWN}).await;
    
    Ok(())
}
