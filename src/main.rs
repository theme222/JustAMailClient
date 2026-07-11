mod net;
mod srv;
mod models;
mod init;
mod gui;
mod consts;
mod funcs;

use std::{collections::BTreeMap, io::Write};

use models::*;

use init::*;
use net::*;
use srv::*;


#[tokio::main]
async fn main() {
    let res = runner().await;
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
    
    let imap_server = std::env::var("ICLOUD_IMAP_SERVER").context("ICLOUD_IMAP_SERVER not set")?;
    let smtp_server = std::env::var("ICLOUD_SMTP_SERVER").context("ICLOUD_SMTP_SERVER not set")?;
    let login = std::env::var("ICLOUD_EMAIL").context("ICLOUD_EMAIL not set")?;
    let password = std::env::var("ICLOUD_PASSWORD").context("ICLOUD_PASSWORD not set")?;
    
    let creds = Credentials {
        login: login,
        secret: password,
        fetch_server: imap_server,
        push_server: smtp_server,
    };

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
        print!("Enter command (send, list, fetch, status, exit, echo): ");
        std::io::stdout().flush()?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let input = input.trim();

        match input {
            "send" => { Senders::net(NetMessage {action: NetAction::SEND(creds.clone()) }).await; }
            "echo" => { Senders::net(NetMessage {action: NetAction::ECHO(creds.clone()) }).await; }
            "fetch" => { Senders::net(NetMessage {action: NetAction::LISTFETCH(creds.clone())}).await; }
            "status" => { Senders::net(NetMessage {action: NetAction::STATUS(creds.clone())}).await; }
            "list" => { Senders::srv(SrvMessage {action: SrvAction::LISTEMAILS}).await; }
            "exit" => { break; }
            _ => { println!("Unknown command: {}", input); }
        }
    }
    
    Senders::net(NetMessage {action: NetAction::SHUTDOWN}).await; 
    Senders::srv(SrvMessage {action: SrvAction::SHUTDOWN}).await;
    
    Ok(())
}
