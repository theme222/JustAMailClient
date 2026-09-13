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
    
    let get_credential = |key: &str| {
        let srv_config = SERVICE_CONFIG.get(key).context("Unknown service").unwrap();
        let login_str = format!("{}_EMAIL", key);
        let password_str = format!("{}_PASSWORD", key);
            
        let login = std::env::var(login_str).context("EMAIL not set").unwrap();
        let password = std::env::var(password_str).context("PASSWORD not set").unwrap();
        
        Credentials {
            service: srv_config.service.clone(),
            login: login,
            secret: password,
            fetch_server: srv_config.fetch_server.to_string(),
            fetch_port: srv_config.fetch_port,
            push_server: srv_config.push_server.to_string(),
            push_port: srv_config.push_port,
            auth_method: AuthMethod::LOGIN,
            encryption_method: EncryptionMethod::SSLTLS,
        }
    };

    CredentialStore::insert(get_credential("ICLOUD")).await;
    CredentialStore::insert(get_credential("AOL")).await;
    CredentialStore::insert(get_credential("YAHOO")).await;
    let mut curr_cred_id = 0;
    
    // For now lets just treat this as a weird shell like interface (right now we are acting as the gui component)
    loop {
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let input = input.trim();

        match input {
            "help" => { println!("Available commands: send, fetch, status, list, help, stop, switch, exit, start"); }
            "send" => { Senders::net(NetMessage {action: NetAction::SEND { cred_id: curr_cred_id }, resolve: NULL_RESOLVE_ID }).await; }
            "fetch" => { Senders::net(NetMessage {action: NetAction::LISTFETCH { cred_id: curr_cred_id, seq_range: fetch::imap::SeqRange::all(true) }, resolve: NULL_RESOLVE_ID}).await; }
            "status" => { Senders::net(NetMessage {action: NetAction::STATUS { cred_id: curr_cred_id }, resolve: NULL_RESOLVE_ID}).await; }
            "list" => { Senders::srv(SrvMessage {action: SrvAction::LISTEMAILS, resolve: NULL_RESOLVE_ID}).await; }
            "poll" => { Senders::net(NetMessage {action: NetAction::POLL, resolve: NULL_RESOLVE_ID}).await; }
            "start" => { Senders::net(NetMessage {action: NetAction::START { cred_id: curr_cred_id }, resolve: NULL_RESOLVE_ID}).await; }
            "stop" => { Senders::net(NetMessage {action: NetAction::STOP { cred_id: curr_cred_id }, resolve: NULL_RESOLVE_ID}).await; }
            "exit" => { break; }
            input if input.starts_with("switch ") => {
                let cred_id = input[7..].parse::<CredentialID>().unwrap();
                curr_cred_id = cred_id;
                println!("Switched to credential {}", cred_id);
            }
            _ => { println!("Unknown command: {}", input); }
        }

        AppStateStore::ping();
    }
    
    Senders::net(NetMessage {action: NetAction::SHUTDOWN, resolve: NULL_RESOLVE_ID}).await; 
    Senders::srv(SrvMessage {action: SrvAction::SHUTDOWN, resolve: NULL_RESOLVE_ID}).await;
    
    Ok(())
}
