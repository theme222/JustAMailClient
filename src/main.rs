mod net;
mod srv;
mod models;
mod init;
mod ui;
mod consts;

use std::io::Write;

use models::*;

use crate::{init::{delete_database_if_exists, ensure_project_dir_structure }};
use net::*;


#[tokio::main]
async fn main() {
    let res = runner().await;
    if let Err(e) = res { eprintln!("Error: {}", e); }
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
    let (ui_sender, ui_receiver) = tokio::sync::mpsc::channel::<ui::UIMessage>(100);

    let senders = Senders {
        net_sender: net_sender,
        srv_sender: srv_sender,
        ui_sender: ui_sender,
    };

    let mut net_actor = net::NetActor::new(net_receiver, senders.clone()).await;
    let mut srv_actor = srv::SrvActor::new(srv_receiver, senders.clone()).await.unwrap();
    // let ui_actor = ui::UiActor::new(ui_receiver);

    tokio::spawn(async move { net_actor.run().await; });
    tokio::spawn(async move { srv_actor.run().await; });


    // initialize_database().await?;

    // println!("Initalizing manager...");
    
    // let mut manager = net::fetch::imap::ImapManager::new(&creds).await?;
    // let mut session = manager.get_owned_session(Idler("INBOX")).await;
    // tokio::spawn(session.idle());

    // println!("Done!");
    
    // For now lets just treat this as a weird shell like interface (right now we are acting as the ui component)
    loop {
        print!("Enter command (send, list, exit, echo): ");
        std::io::stdout().flush()?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let input = input.trim();

        use NetAction::*;
        match input {
            "send" => { senders.net_sender.send(NetMessage {action: SendEmail(creds.clone()) }).await?; }
            "echo" => { senders.net_sender.send(NetMessage {action: SendEcho(creds.clone()) }).await?; }
            "list" => { senders.net_sender.send(NetMessage {action: FetchEmail(creds.clone())}).await?; }
            "exit" => { break; }
            _ => { println!("Unknown command: {}", input); }
        }
    }

    Ok(())
}
