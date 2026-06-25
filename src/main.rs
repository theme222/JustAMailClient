mod net;
mod srv;
mod models;
mod init;

use std::io::Write;

use models::*;

use net::fetch::imap::{FetchType, ImapSessionType::*};
use crate::{init::{delete_database_if_exists, ensure_project_dir_structure, initialize_database}};


#[tokio::main]
async fn main() {
    let res = runner().await;
    if let Err(e) = res { eprintln!("Error: {}", e); }
}

async fn runner() -> Result<()> {
    dotenvy::dotenv().ok();
    
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

    ensure_project_dir_structure()?;
    delete_database_if_exists();
    initialize_database().await?;

    println!("Initalizing manager...");
    
    let mut manager = net::fetch::imap::ImapManager::new(&creds).await?;
    let mut session = manager.get_owned_session(Idler("INBOX")).await;
    tokio::spawn(session.idle());

    println!("Done!");
    
    // For now lets just treat this as a weird shell like interface
    loop {
        print!("Enter command (send, list, exit): ");
        std::io::stdout().flush()?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let input = input.trim();

        match input {
            "send" => { net::push::smtp::send_test_email(&creds).await?; }
            "list" => { }
            "exit" => { break; }
            _ => { println!("Unknown command: {}", input); }
        }
    }
    
    Ok(())
}
