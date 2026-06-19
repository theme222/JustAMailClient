mod fetch;
mod push;
mod models;

use models::*;

use crate::fetch::imap::FetchType;

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

    push::smtp::send_test_email(&login, &password).await;
    Ok(())
}

