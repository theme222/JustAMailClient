use std::fmt::format;
use std::time::UNIX_EPOCH;

use lettre::message::header::ContentType;
use lettre::{AsyncTransport, AsyncSmtpTransport, Message, Tokio1Executor};
use lettre::transport::smtp::authentication::Credentials as lettreCredentials;

use crate::models::*;

pub async fn get_mailer(credentials: &Credentials) -> Result<AsyncSmtpTransport<Tokio1Executor>> {
    let creds = lettreCredentials::new(
        credentials.login.clone(), 
        credentials.secret.clone()
    );

    Ok(
        AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&credentials.push_server)?
            .credentials(creds)
            .build()
    )
}

pub async fn send_test_email(credentials: &Credentials) -> Result<()> {
    let email = Message::builder()
        .from(credentials.login.parse().unwrap())
        .to(TEST_MAIL_DEST.parse().unwrap())
        .subject("Test email")
        .header(ContentType::TEXT_PLAIN)
        .body(
            format!("Test email from JustAMailClient {}", std::time::SystemTime::now().duration_since(UNIX_EPOCH).expect("msg").as_secs())
        )
        .unwrap();


    let mailer = get_mailer(credentials).await?;
    let res = mailer.send(email).await?;
    
    for line in res.message() {
        println!("{}", line);
    }
    
    Ok(())
}

pub async fn send_echo_email(credentials: &Credentials) -> Result<()> {
    let email = Message::builder()
        .from(credentials.login.parse().unwrap())
        .to(credentials.login.parse().unwrap())
        .subject("Test echo email")
        .header(ContentType::TEXT_PLAIN)
        .body(
            format!("Test echo email from JustAMailClient {}", std::time::SystemTime::now().duration_since(UNIX_EPOCH).expect("msg").as_secs())
        )
        .unwrap();


    let mailer = get_mailer(credentials).await?;
    let res = mailer.send(email).await?;
    
    for line in res.message() {
        println!("{}", line);
    }
    
    Ok(())
}