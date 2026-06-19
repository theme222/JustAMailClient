use lettre::message::header::ContentType;
use lettre::{AsyncTransport, AsyncSmtpTransport, Message, Tokio1Executor};
use lettre::transport::smtp::authentication::Credentials;

pub async fn send_test_email(username: &str, password: &str) {
    let email = Message::builder()
        .from("".parse().unwrap())
        .to("".parse().unwrap())
        .subject("")
        .header(ContentType::TEXT_PLAIN)
        .cc("".parse().unwrap())
        .body("This is the body of the email. It was so easy.".to_string())
        .unwrap();

    let creds = Credentials::new(
        username.to_string(), 
        password.to_string()
    );

    let mailer: AsyncSmtpTransport<Tokio1Executor> = 
        AsyncSmtpTransport::<Tokio1Executor>::starttls_relay("smtp.mail.me.com")
            .unwrap()
            .credentials(creds)
            .build();

    match mailer.send(email).await {
        Ok(_) => println!("Email sent successfully!"),
        Err(e) => panic!("Could not send email: {:?}", e),
    }
}