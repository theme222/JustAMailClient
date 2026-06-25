pub use anyhow::{anyhow, Result, Error, Context};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Credentials {
    pub login: String,
    pub secret: String,
    pub fetch_server: String,
    pub push_server: String,
}

pub const APP_NAME: &str = "JustAMailClient";
pub const APP_AUTHOR: &str = "Sira Tongsima";
pub const APP_VERSION: &str = "0.1.0";
pub const TEST_MAIL_DEST: &str = "Sira Tongsima <sira.tongsima@yahoo.com>";

pub fn project_dir() -> directories::ProjectDirs {
    directories::ProjectDirs::from("com", "tongsima", "jamc").expect("Failed to find project directories")
}