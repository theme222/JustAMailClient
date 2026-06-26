use std::str::FromStr;

use crate::models::*;

pub async fn initialize_database() -> Result<sqlx::Pool<sqlx::Sqlite>> {
    let project_dir = project_dir();
    let data_dir = project_dir.data_local_dir();
    let db_path = data_dir.join("mail.db");
    let db_url = format!("sqlite://{}", db_path.display());

    let options = sqlx::sqlite::SqliteConnectOptions::from_str(&db_url)?
        .create_if_missing(true);

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await
        .context("Failed to connect to SQLite database")?;

    println!("Database connected at {:?}", db_path);

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .context("Failed to execute database migrations")?;
    
    println!("Database schema is up to date!");
    
    Ok(pool)
}