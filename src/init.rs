use std::str::FromStr;

// Functions ran at the start of the program
use crate::models::*;
use directories::ProjectDirs;
 
pub fn ensure_project_dir_structure() -> Result<()> {
    let project_dir = ProjectDirs::from("com", "tongsima", "jamc").context("Failed to find project directories")?; 
    std::fs::create_dir_all(project_dir.cache_dir()).context("Failed to create cache directory")?;
    std::fs::create_dir_all(project_dir.config_dir()).context("Failed to create config directory")?;
    std::fs::create_dir_all(project_dir.data_local_dir()).context("Failed to create data directory")?;
    Ok(())
}

pub fn delete_database_if_exists() {
    // TODO: DEBUG
    let project_dir = project_dir();
    let data_dir = project_dir.data_local_dir();
    let db_path = data_dir.join("mail.db");
    println!("Attempting to delete database at {:?}", db_path);
    std::fs::remove_file(&db_path);
}

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