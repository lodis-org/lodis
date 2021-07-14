use std::env;

use num_cpus;

use crate::error::Result;

#[derive(Debug)]
pub struct LodisConfig {
    pub db_path: String,
    pub ip_port: String,
    pub workers: usize,
}

pub fn get_config() -> Result<LodisConfig> {
    let db_path = env::var("LODIS_DB_PATH")?;
    let ip_port = env::var("LODIS_IP_PORT")?;
    let workers = env::var("LODIS_WORKERS")
        .map(|n| n.parse().unwrap())
        .unwrap_or(num_cpus::get());
    Ok(LodisConfig {
        db_path,
        ip_port,
        workers,
    })
}
