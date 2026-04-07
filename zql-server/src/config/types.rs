use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZqlConfig {
    pub connections: HashMap<String, ConnectionConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub provider: String,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub user: Option<String>,
    /// Environment variable name containing the password
    pub password_env: Option<String>,
    /// Environment variable name containing the full connection string
    pub connection_string_env: Option<String>,
    pub ssl: Option<bool>,
    pub default: Option<bool>,
}

impl ConnectionConfig {
    pub fn connection_url(&self) -> Result<String> {
        // If a connection string env var is specified, use that
        if let Some(ref env_name) = self.connection_string_env {
            return std::env::var(env_name)
                .with_context(|| format!("environment variable '{env_name}' not set"));
        }

        let host = self.host.as_deref().unwrap_or("localhost");
        let port = self.port.unwrap_or(5432);
        let database = self
            .database
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("database name required"))?;
        let user = self.user.as_deref().unwrap_or("postgres");

        let password = if let Some(ref env_name) = self.password_env {
            std::env::var(env_name)
                .with_context(|| format!("environment variable '{env_name}' not set"))?
        } else {
            String::new()
        };

        let ssl_mode = if self.ssl.unwrap_or(false) {
            "require"
        } else {
            "disable"
        };

        let url = if password.is_empty() {
            format!("postgres://{user}@{host}:{port}/{database}?sslmode={ssl_mode}")
        } else {
            format!("postgres://{user}:{password}@{host}:{port}/{database}?sslmode={ssl_mode}")
        };

        Ok(url)
    }
}

impl ZqlConfig {
    pub fn load_from_workspace(workspace_path: &str) -> Result<Self> {
        let config_path = Path::new(workspace_path).join(".zql/connections.toml");
        let content = std::fs::read_to_string(&config_path)
            .with_context(|| format!("failed to read {}", config_path.display()))?;

        let config: ZqlConfig =
            toml::from_str(&content).context("failed to parse .zql/connections.toml")?;

        Ok(config)
    }
}
