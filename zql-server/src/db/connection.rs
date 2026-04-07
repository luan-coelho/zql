use std::collections::HashMap;

use anyhow::Result;

use super::provider::DatabaseProvider;
use super::providers;
use super::schema::{DatabaseSchema, QueryResult};
use crate::config::ZqlConfig;

pub struct ConnectionManager {
    config: Option<ZqlConfig>,
    active_connection: Option<String>,
    providers: HashMap<String, Box<dyn DatabaseProvider>>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            config: None,
            active_connection: None,
            providers: HashMap::new(),
        }
    }

    pub fn load_config(&mut self, config: ZqlConfig) {
        let default_name = config
            .connections
            .iter()
            .find(|(_, c)| c.default.unwrap_or(false))
            .or_else(|| config.connections.iter().next())
            .map(|(name, _)| name.clone());

        self.active_connection = default_name;
        // Clear cached providers when config changes
        self.providers.clear();
        self.config = Some(config);
    }

    pub fn set_active_connection(&mut self, name: &str) {
        self.active_connection = Some(name.to_string());
    }

    pub fn active_connection_name(&self) -> Option<&str> {
        self.active_connection.as_deref()
    }

    pub fn connection_names(&self) -> Vec<String> {
        self.config
            .as_ref()
            .map(|c| c.connections.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub fn is_configured(&self) -> bool {
        self.config.is_some() && self.active_connection.is_some()
    }

    fn ensure_provider(&mut self) -> Result<()> {
        let conn_name = self
            .active_connection
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No active connection"))?
            .clone();

        if self.providers.contains_key(&conn_name) {
            return Ok(());
        }

        let config = self
            .config
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No configuration loaded"))?;

        let conn_config = config
            .connections
            .get(&conn_name)
            .ok_or_else(|| anyhow::anyhow!("Connection '{conn_name}' not found"))?;

        let provider = providers::get_provider(&conn_config.provider)
            .ok_or_else(|| anyhow::anyhow!("Unknown provider: {}", conn_config.provider))?;

        self.providers.insert(conn_name, provider);
        Ok(())
    }

    fn active_provider_and_config(
        &mut self,
    ) -> Result<(&dyn DatabaseProvider, &crate::config::ConnectionConfig)> {
        self.ensure_provider()?;

        let conn_name = self.active_connection.as_ref().unwrap();
        let provider = self.providers.get(conn_name.as_str()).unwrap();
        let config = self
            .config
            .as_ref()
            .unwrap()
            .connections
            .get(conn_name.as_str())
            .unwrap();

        Ok((provider.as_ref(), config))
    }

    pub async fn test_connection(&mut self) -> Result<()> {
        let (provider, config) = self.active_provider_and_config()?;
        provider.test_connection(config).await
    }

    pub async fn execute_query(&mut self, sql: &str) -> Result<QueryResult> {
        let (provider, config) = self.active_provider_and_config()?;
        provider.execute_query(config, sql).await
    }

    pub async fn get_schema(&mut self) -> Result<DatabaseSchema> {
        let (provider, config) = self.active_provider_and_config()?;
        provider.get_schema(config).await
    }

    pub async fn explain_query(&mut self, sql: &str) -> Result<String> {
        let (provider, config) = self.active_provider_and_config()?;
        provider.explain_query(config, sql).await
    }
}
