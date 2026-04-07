use anyhow::Result;
use async_trait::async_trait;

use super::schema::{DatabaseSchema, DbColumn, QueryResult};
use crate::config::ConnectionConfig;

#[async_trait]
pub trait DatabaseProvider: Send + Sync {
    /// Returns the provider name (e.g., "postgres", "mysql", "sqlite")
    fn name(&self) -> &str;

    /// Test the connection
    async fn test_connection(&self, config: &ConnectionConfig) -> Result<()>;

    /// Execute a query and return results
    async fn execute_query(&self, config: &ConnectionConfig, sql: &str) -> Result<QueryResult>;

    /// Get the full database schema
    async fn get_schema(&self, config: &ConnectionConfig) -> Result<DatabaseSchema>;

    /// Get columns for a specific table
    async fn get_table_columns(&self, config: &ConnectionConfig, table: &str) -> Result<Vec<DbColumn>>;

    /// Get estimated row count for a table
    async fn get_table_row_count(&self, config: &ConnectionConfig, table: &str) -> Result<i64>;

    /// Execute EXPLAIN on a query
    async fn explain_query(&self, config: &ConnectionConfig, sql: &str) -> Result<String>;
}
