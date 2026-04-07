use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;

use super::server::Backend;
use crate::output::formatter;

pub async fn handle_execute_command(
    backend: &Backend,
    params: ExecuteCommandParams,
) -> Result<Option<serde_json::Value>> {
    match params.command.as_str() {
        "zql.runQuery" => run_query(backend, &params.arguments).await,
        "zql.explainQuery" => explain_query(backend, &params.arguments).await,
        "zql.refreshSchema" => refresh_schema(backend).await,
        "zql.showSchema" => show_schema(backend).await,
        "zql.switchConnection" => switch_connection(backend, &params.arguments).await,
        _ => {
            backend
                .client
                .log_message(
                    MessageType::WARNING,
                    format!("Unknown command: {}", params.command),
                )
                .await;
            Ok(None)
        }
    }
}

async fn run_query(
    backend: &Backend,
    args: &[serde_json::Value],
) -> Result<Option<serde_json::Value>> {
    let (uri_str, start_line, end_line) = parse_query_args(args)?;

    let query = {
        let docs = backend.documents.read().await;
        let uri: Url = uri_str
            .parse()
            .map_err(|_| tower_lsp::jsonrpc::Error::invalid_params("invalid URI"))?;
        let Some(text) = docs.get(&uri) else {
            return Ok(None);
        };
        extract_query(text, start_line, end_line)
    };

    let result = {
        let mut conn_manager = backend.connection_manager.write().await;
        conn_manager.execute_query(&query).await
    };

    match result {
        Ok(result) => {
            let formatted = formatter::format_query_result(&result);
            // Save to file
            save_result_file(backend, &formatted, "query").await;
            // Show notification with summary
            let summary = format!(
                "Query OK: {} rows ({}ms)",
                result.rows_affected, result.execution_time_ms
            );
            backend.client.show_message(MessageType::INFO, &summary).await;
            backend.client.log_message(MessageType::INFO, &formatted).await;
            Ok(Some(serde_json::json!({ "success": true })))
        }
        Err(e) => {
            backend
                .client
                .show_message(MessageType::ERROR, format!("Query error: {e}"))
                .await;
            Ok(Some(
                serde_json::json!({ "success": false, "error": e.to_string() }),
            ))
        }
    }
}

async fn explain_query(
    backend: &Backend,
    args: &[serde_json::Value],
) -> Result<Option<serde_json::Value>> {
    let (uri_str, start_line, end_line) = parse_query_args(args)?;

    let query = {
        let docs = backend.documents.read().await;
        let uri: Url = uri_str
            .parse()
            .map_err(|_| tower_lsp::jsonrpc::Error::invalid_params("invalid URI"))?;
        let Some(text) = docs.get(&uri) else {
            return Ok(None);
        };
        extract_query(text, start_line, end_line)
    };

    let result = {
        let mut conn_manager = backend.connection_manager.write().await;
        conn_manager.explain_query(&query).await
    };

    match result {
        Ok(plan) => {
            let content = format!("## Query Plan\n\n```\n{plan}\n```\n");
            save_result_file(backend, &content, "explain").await;
            backend.client.show_message(MessageType::INFO, &plan).await;
            Ok(Some(serde_json::json!({ "success": true })))
        }
        Err(e) => {
            backend
                .client
                .show_message(MessageType::ERROR, format!("Explain error: {e}"))
                .await;
            Ok(None)
        }
    }
}

pub async fn refresh_schema(backend: &Backend) -> Result<Option<serde_json::Value>> {
    let result = {
        let mut conn_manager = backend.connection_manager.write().await;
        conn_manager.get_schema().await
    };

    match result {
        Ok(schema) => {
            let mut cache = backend.schema_cache.write().await;
            *cache = Some(schema);
            backend
                .client
                .log_message(MessageType::INFO, "Schema refreshed")
                .await;
            Ok(Some(serde_json::json!({ "success": true })))
        }
        Err(e) => {
            backend
                .client
                .log_message(
                    MessageType::WARNING,
                    format!("Schema refresh failed: {e}"),
                )
                .await;
            Ok(None)
        }
    }
}

async fn show_schema(backend: &Backend) -> Result<Option<serde_json::Value>> {
    let schema = backend.schema_cache.read().await;
    let Some(schema) = schema.as_ref() else {
        backend
            .client
            .show_message(
                MessageType::WARNING,
                "No schema cached. Run 'Refresh Schema' first.",
            )
            .await;
        return Ok(None);
    };

    let formatted = formatter::format_schema(schema);

    save_result_file(backend, &formatted, "schema").await;
    backend.client.show_message(MessageType::INFO, "Schema saved to .zql/results/").await;
    Ok(Some(serde_json::json!({ "success": true })))
}

async fn switch_connection(
    backend: &Backend,
    args: &[serde_json::Value],
) -> Result<Option<serde_json::Value>> {
    let connection_name = args
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| tower_lsp::jsonrpc::Error::invalid_params("missing connection name"))?;

    {
        let mut conn_manager = backend.connection_manager.write().await;
        conn_manager.set_active_connection(connection_name);
    }

    backend
        .client
        .log_message(
            MessageType::INFO,
            format!("Switched to connection: {connection_name}"),
        )
        .await;

    refresh_schema(backend).await
}

async fn save_result_file(backend: &Backend, content: &str, prefix: &str) {
    let workspace = backend.workspace_path.read().await;
    let Some(ref ws_path) = *workspace else {
        return;
    };

    let results_dir = format!("{ws_path}/.zql/results");
    let _ = std::fs::create_dir_all(&results_dir);

    let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    let result_path = format!("{results_dir}/{prefix}_{timestamp}.md");
    let _ = std::fs::write(&result_path, content);
}

fn parse_query_args(args: &[serde_json::Value]) -> Result<(String, u32, u32)> {
    let uri = args
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| tower_lsp::jsonrpc::Error::invalid_params("missing URI"))?
        .to_string();
    let start_line = args.get(1).and_then(|v| v.as_u64()).unwrap_or(0) as u32;
    let end_line = args.get(2).and_then(|v| v.as_u64()).unwrap_or(0) as u32;
    Ok((uri, start_line, end_line))
}

fn extract_query(text: &str, start_line: u32, end_line: u32) -> String {
    if start_line == end_line {
        extract_statement_at_line(text, start_line as usize)
    } else {
        text.lines()
            .skip(start_line as usize)
            .take((end_line - start_line + 1) as usize)
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn extract_statement_at_line(text: &str, line: usize) -> String {
    let mut current_line = 0;
    let mut statements: Vec<(usize, usize, String)> = Vec::new();
    let mut current_stmt = String::new();
    let mut stmt_start = 0;

    for (i, l) in text.lines().enumerate() {
        if current_stmt.is_empty() {
            stmt_start = i;
        }
        current_stmt.push_str(l);
        current_stmt.push('\n');

        if l.trim_end().ends_with(';') {
            statements.push((stmt_start, i, current_stmt.trim().to_string()));
            current_stmt.clear();
        }
        current_line = i;
    }

    if !current_stmt.trim().is_empty() {
        statements.push((stmt_start, current_line, current_stmt.trim().to_string()));
    }

    for (start, end, stmt) in &statements {
        if line >= *start && line <= *end {
            return stmt.trim_end_matches(';').trim().to_string();
        }
    }

    text.trim().to_string()
}
