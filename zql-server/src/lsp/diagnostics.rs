use std::sync::Arc;

use sqlparser::ast::{visit_relations, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio::sync::RwLock;
use tower_lsp::lsp_types::*;

use crate::config::ZqlConfig;
use crate::db::schema::DatabaseSchema;

pub async fn compute_diagnostics(
    text: &str,
    config: &Arc<RwLock<Option<ZqlConfig>>>,
    schema_cache: &Arc<RwLock<Option<DatabaseSchema>>>,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();

    let has_config = config.read().await.is_some();
    if !has_config {
        diagnostics.push(Diagnostic {
            range: Range {
                start: Position::new(0, 0),
                end: Position::new(0, 0),
            },
            severity: Some(DiagnosticSeverity::INFORMATION),
            source: Some("zql".to_string()),
            message: "No database connection configured. Create .zql/connections.toml to enable schema features.".to_string(),
            ..Default::default()
        });
    }

    if text.trim().is_empty() {
        return diagnostics;
    }

    let dialect = PostgreSqlDialect {};
    match Parser::parse_sql(&dialect, text) {
        Ok(statements) => {
            // Validate table names against schema
            let schema = schema_cache.read().await;
            if let Some(ref schema) = *schema {
                validate_table_references(text, &statements, schema, &mut diagnostics);
            }
        }
        Err(e) => {
            let (line, col) = extract_error_position(&e.to_string());
            diagnostics.push(Diagnostic {
                range: Range {
                    start: Position::new(line, col),
                    end: Position::new(line, col + 1),
                },
                severity: Some(DiagnosticSeverity::ERROR),
                source: Some("zql".to_string()),
                message: format!("SQL syntax error: {e}"),
                ..Default::default()
            });
        }
    }

    diagnostics
}

fn validate_table_references(
    text: &str,
    statements: &[Statement],
    schema: &DatabaseSchema,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let table_names: Vec<String> = schema
        .tables
        .iter()
        .map(|t| t.name.clone())
        .chain(schema.views.iter().map(|v| v.name.clone()))
        .collect();

    for stmt in statements {
        let _ = visit_relations(stmt, |relation| {
            let table_name = relation.0.last().map(|i| i.to_string());
            if let Some(ref name) = table_name {
                // Skip if it matches a known table/view
                if !table_names.iter().any(|t| t == name) {
                    // Find the position of this table name in the text
                    if let Some(pos) = find_word_position(text, name) {
                        diagnostics.push(Diagnostic {
                            range: Range {
                                start: Position::new(pos.0, pos.1),
                                end: Position::new(pos.0, pos.1 + name.len() as u32),
                            },
                            severity: Some(DiagnosticSeverity::WARNING),
                            source: Some("zql".to_string()),
                            message: format!("Table or view '{name}' not found in schema"),
                            ..Default::default()
                        });
                    }
                }
            }
            std::ops::ControlFlow::<()>::Continue(())
        });
    }
}

fn find_word_position(text: &str, word: &str) -> Option<(u32, u32)> {
    for (line_num, line) in text.lines().enumerate() {
        // Search for the word as a whole word (not part of another word)
        let mut search_from = 0;
        while let Some(col) = line[search_from..].find(word) {
            let abs_col = search_from + col;
            let before_ok = abs_col == 0
                || !line.as_bytes()[abs_col - 1].is_ascii_alphanumeric()
                    && line.as_bytes()[abs_col - 1] != b'_';
            let after_pos = abs_col + word.len();
            let after_ok = after_pos >= line.len()
                || !line.as_bytes()[after_pos].is_ascii_alphanumeric()
                    && line.as_bytes()[after_pos] != b'_';

            if before_ok && after_ok {
                return Some((line_num as u32, abs_col as u32));
            }
            search_from = abs_col + 1;
        }
    }
    None
}

fn extract_error_position(error_msg: &str) -> (u32, u32) {
    if let Some(line_pos) = error_msg.find("Line: ") {
        let rest = &error_msg[line_pos + 6..];
        if let Some(comma) = rest.find(',') {
            let line: u32 = rest[..comma].trim().parse().unwrap_or(1);
            if let Some(col_pos) = rest.find("Column: ") {
                let col_rest = &rest[col_pos + 8..];
                let col_end = col_rest
                    .find(|c: char| !c.is_ascii_digit())
                    .unwrap_or(col_rest.len());
                let col: u32 = col_rest[..col_end].trim().parse().unwrap_or(0);
                return (line.saturating_sub(1), col.saturating_sub(1));
            }
            return (line.saturating_sub(1), 0);
        }
    }
    (0, 0)
}
