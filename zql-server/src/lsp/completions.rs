use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;

use super::server::Backend;

pub async fn handle_completion(
    backend: &Backend,
    params: CompletionParams,
) -> Result<Option<CompletionResponse>> {
    let uri = params.text_document_position.text_document.uri;
    let position = params.text_document_position.position;

    let docs = backend.documents.read().await;
    let Some(text) = docs.get(&uri) else {
        return Ok(None);
    };

    let mut items = Vec::new();

    // Get the current line text up to cursor position
    let line_text = text
        .lines()
        .nth(position.line as usize)
        .unwrap_or_default();
    let prefix = &line_text[..std::cmp::min(position.character as usize, line_text.len())];
    let prefix_upper = prefix.to_uppercase();

    // Context-aware completions
    let needs_table = prefix_upper.ends_with("FROM ")
        || prefix_upper.ends_with("JOIN ")
        || prefix_upper.ends_with("INTO ")
        || prefix_upper.ends_with("UPDATE ")
        || prefix_upper.ends_with("TABLE ");

    let needs_column = prefix.ends_with('.');

    // Add schema-based completions if connected
    let schema = backend.schema_cache.read().await;
    if let Some(schema) = schema.as_ref() {
        if needs_table {
            for table in &schema.tables {
                items.push(CompletionItem {
                    label: table.name.clone(),
                    kind: Some(CompletionItemKind::STRUCT),
                    detail: Some(format!("{}.{}", table.schema, table.name)),
                    documentation: Some(Documentation::MarkupContent(MarkupContent {
                        kind: MarkupKind::Markdown,
                        value: format!(
                            "**{}** columns: {}",
                            table.name,
                            table
                                .columns
                                .iter()
                                .map(|c| format!("`{}`", c.name))
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                    })),
                    ..Default::default()
                });
            }
        } else if needs_column {
            // Find the table name before the dot
            let before_dot = prefix.trim_end_matches('.');
            let table_name = before_dot
                .split_whitespace()
                .last()
                .unwrap_or_default();

            if let Some(table) = schema.tables.iter().find(|t| t.name == table_name) {
                for col in &table.columns {
                    items.push(CompletionItem {
                        label: col.name.clone(),
                        kind: Some(CompletionItemKind::FIELD),
                        detail: Some(format!(
                            "{} {}",
                            col.data_type,
                            if col.is_nullable { "NULL" } else { "NOT NULL" }
                        )),
                        ..Default::default()
                    });
                }
            }
        }
    }

    // Keyword and function completions
    if !needs_column {
        let current_word = prefix.split_whitespace().last().unwrap_or_default();
        let current_word_upper = current_word.to_uppercase();

        if !current_word.is_empty() {
            for keyword in sql_keywords() {
                if keyword.starts_with(&current_word_upper) {
                    items.push(CompletionItem {
                        label: keyword.to_string(),
                        kind: Some(CompletionItemKind::KEYWORD),
                        sort_text: Some(format!("2_{keyword}")),
                        ..Default::default()
                    });
                }
            }

            let current_word_lower = current_word.to_lowercase();
            for (name, signature) in pg_functions() {
                if name.starts_with(&current_word_lower) {
                    items.push(CompletionItem {
                        label: name.to_string(),
                        kind: Some(CompletionItemKind::FUNCTION),
                        detail: Some(signature.to_string()),
                        insert_text: Some(format!("{name}($0)")),
                        insert_text_format: Some(InsertTextFormat::SNIPPET),
                        sort_text: Some(format!("3_{name}")),
                        ..Default::default()
                    });
                }
            }
        }
    }

    if items.is_empty() {
        Ok(None)
    } else {
        Ok(Some(CompletionResponse::Array(items)))
    }
}

fn pg_functions() -> &'static [(&'static str, &'static str)] {
    &[
        ("now", "now() -> timestamptz"),
        ("current_timestamp", "current_timestamp -> timestamptz"),
        ("current_date", "current_date -> date"),
        ("count", "count(expression) -> bigint"),
        ("sum", "sum(expression) -> numeric"),
        ("avg", "avg(expression) -> numeric"),
        ("min", "min(expression) -> same as input"),
        ("max", "max(expression) -> same as input"),
        ("coalesce", "coalesce(value1, value2, ...) -> first non-null"),
        ("nullif", "nullif(value1, value2) -> null if equal"),
        ("concat", "concat(str1, str2, ...) -> text"),
        ("length", "length(string) -> integer"),
        ("lower", "lower(string) -> text"),
        ("upper", "upper(string) -> text"),
        ("trim", "trim(string) -> text"),
        ("substring", "substring(string, start, length) -> text"),
        ("replace", "replace(string, from, to) -> text"),
        ("split_part", "split_part(string, delimiter, n) -> text"),
        ("to_char", "to_char(value, format) -> text"),
        ("to_date", "to_date(text, format) -> date"),
        ("to_timestamp", "to_timestamp(text, format) -> timestamptz"),
        ("extract", "extract(field FROM source) -> numeric"),
        ("date_trunc", "date_trunc(field, source) -> timestamp"),
        ("age", "age(timestamp, timestamp) -> interval"),
        ("gen_random_uuid", "gen_random_uuid() -> uuid"),
        ("array_agg", "array_agg(expression) -> array"),
        ("string_agg", "string_agg(expression, delimiter) -> text"),
        ("json_agg", "json_agg(expression) -> json"),
        ("jsonb_agg", "jsonb_agg(expression) -> jsonb"),
        ("json_build_object", "json_build_object(key, value, ...) -> json"),
        ("jsonb_build_object", "jsonb_build_object(key, value, ...) -> jsonb"),
        ("row_number", "row_number() OVER (...) -> bigint"),
        ("rank", "rank() OVER (...) -> bigint"),
        ("dense_rank", "dense_rank() OVER (...) -> bigint"),
        ("lag", "lag(value, offset, default) OVER (...) -> same as input"),
        ("lead", "lead(value, offset, default) OVER (...) -> same as input"),
        ("first_value", "first_value(value) OVER (...) -> same as input"),
        ("last_value", "last_value(value) OVER (...) -> same as input"),
        ("exists", "EXISTS (subquery) -> boolean"),
        ("cast", "CAST(value AS type) -> type"),
        ("round", "round(numeric, precision) -> numeric"),
        ("ceil", "ceil(numeric) -> numeric"),
        ("floor", "floor(numeric) -> numeric"),
        ("abs", "abs(numeric) -> numeric"),
        ("random", "random() -> double precision"),
    ]
}

fn sql_keywords() -> &'static [&'static str] {
    &[
        "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
        "CREATE", "TABLE", "ALTER", "DROP", "INDEX", "JOIN", "INNER", "LEFT", "RIGHT", "OUTER",
        "FULL", "CROSS", "ON", "AND", "OR", "NOT", "NULL", "IS", "IN", "BETWEEN", "LIKE",
        "EXISTS", "HAVING", "GROUP", "BY", "ORDER", "ASC", "DESC", "LIMIT", "OFFSET", "AS",
        "DISTINCT", "COUNT", "SUM", "AVG", "MIN", "MAX", "CASE", "WHEN", "THEN", "ELSE", "END",
        "UNION", "ALL", "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "CONSTRAINT", "DEFAULT",
        "NOT NULL", "UNIQUE", "CHECK", "CASCADE", "RESTRICT", "RETURNING", "WITH", "RECURSIVE",
        "EXPLAIN", "ANALYZE", "BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION", "GRANT", "REVOKE",
        "SERIAL", "BIGSERIAL", "VARCHAR", "TEXT", "INTEGER", "BIGINT", "BOOLEAN", "TIMESTAMP",
        "DATE", "TIME", "NUMERIC", "DECIMAL", "FLOAT", "DOUBLE", "JSON", "JSONB", "UUID",
        "ARRAY", "COALESCE", "NULLIF", "CAST", "EXTRACT", "INTERVAL",
    ]
}
