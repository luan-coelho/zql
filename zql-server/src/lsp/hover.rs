use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;

use super::server::Backend;

pub async fn handle_hover(
    backend: &Backend,
    params: HoverParams,
) -> Result<Option<Hover>> {
    let uri = params.text_document_position_params.text_document.uri;
    let position = params.text_document_position_params.position;

    let docs = backend.documents.read().await;
    let Some(text) = docs.get(&uri) else {
        return Ok(None);
    };

    let Some(word) = word_at_position(text, position) else {
        return Ok(None);
    };

    let schema = backend.schema_cache.read().await;
    let Some(schema) = schema.as_ref() else {
        return Ok(None);
    };

    // Check if the word is a table name
    if let Some(table) = schema.tables.iter().find(|t| t.name == word) {
        let mut md = format!("### Table: `{}.{}`\n\n", table.schema, table.name);

        md.push_str("| Column | Type | Nullable | Default |\n");
        md.push_str("|--------|------|----------|---------|\n");

        for col in &table.columns {
            let pk = if col.is_primary_key { " PK" } else { "" };
            let fk = col
                .foreign_key
                .as_ref()
                .map(|fk| format!(" -> {}.{}", fk.referenced_table, fk.referenced_column))
                .unwrap_or_default();

            md.push_str(&format!(
                "| `{}`{}{} | {} | {} | {} |\n",
                col.name,
                pk,
                fk,
                col.data_type,
                if col.is_nullable { "YES" } else { "NO" },
                col.default_value.as_deref().unwrap_or("-"),
            ));
        }

        if let Some(count) = table.row_count_estimate {
            md.push_str(&format!("\n*~{count} rows (estimate)*\n"));
        }

        return Ok(Some(Hover {
            contents: HoverContents::Markup(MarkupContent {
                kind: MarkupKind::Markdown,
                value: md,
            }),
            range: None,
        }));
    }

    // Check if the word is a column name in any table
    for table in &schema.tables {
        if let Some(col) = table.columns.iter().find(|c| c.name == word) {
            let mut md = format!(
                "### Column: `{}.{}`\n\n",
                table.name, col.name
            );
            md.push_str(&format!("- **Type:** `{}`\n", col.data_type));
            md.push_str(&format!(
                "- **Nullable:** {}\n",
                if col.is_nullable { "YES" } else { "NO" }
            ));
            if let Some(ref default) = col.default_value {
                md.push_str(&format!("- **Default:** `{default}`\n"));
            }
            if col.is_primary_key {
                md.push_str("- **Primary Key**\n");
            }
            if let Some(ref fk) = col.foreign_key {
                md.push_str(&format!(
                    "- **FK:** -> `{}.{}`\n",
                    fk.referenced_table, fk.referenced_column
                ));
            }

            return Ok(Some(Hover {
                contents: HoverContents::Markup(MarkupContent {
                    kind: MarkupKind::Markdown,
                    value: md,
                }),
                range: None,
            }));
        }
    }

    Ok(None)
}

fn word_at_position(text: &str, position: Position) -> Option<String> {
    let line = text.lines().nth(position.line as usize)?;
    let col = position.character as usize;

    if col > line.len() {
        return None;
    }

    let start = line[..col]
        .rfind(|c: char| !c.is_alphanumeric() && c != '_')
        .map(|i| i + 1)
        .unwrap_or(0);

    let end = line[col..]
        .find(|c: char| !c.is_alphanumeric() && c != '_')
        .map(|i| col + i)
        .unwrap_or(line.len());

    let word = &line[start..end];
    if word.is_empty() {
        None
    } else {
        Some(word.to_string())
    }
}
