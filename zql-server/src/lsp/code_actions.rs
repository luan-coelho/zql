use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;

use super::server::Backend;

pub async fn handle_code_action(
    _backend: &Backend,
    params: CodeActionParams,
) -> Result<Option<CodeActionResponse>> {
    let uri = params.text_document.uri;
    let range = params.range;

    let mut actions = Vec::new();

    // Run Query action
    actions.push(CodeActionOrCommand::Command(Command {
        title: "ZQL: Run Query".to_string(),
        command: "zql.runQuery".to_string(),
        arguments: Some(vec![
            serde_json::json!(uri.to_string()),
            serde_json::json!(range.start.line),
            serde_json::json!(range.end.line),
        ]),
    }));

    // Explain Query action
    actions.push(CodeActionOrCommand::Command(Command {
        title: "ZQL: Explain Query".to_string(),
        command: "zql.explainQuery".to_string(),
        arguments: Some(vec![
            serde_json::json!(uri.to_string()),
            serde_json::json!(range.start.line),
            serde_json::json!(range.end.line),
        ]),
    }));

    Ok(Some(actions))
}
