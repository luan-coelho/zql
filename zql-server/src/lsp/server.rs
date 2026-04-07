use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer, LspService, Server};

use crate::config::ZqlConfig;
use crate::db::connection::ConnectionManager;
use crate::db::schema::DatabaseSchema;

pub struct Backend {
    pub client: Client,
    pub connection_manager: Arc<RwLock<ConnectionManager>>,
    pub schema_cache: Arc<RwLock<Option<DatabaseSchema>>>,
    pub documents: Arc<RwLock<HashMap<Url, String>>>,
    pub config: Arc<RwLock<Option<ZqlConfig>>>,
    pub workspace_path: Arc<RwLock<Option<String>>>,
}

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
    async fn initialize(&self, params: InitializeParams) -> Result<InitializeResult> {
        // Extract workspace path from initialization
        if let Some(root_uri) = params.root_uri {
            if let Ok(path) = root_uri.to_file_path() {
                let mut wp = self.workspace_path.write().await;
                *wp = Some(path.to_string_lossy().to_string());
            }
        }

        Ok(InitializeResult {
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    TextDocumentSyncKind::FULL,
                )),
                completion_provider: Some(CompletionOptions {
                    trigger_characters: Some(vec![
                        ".".to_string(),
                        " ".to_string(),
                        ",".to_string(),
                    ]),
                    resolve_provider: Some(false),
                    ..Default::default()
                }),
                hover_provider: Some(HoverProviderCapability::Simple(true)),
                code_action_provider: Some(CodeActionProviderCapability::Simple(true)),
                execute_command_provider: Some(ExecuteCommandOptions {
                    commands: vec![
                        "zql.runQuery".to_string(),
                        "zql.explainQuery".to_string(),
                        "zql.switchConnection".to_string(),
                        "zql.refreshSchema".to_string(),
                        "zql.showSchema".to_string(),
                    ],
                    ..Default::default()
                }),
                document_formatting_provider: Some(OneOf::Left(true)),
                ..Default::default()
            },
            server_info: Some(ServerInfo {
                name: "zql-server".to_string(),
                version: Some(env!("CARGO_PKG_VERSION").to_string()),
            }),
            ..Default::default()
        })
    }

    async fn initialized(&self, _: InitializedParams) {
        self.client
            .log_message(MessageType::INFO, "ZQL server initialized")
            .await;

        // Try to load config from workspace
        let workspace_path = self.workspace_path.read().await.clone();
        if let Some(path) = workspace_path {
            match ZqlConfig::load_from_workspace(&path) {
                Ok(cfg) => {
                    {
                        let mut config = self.config.write().await;
                        *config = Some(cfg.clone());
                    }
                    {
                        let mut conn_manager = self.connection_manager.write().await;
                        conn_manager.load_config(cfg);
                    }
                    self.client
                        .log_message(MessageType::INFO, "Loaded .zql/connections.toml")
                        .await;

                    // Auto-refresh schema on startup
                    let _ = super::commands::refresh_schema(self).await;
                }
                Err(e) => {
                    self.client
                        .log_message(
                            MessageType::WARNING,
                            format!("No .zql/connections.toml found: {e}"),
                        )
                        .await;
                }
            }
        }
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        let uri = params.text_document.uri;
        let text = params.text_document.text;
        self.documents.write().await.insert(uri.clone(), text);
        self.publish_diagnostics(&uri).await;
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        let uri = params.text_document.uri;
        if let Some(change) = params.content_changes.into_iter().last() {
            self.documents
                .write()
                .await
                .insert(uri.clone(), change.text);
        }
        self.publish_diagnostics(&uri).await;
    }

    async fn did_close(&self, params: DidCloseTextDocumentParams) {
        self.documents.write().await.remove(&params.text_document.uri);
    }

    async fn completion(&self, params: CompletionParams) -> Result<Option<CompletionResponse>> {
        super::completions::handle_completion(self, params).await
    }

    async fn hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        super::hover::handle_hover(self, params).await
    }

    async fn code_action(&self, params: CodeActionParams) -> Result<Option<CodeActionResponse>> {
        super::code_actions::handle_code_action(self, params).await
    }

    async fn execute_command(&self, params: ExecuteCommandParams) -> Result<Option<serde_json::Value>> {
        super::commands::handle_execute_command(self, params).await
    }

    async fn formatting(
        &self,
        params: DocumentFormattingParams,
    ) -> Result<Option<Vec<TextEdit>>> {
        let uri = params.text_document.uri;
        let docs = self.documents.read().await;
        let Some(text) = docs.get(&uri) else {
            return Ok(None);
        };

        let formatted = crate::output::formatter::format_sql(text);
        let line_count = text.lines().count() as u32;

        Ok(Some(vec![TextEdit {
            range: Range {
                start: Position::new(0, 0),
                end: Position::new(line_count, 0),
            },
            new_text: formatted,
        }]))
    }

    async fn did_change_configuration(&self, params: DidChangeConfigurationParams) {
        if let Some(settings) = params.settings.as_object() {
            if let Some(workspace_path) = settings.get("workspacePath").and_then(|v| v.as_str()) {
                let mut wp = self.workspace_path.write().await;
                *wp = Some(workspace_path.to_string());
            }
        }
    }
}

impl Backend {
    async fn publish_diagnostics(&self, uri: &Url) {
        let docs = self.documents.read().await;
        let Some(text) = docs.get(uri) else {
            return;
        };

        let diagnostics =
            super::diagnostics::compute_diagnostics(text, &self.config, &self.schema_cache).await;
        self.client
            .publish_diagnostics(uri.clone(), diagnostics, None)
            .await;
    }
}

pub async fn run_server() -> anyhow::Result<()> {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, socket) = LspService::new(|client| Backend {
        client,
        connection_manager: Arc::new(RwLock::new(ConnectionManager::new())),
        schema_cache: Arc::new(RwLock::new(None)),
        documents: Arc::new(RwLock::new(HashMap::new())),
        config: Arc::new(RwLock::new(None)),
        workspace_path: Arc::new(RwLock::new(None)),
    });

    Server::new(stdin, stdout, socket).serve(service).await;
    Ok(())
}
