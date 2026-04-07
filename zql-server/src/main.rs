mod config;
mod db;
mod lsp;
mod output;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "zql-server", version, about = "ZQL - Database Language Server")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the LSP server over stdin/stdout
    Lsp,
    /// Execute a SQL query and print results
    Exec {
        /// SQL query to execute directly
        #[arg(long)]
        query: Option<String>,
        /// Path to .sql file containing the query
        #[arg(long)]
        file: Option<String>,
        /// Line number in the file (1-based) to find the statement
        #[arg(long)]
        line: Option<usize>,
        /// Workspace root path (to find .zql/connections.toml)
        #[arg(long)]
        workspace: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Lsp => lsp::run_server().await?,
        Commands::Exec {
            query,
            file,
            line,
            workspace,
        } => exec_query(query, file, line, workspace).await?,
    }

    Ok(())
}

async fn exec_query(
    query: Option<String>,
    file: Option<String>,
    line: Option<usize>,
    workspace: Option<String>,
) -> anyhow::Result<()> {
    // Resolve workspace path
    let ws_path = workspace
        .or_else(|| std::env::current_dir().ok().map(|p| p.to_string_lossy().to_string()))
        .ok_or_else(|| anyhow::anyhow!("could not determine workspace path"))?;

    // Load config
    let cfg = config::ZqlConfig::load_from_workspace(&ws_path)
        .map_err(|e| anyhow::anyhow!("failed to load config from {ws_path}: {e}"))?;

    let mut conn = db::connection::ConnectionManager::new();
    conn.load_config(cfg);

    // Resolve the SQL to execute
    let sql = if let Some(q) = query {
        q
    } else if let Some(ref file_path) = file {
        let content = std::fs::read_to_string(file_path)
            .map_err(|e| anyhow::anyhow!("failed to read {file_path}: {e}"))?;
        if let Some(line_num) = line {
            extract_statement_at_line(&content, line_num.saturating_sub(1))
        } else {
            content.trim().to_string()
        }
    } else {
        anyhow::bail!("either --query or --file is required");
    };

    if sql.trim().is_empty() {
        anyhow::bail!("empty query");
    }

    eprintln!("Executing: {}", sql.lines().next().unwrap_or(""));

    let result = conn.execute_query(&sql).await?;
    print!("{}", output::formatter::format_query_result_terminal(&result));

    Ok(())
}

fn extract_statement_at_line(text: &str, line: usize) -> String {
    let mut statements: Vec<(usize, usize, String)> = Vec::new();
    let mut current_stmt = String::new();
    let mut stmt_start = 0;
    let mut last_line = 0;

    for (i, l) in text.lines().enumerate() {
        let trimmed = l.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            if !current_stmt.trim().is_empty() && trimmed.is_empty() {
                statements.push((stmt_start, i.saturating_sub(1), current_stmt.trim().to_string()));
                current_stmt.clear();
            }
            if current_stmt.is_empty() {
                stmt_start = i + 1;
            }
            continue;
        }

        if current_stmt.is_empty() {
            stmt_start = i;
        }
        current_stmt.push_str(l);
        current_stmt.push('\n');

        if trimmed.ends_with(';') {
            statements.push((stmt_start, i, current_stmt.trim().to_string()));
            current_stmt.clear();
        }
        last_line = i;
    }

    if !current_stmt.trim().is_empty() {
        statements.push((stmt_start, last_line, current_stmt.trim().to_string()));
    }

    for (start, end, stmt) in &statements {
        if line >= *start && line <= *end {
            return stmt.trim_end_matches(';').trim().to_string();
        }
    }

    text.trim().to_string()
}
