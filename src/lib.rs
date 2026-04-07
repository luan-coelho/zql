use std::fs;
use zed_extension_api::{self as zed, LanguageServerId, Result};

struct ZqlExtension {
    cached_binary_path: Option<String>,
}

const SERVER_NAME: &str = "zql-server";
const GITHUB_REPO: &str = "luan-coelho/zql";

impl ZqlExtension {
    fn language_server_binary_path(
        &mut self,
        language_server_id: &LanguageServerId,
    ) -> Result<String> {
        // Return cached path if still valid
        if let Some(path) = &self.cached_binary_path {
            if fs::metadata(path).map_or(false, |m| m.is_file()) {
                return Ok(path.clone());
            }
        }

        // Check if binary exists in extension directory (copied manually or from previous download)
        if fs::metadata(SERVER_NAME).map_or(false, |m| m.is_file()) {
            self.cached_binary_path = Some(SERVER_NAME.to_string());
            return Ok(SERVER_NAME.to_string());
        }

        // Try to find in ~/.cargo/bin (installed via `cargo install`)
        if let Ok(home) = std::env::var("HOME") {
            let cargo_path = format!("{home}/.cargo/bin/{SERVER_NAME}");
            if fs::metadata(&cargo_path).map_or(false, |m| m.is_file()) {
                self.cached_binary_path = Some(cargo_path.clone());
                return Ok(cargo_path);
            }
        }

        // Try to download from GitHub Releases
        self.download_from_github(language_server_id)
    }

    fn download_from_github(
        &mut self,
        language_server_id: &LanguageServerId,
    ) -> Result<String> {
        zed::set_language_server_installation_status(
            language_server_id,
            &zed::LanguageServerInstallationStatus::CheckingForUpdate,
        );

        let release = zed::latest_github_release(
            GITHUB_REPO,
            zed::GithubReleaseOptions {
                require_assets: true,
                pre_release: false,
            },
        )
        .map_err(|e| format!("failed to fetch latest release: {e}"))?;

        let (platform, arch) = zed::current_platform();

        let asset_stem = format!(
            "{SERVER_NAME}-{arch}-{platform}",
            arch = match arch {
                zed::Architecture::Aarch64 => "aarch64",
                zed::Architecture::X8664 => "x86_64",
                zed::Architecture::X86 => return Err("x86 not supported".into()),
            },
            platform = match platform {
                zed::Os::Mac => "apple-darwin",
                zed::Os::Linux => "unknown-linux-musl",
                zed::Os::Windows => "pc-windows-msvc",
            },
        );

        let asset_name = format!(
            "{asset_stem}.{ext}",
            ext = match platform {
                zed::Os::Windows => "zip",
                _ => "tar.gz",
            }
        );

        let asset = release
            .assets
            .iter()
            .find(|a| a.name == asset_name)
            .ok_or_else(|| format!("no asset found matching {asset_name}"))?;

        let version_dir = format!("{SERVER_NAME}-{}", release.version);
        let binary_name = match platform {
            zed::Os::Windows => format!("{SERVER_NAME}.exe"),
            _ => SERVER_NAME.to_string(),
        };
        let binary_path = format!("{version_dir}/{binary_name}");

        if !fs::metadata(&binary_path).map_or(false, |m| m.is_file()) {
            zed::set_language_server_installation_status(
                language_server_id,
                &zed::LanguageServerInstallationStatus::Downloading,
            );

            zed::download_file(
                &asset.download_url,
                &version_dir,
                match platform {
                    zed::Os::Windows => zed::DownloadedFileType::Zip,
                    _ => zed::DownloadedFileType::GzipTar,
                },
            )
            .map_err(|e| format!("failed to download server binary: {e}"))?;

            zed::make_file_executable(&binary_path)
                .map_err(|e| format!("failed to make binary executable: {e}"))?;
        }

        self.cached_binary_path = Some(binary_path.clone());
        Ok(binary_path)
    }
}

impl zed::Extension for ZqlExtension {
    fn new() -> Self {
        ZqlExtension {
            cached_binary_path: None,
        }
    }

    fn language_server_command(
        &mut self,
        language_server_id: &LanguageServerId,
        _worktree: &zed::Worktree,
    ) -> Result<zed::Command> {
        let binary_path = self.language_server_binary_path(language_server_id)?;

        Ok(zed::Command {
            command: binary_path,
            args: vec!["lsp".to_string()],
            env: Default::default(),
        })
    }

    fn language_server_workspace_configuration(
        &mut self,
        _language_server_id: &LanguageServerId,
        worktree: &zed::Worktree,
    ) -> Result<Option<zed::serde_json::Value>> {
        let settings = zed::serde_json::json!({
            "workspacePath": worktree.root_path(),
        });
        Ok(Some(settings))
    }
}

zed::register_extension!(ZqlExtension);
