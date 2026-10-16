use std::io;

use clap::{Args, ValueEnum};

use crate::{
    backend::new_backend_with_prompt,
    commands::{self, GlobalArgs, ToExitCode, with_repository_lock},
    common::error::MapacheError,
    ui::tui::{self, Appearance, ColorMode, ThemeVariant},
};

/// Colour scheme for the TUI.
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum ThemeArg {
    /// Detect a light or dark terminal, defaulting to dark.
    Auto,
    /// Always use the dark theme.
    Dark,
    /// Always use the light theme.
    Light,
}

/// Colour depth for the TUI.
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum ColorArg {
    /// Detect the terminal's colour support.
    Auto,
    /// 24-bit colour.
    Truecolor,
    /// The 256-colour xterm palette.
    Ansi256,
    /// The 16 base ANSI colours.
    Ansi16,
    /// Disable colour (same as `NO_COLOR`).
    Never,
}

#[derive(Debug, thiserror::Error)]
pub enum TuiError {
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for TuiError {
    fn to_exit_code(&self) -> i32 {
        match self {
            TuiError::Repo(_) => 1,
            TuiError::Io(_) => 4,
        }
    }
}

#[derive(Args, Debug, Clone)]
#[clap(
    about = "Launch interactive terminal user interface",
    long_about = "Launch an interactive terminal user interface for managing the\n\
        repository: browse snapshots and files, create new snapshots, restore\n\
        data and manage retention policies."
)]
pub struct CmdArgs {
    /// Colour scheme
    #[clap(long, value_enum, default_value = "auto")]
    pub theme: ThemeArg,

    /// Colour depth
    #[clap(long, value_enum, default_value = "auto")]
    pub color: ColorArg,
}

impl CmdArgs {
    /// Resolves the CLI choices into the theme the TUI draws with.
    fn appearance(&self) -> Appearance {
        let variant = match self.theme {
            ThemeArg::Auto => ThemeVariant::detect(),
            ThemeArg::Dark => ThemeVariant::Dark,
            ThemeArg::Light => ThemeVariant::Light,
        };
        let color = match self.color {
            ColorArg::Auto => ColorMode::detect(),
            ColorArg::Truecolor => ColorMode::TrueColor,
            ColorArg::Ansi256 => ColorMode::Ansi256,
            ColorArg::Ansi16 => ColorMode::Ansi16,
            ColorArg::Never => ColorMode::None,
        };
        Appearance { variant, color }
    }
}

pub async fn run(
    global_args: &GlobalArgs,
    args: &CmdArgs,
    snapshot_config: Option<commands::cmd_snapshot::CmdArgs>,
    forget_config: Option<commands::cmd_forget::CmdArgs>,
) -> Result<(), TuiError> {
    let backend = new_backend_with_prompt(global_args.backend_options(false)).await?;
    let repo_path = global_args.repo.clone();

    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.password_command.as_deref(),
        global_args.key.as_ref(),
        backend,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, _secure_storage, lock_handle| async move {
            repo.reload_master_index().await?;
            tui::run(
                repo,
                lock_handle,
                repo_path,
                snapshot_config,
                forget_config,
                args.appearance(),
            )
            .await
            .map_err(|e| TuiError::Repo(MapacheError::Internal(e.to_string())))
        },
    )
    .await
}
