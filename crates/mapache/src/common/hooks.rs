use std::time::Duration;

use tokio::{process::Command, time::timeout};

use crate::{
    common::error::{MapacheError, Result},
    common::{
        config::CommandHooks,
        vars::{PASSWORD_ENVVAR, USERNAME_ENVVAR},
    },
};

/// Run the pre-hook. Fails (aborts the command) if the hook exits non-zero.
/// If `cli_hook` is provided and non-empty, it overrides the TOML pre-hook.
pub(crate) async fn run_pre(
    cmd_hooks: Option<&CommandHooks>,
    command_name: &str,
    repo: &str,
    cli_hook: Option<&str>,
) -> Result<()> {
    // CLI override takes priority
    if let Some(cmd) = cli_hook.filter(|s| !s.is_empty()) {
        if let Err(e) = run_hook(cmd, command_name, repo, None, None).await {
            tracing::error!(target: "hooks", "pre-hook failed: {e}");
            return Err(e);
        }
        return Ok(());
    }

    let Some(hook) = cmd_hooks.and_then(|h| h.pre.as_ref()) else {
        return Ok(());
    };

    if hook.command.is_empty() {
        return Ok(());
    }

    let timeout = hook.timeout.map(Duration::from_secs);
    if let Err(e) = run_hook(&hook.command, command_name, repo, None, timeout).await {
        tracing::error!(target: "hooks", "pre-hook failed: {e}");
        return Err(e);
    }

    Ok(())
}

/// Run the post-hook. Warnings are logged on failure.
/// If `cli_hook` is provided and non-empty, it overrides the TOML post-hook.
pub(crate) async fn run_post(
    cmd_hooks: Option<&CommandHooks>,
    command_name: &str,
    repo: &str,
    result: &str,
    cli_hook: Option<&str>,
) {
    // CLI override takes priority
    if let Some(cmd) = cli_hook.filter(|s| !s.is_empty()) {
        if let Err(e) = run_hook(cmd, command_name, repo, Some(result), None).await {
            tracing::warn!(target: "hooks", "post-hook warning: {e}");
        }
        return;
    }

    let Some(hook) = cmd_hooks.and_then(|h| h.post.as_ref()) else {
        return;
    };

    if hook.command.is_empty() {
        return;
    }

    let timeout = hook.timeout.map(Duration::from_secs);
    if let Err(e) = run_hook(&hook.command, command_name, repo, Some(result), timeout).await {
        tracing::warn!(target: "hooks", "post-hook warning: {e}");
    }
}

/// Runs the pre-hook for a command, skipping in dry-run mode.
pub(crate) async fn run_command_pre(
    cmd_hooks: Option<&CommandHooks>,
    command_name: &str,
    repo: &str,
    cli_hook: Option<&str>,
    dry_run: bool,
) -> Result<()> {
    if !dry_run {
        run_pre(cmd_hooks, command_name, repo, cli_hook).await?;
    }
    Ok(())
}

/// Runs the post-hook for a command, skipping in dry-run mode.
pub(crate) async fn run_command_post<E: std::fmt::Display>(
    cmd_hooks: Option<&CommandHooks>,
    command_name: &str,
    repo: &str,
    result: &std::result::Result<(), E>,
    cli_hook: Option<&str>,
    dry_run: bool,
) {
    if dry_run {
        return;
    }
    let result_str = match result {
        Ok(()) => "success".to_string(),
        Err(e) => format!("{e}"),
    };
    run_post(cmd_hooks, command_name, repo, &result_str, cli_hook).await;
}

async fn run_hook(
    shell_command: &str,
    command_name: &str,
    repo: &str,
    result: Option<&str>,
    timeout_duration: Option<Duration>,
) -> Result<()> {
    tracing::info!(target: "hooks", "Running hook for {command_name}");

    let mut child = if cfg!(windows) {
        let mut c = Command::new("cmd");
        c.arg("/C").arg(shell_command);
        c
    } else {
        let mut c = Command::new("sh");
        c.arg("-c").arg(shell_command);
        c
    };
    child.env("MAPACHE_COMMAND", command_name);
    child.env("MAPACHE_REPOSITORY", repo);

    if let Some(r) = result {
        child.env("MAPACHE_RESULT", r);
    }

    // Strip sensitive env vars so hooks can't read them.
    child.env_remove(USERNAME_ENVVAR);
    child.env_remove(PASSWORD_ENVVAR);

    let mut child = child.spawn().map_err(|e| {
        MapacheError::Hook(format!("failed to execute hook for {command_name}: {e}"))
    })?;

    let status = match timeout_duration {
        Some(t) => match timeout(t, child.wait()).await {
            Ok(Ok(status)) => status,
            Ok(Err(e)) => {
                return Err(MapacheError::Hook(format!(
                    "hook process for {command_name} failed: {e}"
                )));
            }
            Err(_) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                return Err(MapacheError::Hook(format!(
                    "hook for {command_name} timed out after {}s",
                    t.as_secs()
                )));
            }
        },
        None => child.wait().await.map_err(|e| {
            MapacheError::Hook(format!("hook process for {command_name} failed: {e}"))
        })?,
    };

    if !status.success() {
        let code = status
            .code()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "signal".to_string());
        return Err(MapacheError::Hook(format!(
            "hook for {command_name} exited with {code}"
        )));
    }

    Ok(())
}

#[cfg(all(test, not(target_os = "windows")))]
mod tests {
    use super::*;
    use crate::common::config::HookDef;

    fn pre_hooks(command: &str) -> CommandHooks {
        CommandHooks {
            pre: Some(HookDef {
                command: command.into(),
                timeout: None,
            }),
            post: None,
        }
    }

    fn post_hooks(command: &str) -> CommandHooks {
        CommandHooks {
            pre: None,
            post: Some(HookDef {
                command: command.into(),
                timeout: None,
            }),
        }
    }

    #[tokio::test]
    async fn test_run_hook() {
        assert!(run_hook("true", "cmd", "repo", None, None).await.is_ok());
        assert!(run_hook("false", "cmd", "repo", None, None).await.is_err());
        assert!(
            run_hook(
                "sleep 5",
                "cmd",
                "repo",
                None,
                Some(Duration::from_millis(100)),
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn test_run_hook_env() {
        assert!(
            run_hook(
                r#"test "$MAPACHE_COMMAND" = mycmd"#,
                "mycmd",
                "repo",
                None,
                None,
            )
            .await
            .is_ok()
        );
        assert!(
            run_hook(
                r#"test "$MAPACHE_REPOSITORY" = myrepo"#,
                "cmd",
                "myrepo",
                None,
                None,
            )
            .await
            .is_ok()
        );
        assert!(
            run_hook(
                r#"test "$MAPACHE_RESULT" = success"#,
                "cmd",
                "repo",
                Some("success"),
                None,
            )
            .await
            .is_ok()
        );
        assert!(
            run_hook(
                r#"test -z "${MAPACHE_RESULT-}" "#,
                "cmd",
                "repo",
                None,
                None,
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn test_run_pre() {
        assert!(run_pre(None, "cmd", "repo", None).await.is_ok());
        assert!(
            run_pre(Some(&pre_hooks("")), "cmd", "repo", None)
                .await
                .is_ok()
        );
        assert!(
            run_pre(Some(&pre_hooks("true")), "cmd", "repo", None)
                .await
                .is_ok()
        );
        assert!(
            run_pre(Some(&pre_hooks("false")), "cmd", "repo", None)
                .await
                .is_err()
        );
        // CLI override takes priority over TOML
        assert!(
            run_pre(Some(&pre_hooks("false")), "cmd", "repo", Some("true"))
                .await
                .is_ok()
        );
        assert!(run_pre(None, "cmd", "repo", Some("false")).await.is_err());
        // Empty CLI hook falls through to TOML
        assert!(
            run_pre(Some(&pre_hooks("true")), "cmd", "repo", Some(""))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_run_post() {
        run_post(None, "cmd", "repo", "success", None).await;
        run_post(Some(&post_hooks("")), "cmd", "repo", "success", None).await;
        run_post(Some(&post_hooks("false")), "cmd", "repo", "success", None).await;
        // CLI override takes priority
        run_post(
            Some(&post_hooks("false")),
            "cmd",
            "repo",
            "success",
            Some("true"),
        )
        .await;
        run_post(None, "cmd", "repo", "success", Some("false")).await;
    }

    #[tokio::test]
    async fn test_run_post_timeout() {
        assert!(
            run_hook(
                "sleep 5",
                "cmd",
                "repo",
                Some("success"),
                Some(Duration::from_millis(100)),
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn test_sensitive_env_stripped() {
        // Set sensitive env vars in the current process; the hook should NOT
        // see them because run_hook strips MAPACHE_USERNAME and MAPACHE_PASSWORD.
        // SAFETY: single-threaded test, no concurrent env access.
        unsafe {
            std::env::set_var(USERNAME_ENVVAR, "secret_user");
            std::env::set_var(PASSWORD_ENVVAR, "secret_pass");
        }

        // Hook succeeds if neither sensitive var is set.
        assert!(
            run_hook(
                r#"test -z "${MAPACHE_USERNAME-}" && test -z "${MAPACHE_PASSWORD-}" "#,
                "cmd",
                "repo",
                None,
                None,
            )
            .await
            .is_ok()
        );

        // SAFETY: single-threaded test, no concurrent env access.
        unsafe {
            std::env::remove_var(USERNAME_ENVVAR);
            std::env::remove_var(PASSWORD_ENVVAR);
        }
    }
}
