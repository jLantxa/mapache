use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::process::Command;
use tokio::time::timeout;

use crate::common::config::{CommandHooks, HooksConfig};

static HOOKS: OnceLock<HooksConfig> = OnceLock::new();

const DEFAULT_HOOKS: HooksConfig = HooksConfig {
    snapshot: None,
    restore: None,
    forget: None,
    clean: None,
    verify: None,
};

pub(crate) fn init(hooks: HooksConfig) {
    let _ = HOOKS.set(hooks);
}

pub(crate) fn config() -> &'static HooksConfig {
    HOOKS.get().unwrap_or(&DEFAULT_HOOKS)
}

macro_rules! hook_accessor {
    ($name:ident) => {
        pub(crate) fn $name() -> Option<&'static CommandHooks> {
            config().$name.as_ref()
        }
    };
}

hook_accessor!(snapshot);
hook_accessor!(restore);
hook_accessor!(forget);
hook_accessor!(clean);
hook_accessor!(verify);

/// Run the pre-hook. Fails (aborts the command) if the hook exits non-zero.
pub(crate) async fn run_pre(
    cmd_hooks: Option<&CommandHooks>,
    command_name: &str,
    repo: &str,
) -> Result<()> {
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
pub(crate) async fn run_post(
    cmd_hooks: Option<&CommandHooks>,
    command_name: &str,
    repo: &str,
    result: &str,
) {
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

    let mut child = child
        .spawn()
        .with_context(|| format!("Failed to execute hook for {command_name}"))?;

    let status = match timeout_duration {
        Some(t) => match timeout(t, child.wait()).await {
            Ok(Ok(status)) => status,
            Ok(Err(e)) => return Err(e).context(format!("Hook process for {command_name} failed")),
            Err(_) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                anyhow::bail!("Hook for {command_name} timed out after {}s", t.as_secs());
            }
        },
        None => child
            .wait()
            .await
            .context(format!("Hook process for {command_name} failed"))?,
    };

    if !status.success() {
        let code = status
            .code()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "signal".to_string());
        anyhow::bail!("Hook for {command_name} exited with {code}");
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
        assert!(run_pre(None, "cmd", "repo").await.is_ok());
        assert!(run_pre(Some(&pre_hooks("")), "cmd", "repo").await.is_ok());
        assert!(
            run_pre(Some(&pre_hooks("true")), "cmd", "repo")
                .await
                .is_ok()
        );
        assert!(
            run_pre(Some(&pre_hooks("false")), "cmd", "repo")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_run_post() {
        run_post(None, "cmd", "repo", "success").await;
        run_post(Some(&post_hooks("")), "cmd", "repo", "success").await;
        run_post(Some(&post_hooks("false")), "cmd", "repo", "success").await;
    }
}
