//! Copilot CLI status tracking setup.
//!
//! Detects Copilot CLI through its configuration directory and installs a
//! personal hook under `~/.copilot/hooks/`.

use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

use super::StatusCheck;

/// Hooks configuration embedded at compile time.
const HOOKS_JSON: &str = include_str!("../../resources/copilot/hooks/workmux-status/hooks.json");
const HOOKS_FILE_NAME: &str = "workmux-status.json";

fn copilot_dir() -> Option<PathBuf> {
    if let Ok(dir) = std::env::var("COPILOT_CONFIG_DIR") {
        return Some(PathBuf::from(dir));
    }
    home::home_dir().map(|h| h.join(".copilot"))
}

fn hooks_file() -> Option<PathBuf> {
    home::home_dir().map(|home| hooks_file_at(&home))
}

fn hooks_file_at(home: &Path) -> PathBuf {
    home.join(".copilot/hooks").join(HOOKS_FILE_NAME)
}

/// Detect Copilot CLI through its configuration directory.
pub fn detect() -> Option<&'static str> {
    if copilot_dir().is_some_and(|d| d.is_dir()) {
        return Some("found Copilot config directory");
    }
    None
}

/// Check whether the workmux personal hook is installed for Copilot CLI.
pub fn check() -> Result<StatusCheck> {
    let Some(path) = hooks_file() else {
        return Ok(StatusCheck::NotInstalled);
    };
    check_at(&path)
}

fn check_at(path: &Path) -> Result<StatusCheck> {
    if !path.is_file() {
        return Ok(StatusCheck::NotInstalled);
    }

    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read Copilot hooks from {}", path.display()))?;
    if content.contains("workmux set-window-status") {
        Ok(StatusCheck::Installed)
    } else {
        Ok(StatusCheck::NotInstalled)
    }
}

/// Install the workmux personal hook for Copilot CLI.
pub fn install() -> Result<String> {
    let path = hooks_file().context("Could not determine home directory")?;
    install_at(&path)
}

fn install_at(path: &Path) -> Result<String> {
    let hooks_dir = path
        .parent()
        .context("Copilot hooks path has no parent directory")?;
    fs::create_dir_all(hooks_dir)
        .with_context(|| format!("Failed to create {}", hooks_dir.display()))?;
    fs::write(path, HOOKS_JSON)
        .with_context(|| format!("Failed to write Copilot hooks to {}", path.display()))?;

    Ok(format!("Installed hooks to {}", path.display()))
}

/// Remove the workmux personal hook for Copilot CLI.
pub fn uninstall() -> Result<String> {
    let Some(path) = hooks_file() else {
        return Ok("Home directory not found, no Copilot hooks removed".to_string());
    };
    uninstall_at(&path)
}

fn uninstall_at(path: &Path) -> Result<String> {
    if !path.exists() {
        return Ok("No Copilot personal hooks found".to_string());
    }

    fs::remove_file(path)
        .with_context(|| format!("Failed to remove Copilot hooks from {}", path.display()))?;

    if let Some(hooks_dir) = path.parent()
        && hooks_dir
            .read_dir()
            .is_ok_and(|mut entries| entries.next().is_none())
    {
        let _ = fs::remove_dir(hooks_dir);
    }

    Ok(format!("Removed Copilot hooks from {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hooks_json_is_valid() {
        let parsed: serde_json::Value =
            serde_json::from_str(HOOKS_JSON).expect("embedded hooks.json is valid JSON");
        assert_eq!(parsed.get("version").and_then(|v| v.as_u64()), Some(1));
        let hooks = parsed.get("hooks").unwrap().as_object().unwrap();
        assert!(hooks.contains_key("userPromptSubmitted"));
        assert!(hooks.contains_key("postToolUse"));
        assert!(hooks.contains_key("agentStop"));
    }

    #[test]
    fn test_hooks_json_contains_workmux_command() {
        assert!(HOOKS_JSON.contains("workmux set-window-status"));
    }

    #[test]
    fn personal_hooks_path_is_under_home() {
        let home = Path::new("/home/tester");
        assert_eq!(
            hooks_file_at(home),
            home.join(".copilot/hooks/workmux-status.json")
        );
    }

    #[test]
    fn check_requires_personal_hook() {
        let tmp = tempfile::tempdir().unwrap();
        let repository_hook = tmp
            .path()
            .join("repo/.github/hooks/workmux-status/hooks.json");
        fs::create_dir_all(repository_hook.parent().unwrap()).unwrap();
        fs::write(&repository_hook, HOOKS_JSON).unwrap();

        let personal_hook = hooks_file_at(tmp.path());
        assert!(matches!(
            check_at(&personal_hook).unwrap(),
            StatusCheck::NotInstalled
        ));
    }

    #[test]
    fn install_and_check_are_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let path = hooks_file_at(tmp.path());

        install_at(&path).unwrap();
        install_at(&path).unwrap();

        assert_eq!(fs::read_to_string(&path).unwrap(), HOOKS_JSON);
        assert!(matches!(check_at(&path).unwrap(), StatusCheck::Installed));
    }

    #[test]
    fn uninstall_without_personal_hook_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let path = hooks_file_at(tmp.path());

        let result = uninstall_at(&path).unwrap();
        assert!(result.contains("No Copilot personal hooks found"));
    }

    #[test]
    fn uninstall_preserves_other_personal_hooks() {
        let tmp = tempfile::tempdir().unwrap();
        let path = hooks_file_at(tmp.path());
        install_at(&path).unwrap();
        let other_hook = path.parent().unwrap().join("other.json");
        fs::write(&other_hook, "{}").unwrap();

        let result = uninstall_at(&path).unwrap();

        assert!(result.contains("Removed Copilot hooks"));
        assert!(!path.exists());
        assert!(other_hook.exists());
        assert!(path.parent().unwrap().exists());
    }

    #[test]
    fn uninstall_removes_empty_hooks_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let path = hooks_file_at(tmp.path());
        install_at(&path).unwrap();

        uninstall_at(&path).unwrap();

        assert!(!path.exists());
        assert!(!path.parent().unwrap().exists());
        assert!(tmp.path().join(".copilot").exists());
    }
}
