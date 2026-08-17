//! Shared extension file lifecycle helpers for agent setup.

use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

use super::StatusCheck;

/// Check whether an extension file contains the bundled source.
pub fn check_installed(path: Option<&Path>, source: &str) -> Result<StatusCheck> {
    let Some(path) = path else {
        return Ok(StatusCheck::NotInstalled);
    };

    if !path.exists() {
        return Ok(StatusCheck::NotInstalled);
    }

    let installed = fs::read_to_string(path)?;
    if installed == source {
        Ok(StatusCheck::Installed)
    } else {
        Ok(StatusCheck::NotInstalled)
    }
}

/// Create parent directories and write an extension source file.
pub fn install_extension_file(
    path: &Path,
    source: &str,
    mkdir_context: &str,
    write_context: &str,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).context(mkdir_context.to_string())?;
    }

    fs::write(path, source).context(write_context.to_string())?;
    Ok(())
}

/// Remove an extension file and clean up an empty parent directory.
pub fn remove_extension_file(path: &Path) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }

    fs::remove_file(path)?;
    if let Some(parent) = path.parent()
        && parent.read_dir().is_ok_and(|mut it| it.next().is_none())
    {
        let _ = fs::remove_dir(parent);
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_installed_requires_bundled_source() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("workmux-status.ts");
        fs::write(&path, "old extension").unwrap();

        assert!(matches!(
            check_installed(Some(&path), "current extension").unwrap(),
            StatusCheck::NotInstalled
        ));

        fs::write(&path, "current extension").unwrap();

        assert!(matches!(
            check_installed(Some(&path), "current extension").unwrap(),
            StatusCheck::Installed
        ));
    }

    #[test]
    fn check_installed_handles_missing_path() {
        assert!(matches!(
            check_installed(None, "current extension").unwrap(),
            StatusCheck::NotInstalled
        ));
    }
}
