use std::fs::File;
use std::io::{BufReader, Write};
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;

use crate::config::{Config, ConfigLocation};

pub const FROZEN_CONFIG_ENV: &str = "WORKMUX_FROZEN_CONFIG";
const SNAPSHOT_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
struct FrozenConfigFile {
    version: u32,
    config: Config,
    selected_agent: Option<String>,
    agent_type: Option<String>,
    location: Option<ConfigLocation>,
}

/// Owns the host-side configuration snapshot for a sandbox supervisor.
pub struct FrozenConfigGuard {
    file: NamedTempFile,
}

impl FrozenConfigGuard {
    pub fn capture(config: &Config, location: Option<&ConfigLocation>) -> Result<Self> {
        let snapshot = FrozenConfigFile {
            version: SNAPSHOT_VERSION,
            config: config.clone(),
            selected_agent: config.selected_agent.clone(),
            agent_type: config.agent_type.clone(),
            location: location.cloned(),
        };
        let mut file = tempfile::Builder::new()
            .prefix("workmux-frozen-config-")
            .suffix(".json")
            .tempfile()
            .context("Failed to create frozen configuration snapshot")?;

        serde_json::to_writer(file.as_file_mut(), &snapshot)
            .context("Failed to write frozen configuration snapshot")?;
        file.flush()
            .context("Failed to flush frozen configuration snapshot")?;

        Ok(Self { file })
    }

    pub fn path(&self) -> &Path {
        self.file.path()
    }
}

pub fn load(path: &Path) -> Result<(Config, Option<ConfigLocation>)> {
    validate_path(path)?;

    let file = File::open(path).with_context(|| {
        format!(
            "Failed to open frozen configuration snapshot: {}",
            path.display()
        )
    })?;
    let mut snapshot: FrozenConfigFile = serde_json::from_reader(BufReader::new(file))
        .with_context(|| {
            format!(
                "Failed to parse frozen configuration snapshot: {}",
                path.display()
            )
        })?;

    if snapshot.version != SNAPSHOT_VERSION {
        bail!(
            "Unsupported frozen configuration snapshot version {}",
            snapshot.version
        );
    }

    snapshot.config.selected_agent = snapshot.selected_agent;
    snapshot.config.agent_type = snapshot.agent_type;
    Ok((snapshot.config, snapshot.location))
}

fn validate_path(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        bail!(
            "Frozen configuration snapshot path must be absolute: {}",
            path.display()
        );
    }

    let metadata = std::fs::symlink_metadata(path).with_context(|| {
        format!(
            "Frozen configuration snapshot is unavailable: {}",
            path.display()
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        bail!(
            "Frozen configuration snapshot must be a regular file: {}",
            path.display()
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::config::StatusIcons;

    fn sample_config() -> Config {
        Config {
            agent: Some("claude --model sonnet".to_string()),
            agent_type: Some("claude".to_string()),
            selected_agent: Some("reviewer".to_string()),
            status_icons: StatusIcons {
                working: Some("work".to_string()),
                waiting: Some("wait".to_string()),
                done: Some("done".to_string()),
            },
            ..Default::default()
        }
    }

    fn sample_location(root: &Path) -> ConfigLocation {
        ConfigLocation {
            config_path: root.join("app/.workmux.yaml"),
            config_dir: root.join("app"),
            rel_dir: PathBuf::from("app"),
        }
    }

    #[test]
    fn round_trip_preserves_resolved_config_and_location() {
        let root = tempfile::tempdir().unwrap();
        let config = sample_config();
        let location = sample_location(root.path());
        let guard = FrozenConfigGuard::capture(&config, Some(&location)).unwrap();

        let (loaded, loaded_location) = load(guard.path()).unwrap();

        assert_eq!(loaded.agent, config.agent);
        assert_eq!(loaded.agent_type, config.agent_type);
        assert_eq!(loaded.selected_agent, config.selected_agent);
        assert_eq!(loaded.status_icons.working, config.status_icons.working);
        assert_eq!(loaded.status_icons.waiting, config.status_icons.waiting);
        assert_eq!(loaded.status_icons.done, config.status_icons.done);
        assert_eq!(loaded_location, Some(location));
    }

    #[test]
    fn snapshot_file_is_private() {
        let guard = FrozenConfigGuard::capture(&sample_config(), None).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(guard.path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
    }

    #[test]
    fn rejects_relative_path() {
        let error = load(Path::new("snapshot.json")).unwrap_err();
        assert!(error.to_string().contains("must be absolute"));
    }

    #[test]
    fn rejects_directory() {
        let dir = tempfile::tempdir().unwrap();
        let error = load(dir.path()).unwrap_err();
        assert!(error.to_string().contains("must be a regular file"));
    }

    #[test]
    fn rejects_symlink() {
        #[cfg(unix)]
        {
            let dir = tempfile::tempdir().unwrap();
            let target = dir.path().join("target.json");
            std::fs::write(&target, "{}").unwrap();
            let link = dir.path().join("snapshot.json");
            std::os::unix::fs::symlink(target, &link).unwrap();

            let error = load(&link).unwrap_err();
            assert!(error.to_string().contains("must be a regular file"));
        }
    }

    #[test]
    fn rejects_malformed_snapshot() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"not json").unwrap();
        file.flush().unwrap();

        let error = load(file.path()).unwrap_err();
        assert!(error.to_string().contains("Failed to parse"));
    }

    #[test]
    fn rejects_unknown_version() {
        let config = sample_config();
        let snapshot = FrozenConfigFile {
            version: SNAPSHOT_VERSION + 1,
            selected_agent: config.selected_agent.clone(),
            agent_type: config.agent_type.clone(),
            config,
            location: None,
        };
        let mut file = NamedTempFile::new().unwrap();
        serde_json::to_writer(&mut file, &snapshot).unwrap();
        file.flush().unwrap();

        let error = load(file.path()).unwrap_err();
        assert!(error.to_string().contains("Unsupported"));
    }
}
