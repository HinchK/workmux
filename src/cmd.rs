use anyhow::{Context, Result, anyhow};
use std::path::Path;
use std::process::{Command, Output, Stdio};
use tracing::{debug, trace};

/// A builder for executing shell commands with unified error handling
pub struct Cmd<'a> {
    command: &'a str,
    args: Vec<&'a str>,
    workdir: Option<&'a Path>,
}

impl<'a> Cmd<'a> {
    /// Create a new command builder
    pub fn new(command: &'a str) -> Self {
        Self {
            command,
            args: Vec::new(),
            workdir: None,
        }
    }

    /// Add a single argument
    pub fn arg(mut self, arg: &'a str) -> Self {
        self.args.push(arg);
        self
    }

    /// Add multiple arguments
    pub fn args(mut self, args: &[&'a str]) -> Self {
        self.args.extend_from_slice(args);
        self
    }

    /// Set the working directory for the command
    pub fn workdir(mut self, path: &'a Path) -> Self {
        self.workdir = Some(path);
        self
    }

    /// Execute the command and return the output
    /// Returns an error if the command fails (non-zero exit code)
    pub fn run(self) -> Result<Output> {
        let Cmd {
            command,
            args,
            workdir,
        } = self;
        let workdir_display = workdir.map(|p| p.display().to_string());

        trace!(command, args = ?args, workdir = ?workdir_display, "cmd:run start");

        let mut cmd = if command == "git" {
            crate::git::unattended_git(workdir)?
        } else {
            let mut command = Command::new(command);
            if let Some(dir) = workdir {
                command.current_dir(dir);
            }
            command
        };
        let output = cmd.args(&args).output().with_context(|| {
            format!("Failed to execute command: {} {}", command, args.join(" "))
        })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            debug!(
                command,
                args = ?args,
                status = ?output.status.code(),
                stderr = %stderr.trim(),
                "cmd:run failure"
            );
            return Err(anyhow!(
                "Command failed: {} {}\n{}",
                command,
                args.join(" "),
                stderr.trim()
            ));
        }
        trace!(command, "cmd:run success");
        Ok(output)
    }

    /// Execute the command and return stdout as a trimmed string
    pub fn run_and_capture_stdout(self) -> Result<String> {
        let output = self.run()?;
        Ok(String::from_utf8(output.stdout)?.trim().to_string())
    }

    /// Execute the command, returning Ok(true) if it succeeds, Ok(false) if it fails
    /// This is useful for commands that are used as checks (e.g., git rev-parse --verify)
    pub fn run_as_check(self) -> Result<bool> {
        let Cmd {
            command,
            args,
            workdir,
        } = self;
        let workdir_display = workdir.map(|p| p.display().to_string());
        trace!(command, args = ?args, workdir = ?workdir_display, "cmd:check start");

        let mut cmd = if command == "git" {
            crate::git::unattended_git(workdir)?
        } else {
            let mut command = Command::new(command);
            if let Some(dir) = workdir {
                command.current_dir(dir);
            }
            command
        };
        let output = cmd.args(&args).output().with_context(|| {
            format!("Failed to execute command: {} {}", command, args.join(" "))
        })?;

        let success = output.status.success();
        trace!(command, success, "cmd:check result");
        Ok(success)
    }
}

/// Helper to create a shell command with additional environment variables
pub fn shell_command_with_env(
    hook_shell: Option<&[String]>,
    command: &str,
    workdir: &Path,
    env_vars: &[(&str, &str)],
) -> Result<()> {
    shell_command_with_env_output(hook_shell, command, workdir, env_vars, true)
}

/// Run a lifecycle hook with additional environment variables and optional output inheritance.
/// The hook command is appended after all configured shell arguments.
pub fn shell_command_with_env_output(
    hook_shell: Option<&[String]>,
    command: &str,
    workdir: &Path,
    env_vars: &[(&str, &str)],
    inherit_output: bool,
) -> Result<()> {
    let default_shell = ["bash".to_string(), "-c".to_string()];
    let argv = hook_shell.unwrap_or(&default_shell);
    let (executable, args) = argv
        .split_first()
        .ok_or_else(|| anyhow!("'hook_shell' must contain an executable"))?;
    if executable.trim().is_empty() {
        return Err(anyhow!("'hook_shell' executable must not be empty"));
    }

    let mut cmd = Command::new(executable);
    cmd.args(args).arg(command).current_dir(workdir);

    if !inherit_output {
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
    }

    for (key, value) in env_vars {
        cmd.env(key, value);
    }

    let status = cmd.status().with_context(|| {
        format!(
            "Failed to execute lifecycle hook shell '{}': {}",
            executable, command
        )
    })?;

    if !status.success() {
        return Err(anyhow!(
            "Lifecycle hook command failed with exit code {} using '{}': {}",
            status.code().unwrap_or(-1),
            executable,
            command
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn lifecycle_hook_default_is_bash_c() {
        let temp = TempDir::new().unwrap();
        let output = temp.path().join("default-shell");
        let command = format!("printf compatible > '{}'", output.display());

        shell_command_with_env(None, &command, temp.path(), &[]).unwrap();

        assert_eq!(std::fs::read_to_string(output).unwrap(), "compatible");
    }

    #[cfg(unix)]
    #[test]
    fn lifecycle_hook_uses_configured_executable_and_appends_command() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().unwrap();
        let executable = temp.path().join("record-argv");
        let output = temp.path().join("argv");
        std::fs::write(
            &executable,
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$ARGV_OUTPUT\"\n",
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&executable).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&executable, permissions).unwrap();
        let hook_shell = vec![
            executable.to_string_lossy().into_owned(),
            "--configured-argument".to_string(),
        ];

        shell_command_with_env(
            Some(&hook_shell),
            "the hook command",
            temp.path(),
            &[("ARGV_OUTPUT", output.to_str().unwrap())],
        )
        .unwrap();

        assert_eq!(
            std::fs::read_to_string(output).unwrap(),
            "--configured-argument\nthe hook command\n"
        );
    }

    #[test]
    fn unavailable_lifecycle_hook_executable_is_named_in_error() {
        let hook_shell = vec!["/workmux/missing/hook-shell".to_string(), "-c".to_string()];
        let error = shell_command_with_env(
            Some(&hook_shell),
            "true",
            std::env::temp_dir().as_path(),
            &[],
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("/workmux/missing/hook-shell"));
    }
}
