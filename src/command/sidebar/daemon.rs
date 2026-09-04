//! Sidebar daemon: single process that polls tmux and pushes snapshots to clients.

use anyhow::Result;
use ignore::gitignore::Gitignore;
use notify::{EventKindMask, RecursiveMode, Watcher};
use signal_hook::iterator::{Handle as SignalHandle, Signals};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::cmd::Cmd;
use crate::config::{Config, SidebarPosition};
use crate::git::GitStatus;
use crate::github::{CheckSummary, PrSummary};
use crate::multiplexer::{LivePaneInfo, Multiplexer, TmuxBackend, create_backend, detect_backend};
use crate::state::StateStore;

use super::app::{SidebarFilterMode, SidebarLayoutMode};
use super::snapshot::{CheckPathEntry, PrPathEntry, build_snapshot};

/// Compute socket path from instance_id.
pub fn socket_path(instance_id: &str) -> PathBuf {
    let safe_id = instance_id.replace(['/', '\\'], "-");
    std::env::temp_dir().join(format!("workmux-sidebar-{}.sock", safe_id))
}

/// Result of a batched tmux query.
struct TmuxState {
    live_panes: HashMap<String, LivePaneInfo>,
    window_statuses: HashMap<String, Option<String>>,
    active_windows: HashSet<(String, String)>,
    pane_window_ids: HashMap<String, String>,
    pane_window_indexes: HashMap<String, u32>,
    active_pane_ids: HashSet<String>,
    window_pane_counts: HashMap<String, usize>,
    server_boot_id: Option<String>,
    position: Option<String>,
    layout: Option<String>,
    filter: Option<String>,
    sleeping_panes: Option<String>,
}

/// Query all sidebar-relevant tmux state in a single server observation.
fn query_tmux_state() -> Result<TmuxState> {
    let snapshot = TmuxBackend::new().sidebar_snapshot()?;
    Ok(TmuxState {
        live_panes: snapshot.live_panes,
        window_statuses: snapshot.window_statuses,
        active_windows: snapshot.active_windows,
        pane_window_ids: snapshot.pane_window_ids,
        pane_window_indexes: snapshot.pane_window_indexes,
        active_pane_ids: snapshot.active_pane_ids,
        window_pane_counts: snapshot.window_pane_counts,
        server_boot_id: snapshot.server_boot_id,
        position: snapshot.position,
        layout: snapshot.layout,
        filter: snapshot.filter,
        sleeping_panes: snapshot.sleeping_panes,
    })
}

struct BroadcastCache {
    snapshot: Option<super::snapshot::SidebarSnapshot>,
    payload: Vec<u8>,
    generation: u64,
}

struct SocketState {
    clients: Vec<UnixStream>,
    cached: BroadcastCache,
}

fn git_status_maps_equal(
    left: &HashMap<PathBuf, GitStatus>,
    right: &HashMap<PathBuf, GitStatus>,
) -> bool {
    left.len() == right.len()
        && left.iter().all(|(path, status)| {
            right
                .get(path)
                .is_some_and(|other| git_status_semantically_equal(status, other))
        })
}

fn snapshots_equal(
    left: &super::snapshot::SidebarSnapshot,
    right: &super::snapshot::SidebarSnapshot,
) -> bool {
    let super::snapshot::SidebarSnapshot {
        position: _,
        layout_mode: _,
        filter_mode: _,
        active_windows: _,
        active_pane_ids: _,
        window_pane_counts: _,
        git_statuses: _,
        pr_statuses: _,
        check_statuses: _,
        interrupted_pane_ids: _,
        sleeping_pane_ids: _,
        agents: _,
        config_version: _,
    } = left;

    left.position == right.position
        && left.layout_mode == right.layout_mode
        && left.filter_mode == right.filter_mode
        && left.active_windows == right.active_windows
        && left.active_pane_ids == right.active_pane_ids
        && left.window_pane_counts == right.window_pane_counts
        && git_status_maps_equal(&left.git_statuses, &right.git_statuses)
        && left.pr_statuses == right.pr_statuses
        && left.check_statuses == right.check_statuses
        && left.interrupted_pane_ids == right.interrupted_pane_ids
        && left.sleeping_pane_ids == right.sleeping_pane_ids
        && left.agents == right.agents
        && left.config_version == right.config_version
}

fn stream_is_connected(stream: &UnixStream) -> bool {
    let mut byte = 0u8;
    // SAFETY: recv receives a valid stream fd and a writable one-byte buffer.
    let result = unsafe {
        libc::recv(
            stream.as_raw_fd(),
            (&mut byte as *mut u8).cast(),
            1,
            libc::MSG_PEEK | libc::MSG_DONTWAIT,
        )
    };
    if result >= 0 {
        if result > 0 {
            tracing::warn!("sidebar client sent unexpected data");
        }
        return false;
    }

    matches!(
        std::io::Error::last_os_error().kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
    )
}

/// Unix socket server for broadcasting snapshots to clients.
struct SocketServer {
    state: Arc<Mutex<SocketState>>,
}

impl SocketServer {
    fn bind(path: &Path) -> std::io::Result<Self> {
        let listener = UnixListener::bind(path)?;
        // Restrict socket to owner only (prevent other local users from reading snapshots)
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        let state = Arc::new(Mutex::new(SocketState {
            clients: Vec::new(),
            cached: BroadcastCache {
                snapshot: None,
                payload: Vec::new(),
                generation: 0,
            },
        }));
        let accept_state = state.clone();

        thread::spawn(move || {
            loop {
                let mut stream = match listener.accept() {
                    Ok((stream, _)) => stream,
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(error) => {
                        tracing::error!(%error, "sidebar socket accept failed");
                        break;
                    }
                };
                let _ = stream.set_write_timeout(Some(Duration::from_millis(100)));
                loop {
                    let (generation, payload) = {
                        let state = accept_state.lock().unwrap();
                        (state.cached.generation, state.cached.payload.clone())
                    };
                    if !payload.is_empty() && stream.write_all(&payload).is_err() {
                        break;
                    }

                    let mut state = accept_state.lock().unwrap();
                    if state.cached.generation != generation {
                        continue;
                    }
                    state.clients.push(stream);
                    tracing::debug!(clients = state.clients.len(), "sidebar client connected");
                    break;
                }
            }
        });

        Ok(Self { state })
    }

    /// Publish a snapshot when its sidebar-visible contents change.
    fn broadcast(&self, snapshot: &super::snapshot::SidebarSnapshot) -> bool {
        let mut state = self.state.lock().unwrap();
        if state
            .cached
            .snapshot
            .as_ref()
            .is_some_and(|cached| snapshots_equal(cached, snapshot))
        {
            return false;
        }

        let data = match serde_json::to_vec(snapshot) {
            Ok(data) => data,
            Err(error) => {
                tracing::error!(%error, "failed to serialize sidebar snapshot");
                return false;
            }
        };
        if data.len() > 1024 * 1024 {
            tracing::error!(
                payload_bytes = data.len(),
                "sidebar snapshot exceeds client limit"
            );
            return false;
        }
        let mut payload = Vec::with_capacity(4 + data.len());
        payload.extend_from_slice(&(data.len() as u32).to_be_bytes());
        payload.extend_from_slice(&data);

        state.cached.snapshot = Some(snapshot.clone());
        state.cached.payload.clone_from(&payload);
        state.cached.generation = state.cached.generation.wrapping_add(1);
        let mut clients = std::mem::take(&mut state.clients);
        drop(state);

        let before = clients.len();
        clients.retain_mut(|stream| stream.write_all(&payload).is_ok());
        let dropped = before - clients.len();
        let mut state = self.state.lock().unwrap();
        let remaining = state.clients.len() + clients.len();
        state.clients.append(&mut clients);
        if dropped > 0 {
            tracing::info!(
                dropped,
                remaining,
                payload_bytes = data.len(),
                "sidebar broadcast: clients disconnected"
            );
        }
        true
    }

    fn client_count(&self) -> usize {
        let mut state = self.state.lock().unwrap();
        state.clients.retain(stream_is_connected);
        state.clients.len()
    }
}

/// Read the sidebar layout mode from tmux global, falling back to settings.json, then config.
fn read_sidebar_layout_mode(
    config: &Config,
    tmux_value: Option<&str>,
) -> Option<SidebarLayoutMode> {
    match tmux_value.map(str::trim) {
        Some("tiles") => return Some(SidebarLayoutMode::Tiles),
        Some("compact") => return Some(SidebarLayoutMode::Compact),
        _ => {}
    }

    // Fall back to persisted setting (user toggled layout in a previous tmux session)
    if let Ok(store) = StateStore::new()
        && let Ok(settings) = store.load_settings()
    {
        match settings.sidebar_layout.as_deref() {
            Some("tiles") => return Some(SidebarLayoutMode::Tiles),
            Some("compact") => return Some(SidebarLayoutMode::Compact),
            _ => {}
        }
    }

    // Fall back to config file
    match config.sidebar.layout.as_deref() {
        Some("tiles") => return Some(SidebarLayoutMode::Tiles),
        Some("compact") => return Some(SidebarLayoutMode::Compact),
        _ => {}
    }

    None
}

/// Read the sidebar filter mode from tmux global, falling back to settings.json.
fn read_sidebar_filter_mode(tmux_value: Option<&str>) -> SidebarFilterMode {
    if let Some(value) = tmux_value {
        return SidebarFilterMode::from_str(value);
    }

    // Fall back to persisted setting
    if let Ok(store) = StateStore::new()
        && let Ok(settings) = store.load_settings()
        && let Some(ref mode) = settings.sidebar_filter
    {
        return SidebarFilterMode::from_str(mode);
    }

    SidebarFilterMode::default()
}

/// Read pane IDs manually marked as sleeping from the tmux global option.
fn read_sleeping_panes(tmux_value: Option<&str>) -> HashSet<String> {
    tmux_value
        .map(|value| value.split_whitespace().map(String::from).collect())
        .unwrap_or_default()
}

fn read_sidebar_position(config: &Config, tmux_value: Option<&str>) -> SidebarPosition {
    match tmux_value.map(str::trim) {
        Some("top") => SidebarPosition::Top,
        Some("left") => SidebarPosition::Left,
        _ => config.sidebar.position.unwrap_or_default(),
    }
}

/// Shared git status cache, updated by a background worker thread.
type GitCache = Arc<Mutex<HashMap<PathBuf, GitStatus>>>;

/// Resolve the .git directory for a worktree path.
/// For linked worktrees, .git is a file containing "gitdir: /path/to/real/gitdir".
fn resolve_git_dir(worktree_path: &Path) -> Option<PathBuf> {
    let dot_git = worktree_path.join(".git");
    if dot_git.is_dir() {
        return Some(dot_git);
    }
    if dot_git.is_file() {
        // Linked worktree: read the gitdir pointer
        let content = std::fs::read_to_string(&dot_git).ok()?;
        let gitdir = content.strip_prefix("gitdir: ")?.trim();
        let path = PathBuf::from(gitdir);
        if path.is_absolute() {
            return Some(path);
        }
        // Relative path: resolve relative to worktree
        Some(worktree_path.join(path))
    } else {
        None
    }
}

/// Resolve the common git directory for linked worktrees.
/// Returns None for normal (non-linked) worktrees.
fn resolve_common_git_dir(gitdir: &Path) -> Option<PathBuf> {
    let content = std::fs::read_to_string(gitdir.join("commondir")).ok()?;
    let rel = content.trim();
    let path = if Path::new(rel).is_absolute() {
        PathBuf::from(rel)
    } else {
        gitdir.join(rel)
    };
    path.canonicalize().ok().or(Some(path))
}

/// Build a gitignore matcher for a worktree root.
/// Loads the root .gitignore (covers the vast majority of ignored paths like
/// target/, node_modules/, .venv/, build/, etc.) without needing to walk
/// nested .gitignore files.
fn build_gitignore(worktree: &Path) -> Gitignore {
    let mut builder = ignore::gitignore::GitignoreBuilder::new(worktree);
    if let Some(err) = builder.add(worktree.join(".gitignore")) {
        tracing::debug!(
            "failed to parse .gitignore for {}: {}",
            worktree.display(),
            err
        );
    }
    builder.build().unwrap_or_else(|_| Gitignore::empty())
}

/// Find tracked files that match ignore rules so their mutation events remain visible.
fn load_ignored_tracked_paths(worktree: &Path) -> HashSet<PathBuf> {
    let Ok(mut command) = crate::git::unattended_git(Some(worktree)) else {
        return HashSet::new();
    };
    let Ok(output) = command
        .args(["ls-files", "-ci", "--exclude-standard", "-z"])
        .output()
    else {
        return HashSet::new();
    };
    if !output.status.success() {
        return HashSet::new();
    }
    output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|path| worktree.join(String::from_utf8_lossy(path).as_ref()))
        .collect()
}

/// Check if a filesystem event path should be skipped based on gitignore rules.
/// Returns true if the path is inside a .git directory (non-working-tree change)
/// or matches the worktree's .gitignore patterns.
fn is_event_ignored(
    event_path: &Path,
    worktree: &Path,
    gitignores: &HashMap<PathBuf, Gitignore>,
    ignored_tracked_paths: &HashMap<PathBuf, HashSet<PathBuf>>,
) -> bool {
    // Linked-worktree git metadata events (e.g. shared gitdir, common refs)
    // live outside the worktree root. They are git events, not working-tree
    // files, so they should never be ignored.
    let Ok(rel) = event_path.strip_prefix(worktree) else {
        return false;
    };

    let rel_str = rel.to_string_lossy();
    // Always process .git metadata changes (HEAD, index, refs) - they affect git status
    if rel_str.starts_with(".git/") || rel_str == ".git" {
        // Skip .git/objects and .git/logs (high volume, don't affect status)
        // but allow .git/index, .git/HEAD, .git/refs, etc.
        return rel_str.starts_with(".git/objects/") || rel_str.starts_with(".git/logs/");
    }

    if ignored_tracked_paths
        .get(worktree)
        .is_some_and(|paths| paths.contains(event_path))
    {
        return false;
    }

    if let Some(gi) = gitignores.get(worktree) {
        // Pass false for is_dir to avoid a synchronous stat syscall per event.
        // Directory-level ignore rules (e.g. "target/") still match because
        // matched_path_or_any_parents checks ancestor components.
        gi.matched_path_or_any_parents(event_path, false)
            .is_ignore()
    } else {
        false
    }
}

/// Compare sidebar-visible Git state while ignoring cache freshness.
fn git_status_semantically_equal(a: &GitStatus, b: &GitStatus) -> bool {
    let GitStatus {
        ahead: _,
        behind: _,
        has_conflict: _,
        is_dirty: _,
        lines_added: _,
        lines_removed: _,
        uncommitted_added: _,
        uncommitted_removed: _,
        cached_at: _,
        base_branch: _,
        branch: _,
        has_upstream: _,
        is_rebasing: _,
    } = a;

    a.ahead == b.ahead
        && a.behind == b.behind
        && a.has_conflict == b.has_conflict
        && a.is_dirty == b.is_dirty
        && a.lines_added == b.lines_added
        && a.lines_removed == b.lines_removed
        && a.uncommitted_added == b.uncommitted_added
        && a.uncommitted_removed == b.uncommitted_removed
        && a.base_branch == b.base_branch
        && a.branch == b.branch
        && a.has_upstream == b.has_upstream
        && a.is_rebasing == b.is_rebasing
}

/// Find which worktrees are affected by a filesystem event at the given path.
fn find_worktrees_for_path(
    event_path: &Path,
    watch_to_worktrees: &HashMap<PathBuf, HashSet<PathBuf>>,
) -> Vec<PathBuf> {
    let mut result = Vec::new();
    for (watched_dir, worktrees) in watch_to_worktrees {
        if event_path.starts_with(watched_dir) {
            result.extend(worktrees.iter().cloned());
        }
    }
    result
}

/// Register a watch path and associate it with a worktree.
/// If the path is already watched by another worktree, just adds the mapping.
/// Only records the mapping after the OS watch succeeds (or was already active).
fn add_watch(
    watcher: &mut notify::RecommendedWatcher,
    path: &Path,
    mode: RecursiveMode,
    worktree: &Path,
    watch_to_worktrees: &mut HashMap<PathBuf, HashSet<PathBuf>>,
) -> bool {
    let already_watching = watch_to_worktrees.get(path).is_some_and(|s| !s.is_empty());

    if !already_watching && let Err(e) = watcher.watch(path, mode) {
        tracing::warn!("failed to watch {}: {}", path.display(), e);
        return false;
    }

    watch_to_worktrees
        .entry(path.to_path_buf())
        .or_default()
        .insert(worktree.to_path_buf());
    true
}

/// Remove watch association for a worktree. Unwatches the path if no other worktree needs it.
fn remove_worktree_watch(
    watcher: &mut notify::RecommendedWatcher,
    watch_path: &Path,
    worktree: &Path,
    watch_to_worktrees: &mut HashMap<PathBuf, HashSet<PathBuf>>,
) {
    if let Some(worktrees) = watch_to_worktrees.get_mut(watch_path) {
        worktrees.remove(worktree);
        if worktrees.is_empty() {
            watch_to_worktrees.remove(watch_path);
            let _ = watcher.unwatch(watch_path);
        }
    }
}

/// Whether the platform can handle recursive worktree watches efficiently.
/// macOS FSEvents aggregates events at the directory level in the kernel and
/// handles heavy I/O well. Linux inotify sets a watch per directory and
/// generates an event per file operation, which overwhelms the system under
/// heavy AI/MCP file activity.
fn platform_supports_worktree_watches() -> bool {
    cfg!(target_os = "macos")
}

#[derive(Debug)]
struct WorktreeWatchSpec {
    path: PathBuf,
    mode: RecursiveMode,
}

/// Describe the filesystem watches needed for a verified worktree root.
fn worktree_watch_specs(worktree: &Path, watch_worktree_files: bool) -> Vec<WorktreeWatchSpec> {
    let mut specs = Vec::new();
    let dot_git = worktree.join(".git");

    if dot_git.is_file() {
        if let Some(git_dir) = resolve_git_dir(worktree) {
            specs.push(WorktreeWatchSpec {
                path: git_dir.clone(),
                mode: RecursiveMode::NonRecursive,
            });

            if let Some(common_dir) = resolve_common_git_dir(&git_dir) {
                let refs_dir = common_dir.join("refs");
                if refs_dir.is_dir() {
                    specs.push(WorktreeWatchSpec {
                        path: refs_dir,
                        mode: RecursiveMode::Recursive,
                    });
                }
                specs.push(WorktreeWatchSpec {
                    path: common_dir,
                    mode: RecursiveMode::NonRecursive,
                });
            }
        }
    } else if dot_git.is_dir() {
        specs.push(WorktreeWatchSpec {
            path: dot_git.clone(),
            mode: RecursiveMode::NonRecursive,
        });
        let refs_dir = dot_git.join("refs");
        if refs_dir.is_dir() {
            specs.push(WorktreeWatchSpec {
                path: refs_dir,
                mode: RecursiveMode::Recursive,
            });
        }
    }

    if watch_worktree_files {
        specs.push(WorktreeWatchSpec {
            path: worktree.to_path_buf(),
            mode: RecursiveMode::Recursive,
        });
    }

    specs
}

/// Set up filesystem watches for a verified worktree root.
///
/// Git metadata watches detect commits, staging, and branch changes. macOS also
/// watches worktree files recursively, while Linux detects them through polling.
fn setup_worktree_watches(
    watcher: &mut notify::RecommendedWatcher,
    worktree: &Path,
    watch_to_worktrees: &mut HashMap<PathBuf, HashSet<PathBuf>>,
) -> (Vec<PathBuf>, bool) {
    let mut watched = Vec::new();
    let specs = worktree_watch_specs(worktree, platform_supports_worktree_watches());
    let mut complete = !specs.is_empty();
    for spec in specs {
        if add_watch(watcher, &spec.path, spec.mode, worktree, watch_to_worktrees) {
            watched.push(spec.path);
        } else {
            complete = false;
        }
    }
    (watched, complete)
}

fn detach_worktree_watches(
    watcher: &mut notify::RecommendedWatcher,
    worktree: &Path,
    worktree_watches: &mut HashMap<PathBuf, Vec<PathBuf>>,
    watch_to_worktrees: &mut HashMap<PathBuf, HashSet<PathBuf>>,
) {
    if let Some(watched_paths) = worktree_watches.remove(worktree) {
        for watched_path in watched_paths {
            remove_worktree_watch(watcher, &watched_path, worktree, watch_to_worktrees);
        }
    }
}

fn replace_worktree_watches(
    watcher: &mut notify::RecommendedWatcher,
    worktree: &Path,
    worktree_watches: &mut HashMap<PathBuf, Vec<PathBuf>>,
    watch_complete: &mut HashMap<PathBuf, bool>,
    watch_to_worktrees: &mut HashMap<PathBuf, HashSet<PathBuf>>,
) {
    detach_worktree_watches(watcher, worktree, worktree_watches, watch_to_worktrees);
    let (watched, complete) = setup_worktree_watches(watcher, worktree, watch_to_worktrees);
    worktree_watches.insert(worktree.to_path_buf(), watched);
    watch_complete.insert(worktree.to_path_buf(), complete);
}

/// Calculate the next timeout for debounced work, capped at one second so
/// termination and maintenance remain responsive.
fn next_worker_timeout(pending: &HashMap<PathBuf, Instant>, debounce: Duration) -> Duration {
    let now = Instant::now();
    let mut min_wait = Duration::from_secs(1);

    for last_event in pending.values() {
        let ready_at = *last_event + debounce;
        if ready_at <= now {
            return Duration::from_millis(1);
        }
        let wait = ready_at - now;
        if wait < min_wait {
            min_wait = wait;
        }
    }

    // Cap at 1s to check term flag periodically
    min_wait.min(Duration::from_secs(1))
}

/// Refresh git status once for a worktree and publish it for every agent path.
/// Returns true if any published status changed, ignoring cached_at.
fn refresh_git_status(worktree: &Path, agent_paths: &[PathBuf], cache: &GitCache) -> bool {
    let new_status = crate::git::get_git_status(worktree, None);
    let Ok(mut cache) = cache.lock() else {
        return true;
    };
    let mut changed = false;
    for path in agent_paths {
        if cache
            .get(path)
            .is_some_and(|old| git_status_semantically_equal(old, &new_status))
        {
            continue;
        }
        cache.insert(path.clone(), new_status.clone());
        changed = true;
    }
    changed
}

fn reconcile_git_cache(
    previous: &HashMap<PathBuf, ResolvedGitWorktree>,
    current: &HashMap<PathBuf, ResolvedGitWorktree>,
    cache: &mut HashMap<PathBuf, GitStatus>,
) -> (bool, Vec<PathBuf>) {
    let active_agent_paths: HashSet<PathBuf> = current
        .values()
        .flat_map(|entry| entry.agent_paths.iter().cloned())
        .collect();
    let root_statuses: HashMap<PathBuf, GitStatus> = previous
        .iter()
        .filter_map(|(root, old)| {
            old.agent_paths
                .iter()
                .find_map(|path| cache.get(path).cloned())
                .map(|status| (root.clone(), status))
        })
        .collect();

    let before = cache.len();
    cache.retain(|path, _| active_agent_paths.contains(path));
    let mut changed = cache.len() != before;
    let mut missing = Vec::new();
    for (root, worktree) in current {
        let existing = root_statuses.get(root).cloned().or_else(|| {
            worktree
                .agent_paths
                .iter()
                .find_map(|path| cache.get(path).cloned())
        });
        if let Some(status) = existing {
            for path in &worktree.agent_paths {
                if !cache.contains_key(path) {
                    cache.insert(path.clone(), status.clone());
                    changed = true;
                }
            }
        } else {
            missing.push(root.clone());
        }
    }
    (changed, missing)
}

/// Info about an active agent path sent to the git worker.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct GitWorkerPath {
    path: PathBuf,
    is_stale: bool,
    is_focused: bool,
}

#[derive(Debug, Eq, PartialEq)]
struct ResolvedGitWorktree {
    agent_paths: Vec<PathBuf>,
    is_stale: bool,
    is_focused: bool,
}

/// Resolve agent directories to verified worktree roots and group shared roots.
/// Paths outside a non-bare Git worktree are absent from the result.
fn resolve_git_worktrees_cached(
    entries: &[GitWorkerPath],
    roots_by_agent: &mut HashMap<PathBuf, Option<PathBuf>>,
) -> HashMap<PathBuf, ResolvedGitWorktree> {
    let active_paths: HashSet<&PathBuf> = entries.iter().map(|entry| &entry.path).collect();
    roots_by_agent.retain(|path, _| active_paths.contains(path));

    let mut worktrees: HashMap<PathBuf, ResolvedGitWorktree> = HashMap::new();
    for entry in entries {
        let root = roots_by_agent.entry(entry.path.clone()).or_insert_with(|| {
            crate::git::get_repo_root_for(&entry.path)
                .ok()
                .map(|root| crate::util::canon_or_self(&root))
        });
        let Some(root) = root else {
            continue;
        };
        let worktree = worktrees
            .entry(root.clone())
            .or_insert_with(|| ResolvedGitWorktree {
                agent_paths: Vec::new(),
                is_stale: true,
                is_focused: false,
            });
        if !worktree.agent_paths.contains(&entry.path) {
            worktree.agent_paths.push(entry.path.clone());
        }
        worktree.is_stale &= entry.is_stale;
        worktree.is_focused |= entry.is_focused;
    }
    for worktree in worktrees.values_mut() {
        worktree.agent_paths.sort();
    }
    worktrees
}

/// Info about an active agent path sent to the GitHub worker.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct GithubWorkerPath {
    path: PathBuf,
    branch: String,
}

type PrPathCache = Arc<Mutex<HashMap<PathBuf, PrPathEntry>>>;
type PrRepoCache = Arc<Mutex<HashMap<PathBuf, HashMap<String, PrSummary>>>>;
type CheckPathCache = Arc<Mutex<HashMap<PathBuf, CheckPathEntry>>>;
type CheckRepoCache = Arc<Mutex<HashMap<PathBuf, HashMap<String, CheckSummary>>>>;

const GITHUB_FETCH_INTERVAL: Duration = Duration::from_secs(30);

fn github_fetch_due(branch_set_changed: bool, elapsed: Duration) -> bool {
    branch_set_changed || elapsed >= GITHUB_FETCH_INTERVAL
}

fn github_repo_key(path: &Path) -> Option<PathBuf> {
    crate::git::get_git_common_dir_in(Some(path))
        .ok()
        .and_then(|git_dir| git_dir.canonicalize().ok().or(Some(git_dir)))
}

fn group_github_branches(
    entries: &[GithubWorkerPath],
    repo_keys: &HashMap<PathBuf, PathBuf>,
) -> HashMap<PathBuf, (PathBuf, Vec<String>)> {
    let mut grouped = HashMap::new();
    for entry in entries {
        if let Some(repo_key) = repo_keys.get(&entry.path) {
            let (_, branches) = grouped
                .entry(repo_key.clone())
                .or_insert_with(|| (entry.path.clone(), Vec::new()));
            branches.push(entry.branch.clone());
        }
    }
    for (_, branches) in grouped.values_mut() {
        branches.sort();
        branches.dedup();
    }
    grouped
}

fn clear_pr_path_cache(path_cache: &PrPathCache) -> bool {
    if let Ok(mut cache) = path_cache.lock() {
        let changed = !cache.is_empty();
        cache.clear();
        changed
    } else {
        false
    }
}

fn clear_check_path_cache(path_cache: &CheckPathCache) -> bool {
    if let Ok(mut cache) = path_cache.lock() {
        let changed = !cache.is_empty();
        cache.clear();
        changed
    } else {
        false
    }
}

fn publish_pr_path_cache(
    entries: &[GithubWorkerPath],
    repo_keys: &HashMap<PathBuf, PathBuf>,
    repo_cache: &HashMap<PathBuf, HashMap<String, PrSummary>>,
    path_cache: &PrPathCache,
    dirty_flag: &Arc<AtomicBool>,
    wake_tx: &std::sync::mpsc::SyncSender<()>,
) {
    let mut next = HashMap::new();
    for entry in entries {
        if let Some(repo_root) = repo_keys.get(&entry.path)
            && let Some(pr) = repo_cache
                .get(repo_root)
                .and_then(|prs| prs.get(&entry.branch))
        {
            next.insert(
                entry.path.clone(),
                PrPathEntry {
                    branch: entry.branch.clone(),
                    summary: pr.clone(),
                },
            );
        }
    }
    let changed = if let Ok(mut cache) = path_cache.lock() {
        if *cache == next {
            false
        } else {
            *cache = next;
            true
        }
    } else {
        false
    };
    if changed {
        dirty_flag.store(true, Ordering::Relaxed);
        let _ = wake_tx.try_send(());
    }
}

fn publish_check_path_cache(
    entries: &[GithubWorkerPath],
    repo_keys: &HashMap<PathBuf, PathBuf>,
    repo_cache: &HashMap<PathBuf, HashMap<String, CheckSummary>>,
    path_cache: &CheckPathCache,
    dirty_flag: &Arc<AtomicBool>,
    wake_tx: &std::sync::mpsc::SyncSender<()>,
) {
    let mut next = HashMap::new();
    for entry in entries {
        if let Some(repo_root) = repo_keys.get(&entry.path)
            && let Some(checks) = repo_cache
                .get(repo_root)
                .and_then(|checks| checks.get(&entry.branch))
        {
            next.insert(
                entry.path.clone(),
                CheckPathEntry {
                    branch: entry.branch.clone(),
                    summary: checks.clone(),
                },
            );
        }
    }
    let changed = if let Ok(mut cache) = path_cache.lock() {
        if *cache == next {
            false
        } else {
            *cache = next;
            true
        }
    } else {
        false
    };
    if changed {
        dirty_flag.store(true, Ordering::Relaxed);
        let _ = wake_tx.try_send(());
    }
}

fn spawn_github_worker(
    term: Arc<AtomicBool>,
    dirty_flag: Arc<AtomicBool>,
    wake_tx: std::sync::mpsc::SyncSender<()>,
) -> (
    PrPathCache,
    CheckPathCache,
    std::sync::mpsc::Sender<Vec<GithubWorkerPath>>,
) {
    let path_cache: PrPathCache = Arc::new(Mutex::new(HashMap::new()));
    let path_cache_clone = path_cache.clone();
    let check_path_cache: CheckPathCache = Arc::new(Mutex::new(HashMap::new()));
    let check_path_cache_clone = check_path_cache.clone();
    let repo_cache: PrRepoCache = Arc::new(Mutex::new(crate::github::load_pr_cache()));
    let check_repo_cache: CheckRepoCache = Arc::new(Mutex::new(crate::github::load_check_cache()));
    let (tx, rx) = std::sync::mpsc::channel::<Vec<GithubWorkerPath>>();

    thread::spawn(move || {
        let mut active_entries: Vec<GithubWorkerPath> = Vec::new();
        let mut repo_keys: HashMap<PathBuf, PathBuf> = HashMap::new();
        let mut last_key: Vec<(PathBuf, String)> = Vec::new();
        let mut last_fetch = Instant::now() - GITHUB_FETCH_INTERVAL;

        while !term.load(Ordering::Relaxed) {
            let mut paths_changed = false;
            match rx.recv_timeout(Duration::from_secs(1)) {
                Ok(entries) => {
                    active_entries = entries;
                    paths_changed = true;
                    while let Ok(entries) = rx.try_recv() {
                        active_entries = entries;
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }

            if active_entries.is_empty() {
                if paths_changed
                    && (clear_pr_path_cache(&path_cache_clone)
                        | clear_check_path_cache(&check_path_cache_clone))
                {
                    dirty_flag.store(true, Ordering::Relaxed);
                    let _ = wake_tx.try_send(());
                }
                continue;
            }

            active_entries.sort();
            active_entries.dedup();
            let key: Vec<(PathBuf, String)> = active_entries
                .iter()
                .map(|entry| (entry.path.clone(), entry.branch.clone()))
                .collect();
            let branch_set_changed = key != last_key;

            if paths_changed {
                let active_paths: HashSet<&PathBuf> =
                    active_entries.iter().map(|entry| &entry.path).collect();
                repo_keys.retain(|path, _| active_paths.contains(path));
                for entry in &active_entries {
                    if !repo_keys.contains_key(&entry.path)
                        && let Some(repo_key) = github_repo_key(&entry.path)
                    {
                        repo_keys.insert(entry.path.clone(), repo_key);
                    }
                }
                let snapshot = repo_cache
                    .lock()
                    .ok()
                    .map(|c| c.clone())
                    .unwrap_or_default();
                publish_pr_path_cache(
                    &active_entries,
                    &repo_keys,
                    &snapshot,
                    &path_cache_clone,
                    &dirty_flag,
                    &wake_tx,
                );
                let check_snapshot = check_repo_cache
                    .lock()
                    .ok()
                    .map(|cache| cache.clone())
                    .unwrap_or_default();
                publish_check_path_cache(
                    &active_entries,
                    &repo_keys,
                    &check_snapshot,
                    &check_path_cache_clone,
                    &dirty_flag,
                    &wake_tx,
                );
            }

            if !github_fetch_due(branch_set_changed, last_fetch.elapsed()) {
                continue;
            }

            let repo_branches = group_github_branches(&active_entries, &repo_keys);
            if repo_branches.is_empty() {
                last_key = key;
                continue;
            }

            let mut fetched_prs = HashMap::new();
            let mut fetched_checks = HashMap::new();
            for (repo_key, (query_path, branches)) in repo_branches {
                match crate::github::list_branch_summaries(&query_path, &branches) {
                    Ok(summaries) => {
                        let mut prs = HashMap::new();
                        let mut checks = HashMap::new();
                        for (branch, summary) in summaries {
                            if let Some(pr) = summary.pr {
                                prs.insert(branch.clone(), pr);
                            }
                            if let Some(check_summary) = summary.checks {
                                checks.insert(branch, check_summary);
                            }
                        }
                        fetched_prs.insert(repo_key.clone(), prs);
                        fetched_checks.insert(repo_key, checks);
                    }
                    Err(e) => {
                        tracing::warn!("failed to fetch GitHub state for {:?}: {}", query_path, e);
                    }
                }
            }
            if !fetched_prs.is_empty()
                && let Ok(mut cache) = repo_cache.lock()
            {
                for (repo_root, prs) in &fetched_prs {
                    if prs.is_empty() {
                        cache.remove(repo_root);
                    } else {
                        cache.insert(repo_root.clone(), prs.clone());
                    }
                }
                crate::github::save_pr_cache(&fetched_prs);
                publish_pr_path_cache(
                    &active_entries,
                    &repo_keys,
                    &cache,
                    &path_cache_clone,
                    &dirty_flag,
                    &wake_tx,
                );
            }
            if !fetched_checks.is_empty()
                && let Ok(mut cache) = check_repo_cache.lock()
            {
                for (repo_root, checks) in &fetched_checks {
                    if checks.is_empty() {
                        cache.remove(repo_root);
                    } else {
                        cache.insert(repo_root.clone(), checks.clone());
                    }
                }
                crate::github::save_check_cache(&fetched_checks);
                publish_check_path_cache(
                    &active_entries,
                    &repo_keys,
                    &cache,
                    &check_path_cache_clone,
                    &dirty_flag,
                    &wake_tx,
                );
            }
            last_key = key;
            last_fetch = Instant::now();
        }
    });

    (path_cache, check_path_cache, tx)
}

/// Configure filesystem watchers for mutation events without read noise.
fn mutation_watcher_config() -> notify::Config {
    notify::Config::default().with_event_kinds(EventKindMask::CORE)
}

fn git_event_requires_recovery(event: &notify::Event) -> bool {
    event.need_rescan()
}

fn recovery_ready(due: bool, last_recovery: Instant, cooldown: Duration) -> bool {
    due && last_recovery.elapsed() >= cooldown
}

fn next_audit_path(
    paths: &[PathBuf],
    watch_complete: &HashMap<PathBuf, bool>,
    cursor: &mut usize,
) -> Option<PathBuf> {
    for _ in 0..paths.len() {
        *cursor %= paths.len();
        let path = paths[*cursor].clone();
        *cursor = (*cursor + 1) % paths.len();
        if watch_complete.get(&path).copied().unwrap_or(false) {
            return Some(path);
        }
    }
    None
}

/// Spawn a background thread that watches for git changes and updates the cache.
///
/// Uses the `notify` crate for OS-level filesystem event detection (FSEvents on macOS).
/// Watches Git metadata and, where efficient, worktree roots. Events are debounced
/// per worktree before status refresh. Polling covers platforms or roots without
/// complete worktree watches, and a rolling audit detects silent event loss.
fn spawn_git_worker(
    term: Arc<AtomicBool>,
    dirty_flag: Arc<AtomicBool>,
    wake_tx: std::sync::mpsc::SyncSender<()>,
) -> (GitCache, std::sync::mpsc::Sender<Vec<GitWorkerPath>>) {
    let cache: GitCache = Arc::new(Mutex::new(HashMap::new()));
    let cache_clone = cache.clone();
    let (tx, rx) = std::sync::mpsc::channel::<Vec<GitWorkerPath>>();

    thread::spawn(move || {
        // Bounded filesystem event channel to prevent unbounded memory growth
        // under heavy file I/O (e.g. MCP servers, Claude sessions).
        // On overflow, all worktrees are marked pending for an early refresh.
        let (fs_tx, fs_rx) = std::sync::mpsc::sync_channel(256);
        let fs_overflow = Arc::new(AtomicBool::new(false));
        let fs_overflow_clone = fs_overflow.clone();
        let mut watcher: Option<notify::RecommendedWatcher> = match notify::RecommendedWatcher::new(
            move |event: notify::Result<notify::Event>| {
                if let Ok(ref e) = event {
                    // Filter out .git internal traffic that doesn't affect status.
                    // Gitignore-based filtering (node_modules, target, etc.) happens
                    // in the worker thread where matchers are available.
                    let dominated_by_noise = e.paths.iter().all(|p| {
                        let s = p.to_string_lossy();
                        s.contains("/.git/objects/") || s.contains("/.git/logs/")
                    });
                    if dominated_by_noise {
                        return;
                    }
                }
                if let Err(std::sync::mpsc::TrySendError::Full(_)) = fs_tx.try_send(event) {
                    fs_overflow_clone.store(true, Ordering::Relaxed);
                }
            },
            mutation_watcher_config(),
        ) {
            Ok(w) => Some(w),
            Err(e) => {
                tracing::warn!(
                    "filesystem watcher unavailable, falling back to polling: {}",
                    e
                );
                None
            }
        };

        let mut active_entries: Vec<GitWorkerPath> = Vec::new();
        let mut roots_by_agent: HashMap<PathBuf, Option<PathBuf>> = HashMap::new();
        let mut resolved_worktrees: HashMap<PathBuf, ResolvedGitWorktree> = HashMap::new();
        let mut watch_to_worktrees: HashMap<PathBuf, HashSet<PathBuf>> = HashMap::new();
        let mut worktree_watches: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        let mut watch_complete: HashMap<PathBuf, bool> = HashMap::new();
        let mut gitignores: HashMap<PathBuf, Gitignore> = HashMap::new();
        let mut ignored_tracked_paths: HashMap<PathBuf, HashSet<PathBuf>> = HashMap::new();
        let mut pending_worktrees: HashMap<PathBuf, Instant> = HashMap::new();
        let mut last_refreshed: HashMap<PathBuf, Instant> = HashMap::new();
        let mut unique_active: Vec<PathBuf> = Vec::new();
        let mut audit_cursor = 0usize;
        let mut last_maintenance = Instant::now();
        let mut last_audit = Instant::now();
        let mut last_recovery = Instant::now() - Duration::from_secs(30);
        let mut last_negative_retry = Instant::now();
        let mut recovery_due = false;
        let mut watcher_degraded = watcher.is_none();
        let debounce_duration = Duration::from_millis(300);
        let min_refresh_interval = Duration::from_secs(2);
        let recovery_cooldown = Duration::from_secs(30);
        let audit_interval = Duration::from_secs(5);

        while !term.load(Ordering::Relaxed) {
            if watcher.is_some() {
                let timeout = next_worker_timeout(&pending_worktrees, debounce_duration);
                let mut process_event = |event: notify::Event| -> bool {
                    if git_event_requires_recovery(&event) {
                        return true;
                    }
                    for path in &event.paths {
                        let worktrees = find_worktrees_for_path(path, &watch_to_worktrees);
                        if path.file_name().is_some_and(|name| name == ".gitignore") {
                            for worktree in &worktrees {
                                gitignores.insert(worktree.clone(), build_gitignore(worktree));
                                ignored_tracked_paths
                                    .insert(worktree.clone(), load_ignored_tracked_paths(worktree));
                            }
                        }
                        for worktree in worktrees {
                            if is_event_ignored(
                                path,
                                &worktree,
                                &gitignores,
                                &ignored_tracked_paths,
                            ) {
                                continue;
                            }
                            pending_worktrees
                                .entry(worktree)
                                .or_insert_with(Instant::now);
                        }
                    }
                    false
                };

                match fs_rx.recv_timeout(timeout) {
                    Ok(Ok(event)) => recovery_due |= process_event(event),
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "filesystem watch error; using polling fallback");
                        watcher_degraded = true;
                        recovery_due = true;
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }
                while let Ok(event_result) = fs_rx.try_recv() {
                    match event_result {
                        Ok(event) => recovery_due |= process_event(event),
                        Err(error) => {
                            tracing::warn!(%error, "filesystem watch error; using polling fallback");
                            watcher_degraded = true;
                            recovery_due = true;
                        }
                    }
                }
                if fs_overflow.swap(false, Ordering::Relaxed) {
                    recovery_due = true;
                }
            } else {
                thread::sleep(Duration::from_secs(1));
            }

            if last_negative_retry.elapsed() >= Duration::from_secs(30) {
                last_negative_retry = Instant::now();
                if roots_by_agent.values().any(Option::is_none) {
                    roots_by_agent.retain(|_, root| root.is_some());
                    active_entries.clear();
                }
            }

            let mut latest_entries = None;
            while let Ok(entries) = rx.try_recv() {
                latest_entries = Some(entries);
            }
            if let Some(mut entries) = latest_entries {
                entries.sort();
                if entries != active_entries {
                    let previous = std::mem::take(&mut resolved_worktrees);
                    active_entries = entries;
                    resolved_worktrees =
                        resolve_git_worktrees_cached(&active_entries, &mut roots_by_agent);
                    unique_active = resolved_worktrees.keys().cloned().collect();
                    unique_active.sort();
                    let unique_set: HashSet<PathBuf> = unique_active.iter().cloned().collect();
                    if let Some(ref mut active_watcher) = watcher {
                        let removed: Vec<PathBuf> = worktree_watches
                            .keys()
                            .filter(|path| !unique_set.contains(*path))
                            .cloned()
                            .collect();
                        for path in removed {
                            detach_worktree_watches(
                                active_watcher,
                                &path,
                                &mut worktree_watches,
                                &mut watch_to_worktrees,
                            );
                            watch_complete.remove(&path);
                            gitignores.remove(&path);
                            ignored_tracked_paths.remove(&path);
                            pending_worktrees.remove(&path);
                            last_refreshed.remove(&path);
                        }

                        for path in &unique_active {
                            if !worktree_watches.contains_key(path) {
                                replace_worktree_watches(
                                    active_watcher,
                                    path,
                                    &mut worktree_watches,
                                    &mut watch_complete,
                                    &mut watch_to_worktrees,
                                );
                                gitignores.insert(path.clone(), build_gitignore(path));
                                ignored_tracked_paths
                                    .insert(path.clone(), load_ignored_tracked_paths(path));
                            }
                        }
                    }

                    let (projected_change, missing_roots) = cache_clone
                        .lock()
                        .ok()
                        .map(|mut cache| {
                            reconcile_git_cache(&previous, &resolved_worktrees, &mut cache)
                        })
                        .unwrap_or_default();
                    for root in missing_roots {
                        pending_worktrees.insert(root, Instant::now() - debounce_duration);
                    }
                    if projected_change {
                        dirty_flag.store(true, Ordering::Relaxed);
                        let _ = wake_tx.try_send(());
                    }

                    for (root, worktree) in &resolved_worktrees {
                        if let Some(old) = previous.get(root) {
                            let became_recent = old.is_stale && !worktree.is_stale;
                            let became_focused = !old.is_focused && worktree.is_focused;
                            if became_recent || became_focused {
                                pending_worktrees
                                    .insert(root.clone(), Instant::now() - debounce_duration);
                            }
                        }
                    }
                }
            }

            let now = Instant::now();
            if recovery_ready(recovery_due, last_recovery, recovery_cooldown) {
                recovery_due = false;
                last_recovery = now;
                for path in &unique_active {
                    pending_worktrees
                        .entry(path.clone())
                        .or_insert(now - debounce_duration);
                }
            }

            let poll_interval = if watcher_degraded || watcher.is_none() {
                Duration::from_secs(2)
            } else {
                Duration::from_secs(5)
            };
            if last_maintenance.elapsed() >= poll_interval {
                last_maintenance = now;
                if watcher.is_none() || !platform_supports_worktree_watches() || watcher_degraded {
                    for path in &unique_active {
                        pending_worktrees
                            .entry(path.clone())
                            .or_insert(now - debounce_duration);
                    }
                } else {
                    let incomplete: Vec<PathBuf> = unique_active
                        .iter()
                        .filter(|path| !watch_complete.get(*path).copied().unwrap_or(false))
                        .cloned()
                        .collect();
                    for path in &incomplete {
                        pending_worktrees
                            .entry(path.clone())
                            .or_insert(now - debounce_duration);
                    }
                    if let Some(ref mut active_watcher) = watcher {
                        for path in incomplete {
                            replace_worktree_watches(
                                active_watcher,
                                &path,
                                &mut worktree_watches,
                                &mut watch_complete,
                                &mut watch_to_worktrees,
                            );
                        }
                    }
                }
            }

            if watcher.is_some()
                && platform_supports_worktree_watches()
                && !watcher_degraded
                && last_audit.elapsed() >= audit_interval
                && !unique_active.is_empty()
            {
                last_audit = now;
                if let Some(path) =
                    next_audit_path(&unique_active, &watch_complete, &mut audit_cursor)
                {
                    pending_worktrees
                        .entry(path)
                        .or_insert(now - debounce_duration);
                }
            }

            let ready: Vec<PathBuf> = pending_worktrees
                .iter()
                .filter(|(_, event_at)| {
                    now.saturating_duration_since(**event_at) >= debounce_duration
                })
                .map(|(path, _)| path.clone())
                .collect();
            let mut any_changed = false;
            for path in ready {
                if let Some(last) = last_refreshed.get(&path)
                    && last.elapsed() < min_refresh_interval
                {
                    let ready_at = *last + min_refresh_interval;
                    let event_at = ready_at.checked_sub(debounce_duration).unwrap_or(ready_at);
                    pending_worktrees.insert(path, event_at);
                    continue;
                }
                pending_worktrees.remove(&path);
                if let Some(worktree) = resolved_worktrees.get(&path) {
                    if refresh_git_status(&path, &worktree.agent_paths, &cache_clone) {
                        any_changed = true;
                    }
                    last_refreshed.insert(path, Instant::now());
                }
            }

            if any_changed {
                dirty_flag.store(true, Ordering::Relaxed);
                let _ = wake_tx.try_send(());
            }
        }
    });

    (cache, tx)
}

const CONFIG_BASENAMES: [&str; 4] = ["config.yaml", "config.yml", ".workmux.yaml", ".workmux.yml"];

/// Whether a filesystem event on a watched config dir should schedule a
/// config reload.
///
/// Access events must be ignored because reading a config file produces them
/// on Linux. Reacting to those reads makes each reload schedule another one.
/// Rescan events reload defensively because their paths may be incomplete.
fn config_event_triggers_reload(event: &notify::Event) -> bool {
    if event.need_rescan() {
        return true;
    }

    !matches!(event.kind, notify::EventKind::Access(_))
        && event.paths.iter().any(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| CONFIG_BASENAMES.contains(&n))
        })
}

/// Spawn a thread that watches the global config file and per-project
/// `.workmux.yaml` files and bumps `config_version` whenever a reload succeeds.
///
/// Returns a channel for the daemon main loop to send the current set of
/// project config directories (parents of `.workmux.yaml`) to watch.
fn spawn_config_watcher(
    term: Arc<AtomicBool>,
    config: Arc<Mutex<Config>>,
    config_version: Arc<AtomicU64>,
    dirty_flag: Arc<AtomicBool>,
    wake_tx: mpsc::SyncSender<()>,
) -> mpsc::Sender<HashSet<PathBuf>> {
    let (paths_tx, paths_rx) = mpsc::channel::<HashSet<PathBuf>>();
    thread::spawn(move || {
        // Bounded fs event channel; on overflow force a reload.
        let (fs_tx, fs_rx) = mpsc::sync_channel::<notify::Result<notify::Event>>(64);
        let overflow = Arc::new(AtomicBool::new(false));
        let overflow_clone = overflow.clone();
        let mut watcher: notify::RecommendedWatcher = match notify::RecommendedWatcher::new(
            move |event: notify::Result<notify::Event>| {
                if event
                    .as_ref()
                    .is_ok_and(|event| !config_event_triggers_reload(event))
                {
                    return;
                }
                if let Err(mpsc::TrySendError::Full(_)) = fs_tx.try_send(event) {
                    overflow_clone.store(true, Ordering::Relaxed);
                }
            },
            mutation_watcher_config(),
        ) {
            Ok(w) => w,
            Err(e) => {
                tracing::warn!("config watcher unavailable: {}", e);
                return;
            }
        };

        // Track watched directories so we can reconcile add/remove and avoid
        // re-watching the same path twice.
        let mut watched_global: Option<PathBuf> = None;
        let mut watched_project_dirs: HashSet<PathBuf> = HashSet::new();
        let mut pending_reload_at: Option<Instant> = None;
        let debounce = Duration::from_millis(200);

        // Watch the global config dir non-recursively. Watching the parent dir
        // (rather than the file) catches atomic-rename saves: write to a
        // sibling temp file, then rename(temp, target). This is what vim,
        // claude-code's Edit/Write tools, and most editors do. A direct file
        // watch would lose the inode on rename and miss subsequent edits. It
        // also fires on first-time creation when no config exists yet.
        if let Some(p) = crate::config::global_config_path()
            && let Some(dir) = p.parent()
        {
            match watcher.watch(dir, RecursiveMode::NonRecursive) {
                Ok(()) => {
                    tracing::info!(
                        op = "watch",
                        path = %dir.display(),
                        kind = "global",
                        "fd-leak debug (config)"
                    );
                    watched_global = Some(dir.to_path_buf());
                }
                Err(e) => {
                    tracing::warn!("failed to watch global config dir {}: {}", dir.display(), e);
                }
            }
        }

        while !term.load(Ordering::Relaxed) {
            // 1. Reconcile per-project watches from incoming path sets.
            while let Ok(new_dirs) = paths_rx.try_recv() {
                let to_remove: Vec<PathBuf> = watched_project_dirs
                    .difference(&new_dirs)
                    .cloned()
                    .collect();
                for dir in &to_remove {
                    // Never unwatch the global config dir, even if it was
                    // tracked under watched_project_dirs (we never issued an
                    // OS-level watch for it from the project path; it's still
                    // watched as the global watch).
                    if Some(dir) == watched_global.as_ref() {
                        watched_project_dirs.remove(dir);
                        continue;
                    }
                    let res = watcher.unwatch(dir);
                    tracing::info!(
                        op = "unwatch",
                        path = %dir.display(),
                        ok = res.is_ok(),
                        kind = "project",
                        total = watched_project_dirs.len() - 1,
                        "fd-leak debug (config)"
                    );
                    watched_project_dirs.remove(dir);
                }
                let to_add: Vec<PathBuf> = new_dirs
                    .difference(&watched_project_dirs)
                    .cloned()
                    .collect();
                for dir in to_add {
                    // Skip if it's the same as the global watched dir to avoid
                    // double-watching the same path.
                    if Some(&dir) == watched_global.as_ref() {
                        watched_project_dirs.insert(dir);
                        continue;
                    }
                    match watcher.watch(&dir, RecursiveMode::NonRecursive) {
                        Ok(()) => {
                            tracing::info!(
                                op = "watch",
                                path = %dir.display(),
                                kind = "project",
                                total = watched_project_dirs.len() + 1,
                                "fd-leak debug (config)"
                            );
                            watched_project_dirs.insert(dir);
                        }
                        Err(e) => {
                            tracing::warn!(
                                "failed to watch project config dir {}: {}",
                                dir.display(),
                                e
                            );
                        }
                    }
                }
            }

            // 2. Wait for the next event, capped by the pending debounce deadline.
            let timeout = pending_reload_at
                .map(|t| t.saturating_duration_since(Instant::now()))
                .unwrap_or_else(|| Duration::from_millis(500));

            match fs_rx.recv_timeout(timeout) {
                Ok(Ok(event)) => {
                    if config_event_triggers_reload(&event) {
                        // Lock the deadline on the FIRST event in a burst; do
                        // not slide it forward on every subsequent event.
                        pending_reload_at.get_or_insert(Instant::now() + debounce);
                    }
                }
                Ok(Err(e)) => tracing::warn!("config watch error: {}", e),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }

            if overflow.swap(false, Ordering::Relaxed) {
                pending_reload_at.get_or_insert(Instant::now() + debounce);
            }

            // 3. Reload if the debounce deadline has passed.
            if let Some(t) = pending_reload_at
                && Instant::now() >= t
            {
                pending_reload_at = None;
                // Always bump the version so clients try their own per-project
                // load (their anchor path may differ from the daemon CWD; a
                // failure here doesn't necessarily mean clients will fail).
                // Only update the daemon-side cached Config on success.
                match Config::load(None) {
                    Ok(new_cfg) => {
                        if let Ok(mut slot) = config.lock() {
                            *slot = new_cfg;
                        }
                        tracing::debug!("daemon config reloaded");
                    }
                    Err(e) => {
                        tracing::warn!("daemon-side config load failed, keeping previous: {}", e);
                    }
                }
                let v = config_version.fetch_add(1, Ordering::Relaxed) + 1;
                tracing::info!(version = v, "sidebar config_version bumped");
                dirty_flag.store(true, Ordering::Relaxed);
                let _ = wake_tx.try_send(());
            }
        }
    });

    paths_tx
}

/// Detects working agents that have stopped producing output.
///
/// # Behavior
/// - A working agent with no pane output and no RPC activity for >= timeout
///   is considered interrupted.
/// - Interrupted state is sticky: only an RPC update from the agent clears
///   it. User typing or cursor movement in the pane does not.
/// - After clearing, the agent gets a fresh timeout window before it can
///   be marked interrupted again.
/// - Interrupted agents show no icon and no timer in the sidebar.
/// - When an agent resumes, the timer resets to zero.
struct InactivityTracker {
    /// pane_id -> (content_hash, first_seen_at, updated_ts at recording time)
    entries: HashMap<String, (u64, Instant, u64)>,
    /// pane_id -> updated_ts at the time interruption was confirmed.
    /// Cleared when updated_ts changes (agent sent a new RPC status update).
    confirmed: HashMap<String, u64>,
    /// How long content must be unchanged before marking as interrupted.
    timeout: Duration,
}

impl InactivityTracker {
    fn new(timeout: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            confirmed: HashMap::new(),
            timeout,
        }
    }

    /// Whether this pane is confirmed interrupted and capture can be skipped.
    fn is_confirmed(&self, pane_id: &str, updated_ts: u64) -> bool {
        self.confirmed
            .get(pane_id)
            .is_some_and(|&ts| updated_ts <= ts)
    }

    /// Check all working agents for inactivity. Returns the set of pane IDs
    /// that appear interrupted (content unchanged for longer than timeout).
    fn check_with(
        &mut self,
        agents: &[crate::multiplexer::AgentPane],
        now: Instant,
        capture: impl Fn(&str) -> Option<String>,
    ) -> HashSet<String> {
        use std::hash::{Hash, Hasher};

        // Build lookup of working agents
        let working: HashMap<&str, &crate::multiplexer::AgentPane> = agents
            .iter()
            .filter(|a| a.status == Some(crate::multiplexer::AgentStatus::Working))
            .map(|a| (a.pane_id.as_str(), a))
            .collect();

        // Remove entries for agents no longer in Working status
        self.entries
            .retain(|id, _| working.contains_key(id.as_str()));
        self.confirmed
            .retain(|id, _| working.contains_key(id.as_str()));

        // Clear interrupted state if the agent's state was updated via RPC
        // (updated_ts changed since we confirmed the interruption).
        // Collect resumed pane IDs first, then clear their entries for a fresh
        // inactivity window.
        let resumed: Vec<String> = self
            .confirmed
            .iter()
            .filter(|(id, confirmed_ts)| {
                working
                    .get(id.as_str())
                    .is_some_and(|a| a.updated_ts.unwrap_or(0) > **confirmed_ts)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in &resumed {
            if let Some(confirmed_ts) = self.confirmed.remove(id) {
                let updated_ts = working
                    .get(id.as_str())
                    .and_then(|a| a.updated_ts)
                    .unwrap_or(0);
                tracing::info!(
                    pane_id = %id,
                    confirmed_ts,
                    updated_ts,
                    "agent inactivity cleared"
                );
            }
            self.entries.remove(id);
        }

        for (pane_id, agent) in &working {
            // Already confirmed interrupted - skip capture
            if self.confirmed.contains_key(*pane_id) {
                continue;
            }

            let Some(raw) = capture(pane_id) else {
                continue;
            };

            // Strip ANSI escapes and normalize whitespace for stable hashing
            let stripped = console::strip_ansi_codes(&raw);
            let normalized = stripped.trim();

            let mut hasher = std::hash::DefaultHasher::new();
            normalized.hash(&mut hasher);
            let hash = hasher.finish();

            let current_rpc = agent.updated_ts.unwrap_or(0);

            match self.entries.get(*pane_id) {
                Some(&(prev_hash, first_seen, prev_rpc))
                    if prev_hash == hash && prev_rpc == current_rpc =>
                {
                    // Same content and same RPC state: check timeout
                    let idle_for = now.duration_since(first_seen);
                    if idle_for >= self.timeout
                        && self
                            .confirmed
                            .insert(pane_id.to_string(), current_rpc)
                            .is_none()
                    {
                        tracing::info!(
                            pane_id = %pane_id,
                            updated_ts = current_rpc,
                            idle_for_ms = idle_for.as_millis(),
                            timeout_ms = self.timeout.as_millis(),
                            "agent inactivity detected"
                        );
                    }
                }
                _ => {
                    // Content changed or RPC updated: reset inactivity window
                    self.entries
                        .insert(pane_id.to_string(), (hash, now, current_rpc));
                }
            }
        }

        self.confirmed.keys().cloned().collect()
    }
}

fn spawn_signal_listener(
    term: Arc<AtomicBool>,
    dirty_flag: Arc<AtomicBool>,
    wake_tx: mpsc::SyncSender<()>,
) -> Result<(SignalHandle, thread::JoinHandle<()>)> {
    let mut signals = Signals::new([signal_hook::consts::SIGTERM, signal_hook::consts::SIGUSR1])?;
    let handle = signals.handle();
    let thread = thread::spawn(move || {
        for signal in signals.forever() {
            match signal {
                signal_hook::consts::SIGTERM => term.store(true, Ordering::Relaxed),
                signal_hook::consts::SIGUSR1 => dirty_flag.store(true, Ordering::Relaxed),
                _ => continue,
            }
            let _ = wake_tx.try_send(());
            if signal == signal_hook::consts::SIGTERM {
                break;
            }
        }
    });
    Ok((handle, thread))
}

/// Run the sidebar daemon (headless, no TUI).
pub fn run() -> Result<()> {
    let mux = create_backend(detect_backend());
    let instance_id = mux.instance_id();
    let config = Arc::new(Mutex::new(Config::load(None)?));
    // Captured at startup and intentionally not live-reloaded. tmux's
    // @workmux_pane_status holds the icon string itself; build_snapshot
    // compares pane statuses to these exact strings to suppress stale
    // done/waiting markers, so swapping the icons mid-run would mis-suppress.
    let status_icons = config.lock().unwrap().status_icons.clone();
    let config_version = Arc::new(AtomicU64::new(0));

    tracing::info!(instance_id = %instance_id, "sidebar daemon starting");

    // Signal state for clean shutdown and dirty notification.
    let term = Arc::new(AtomicBool::new(false));
    let dirty_flag = Arc::new(AtomicBool::new(false));

    // Producers wake the main loop without polling. Signal delivery uses a
    // dedicated thread because signal handlers cannot send on channels.
    let (wake_tx, wake_rx) = std::sync::mpsc::sync_channel::<()>(1);
    let (signal_handle, signal_thread) =
        spawn_signal_listener(term.clone(), dirty_flag.clone(), wake_tx.clone())?;
    // Keep a sender alive so recv_timeout won't return Disconnected if a
    // worker thread panics.
    let _wake_tx_keepalive = wake_tx.clone();

    let sock_path = socket_path(&instance_id);
    let _ = std::fs::remove_file(&sock_path); // Clean stale
    let server = SocketServer::bind(&sock_path)?;

    // Config watcher: bumps config_version on global / project .workmux.yaml changes.
    let config_paths_tx = spawn_config_watcher(
        term.clone(),
        config.clone(),
        config_version.clone(),
        dirty_flag.clone(),
        wake_tx.clone(),
    );

    // Background git status worker (shares dirty_flag for immediate broadcast on changes)
    let (git_cache, git_path_tx) =
        spawn_git_worker(term.clone(), dirty_flag.clone(), wake_tx.clone());
    let (pr_cache, check_cache, github_path_tx) =
        spawn_github_worker(term.clone(), dirty_flag.clone(), wake_tx);

    // Store PID so toggle-off can kill us and hooks can signal us
    Cmd::new("tmux")
        .args(&[
            "set-option",
            "-g",
            "@workmux_sidebar_daemon_pid",
            &std::process::id().to_string(),
        ])
        .run()?;

    let mut inactivity_tracker = InactivityTracker::new(Duration::from_secs(10));
    let mut last_interrupted: HashSet<String> = HashSet::new();
    let mut last_runtime_write = Instant::now();
    let backend_name = mux.name().to_string();

    let mut last_refresh = Instant::now();
    let mut last_client_seen = Instant::now();
    let mut dirty_pending = false;
    let mut last_agent_list = String::new();
    let mut last_health_log = Instant::now();
    let refresh_interval = Duration::from_secs(2);
    let debounce_interval = Duration::from_millis(50);

    // Cache of agent_path -> project_config_dir so we don't run the walk-up
    // filesystem search on every tick. Misses (no config found) are NOT
    // cached, so a newly-created `.workmux.yaml` in or above an agent's path
    // is picked up on the next tick.
    let mut project_config_cache: HashMap<PathBuf, PathBuf> = HashMap::new();
    let mut last_config_dirs: HashSet<PathBuf> = HashSet::new();

    while !term.load(Ordering::Relaxed) {
        // Coalesce dirty signals: SIGUSR1 sets the flag, we service it once
        // per debounce interval to prevent signal floods from causing CPU storms
        if dirty_flag.swap(false, Ordering::Relaxed) {
            dirty_pending = true;
        }

        let time_since_refresh = last_refresh.elapsed();
        let debounce_cleared = dirty_pending && time_since_refresh >= debounce_interval;
        let timer_expired = time_since_refresh >= refresh_interval;

        if debounce_cleared || timer_expired {
            dirty_pending = false;
            last_refresh = Instant::now();

            // ── Gather inputs ──
            let tmux_state = match query_tmux_state() {
                Ok(state) => state,
                Err(error) => {
                    tracing::warn!(%error, "failed to query sidebar tmux state");
                    continue;
                }
            };
            let agents = StateStore::new()
                .and_then(|store| {
                    store.load_reconciled_agents_from_snapshot(
                        mux.as_ref(),
                        &tmux_state.live_panes,
                        tmux_state.server_boot_id.as_deref(),
                    )
                })
                .ok();
            let Some(agents) = agents else { continue };

            let (position, layout_mode, sort) = {
                let cfg = config.lock().unwrap();
                (
                    read_sidebar_position(&cfg, tmux_state.position.as_deref()),
                    read_sidebar_layout_mode(&cfg, tmux_state.layout.as_deref())
                        .unwrap_or_default(),
                    cfg.sidebar.sort.unwrap_or_default(),
                )
            };
            let filter_mode = read_sidebar_filter_mode(tmux_state.filter.as_deref());
            let sleeping_pane_ids = read_sleeping_panes(tmux_state.sleeping_panes.as_deref());
            let git_statuses = git_cache.lock().ok().map(|c| c.clone()).unwrap_or_default();
            let pr_statuses = pr_cache.lock().ok().map(|c| c.clone()).unwrap_or_default();
            let check_statuses = check_cache
                .lock()
                .ok()
                .map(|cache| cache.clone())
                .unwrap_or_default();
            let captured_panes = gather_captures(&agents, mux.as_ref(), &inactivity_tracker);
            let now = Instant::now();
            let now_ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let heartbeat_due = last_runtime_write.elapsed() >= Duration::from_secs(10);

            // ── Compute tick (no I/O) ──
            let mut output = compute_tick(
                TickInput {
                    agents,
                    tmux_state,
                    captured_panes,
                    now,
                    now_ts,
                    position,
                    layout_mode,
                    filter_mode,
                    sort,
                    git_statuses,
                    pr_statuses,
                    check_statuses,
                    sleeping_pane_ids,
                },
                &mut inactivity_tracker,
                &last_interrupted,
                &status_icons,
                heartbeat_due,
            );

            // ── Apply side effects, then commit state ──
            if let Ok(store) = StateStore::new()
                && apply_tick_effects(&output, &store, &backend_name, &instance_id)
            {
                last_runtime_write = Instant::now();
            }
            last_interrupted = output.next_interrupted;

            // ── Stamp config version + broadcast ──
            output.snapshot.config_version = config_version.load(Ordering::Relaxed);
            server.broadcast(&output.snapshot);

            // Update git worker with current agent paths and relevance.
            let stale_threshold = 60 * 60; // 1 hour, matches sidebar UI
            let entries: Vec<GitWorkerPath> = output
                .snapshot
                .agents
                .iter()
                .map(|a| GitWorkerPath {
                    path: a.path.clone(),
                    is_stale: a
                        .activity_ts()
                        .map(|ts| now_ts.saturating_sub(ts) > stale_threshold)
                        .unwrap_or(false),
                    is_focused: output.snapshot.active_pane_ids.contains(&a.pane_id)
                        || (!a.window_id.is_empty()
                            && output
                                .snapshot
                                .active_windows
                                .contains(&(a.session.clone(), a.window_id.clone()))),
                })
                .collect();
            let _ = git_path_tx.send(entries);

            let github_entries: Vec<GithubWorkerPath> = output
                .snapshot
                .agents
                .iter()
                .filter_map(|a| {
                    let branch = output.snapshot.git_statuses.get(&a.path)?.branch.as_ref()?;
                    Some(GithubWorkerPath {
                        path: a.path.clone(),
                        branch: branch.clone(),
                    })
                })
                .collect();
            let _ = github_path_tx.send(github_entries);

            // Update config watcher with current project-config dirs.
            // find_project_config does fs walks, so cache by agent path.
            let live_paths: HashSet<PathBuf> = output
                .snapshot
                .agents
                .iter()
                .map(|a| a.path.clone())
                .collect();
            project_config_cache.retain(|p, _| live_paths.contains(p));
            let mut config_dirs: HashSet<PathBuf> = HashSet::new();
            for a in &output.snapshot.agents {
                let dir = if let Some(d) = project_config_cache.get(&a.path) {
                    Some(d.clone())
                } else {
                    let found = crate::config::find_project_config(&a.path)
                        .ok()
                        .flatten()
                        .map(|loc| loc.config_dir);
                    if let Some(ref d) = found {
                        project_config_cache.insert(a.path.clone(), d.clone());
                    }
                    found
                };
                if let Some(d) = dir {
                    config_dirs.insert(d);
                }
            }
            if config_dirs != last_config_dirs {
                let _ = config_paths_tx.send(config_dirs.clone());
                last_config_dirs = config_dirs;
            }

            let agent_list: String = output
                .snapshot
                .agents
                .iter()
                .map(|a| a.pane_id.as_str())
                .collect::<Vec<_>>()
                .join(" ");
            if agent_list != last_agent_list {
                if !agent_list.is_empty() {
                    let _ = Cmd::new("tmux")
                        .args(&["set-option", "-g", "@workmux_sidebar_agents", &agent_list])
                        .run();
                } else {
                    let _ = Cmd::new("tmux")
                        .args(&["set-option", "-gu", "@workmux_sidebar_agents"])
                        .run();
                }
                last_agent_list = agent_list;
            }
        }

        // Track client activity for auto-exit
        let cc = server.client_count();
        if cc > 0 {
            last_client_seen = Instant::now();
        } else if last_client_seen.elapsed() > Duration::from_secs(10) {
            tracing::info!("sidebar daemon exiting: no clients for 10s");
            break;
        }

        // Periodic health log (every 60s)
        if last_health_log.elapsed() >= Duration::from_secs(60) {
            tracing::info!(clients = cc, "sidebar daemon alive");
            last_health_log = Instant::now();
        }

        // Block until a producer wakes the loop or the next refresh is due.
        let wait = if dirty_pending {
            debounce_interval.saturating_sub(last_refresh.elapsed())
        } else {
            refresh_interval.saturating_sub(last_refresh.elapsed())
        };
        let _ = wake_rx.recv_timeout(wait);
    }

    if term.load(Ordering::Relaxed) {
        tracing::info!("sidebar daemon exiting: SIGTERM received");
    }

    signal_handle.close();
    let _ = signal_thread.join();

    // Cleanup
    let _ = std::fs::remove_file(&sock_path);
    if let Ok(store) = StateStore::new() {
        store.delete_runtime(&backend_name, &instance_id);
    }
    let _ = Cmd::new("tmux")
        .args(&["set-option", "-gu", "@workmux_sidebar_daemon_pid"])
        .run();
    let _ = Cmd::new("tmux")
        .args(&["set-option", "-gu", "@workmux_sidebar_agents"])
        .run();
    let _ = Cmd::new("tmux")
        .args(&["set-option", "-gu", "@workmux_sleeping_panes"])
        .run();
    let _ = Cmd::new("tmux")
        .args(&["set-option", "-gu", "@workmux_sidebar_scope"])
        .run();
    Ok(())
}

// ── Tick core ────────────────────────────────────────────────────────────

/// Inputs gathered from the environment for one daemon tick.
struct TickInput {
    agents: Vec<crate::multiplexer::AgentPane>,
    tmux_state: TmuxState,
    captured_panes: HashMap<String, String>,
    now: Instant,
    now_ts: u64,
    position: SidebarPosition,
    layout_mode: SidebarLayoutMode,
    filter_mode: SidebarFilterMode,
    sort: crate::config::SidebarSort,
    git_statuses: HashMap<PathBuf, GitStatus>,
    pr_statuses: HashMap<PathBuf, PrPathEntry>,
    check_statuses: HashMap<PathBuf, CheckPathEntry>,
    sleeping_pane_ids: HashSet<String>,
}

/// A state-file write to apply after computing the tick.
struct AgentWrite {
    pane_id: String,
    resumed_ts: u64,
}

/// Output of a single tick computation.
struct TickOutput {
    snapshot: super::snapshot::SidebarSnapshot,
    agent_writes: Vec<AgentWrite>,
    runtime_write: Option<crate::state::RuntimeState>,
    /// The new interrupted set. Caller should commit to `last_interrupted`
    /// only after side effects are applied successfully.
    next_interrupted: HashSet<String>,
}

/// Compute one daemon tick from in-memory inputs.
///
/// 1. Runs inactivity detection
/// 2. Mutates agents in memory (status and activity timestamps reset for resumed agents)
/// 3. Builds the snapshot from the already-mutated agents
/// 4. Returns side effects (state file writes, runtime file write)
#[allow(clippy::too_many_arguments)]
fn compute_tick(
    input: TickInput,
    tracker: &mut InactivityTracker,
    last_interrupted: &HashSet<String>,
    status_icons: &crate::config::StatusIcons,
    heartbeat_due: bool,
) -> TickOutput {
    let TickInput {
        mut agents,
        tmux_state,
        captured_panes,
        now,
        now_ts,
        position,
        layout_mode,
        filter_mode,
        sort,
        git_statuses,
        pr_statuses,
        check_statuses,
        sleeping_pane_ids,
    } = input;

    // Phase 1: Inactivity detection
    let interrupted =
        tracker.check_with(&agents, now, |pane_id| captured_panes.get(pane_id).cloned());

    // Phase 2: Mutate agents in memory for resumed agents
    let mut agent_writes = Vec::new();
    if !last_interrupted.is_empty() {
        for agent in &mut agents {
            if last_interrupted.contains(&agent.pane_id) && !interrupted.contains(&agent.pane_id) {
                agent.status_ts = Some(now_ts);
                agent.activity_ts = Some(now_ts);
                agent_writes.push(AgentWrite {
                    pane_id: agent.pane_id.clone(),
                    resumed_ts: now_ts,
                });
            }
        }
    }

    // Phase 3: Build snapshot from already-mutated agents
    let mut snapshot = build_snapshot(
        agents,
        &tmux_state.window_statuses,
        &tmux_state.pane_window_ids,
        &tmux_state.pane_window_indexes,
        tmux_state.active_windows,
        tmux_state.active_pane_ids,
        tmux_state.window_pane_counts,
        position,
        layout_mode,
        filter_mode,
        sort,
        status_icons,
        git_statuses,
        pr_statuses,
        check_statuses,
        &sleeping_pane_ids,
    );
    snapshot.interrupted_pane_ids = interrupted.clone();

    // Phase 4: Determine runtime write side effect
    let runtime_write = if interrupted != *last_interrupted || heartbeat_due {
        Some(crate::state::RuntimeState {
            interrupted_pane_ids: interrupted.clone(),
            updated_ts: now_ts,
        })
    } else {
        None
    };

    TickOutput {
        snapshot,
        agent_writes,
        runtime_write,
        next_interrupted: interrupted,
    }
}

/// Apply side effects computed by `compute_tick`.
/// Returns true if runtime state was written.
fn apply_tick_effects(
    output: &TickOutput,
    store: &StateStore,
    backend: &str,
    instance: &str,
) -> bool {
    for write in &output.agent_writes {
        let pane_key = crate::state::PaneKey {
            backend: backend.to_string(),
            instance: instance.to_string(),
            pane_id: write.pane_id.clone(),
        };
        if let Ok(Some(mut state)) = store.get_agent(&pane_key) {
            state.status_ts = Some(write.resumed_ts);
            state.activity_ts = Some(write.resumed_ts);
            let _ = store.upsert_agent(&state);
        }
    }

    if let Some(ref runtime) = output.runtime_write {
        let _ = store.write_runtime(backend, instance, runtime);
        true
    } else {
        false
    }
}

/// Capture pane content for working agents that need checking.
/// Skips agents already confirmed as interrupted (no I/O needed until they resume).
fn gather_captures(
    agents: &[crate::multiplexer::AgentPane],
    mux: &dyn Multiplexer,
    tracker: &InactivityTracker,
) -> HashMap<String, String> {
    agents
        .iter()
        .filter(|a| a.status == Some(crate::multiplexer::AgentStatus::Working))
        .filter(|a| !tracker.is_confirmed(&a.pane_id, a.updated_ts.unwrap_or(0)))
        .filter_map(|a| {
            mux.capture_pane(&a.pane_id, 5)
                .map(|content| (a.pane_id.clone(), content))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multiplexer::{AgentPane, AgentStatus};
    use std::cell::RefCell;
    use std::path::PathBuf;
    use std::process::Command;

    fn run_git(dir: &Path, args: &[&str]) {
        let status = Command::new("git")
            .args(args)
            .current_dir(dir)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?} failed in {}", dir.display());
    }

    fn init_repo(path: &Path) {
        std::fs::create_dir_all(path).unwrap();
        run_git(path, &["init", "-q"]);
    }

    fn working_agent(pane_id: &str, updated_ts: u64) -> AgentPane {
        AgentPane {
            session: String::new(),
            window_name: String::new(),
            pane_id: pane_id.to_string(),
            window_id: String::new(),
            window_index: None,
            path: PathBuf::new(),
            pane_title: None,
            status: Some(AgentStatus::Working),
            status_ts: Some(100),
            activity_ts: Some(100),
            updated_ts: Some(updated_ts),
            window_cmd: None,
            agent_command: None,
            agent_kind: None,
        }
    }

    fn done_agent(pane_id: &str) -> AgentPane {
        AgentPane {
            status: Some(AgentStatus::Done),
            ..working_agent(pane_id, 1)
        }
    }

    #[test]
    fn github_fetch_is_throttled_between_intervals() {
        assert!(!github_fetch_due(false, Duration::from_secs(29)));
        assert!(github_fetch_due(false, Duration::from_secs(30)));
        assert!(github_fetch_due(true, Duration::ZERO));
    }

    fn empty_snapshot() -> super::super::snapshot::SidebarSnapshot {
        super::super::snapshot::SidebarSnapshot {
            position: SidebarPosition::Left,
            layout_mode: SidebarLayoutMode::Tiles,
            filter_mode: SidebarFilterMode::None,
            active_windows: HashSet::new(),
            active_pane_ids: HashSet::new(),
            window_pane_counts: HashMap::new(),
            git_statuses: HashMap::new(),
            pr_statuses: HashMap::new(),
            check_statuses: HashMap::new(),
            interrupted_pane_ids: HashSet::new(),
            sleeping_pane_ids: HashSet::new(),
            agents: Vec::new(),
            config_version: 0,
        }
    }

    fn read_snapshot(stream: &mut UnixStream) -> super::super::snapshot::SidebarSnapshot {
        use std::io::Read;

        let mut header = [0; 4];
        stream.read_exact(&mut header).unwrap();
        let mut payload = vec![0; u32::from_be_bytes(header) as usize];
        stream.read_exact(&mut payload).unwrap();
        serde_json::from_slice(&payload).unwrap()
    }

    fn wait_for_clients(server: &SocketServer, expected: usize) {
        for _ in 0..100 {
            if server.client_count() == expected {
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(server.client_count(), expected);
    }

    #[test]
    fn mutation_watchers_exclude_access_events() {
        assert_eq!(mutation_watcher_config().event_kinds(), EventKindMask::CORE);
    }

    #[test]
    fn snapshot_comparison_covers_every_sidebar_state_category() {
        use crate::github::{CheckState, CheckSummary, PrSummary};

        let original = empty_snapshot();
        let mut variants = Vec::new();

        let mut changed = original.clone();
        changed.position = SidebarPosition::Top;
        variants.push(changed);
        let mut changed = original.clone();
        changed.layout_mode = SidebarLayoutMode::Compact;
        variants.push(changed);
        let mut changed = original.clone();
        changed.filter_mode = SidebarFilterMode::Session;
        variants.push(changed);
        let mut changed = original.clone();
        changed.active_windows.insert(("s".into(), "@1".into()));
        variants.push(changed);
        let mut changed = original.clone();
        changed.active_pane_ids.insert("%1".into());
        variants.push(changed);
        let mut changed = original.clone();
        changed.window_pane_counts.insert("@1".into(), 2);
        variants.push(changed);
        let mut changed = original.clone();
        changed.git_statuses.insert(
            PathBuf::from("/repo"),
            GitStatus {
                is_dirty: true,
                ..GitStatus::default()
            },
        );
        variants.push(changed);
        let mut changed = original.clone();
        changed.pr_statuses.insert(
            PathBuf::from("/repo"),
            PrSummary {
                number: 1,
                title: "title".into(),
                state: "OPEN".into(),
                is_draft: false,
                checks: None,
                check_meta: None,
                url: None,
            },
        );
        variants.push(changed);
        let mut changed = original.clone();
        changed.check_statuses.insert(
            PathBuf::from("/repo"),
            CheckSummary {
                state: CheckState::Success,
                meta: None,
            },
        );
        variants.push(changed);
        let mut changed = original.clone();
        changed.interrupted_pane_ids.insert("%1".into());
        variants.push(changed);
        let mut changed = original.clone();
        changed.sleeping_pane_ids.insert("%1".into());
        variants.push(changed);
        let mut changed = original.clone();
        changed.agents.push(working_agent("%1", 1));
        variants.push(changed);
        let mut changed = original.clone();
        changed.config_version = 1;
        variants.push(changed);

        assert!(
            variants
                .iter()
                .all(|changed| !snapshots_equal(&original, changed))
        );
    }

    #[test]
    fn agent_display_and_sorting_changes_are_meaningful() {
        let mut first = empty_snapshot();
        first.agents.push(working_agent("%1", 1));

        let mut prompt = first.clone();
        prompt.agents[0].pane_title = Some("updated prompt".into());
        assert!(!snapshots_equal(&first, &prompt));

        let mut status = first.clone();
        status.agents[0].status = Some(AgentStatus::Waiting);
        assert!(!snapshots_equal(&first, &status));

        let mut activity = first.clone();
        activity.agents[0].activity_ts = Some(200);
        assert!(!snapshots_equal(&first, &activity));

        let mut window_order = first.clone();
        window_order.agents[0].window_index = Some(2);
        assert!(!snapshots_equal(&first, &window_order));
    }

    #[test]
    fn snapshot_comparison_is_set_order_independent() {
        let mut first = empty_snapshot();
        first.active_pane_ids.extend(["%1".into(), "%2".into()]);
        first
            .active_windows
            .extend([("one".into(), "@1".into()), ("two".into(), "@2".into())]);
        let mut second = empty_snapshot();
        second.active_pane_ids.extend(["%2".into(), "%1".into()]);
        second
            .active_windows
            .extend([("two".into(), "@2".into()), ("one".into(), "@1".into())]);

        assert!(snapshots_equal(&first, &second));
    }

    #[test]
    fn git_snapshot_comparison_ignores_only_cache_freshness() {
        let mut first = empty_snapshot();
        first.git_statuses.insert(
            PathBuf::from("/repo"),
            GitStatus {
                cached_at: Some(1),
                ..GitStatus::default()
            },
        );
        let mut second = first.clone();
        second
            .git_statuses
            .get_mut(Path::new("/repo"))
            .unwrap()
            .cached_at = Some(2);
        assert!(snapshots_equal(&first, &second));

        second
            .git_statuses
            .get_mut(Path::new("/repo"))
            .unwrap()
            .is_rebasing = true;
        assert!(!snapshots_equal(&first, &second));
    }

    #[test]
    fn socket_server_delivers_initial_cache_and_changed_state_only() {
        use std::io::{Read, Write};

        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("sidebar.sock");
        let server = SocketServer::bind(&socket).unwrap();
        let mut first = UnixStream::connect(&socket).unwrap();
        first
            .set_read_timeout(Some(Duration::from_millis(250)))
            .unwrap();

        let mut snapshot = empty_snapshot();
        assert!(server.broadcast(&snapshot));
        assert_eq!(read_snapshot(&mut first).config_version, 0);

        let mut late = UnixStream::connect(&socket).unwrap();
        late.set_read_timeout(Some(Duration::from_millis(250)))
            .unwrap();
        assert_eq!(read_snapshot(&mut late).config_version, 0);

        assert!(!server.broadcast(&snapshot));
        let mut header = [0; 4];
        let error = first.read_exact(&mut header).unwrap_err();
        assert!(matches!(
            error.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        ));

        snapshot.git_statuses.insert(
            PathBuf::from("/repo"),
            GitStatus {
                cached_at: Some(1),
                ..GitStatus::default()
            },
        );
        assert!(server.broadcast(&snapshot));
        let _ = read_snapshot(&mut first);
        let _ = read_snapshot(&mut late);
        snapshot
            .git_statuses
            .get_mut(Path::new("/repo"))
            .unwrap()
            .cached_at = Some(2);
        assert!(!server.broadcast(&snapshot));

        snapshot.active_pane_ids.insert("%1".into());
        assert!(server.broadcast(&snapshot));
        assert!(read_snapshot(&mut first).active_pane_ids.contains("%1"));
        wait_for_clients(&server, 2);

        drop(first);
        drop(late);
        wait_for_clients(&server, 0);

        let mut malformed = UnixStream::connect(&socket).unwrap();
        wait_for_clients(&server, 1);
        malformed.write_all(b"unexpected").unwrap();
        wait_for_clients(&server, 0);
    }

    #[test]
    fn concurrent_accept_observes_latest_published_generation() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("sidebar.sock");
        let server = Arc::new(SocketServer::bind(&socket).unwrap());
        assert!(server.broadcast(&empty_snapshot()));

        let publisher = Arc::clone(&server);
        let publish = thread::spawn(move || {
            for version in 1..=20 {
                let mut snapshot = empty_snapshot();
                snapshot.config_version = version;
                assert!(publisher.broadcast(&snapshot));
                thread::sleep(Duration::from_millis(1));
            }
        });
        let mut client = UnixStream::connect(&socket).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        let mut seen = 0;
        while seen < 20 {
            let next = read_snapshot(&mut client).config_version;
            assert!(next >= seen);
            seen = next;
        }
        publish.join().unwrap();
        assert_eq!(seen, 20);
    }

    #[test]
    fn rescan_events_request_recovery() {
        let event =
            notify::Event::new(notify::EventKind::Other).set_flag(notify::event::Flag::Rescan);
        assert!(git_event_requires_recovery(&event));
        assert!(!git_event_requires_recovery(&notify::Event::new(
            notify::EventKind::Other,
        )));
    }

    #[test]
    fn recovery_cooldown_coalesces_repeated_failures() {
        let cooldown = Duration::from_secs(30);
        assert!(recovery_ready(true, Instant::now() - cooldown, cooldown));
        assert!(!recovery_ready(true, Instant::now(), cooldown));
        assert!(!recovery_ready(false, Instant::now() - cooldown, cooldown));
    }

    #[test]
    fn rolling_audit_is_fair_and_skips_incomplete_watches() {
        let paths = vec![
            PathBuf::from("/one"),
            PathBuf::from("/two"),
            PathBuf::from("/three"),
        ];
        let complete = HashMap::from([
            (paths[0].clone(), true),
            (paths[1].clone(), false),
            (paths[2].clone(), true),
        ]);
        let mut cursor = 0;

        assert_eq!(
            next_audit_path(&paths, &complete, &mut cursor),
            Some(paths[0].clone())
        );
        assert_eq!(
            next_audit_path(&paths, &complete, &mut cursor),
            Some(paths[2].clone())
        );
        assert_eq!(
            next_audit_path(&paths, &complete, &mut cursor),
            Some(paths[0].clone())
        );
    }

    #[test]
    fn ignored_tracked_files_still_trigger_refresh() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        init_repo(&repo);
        std::fs::write(repo.join(".gitignore"), "*.log\n").unwrap();
        std::fs::write(repo.join("tracked.log"), "tracked\n").unwrap();
        run_git(&repo, &["add", ".gitignore"]);
        run_git(&repo, &["add", "-f", "tracked.log"]);

        let root = repo.canonicalize().unwrap();
        let gitignores = HashMap::from([(root.clone(), build_gitignore(&root))]);
        let ignored_tracked_paths =
            HashMap::from([(root.clone(), load_ignored_tracked_paths(&root))]);

        assert!(!is_event_ignored(
            &root.join("tracked.log"),
            &root,
            &gitignores,
            &ignored_tracked_paths,
        ));
        assert!(is_event_ignored(
            &root.join("generated.log"),
            &root,
            &gitignores,
            &ignored_tracked_paths,
        ));
    }

    #[test]
    fn cache_projects_status_when_agent_path_changes_under_same_root() {
        let root = PathBuf::from("/repo");
        let old_path = root.clone();
        let nested = root.join("nested");
        let previous = HashMap::from([(
            root.clone(),
            ResolvedGitWorktree {
                agent_paths: vec![old_path.clone()],
                is_stale: true,
                is_focused: false,
            },
        )]);
        let current = HashMap::from([(
            root,
            ResolvedGitWorktree {
                agent_paths: vec![nested.clone()],
                is_stale: true,
                is_focused: false,
            },
        )]);
        let status = GitStatus {
            branch: Some("main".to_string()),
            ..GitStatus::default()
        };
        let mut cache = HashMap::from([(old_path.clone(), status.clone())]);

        let (changed, missing) = reconcile_git_cache(&previous, &current, &mut cache);

        assert!(changed);
        assert!(missing.is_empty());
        assert_eq!(cache, HashMap::from([(nested, status)]));
        assert!(!cache.contains_key(&old_path));
    }

    #[test]
    fn cached_resolution_updates_relevance_without_repository_discovery() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        init_repo(&repo);
        let root = repo.canonicalize().unwrap();
        let mut roots = HashMap::from([(repo.clone(), Some(root.clone()))]);

        let resolved = resolve_git_worktrees_cached(
            &[GitWorkerPath {
                path: repo.clone(),
                is_stale: false,
                is_focused: true,
            }],
            &mut roots,
        );

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[&root].agent_paths, vec![repo]);
        assert!(!resolved[&root].is_stale);
        assert!(resolved[&root].is_focused);
    }

    #[test]
    fn non_repository_agent_path_produces_no_watch_or_refresh_roots() {
        let dir = tempfile::tempdir().unwrap();
        let entries = vec![GitWorkerPath {
            path: dir.path().to_path_buf(),
            is_stale: false,
            is_focused: false,
        }];

        let resolved = resolve_git_worktrees_cached(&entries, &mut HashMap::new());
        let watch_specs: Vec<_> = resolved
            .keys()
            .flat_map(|root| worktree_watch_specs(root, true))
            .collect();

        assert!(resolved.is_empty());
        assert!(watch_specs.is_empty());
    }

    #[test]
    fn nested_agent_paths_share_the_repository_root() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        let nested = repo.join("src/nested");
        init_repo(&repo);
        std::fs::create_dir_all(&nested).unwrap();
        let entries = vec![
            GitWorkerPath {
                path: repo.clone(),
                is_stale: true,
                is_focused: false,
            },
            GitWorkerPath {
                path: nested.clone(),
                is_stale: false,
                is_focused: true,
            },
        ];

        let resolved = resolve_git_worktrees_cached(&entries, &mut HashMap::new());
        let root = repo.canonicalize().unwrap();

        assert_eq!(resolved.len(), 1);
        assert_eq!(
            resolved.get(&root),
            Some(&ResolvedGitWorktree {
                agent_paths: vec![repo, nested],
                is_stale: false,
                is_focused: true,
            })
        );
    }

    #[test]
    fn linked_worktree_uses_its_root_and_shared_git_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        let linked = dir.path().join("linked");
        init_repo(&repo);
        run_git(&repo, &["config", "user.name", "Workmux Tests"]);
        run_git(&repo, &["config", "user.email", "workmux@example.com"]);
        std::fs::write(repo.join("tracked"), "content").unwrap();
        run_git(&repo, &["add", "tracked"]);
        run_git(&repo, &["commit", "-q", "-m", "initial"]);
        run_git(
            &repo,
            &[
                "worktree",
                "add",
                "-q",
                "-b",
                "linked-test",
                linked.to_str().unwrap(),
            ],
        );
        let nested = linked.join("nested");
        std::fs::create_dir(&nested).unwrap();

        let entries = [GitWorkerPath {
            path: nested.clone(),
            is_stale: false,
            is_focused: false,
        }];
        let resolved = resolve_git_worktrees_cached(&entries, &mut HashMap::new());
        let linked_root = linked.canonicalize().unwrap();
        assert_eq!(resolved.keys().collect::<Vec<_>>(), vec![&linked_root]);

        let git_dir = resolve_git_dir(&linked_root)
            .unwrap()
            .canonicalize()
            .unwrap();
        let common_dir = resolve_common_git_dir(&git_dir).unwrap();
        let specs = worktree_watch_specs(&linked_root, true);

        assert!(specs.iter().any(|spec| {
            spec.path == git_dir && matches!(spec.mode, RecursiveMode::NonRecursive)
        }));
        assert!(specs.iter().any(|spec| {
            spec.path == common_dir && matches!(spec.mode, RecursiveMode::NonRecursive)
        }));
        assert!(specs.iter().any(|spec| {
            spec.path == common_dir.join("refs") && matches!(spec.mode, RecursiveMode::Recursive)
        }));
        assert!(specs.iter().any(|spec| {
            spec.path == linked_root && matches!(spec.mode, RecursiveMode::Recursive)
        }));
    }

    #[test]
    fn config_reload_triggers_on_mutations_of_config_files() {
        use notify::event::{CreateKind, ModifyKind, RemoveKind, RenameMode};

        let config = PathBuf::from("/home/u/.config/workmux/config.yaml");
        for kind in [
            notify::EventKind::Modify(ModifyKind::Any),
            notify::EventKind::Create(CreateKind::File),
            notify::EventKind::Remove(RemoveKind::File),
        ] {
            let event = notify::Event::new(kind).add_path(config.clone());
            assert!(config_event_triggers_reload(&event), "kind: {kind:?}");
        }

        let project = notify::Event::new(notify::EventKind::Modify(ModifyKind::Any))
            .add_path(PathBuf::from("/repo/.workmux.yaml"));
        assert!(config_event_triggers_reload(&project));

        let atomic_rename = notify::Event::new(notify::EventKind::Modify(ModifyKind::Name(
            RenameMode::Both,
        )))
        .add_path(PathBuf::from("/home/u/.config/workmux/config.yaml.tmp"))
        .add_path(config);
        assert!(config_event_triggers_reload(&atomic_rename));
    }

    #[test]
    fn config_reload_ignores_access_events_and_other_files() {
        use notify::event::{AccessKind, AccessMode, ModifyKind};

        let config = PathBuf::from("/home/u/.config/workmux/config.yaml");
        for kind in [
            notify::EventKind::Access(AccessKind::Open(AccessMode::Any)),
            notify::EventKind::Access(AccessKind::Close(AccessMode::Read)),
            notify::EventKind::Access(AccessKind::Close(AccessMode::Write)),
        ] {
            let event = notify::Event::new(kind).add_path(config.clone());
            assert!(!config_event_triggers_reload(&event), "kind: {kind:?}");
        }

        let unrelated = notify::Event::new(notify::EventKind::Modify(ModifyKind::Any))
            .add_path(PathBuf::from("/home/u/.config/workmux/other.txt"));
        assert!(!config_event_triggers_reload(&unrelated));
    }

    #[test]
    fn config_reload_triggers_on_rescan() {
        let event =
            notify::Event::new(notify::EventKind::Other).set_flag(notify::event::Flag::Rescan);

        assert!(config_event_triggers_reload(&event));
    }

    #[test]
    fn github_branches_are_grouped_by_common_repository() {
        let first = PathBuf::from("/repo__worktrees/first");
        let second = PathBuf::from("/repo__worktrees/second");
        let other = PathBuf::from("/other");
        let common = PathBuf::from("/repo/.git");
        let other_common = PathBuf::from("/other/.git");
        let entries = vec![
            GithubWorkerPath {
                path: first.clone(),
                branch: "feature-a".to_string(),
            },
            GithubWorkerPath {
                path: second.clone(),
                branch: "feature-b".to_string(),
            },
            GithubWorkerPath {
                path: other.clone(),
                branch: "main".to_string(),
            },
        ];
        let repo_keys = HashMap::from([
            (first.clone(), common.clone()),
            (second, common.clone()),
            (other.clone(), other_common.clone()),
        ]);

        let grouped = group_github_branches(&entries, &repo_keys);

        assert_eq!(grouped.len(), 2);
        assert_eq!(
            grouped.get(&common),
            Some(&(
                first,
                vec!["feature-a".to_string(), "feature-b".to_string()]
            ))
        );
        assert_eq!(
            grouped.get(&other_common),
            Some(&(other, vec!["main".to_string()]))
        );
    }

    #[test]
    fn clear_pr_path_cache_only_reports_content_changes() {
        let cache: PrPathCache = Arc::new(Mutex::new(HashMap::new()));

        assert!(!clear_pr_path_cache(&cache));

        cache.lock().unwrap().insert(
            PathBuf::from("/repo"),
            PrPathEntry {
                branch: "feature".to_string(),
                summary: PrSummary {
                    number: 123,
                    title: "test".to_string(),
                    state: "OPEN".to_string(),
                    is_draft: false,
                    checks: None,
                    check_meta: None,
                    url: None,
                },
            },
        );

        assert!(clear_pr_path_cache(&cache));
        assert!(cache.lock().unwrap().is_empty());
        assert!(!clear_pr_path_cache(&cache));
    }

    #[test]
    fn pr_path_cache_records_branch() {
        let path = PathBuf::from("/repo");
        let repo_root = PathBuf::from("/repo");
        let summary = PrSummary {
            number: 123,
            title: "test".to_string(),
            state: "OPEN".to_string(),
            is_draft: false,
            checks: None,
            check_meta: None,
            url: None,
        };
        let entries = vec![GithubWorkerPath {
            path: path.clone(),
            branch: "feature".to_string(),
        }];
        let repo_keys = HashMap::from([(path.clone(), repo_root.clone())]);
        let repo_cache =
            HashMap::from([(repo_root, HashMap::from([("feature".to_string(), summary)]))]);
        let path_cache: PrPathCache = Arc::new(Mutex::new(HashMap::new()));
        let dirty_flag = Arc::new(AtomicBool::new(false));
        let (wake_tx, _wake_rx) = std::sync::mpsc::sync_channel(1);

        publish_pr_path_cache(
            &entries,
            &repo_keys,
            &repo_cache,
            &path_cache,
            &dirty_flag,
            &wake_tx,
        );

        let cache = path_cache.lock().unwrap();
        let entry = cache.get(&path).unwrap();
        assert_eq!(entry.branch, "feature");
        assert_eq!(entry.summary.number, 123);
        assert!(dirty_flag.load(Ordering::Relaxed));
    }

    #[test]
    fn no_interruption_before_timeout() {
        let mut tracker = InactivityTracker::new(Duration::from_secs(10));
        let agents = vec![working_agent("%1", 1)];
        let t0 = Instant::now();

        // First check: records the hash
        let result = tracker.check_with(&agents, t0, |_| Some("hello".into()));
        assert!(result.is_empty());

        // 5s later, same content: not yet interrupted
        let result = tracker.check_with(&agents, t0 + Duration::from_secs(5), |_| {
            Some("hello".into())
        });
        assert!(result.is_empty());
    }

    #[test]
    fn interruption_after_timeout() {
        let mut tracker = InactivityTracker::new(Duration::from_secs(10));
        let agents = vec![working_agent("%1", 1)];
        let t0 = Instant::now();

        // First check records hash
        tracker.check_with(&agents, t0, |_| Some("hello".into()));

        // 11s later, same content: interrupted
        let result = tracker.check_with(&agents, t0 + Duration::from_secs(11), |_| {
            Some("hello".into())
        });
        assert!(result.contains("%1"));
    }

    #[test]
    fn changing_content_resets_window() {
        let mut tracker = InactivityTracker::new(Duration::from_secs(10));
        let agents = vec![working_agent("%1", 1)];
        let t0 = Instant::now();

        // First check
        tracker.check_with(&agents, t0, |_| Some("hello".into()));

        // 8s later, content changes: resets the window
        tracker.check_with(&agents, t0 + Duration::from_secs(8), |_| {
            Some("world".into())
        });

        // 5s after the change (13s total): not interrupted (only 5s since reset)
        let result = tracker.check_with(&agents, t0 + Duration::from_secs(13), |_| {
            Some("world".into())
        });
        assert!(result.is_empty());

        // 11s after the change (19s total): now interrupted
        let result = tracker.check_with(&agents, t0 + Duration::from_secs(19), |_| {
            Some("world".into())
        });
        assert!(result.contains("%1"));
    }

    #[test]
    fn sticky_despite_content_change() {
        let mut tracker = InactivityTracker::new(Duration::from_secs(10));
        let agents = vec![working_agent("%1", 1)];
        let t0 = Instant::now();

        // Become interrupted
        tracker.check_with(&agents, t0, |_| Some("hello".into()));
        let result = tracker.check_with(&agents, t0 + Duration::from_secs(11), |_| {
            Some("hello".into())
        });
        assert!(result.contains("%1"));

        // Content changes (user typing): still interrupted
        let result = tracker.check_with(&agents, t0 + Duration::from_secs(12), |_| {
            Some("user typed something".into())
        });
        assert!(result.contains("%1"));
    }

    #[test]
    fn clears_on_updated_ts_change() {
        let mut tracker = InactivityTracker::new(Duration::from_secs(10));
        let agents = vec![working_agent("%1", 1)];
        let t0 = Instant::now();

        // Become interrupted
        tracker.check_with(&agents, t0, |_| Some("hello".into()));
        tracker.check_with(&agents, t0 + Duration::from_secs(11), |_| {
            Some("hello".into())
        });

        // Agent sends new RPC (updated_ts changes): clears interrupted
        let resumed_agents = vec![working_agent("%1", 2)];
        let result = tracker.check_with(&resumed_agents, t0 + Duration::from_secs(12), |_| {
            Some("hello".into())
        });
        assert!(result.is_empty());
    }

    #[test]
    fn fresh_window_after_resume() {
        let mut tracker = InactivityTracker::new(Duration::from_secs(10));
        let agents = vec![working_agent("%1", 1)];
        let t0 = Instant::now();

        // Become interrupted
        tracker.check_with(&agents, t0, |_| Some("hello".into()));
        tracker.check_with(&agents, t0 + Duration::from_secs(11), |_| {
            Some("hello".into())
        });

        // Resume (updated_ts changes) at t=12s
        let resumed = vec![working_agent("%1", 2)];
        tracker.check_with(&resumed, t0 + Duration::from_secs(12), |_| {
            Some("hello".into())
        });

        // 5s after resume (t=17s): same content but not interrupted yet (fresh window)
        let result = tracker.check_with(&resumed, t0 + Duration::from_secs(17), |_| {
            Some("hello".into())
        });
        assert!(result.is_empty());

        // 11s after resume (t=23s): now interrupted again
        let result = tracker.check_with(&resumed, t0 + Duration::from_secs(23), |_| {
            Some("hello".into())
        });
        assert!(result.contains("%1"));
    }

    #[test]
    fn non_working_agents_ignored() {
        let mut tracker = InactivityTracker::new(Duration::from_secs(10));
        let agents = vec![done_agent("%1")];
        let t0 = Instant::now();

        tracker.check_with(&agents, t0, |_| Some("hello".into()));
        let result = tracker.check_with(&agents, t0 + Duration::from_secs(11), |_| {
            Some("hello".into())
        });
        assert!(result.is_empty());
    }

    #[test]
    fn leaves_working_clears_tracking() {
        let mut tracker = InactivityTracker::new(Duration::from_secs(10));
        let working = vec![working_agent("%1", 1)];
        let t0 = Instant::now();

        // Become interrupted
        tracker.check_with(&working, t0, |_| Some("hello".into()));
        tracker.check_with(&working, t0 + Duration::from_secs(11), |_| {
            Some("hello".into())
        });

        // Agent transitions to Done
        let done = vec![done_agent("%1")];
        let result = tracker.check_with(&done, t0 + Duration::from_secs(12), |_| {
            Some("hello".into())
        });
        assert!(result.is_empty());

        // Comes back as Working: starts fresh
        let working_again = vec![working_agent("%1", 3)];
        let result = tracker.check_with(&working_again, t0 + Duration::from_secs(13), |_| {
            Some("hello".into())
        });
        assert!(result.is_empty()); // just recorded, not yet timed out
    }

    #[test]
    fn capture_failure_skips_pane() {
        let mut tracker = InactivityTracker::new(Duration::from_secs(10));
        let agents = vec![working_agent("%1", 1)];
        let t0 = Instant::now();

        // Capture fails: no entry recorded
        tracker.check_with(&agents, t0, |_| None);
        let result = tracker.check_with(&agents, t0 + Duration::from_secs(11), |_| None);
        assert!(result.is_empty());
    }

    #[test]
    fn multiple_agents_tracked_independently() {
        let mut tracker = InactivityTracker::new(Duration::from_secs(10));
        let agents = vec![working_agent("%1", 1), working_agent("%2", 1)];
        let t0 = Instant::now();

        let content = RefCell::new(HashMap::from([
            ("%1".to_string(), "static".to_string()),
            ("%2".to_string(), "changing".to_string()),
        ]));

        // First check
        tracker.check_with(&agents, t0, |id| content.borrow().get(id).cloned());

        // Change %2's content at 5s
        content
            .borrow_mut()
            .insert("%2".to_string(), "new output".into());
        tracker.check_with(&agents, t0 + Duration::from_secs(5), |id| {
            content.borrow().get(id).cloned()
        });

        // At 11s: %1 is interrupted (11s unchanged), %2 is not (only 6s since change)
        let result = tracker.check_with(&agents, t0 + Duration::from_secs(11), |id| {
            content.borrow().get(id).cloned()
        });
        assert_eq!(result, HashSet::from(["%1".to_string()]));
    }

    #[test]
    fn rpc_update_before_timeout_resets_window() {
        let mut tracker = InactivityTracker::new(Duration::from_secs(10));
        let t0 = Instant::now();

        // Start tracking
        tracker.check_with(&[working_agent("%1", 1)], t0, |_| Some("hello".into()));

        // Agent sends RPC at 5s (updated_ts changes) but content unchanged
        tracker.check_with(
            &[working_agent("%1", 2)],
            t0 + Duration::from_secs(5),
            |_| Some("hello".into()),
        );

        // At 11s: only 6s since RPC update, should NOT be interrupted
        let result = tracker.check_with(
            &[working_agent("%1", 2)],
            t0 + Duration::from_secs(11),
            |_| Some("hello".into()),
        );
        assert!(result.is_empty());

        // At 16s: 11s since RPC update, now interrupted
        let result = tracker.check_with(
            &[working_agent("%1", 2)],
            t0 + Duration::from_secs(16),
            |_| Some("hello".into()),
        );
        assert_eq!(result, HashSet::from(["%1".to_string()]));
    }

    #[test]
    fn interruption_at_exact_timeout() {
        let mut tracker = InactivityTracker::new(Duration::from_secs(10));
        let agents = vec![working_agent("%1", 1)];
        let t0 = Instant::now();

        tracker.check_with(&agents, t0, |_| Some("hello".into()));
        let result = tracker.check_with(&agents, t0 + Duration::from_secs(10), |_| {
            Some("hello".into())
        });
        assert_eq!(result, HashSet::from(["%1".to_string()]));
    }

    #[test]
    fn ansi_and_whitespace_normalized() {
        let mut tracker = InactivityTracker::new(Duration::from_secs(10));
        let agents = vec![working_agent("%1", 1)];
        let t0 = Instant::now();

        // Plain text first
        tracker.check_with(&agents, t0, |_| Some("hello\n".into()));

        // Same text wrapped in ANSI codes + trailing whitespace: should hash the same
        let result = tracker.check_with(&agents, t0 + Duration::from_secs(11), |_| {
            Some("\x1b[31mhello\x1b[0m   ".into())
        });
        assert_eq!(result, HashSet::from(["%1".to_string()]));
    }

    #[test]
    fn capture_failure_does_not_create_baseline() {
        let mut tracker = InactivityTracker::new(Duration::from_secs(10));
        let agents = vec![working_agent("%1", 1)];
        let t0 = Instant::now();

        // Capture fails: no baseline recorded
        tracker.check_with(&agents, t0, |_| None);

        // Capture succeeds later: this is the first successful capture, not a timeout
        let result = tracker.check_with(&agents, t0 + Duration::from_secs(11), |_| {
            Some("hello".into())
        });
        assert!(result.is_empty());
    }

    // ── Tick-level tests (tracker + state store + runtime) ──────────────

    mod tick {
        use super::*;
        use crate::config::StatusIcons;
        use crate::multiplexer::AgentStatus;
        use crate::state::{PaneKey, StateStore};

        const BACKEND: &str = "tmux";
        const INSTANCE: &str = "test";

        fn test_store() -> (StateStore, tempfile::TempDir) {
            let dir = tempfile::TempDir::new().unwrap();
            let store = StateStore::with_path(dir.path().to_path_buf()).unwrap();
            (store, dir)
        }

        fn pane_key(pane_id: &str) -> PaneKey {
            PaneKey {
                backend: BACKEND.to_string(),
                instance: INSTANCE.to_string(),
                pane_id: pane_id.to_string(),
            }
        }

        fn seed_agent(store: &StateStore, pane_id: &str, status_ts: u64, updated_ts: u64) {
            let state = crate::state::AgentState {
                pane_key: pane_key(pane_id),
                workdir: PathBuf::from("/tmp"),
                status: Some(AgentStatus::Working),
                status_ts: Some(status_ts),
                activity_ts: Some(status_ts),
                pane_title: None,
                pane_pid: 1,
                command: "node".to_string(),
                updated_ts,
                window_name: None,
                session_name: None,
                boot_id: None,
                agent_kind: None,
                agent_session_id: None,
            };
            store.upsert_agent(&state).unwrap();
        }

        fn do_tick(
            tracker: &mut InactivityTracker,
            last: &mut HashSet<String>,
            agents: Vec<crate::multiplexer::AgentPane>,
            captures: HashMap<String, String>,
            now: Instant,
            now_ts: u64,
        ) -> TickOutput {
            let output = compute_tick(
                TickInput {
                    agents,
                    tmux_state: TmuxState {
                        live_panes: HashMap::new(),
                        window_statuses: HashMap::new(),
                        active_windows: HashSet::new(),
                        pane_window_ids: HashMap::new(),
                        pane_window_indexes: HashMap::new(),
                        active_pane_ids: HashSet::new(),
                        window_pane_counts: HashMap::new(),
                        server_boot_id: None,
                        position: None,
                        layout: None,
                        filter: None,
                        sleeping_panes: None,
                    },
                    captured_panes: captures,
                    sort: crate::config::SidebarSort::default(),
                    now,
                    now_ts,
                    position: SidebarPosition::Left,
                    layout_mode: SidebarLayoutMode::default(),
                    filter_mode: SidebarFilterMode::default(),
                    git_statuses: HashMap::new(),
                    pr_statuses: HashMap::new(),
                    check_statuses: HashMap::new(),
                    sleeping_pane_ids: HashSet::new(),
                },
                tracker,
                last,
                &StatusIcons::default(),
                false,
            );
            // Commit state like the daemon loop does after apply_tick_effects
            *last = output.next_interrupted.clone();
            output
        }

        fn cap(content: &str) -> HashMap<String, String> {
            HashMap::from([("%1".to_string(), content.to_string())])
        }

        fn cap2(content: &str) -> HashMap<String, String> {
            HashMap::from([
                ("%1".to_string(), content.to_string()),
                ("%2".to_string(), content.to_string()),
            ])
        }

        #[test]
        fn resumed_agent_gets_status_ts_reset() {
            let (store, _dir) = test_store();
            seed_agent(&store, "%1", 100, 1);

            let mut tracker = InactivityTracker::new(Duration::from_secs(10));
            let mut last = HashSet::new();
            let t0 = Instant::now();

            // Tick 1: start observing
            do_tick(
                &mut tracker,
                &mut last,
                vec![working_agent("%1", 1)],
                cap("hello"),
                t0,
                1000,
            );

            // Tick 2: interrupted
            let output = do_tick(
                &mut tracker,
                &mut last,
                vec![working_agent("%1", 1)],
                cap("hello"),
                t0 + Duration::from_secs(11),
                1011,
            );
            assert!(output.snapshot.interrupted_pane_ids.contains("%1"));

            // Tick 3: agent resumes (updated_ts 1 -> 2)
            let output = do_tick(
                &mut tracker,
                &mut last,
                vec![working_agent("%1", 2)],
                cap("hello"),
                t0 + Duration::from_secs(12),
                1012,
            );
            assert!(output.snapshot.interrupted_pane_ids.is_empty());

            // Snapshot has corrected status_ts (no stale one-tick race)
            let agent = output
                .snapshot
                .agents
                .iter()
                .find(|a| a.pane_id == "%1")
                .unwrap();
            assert_eq!(agent.status_ts, Some(1012));
            assert_eq!(agent.activity_ts, Some(1012));

            // Side effect says to write it to disk
            assert_eq!(output.agent_writes.len(), 1);
            assert_eq!(output.agent_writes[0].resumed_ts, 1012);

            // Apply effects and verify store
            apply_tick_effects(&output, &store, BACKEND, INSTANCE);
            let persisted = store.get_agent(&pane_key("%1")).unwrap().unwrap();
            assert_eq!(persisted.status_ts, Some(1012));
            assert_eq!(persisted.activity_ts, Some(1012));
        }

        #[test]
        fn only_resumed_agent_gets_reset() {
            let (store, _dir) = test_store();
            seed_agent(&store, "%1", 100, 1);
            seed_agent(&store, "%2", 200, 1);

            let mut tracker = InactivityTracker::new(Duration::from_secs(10));
            let mut last = HashSet::new();
            let t0 = Instant::now();

            let agents = vec![working_agent("%1", 1), working_agent("%2", 1)];

            // Tick 1 + 2: both interrupted
            do_tick(
                &mut tracker,
                &mut last,
                agents.clone(),
                cap2("hello"),
                t0,
                1000,
            );
            do_tick(
                &mut tracker,
                &mut last,
                agents,
                cap2("hello"),
                t0 + Duration::from_secs(11),
                1011,
            );

            // Tick 3: only %1 resumes
            let mixed = vec![working_agent("%1", 2), working_agent("%2", 1)];
            let output = do_tick(
                &mut tracker,
                &mut last,
                mixed,
                cap2("hello"),
                t0 + Duration::from_secs(12),
                1012,
            );

            // Only %1 in agent_writes
            assert_eq!(output.agent_writes.len(), 1);
            assert_eq!(output.agent_writes[0].pane_id, "%1");

            // Apply and verify
            apply_tick_effects(&output, &store, BACKEND, INSTANCE);
            let resumed = store.get_agent(&pane_key("%1")).unwrap().unwrap();
            assert_eq!(resumed.status_ts, Some(1012));
            assert_eq!(resumed.activity_ts, Some(1012));
            let untouched = store.get_agent(&pane_key("%2")).unwrap().unwrap();
            assert_eq!(untouched.status_ts, Some(200));
            assert_eq!(untouched.activity_ts, Some(200));
        }

        #[test]
        fn runtime_file_reflects_interrupted_set() {
            let (store, _dir) = test_store();

            let mut tracker = InactivityTracker::new(Duration::from_secs(10));
            let mut last = HashSet::new();
            let t0 = Instant::now();

            // Tick 1: not interrupted yet
            let output = do_tick(
                &mut tracker,
                &mut last,
                vec![working_agent("%1", 1)],
                cap("hello"),
                t0,
                1000,
            );
            apply_tick_effects(&output, &store, BACKEND, INSTANCE);

            // Tick 2: interrupted
            let output = do_tick(
                &mut tracker,
                &mut last,
                vec![working_agent("%1", 1)],
                cap("hello"),
                t0 + Duration::from_secs(11),
                1011,
            );
            apply_tick_effects(&output, &store, BACKEND, INSTANCE);
            assert!(
                store
                    .read_runtime(BACKEND, INSTANCE)
                    .interrupted_pane_ids
                    .contains("%1")
            );

            // Tick 3: resumes
            let output = do_tick(
                &mut tracker,
                &mut last,
                vec![working_agent("%1", 2)],
                cap("hello"),
                t0 + Duration::from_secs(12),
                1012,
            );
            apply_tick_effects(&output, &store, BACKEND, INSTANCE);
            assert!(
                store
                    .read_runtime(BACKEND, INSTANCE)
                    .interrupted_pane_ids
                    .is_empty()
            );
        }

        #[test]
        fn missing_agent_file_does_not_panic() {
            let (store, _dir) = test_store();

            let mut tracker = InactivityTracker::new(Duration::from_secs(10));
            let mut last = HashSet::new();
            let t0 = Instant::now();

            // Tick 1 + 2: become interrupted
            do_tick(
                &mut tracker,
                &mut last,
                vec![working_agent("%1", 1)],
                cap("hello"),
                t0,
                1000,
            );
            do_tick(
                &mut tracker,
                &mut last,
                vec![working_agent("%1", 1)],
                cap("hello"),
                t0 + Duration::from_secs(11),
                1011,
            );

            // Tick 3: resume with no agent file - should not panic
            let output = do_tick(
                &mut tracker,
                &mut last,
                vec![working_agent("%1", 2)],
                cap("hello"),
                t0 + Duration::from_secs(12),
                1012,
            );
            assert!(output.snapshot.interrupted_pane_ids.is_empty());
            apply_tick_effects(&output, &store, BACKEND, INSTANCE);
        }

        #[test]
        fn snapshot_has_correct_status_ts_on_resume_tick() {
            // Proves the one-tick race is structurally impossible: agents
            // are mutated before build_snapshot, not patched after.
            let mut tracker = InactivityTracker::new(Duration::from_secs(10));
            let mut last = HashSet::new();
            let t0 = Instant::now();

            do_tick(
                &mut tracker,
                &mut last,
                vec![working_agent("%1", 1)],
                cap("hello"),
                t0,
                1000,
            );
            do_tick(
                &mut tracker,
                &mut last,
                vec![working_agent("%1", 1)],
                cap("hello"),
                t0 + Duration::from_secs(11),
                1011,
            );

            // Resume tick: snapshot must have the fresh status_ts, not the stale 100
            let output = do_tick(
                &mut tracker,
                &mut last,
                vec![working_agent("%1", 2)],
                cap("hello"),
                t0 + Duration::from_secs(12),
                1012,
            );
            let agent = output
                .snapshot
                .agents
                .iter()
                .find(|a| a.pane_id == "%1")
                .unwrap();
            assert_eq!(agent.status_ts, Some(1012));
            assert_eq!(agent.activity_ts, Some(1012));
            assert!(!output.snapshot.interrupted_pane_ids.contains("%1"));
        }
    }
}
