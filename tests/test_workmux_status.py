"""
Tests for `workmux status` command.

Tests output format, JSON mode, filtering, and behavior with real agent state.
"""

import json
from pathlib import Path
from typing import cast

import pytest

from .conftest import (
    MuxEnvironment,
    TmuxEnvironment,
    get_window_name,
    get_worktree_path,
    poll_until,
    run_workmux_add,
    run_workmux_command,
    wait_for_window_ready,
    write_workmux_config,
)
from .support.agent_state import (
    build_status_cmd_with_marker,
    list_agent_state_files,
    start_active_agent,
)


def test_status_no_agents(
    mux_server: MuxEnvironment, workmux_exe_path: Path, mux_repo_path: Path
):
    """Status shows 'No active agents' when no agents are running."""
    result = run_workmux_command(mux_server, workmux_exe_path, mux_repo_path, "status")
    assert "No active agents" in result.stdout


def test_status_json_no_agents(
    mux_server: MuxEnvironment, workmux_exe_path: Path, mux_repo_path: Path
):
    """Status --json returns empty array when no agents are running."""
    result = run_workmux_command(
        mux_server, workmux_exe_path, mux_repo_path, "status --json"
    )
    parsed = json.loads(result.stdout)
    assert parsed == []


def test_status_with_active_agent(
    mux_server: MuxEnvironment, workmux_exe_path: Path, mux_repo_path: Path
):
    """Status shows agent info when an agent is active."""
    env = mux_server
    start_active_agent(
        env,
        workmux_exe_path,
        mux_repo_path,
        "feature-status-active",
        status="working",
    )

    result = run_workmux_command(env, workmux_exe_path, mux_repo_path, "status")
    assert "working" in result.stdout
    assert "WORKTREE" in result.stdout
    assert "STATUS" in result.stdout


def test_status_json_with_active_agent(
    mux_server: MuxEnvironment, workmux_exe_path: Path, mux_repo_path: Path
):
    """Status --json returns agent data when an agent is active."""
    env = mux_server
    agent = start_active_agent(
        env,
        workmux_exe_path,
        mux_repo_path,
        "feature-status-json",
        status="done",
    )

    result = run_workmux_command(env, workmux_exe_path, mux_repo_path, "status --json")
    parsed = json.loads(result.stdout)
    assert isinstance(parsed, list)
    assert len(parsed) >= 1

    entry = parsed[0]
    assert entry["worktree"] == agent.worktree.name
    assert entry["branch"] == agent.branch
    assert entry["status"] == "done"
    assert entry["pane_id"]
    assert entry["workdir"] == str(agent.worktree)
    assert entry["agent_kind"] is None
    assert entry["session"]
    assert entry["window_name"] == agent.window
    assert isinstance(entry["updated_ts"], int)


@pytest.mark.tmux_only
def test_status_json_attributes_multiple_agents(
    mux_server: MuxEnvironment, workmux_exe_path: Path, mux_repo_path: Path
):
    """Status --json identifies each agent sharing a worktree."""
    env = cast(TmuxEnvironment, mux_server)
    branch_name = "feature-status-multi"
    window_name = get_window_name(branch_name)
    write_workmux_config(
        mux_repo_path,
        panes=[
            {"focus": True},
            {"split": "horizontal"},
        ],
    )
    run_workmux_add(env, workmux_exe_path, mux_repo_path, branch_name)
    wait_for_window_ready(env, window_name)

    pane_ids = env.tmux(
        ["list-panes", "-t", window_name, "-F", "#{pane_id}"]
    ).stdout.splitlines()
    assert len(pane_ids) == 2

    expected = {}
    for index, (pane_id, status, agent_kind) in enumerate(
        zip(pane_ids, ["working", "done"], ["claude", None], strict=True)
    ):
        marker = env.tmp_path / f"status-multi-{index}"
        env.send_keys(
            pane_id,
            build_status_cmd_with_marker(env, workmux_exe_path, status, marker),
        )
        assert poll_until(marker.exists, timeout=5.0)
        expected[pane_id] = {"status": status, "agent_kind": agent_kind}

    for pane_id in pane_ids:
        env.send_keys(pane_id, "exec sleep 30")
    assert poll_until(
        lambda: env.tmux(
            ["list-panes", "-t", window_name, "-F", "#{pane_current_command}"]
        ).stdout.splitlines()
        == ["sleep", "sleep"],
        timeout=5.0,
    )

    assert poll_until(lambda: len(list_agent_state_files(env)) == 2, timeout=5.0)
    live_panes = {
        pane_id: (command, int(pid))
        for pane_id, command, pid in (
            line.split("|", 2)
            for line in env.tmux(
                [
                    "list-panes",
                    "-t",
                    window_name,
                    "-F",
                    "#{pane_id}|#{pane_current_command}|#{pane_pid}",
                ]
            ).stdout.splitlines()
        )
    }
    for state_file in list_agent_state_files(env):
        state = json.loads(state_file.read_text())
        pane_id = state["pane_key"]["pane_id"]
        state["command"], state["pane_pid"] = live_panes[pane_id]
        state["workdir"] = str(get_worktree_path(mux_repo_path, branch_name))
        state["agent_kind"] = expected[pane_id]["agent_kind"]
        state_file.write_text(json.dumps(state))

    runner_window = next(name for name in env.list_windows() if name != window_name)
    env.select_window(runner_window)
    result = run_workmux_command(env, workmux_exe_path, mux_repo_path, "status --json")
    entries = [
        entry for entry in json.loads(result.stdout) if entry["branch"] == branch_name
    ]

    assert len(entries) == 2
    assert {
        entry["pane_id"]: {
            "status": entry["status"],
            "agent_kind": entry["agent_kind"],
        }
        for entry in entries
    } == expected
    assert {entry["workdir"] for entry in entries} == {
        str(get_worktree_path(mux_repo_path, branch_name))
    }
    assert {entry["window_name"] for entry in entries} == {window_name}


def test_status_filter_by_worktree(
    mux_server: MuxEnvironment, workmux_exe_path: Path, mux_repo_path: Path
):
    """Status filters to show only the specified worktree."""
    env = mux_server
    branch_name = "feature-status-filt"
    start_active_agent(
        env,
        workmux_exe_path,
        mux_repo_path,
        branch_name,
        status="working",
    )

    result = run_workmux_command(
        env,
        workmux_exe_path,
        mux_repo_path,
        f"status --json {branch_name}",
    )
    parsed = json.loads(result.stdout)
    assert len(parsed) >= 1
    for entry in parsed:
        assert entry["branch"] == branch_name
        assert {
            "workdir",
            "agent_kind",
            "session",
            "window_name",
            "updated_ts",
        } <= entry.keys()


def test_status_filter_no_match(
    mux_server: MuxEnvironment, workmux_exe_path: Path, mux_repo_path: Path
):
    """Status with a filter that matches no agents shows 'No active agents'."""
    env = mux_server
    start_active_agent(
        env,
        workmux_exe_path,
        mux_repo_path,
        "feature-status-exists",
        status="working",
    )

    result = run_workmux_command(
        env,
        workmux_exe_path,
        mux_repo_path,
        "status nonexistent-worktree",
    )
    assert "No active agents" in result.stdout
