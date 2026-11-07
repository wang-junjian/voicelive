#!/usr/bin/env python3
"""Verify native output recovery without touching any pre-existing Core."""

import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile

from harness import CoreProcess, ensure_core, ensure_executable, terminate_process, wait_for


ROOT = Path(__file__).resolve().parents[2]
PLAYER = ROOT / "target/native/simulmt-tts-host"


def ensure_player():
    return ensure_executable(
        PLAYER,
        [
            "swiftc",
            "-O",
            "adapters/sinks/native-player/tts_host.swift",
            "-o",
            str(PLAYER),
            "-framework",
            "Foundation",
            "-framework",
            "AVFoundation",
        ],
        ROOT,
    )


def direct_player_pids(core, player):
    """Return only exact player executables directly owned by this Core."""
    result = subprocess.check_output(
        ["ps", "-ax", "-o", "pid=,ppid=,pgid=,command="], text=True
    )
    expected_command = str(player)
    pids = []
    for line in result.splitlines():
        parts = line.strip().split(None, 3)
        if len(parts) != 4:
            continue
        pid, ppid, pgid = map(int, parts[:3])
        if (
            ppid == core.pid
            and pgid == core.pgid
            and parts[3] == expected_command
        ):
            pids.append(pid)
    return pids


def kill_direct_player(core, player, pid):
    # Recheck identity immediately before signalling; never kill by process name.
    if pid not in direct_player_pids(core, player):
        raise AssertionError(f"refusing to kill unverified player PID {pid}")
    os.kill(pid, signal.SIGKILL)


def start_decoy(runtime):
    """A same-named executable outside the Core's process group."""
    decoy = runtime / "decoy" / PLAYER.name
    decoy.parent.mkdir()
    return subprocess.Popen(
        [str(decoy), "120"],
        executable="/bin/sleep",
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def main():
    binary = ensure_core(ROOT)
    player = ensure_player()
    with tempfile.TemporaryDirectory(prefix="output-recovery-") as temporary:
        runtime = Path(temporary)
        core = CoreProcess(ROOT, runtime, binary)
        decoy = None
        sid = None
        try:
            core.start()
            decoy = start_decoy(runtime)
            config = core.request("/api/default-config")
            assert config is not None
            config["source"] = {"type": "mock", "frames": None}
            config["translator"] = {"type": "mock"}
            config["output"] = {
                "type": "macos_system",
                "voice": "Tingting",
                "speech_rate": 220,
            }
            session = core.request("/api/sessions", "POST", config)
            assert session is not None
            sid = session["session_id"]
            core.request(f"/api/sessions/{sid}/prepare", "POST", {})

            def is_ready():
                snapshot = core.request(f"/api/sessions/{sid}")
                return snapshot is not None and snapshot["state"] == "ready"

            wait_for(is_ready, "model ready")
            core.request(f"/api/sessions/{sid}/start", "POST", {})

            def has_initial_playback():
                snapshot = core.request(f"/api/sessions/{sid}")
                return (
                    snapshot is not None
                    and snapshot["metrics"]["playback_started"] >= 1
                )

            wait_for(has_initial_playback, "initial playback")
            old_pids = wait_for(
                lambda: direct_player_pids(core, player), "initial player PID"
            )
            assert len(old_pids) == 1, old_pids
            snapshot = core.request(f"/api/sessions/{sid}")
            assert snapshot is not None
            old_started = snapshot["metrics"]["playback_started"]
            for pid in old_pids:
                kill_direct_player(core, player, pid)
            assert decoy.poll() is None, (
                f"same-named decoy was killed: decoy_pid={decoy.pid}, "
                f"returncode={decoy.returncode}, player_pids={old_pids}"
            )

            def recovered():
                current = direct_player_pids(core, player)
                snapshot = core.request(f"/api/sessions/{sid}")
                if (
                    current
                    and snapshot is not None
                    and not set(current).intersection(old_pids)
                    and snapshot["metrics"]["playback_started"] > old_started
                ):
                    return current, snapshot
                return None

            new_pids, snapshot = wait_for(
                recovered, "player recovery and resumed playback"
            )
            assert len(new_pids) == 1, new_pids
            assert snapshot["state"] == "running", snapshot
            assert snapshot["metrics"]["model_restarts"] == 0, snapshot["metrics"]
            assert decoy.poll() is None, "same-named decoy was killed during recovery"

            core.request(f"/api/sessions/{sid}/stop-model", "POST", {})
            sid = None
            core_pid = core.pid
            core.stop()
            assert decoy.poll() is None, "Core cleanup killed the decoy"
            print(
                json.dumps(
                    {
                        "ok": True,
                        "core_pid": core_pid,
                        "old_player_pids": old_pids,
                        "new_player_pids": new_pids,
                        "playback_started": snapshot["metrics"]["playback_started"],
                        "model_restarts": snapshot["metrics"]["model_restarts"],
                        "decoy_survived": True,
                    }
                )
            )
        finally:
            if sid is not None:
                try:
                    core.request(f"/api/sessions/{sid}/stop-model", "POST", {})
                except Exception:
                    pass
            try:
                core.stop()
            finally:
                if decoy is not None:
                    terminate_process(decoy)


if __name__ == "__main__":
    main()
