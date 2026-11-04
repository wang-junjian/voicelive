#!/usr/bin/env python3
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
BASE = os.environ.get("SIMULMT_TEST_URL", "http://127.0.0.1:8780")
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def request(path, method="GET", body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(BASE + path, data=data, method=method, headers={"content-type": "application/json"})
    with OPENER.open(req, timeout=20) as response:
        return json.load(response)


def player_pids():
    result = subprocess.check_output(["ps", "-ax", "-o", "pid=,ppid=,command="], text=True)
    pids = []
    for line in result.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) == 3 and parts[2].endswith("simulmt-tts-host"):
            pids.append(int(parts[0]))
    return pids


def wait_for(predicate, description, timeout=25):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.2)
    raise AssertionError(f"timed out: {description}")


config = request("/api/default-config")
config["source"] = {"type": "mock"}
config["translator"] = {"type": "mock"}
session = request("/api/sessions", "POST", config)
sid = session["session_id"]
try:
    request(f"/api/sessions/{sid}/prepare", "POST", {})
    wait_for(lambda: request(f"/api/sessions/{sid}")["state"] == "ready", "model ready")
    request(f"/api/sessions/{sid}/start", "POST", {})
    wait_for(lambda: request(f"/api/sessions/{sid}")["metrics"]["playback_started"] >= 1, "initial playback")
    old_pids = wait_for(lambda: player_pids(), "initial player PID")
    old_started = request(f"/api/sessions/{sid}")["metrics"]["playback_started"]
    for pid in old_pids:
        os.kill(pid, signal.SIGKILL)

    def recovered():
        current = player_pids()
        snapshot = request(f"/api/sessions/{sid}")
        if current and not set(current).intersection(old_pids) and snapshot["metrics"]["playback_started"] > old_started:
            return current, snapshot
        return None

    new_pids, snapshot = wait_for(recovered, "player recovery and resumed playback")
    assert snapshot["state"] == "running", snapshot
    assert snapshot["metrics"]["model_restarts"] == 0, snapshot["metrics"]
    print(json.dumps({"ok": True, "old_player_pids": old_pids, "new_player_pids": new_pids, "playback_started": snapshot["metrics"]["playback_started"], "model_restarts": snapshot["metrics"]["model_restarts"]}))
finally:
    request(f"/api/sessions/{sid}/stop-model", "POST", {})
