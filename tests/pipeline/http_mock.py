#!/usr/bin/env python3
import json
import os
import time
import urllib.request

BASE = os.environ.get("SIMULMT_TEST_URL", "http://127.0.0.1:8780")

def request(path, method="GET", body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(BASE + path, data=data, method=method, headers={"content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as response:
        return json.load(response)

config = request("/api/default-config")
config["source"] = {"type": "mock", "frames": 30}
config["translator"] = {"type": "mock"}
config["output"] = {"type": "mock"}
session = request("/api/sessions", "POST", config)
sid = session["session_id"]
request(f"/api/sessions/{sid}/prepare", "POST", {})
for _ in range(100):
    snapshot = request(f"/api/sessions/{sid}")
    if snapshot["state"] == "ready":
        break
    time.sleep(0.05)
else:
    raise AssertionError("mock translator did not become ready")
request(f"/api/sessions/{sid}/start", "POST", {})
time.sleep(1.1)
request(f"/api/sessions/{sid}/pause", "POST", {})
request(f"/api/sessions/{sid}/resume", "POST", {})
request(f"/api/sessions/{sid}/source", "PUT", {"source": {"type": "mock", "frames": 30}, "mute_original": True})
snapshot = request(f"/api/sessions/{sid}")
assert snapshot["metrics"]["translations"] >= 1, snapshot
assert snapshot["metrics"]["sent_duration_us"] >= 800_000, snapshot
request(f"/api/sessions/{sid}/stop", "POST", {})
snapshot = request(f"/api/sessions/{sid}")
assert snapshot["state"] == "ready", snapshot
assert snapshot["epoch"] == 1, snapshot
history = request(f"/api/sessions/{sid}/history")
assert len(history) >= 1, history
assert history[-1]["is_final"], history[-1]
for extension, marker in [("srt", b" --> "), ("vtt", b"WEBVTT")]:
    with urllib.request.urlopen(f"{BASE}/api/sessions/{sid}/export.{extension}", timeout=20) as response:
        exported = response.read()
        assert marker in exported, exported
request(f"/api/sessions/{sid}/history", "DELETE")
assert request(f"/api/sessions/{sid}/history") == []
sessions = request("/api/sessions")
assert len([item for item in sessions if item["session_id"] == sid]) == 1
assert request(f"/api/sessions/{sid}")["state"] == "ready"
request(f"/api/sessions/{sid}/start", "POST", {})
time.sleep(0.2)
request(f"/api/sessions/{sid}/stop", "POST", {})
assert request(f"/api/sessions/{sid}")["state"] == "ready"
request(f"/api/sessions/{sid}/stop-model", "POST", {})
snapshot = request(f"/api/sessions/{sid}")
assert snapshot["state"] == "idle", snapshot
assert snapshot["epoch"] >= 2, snapshot
print(json.dumps({"ok": True, "translations": snapshot["metrics"]["translations"], "sent_ms": snapshot["metrics"]["sent_duration_us"] / 1000, "history_cues": len(history)}))
