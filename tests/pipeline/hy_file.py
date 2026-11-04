#!/usr/bin/env python3
import json
import os
import time
import urllib.request

BASE = os.environ.get("SIMULMT_TEST_URL", "http://127.0.0.1:8780")
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_VENDOR = os.path.join(_PROJECT_ROOT, "vendor")
DEMO = os.environ.get("SIMULMT_DEMO_ROOT", _VENDOR if os.path.isdir(os.path.join(_VENDOR, "models")) else os.path.dirname(_PROJECT_ROOT))
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

def request(path, method="GET", body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(BASE + path, data=data, method=method, headers={"content-type": "application/json"})
    with OPENER.open(req, timeout=30) as response:
        return json.load(response)

config = request("/api/default-config")
config["source"] = {"type": "file", "path": os.path.join(DEMO, "audio/test_en.wav"), "realtime": True}
if os.environ.get("SIMULMT_TEST_OUTPUT") != "native":
    config["output"] = {"type": "mock"}
session = request("/api/sessions", "POST", config)
sid = session["session_id"]
try:
    request(f"/api/sessions/{sid}/prepare", "POST", {})
    deadline = time.time() + 90
    while time.time() < deadline:
        snapshot = request(f"/api/sessions/{sid}")
        if snapshot["state"] == "ready":
            break
        if snapshot["state"] == "faulted":
            raise AssertionError(snapshot)
        time.sleep(0.25)
    else:
        raise AssertionError("real model did not become ready")
    request(f"/api/sessions/{sid}/start", "POST", {})
    deadline = time.time() + 90
    while time.time() < deadline:
        snapshot = request(f"/api/sessions/{sid}")
        if snapshot["metrics"]["translations"] >= 1 and snapshot["metrics"]["playback_finished"] >= 1:
            break
        time.sleep(0.5)
    else:
        raise AssertionError("no real translation received: " + json.dumps(snapshot, ensure_ascii=False))
    assert snapshot["metrics"]["sent_duration_us"] >= 1_600_000, snapshot
    print(json.dumps({"ok": True, "translations": snapshot["metrics"]["translations"], "sent_ms": snapshot["metrics"]["sent_duration_us"] / 1000, "done_ms": snapshot["metrics"]["completed_duration_us"] / 1000}))
finally:
    request(f"/api/sessions/{sid}/stop-model", "POST", {})
