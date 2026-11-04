#!/usr/bin/env python3
import json
import os
import time
import urllib.request

BASE = os.environ.get("SIMULMT_TEST_URL", "http://127.0.0.1:8780")
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

def request(path, method="GET", body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(BASE + path, data=data, method=method, headers={"content-type": "application/json"})
    with OPENER.open(req, timeout=20) as response:
        return json.load(response)

config = request("/api/default-config")
config["source"] = {"type": "mock", "frames": 8}
config["translator"] = {"type": "mock"}
session = request("/api/sessions", "POST", config)
sid = session["session_id"]
try:
    request(f"/api/sessions/{sid}/prepare", "POST", {})
    for _ in range(100):
        if request(f"/api/sessions/{sid}")["state"] == "ready":
            break
        time.sleep(0.05)
    request(f"/api/sessions/{sid}/start", "POST", {})
    deadline = time.time() + 20
    while time.time() < deadline:
        snapshot = request(f"/api/sessions/{sid}")
        if snapshot["metrics"]["playback_finished"] >= 1:
            with OPENER.open(f"{BASE}/api/sessions/{sid}/export.wav", timeout=20) as response:
                audio = response.read()
                assert audio.startswith(b"RIFF") and len(audio) > 1000, len(audio)
            print(json.dumps({"ok": True, "playback_started": snapshot["metrics"]["playback_started"], "playback_finished": snapshot["metrics"]["playback_finished"], "wav_bytes": len(audio)}))
            break
        time.sleep(0.2)
    else:
        raise AssertionError("native TTS did not finish: " + json.dumps(snapshot))
finally:
    request(f"/api/sessions/{sid}/stop-model", "POST", {})
