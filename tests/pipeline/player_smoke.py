#!/usr/bin/env python3
import json
import os
import select
import subprocess
import time

root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
process = subprocess.Popen(
    [os.path.join(root, "target/native/simulmt-tts-host")],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
)
command = {
    "cmd": "speak",
    "token": 1,
    "path": os.path.join(root, "runtime/cache/say-smoke.wav"),
    "text": "同声传译常驻语音合成测试。",
    "voice": "Tingting",
    "rate": 220,
}
process.stdin.write(json.dumps(command) + "\n")
process.stdin.flush()
events = []
deadline = time.time() + 25
while time.time() < deadline and not any(event.get("type") in ("done", "error") for event in events):
    ready, _, _ = select.select([process.stdout], [], [], 0.5)
    if ready:
        line = process.stdout.readline()
        if line:
            events.append(json.loads(line))
process.kill()
process.wait()
print(json.dumps({"events": events, "stderr": process.stderr.read()}))
if not any(event.get("type") == "done" and event.get("token") == 1 for event in events):
    raise SystemExit(1)
if not os.path.exists(command["path"]) or os.path.getsize(command["path"]) <= 1000:
    raise SystemExit(1)
