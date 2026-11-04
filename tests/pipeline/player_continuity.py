#!/usr/bin/env python3
import json
import math
import os
from pathlib import Path
import select
import struct
import subprocess
import time
import wave

ROOT = Path(__file__).resolve().parents[2]
CACHE = ROOT / "runtime/cache"
CACHE.mkdir(parents=True, exist_ok=True)
process = subprocess.Popen(
    [str(ROOT / "target/native/simulmt-tts-host")],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
)
events = []
try:
    for token in (1, 2, 3):
        path = CACHE / f"continuity-{token}.wav"
        with wave.open(str(path), "wb") as output:
            output.setnchannels(1)
            output.setsampwidth(2)
            output.setframerate(24_000)
            frames = (struct.pack("<h", int(80 * math.sin(n * 0.08))) for n in range(24_000))
            output.writeframes(b"".join(frames))
        process.stdin.write(json.dumps({
            "cmd": "play",
            "token": token,
            "path": str(path),
            "source_duration_ms": 1000,
        }) + "\n")
    process.stdin.flush()

    deadline = time.time() + 12
    while time.time() < deadline and not any(event.get("type") == "done" and event.get("token") == 3 for event in events):
        ready, _, _ = select.select([process.stdout], [], [], 0.25)
        if ready:
            line = process.stdout.readline()
            if line:
                events.append(json.loads(line))
        if any(event.get("type") == "error" for event in events):
            break

    scheduled = {event["token"]: event for event in events if event.get("type") == "scheduled"}
    assert set(scheduled) == {1, 2, 3}, events
    for first, second in ((1, 2), (2, 3)):
        gap = scheduled[second]["start_time"] - scheduled[first]["start_time"]
        assert abs(gap - 1.0) < 0.03, (first, second, gap, events)
    assert all(abs(scheduled[token]["rate"] - 1.0) < 0.01 for token in (1, 2, 3)), events
    assert any(event.get("type") == "done" and event.get("token") == 3 for event in events), events

    for token, source_duration in ((4, 500), (5, 2000), (6, 1000)):
        path = CACHE / f"continuity-{token - 3}.wav"
        process.stdin.write(json.dumps({
            "cmd": "play",
            "token": token,
            "path": str(path),
            "source_duration_ms": source_duration,
        }) + "\n")
    process.stdin.flush()
    deadline = time.time() + 12
    while time.time() < deadline and not any(event.get("type") == "done" and event.get("token") == 6 for event in events):
        ready, _, _ = select.select([process.stdout], [], [], 0.25)
        if ready:
            line = process.stdout.readline()
            if line:
                events.append(json.loads(line))
        if any(event.get("type") == "error" for event in events):
            break
    scheduled = {event["token"]: event for event in events if event.get("type") == "scheduled"}
    expected_rates = {4: 1.25, 5: 0.85, 6: 1.0}
    for token, expected in expected_rates.items():
        assert abs(scheduled[token]["rate"] - expected) < 0.01, (token, scheduled[token], events)
    assert abs((scheduled[5]["start_time"] - scheduled[4]["start_time"]) - 1 / 1.25) < 0.03, events
    assert abs((scheduled[6]["start_time"] - scheduled[5]["start_time"]) - 1 / 0.85) < 0.03, events
    assert any(event.get("type") == "done" and event.get("token") == 6 for event in events), events
    print(json.dumps({
        "ok": True,
        "schedule_gaps_ms": [round((scheduled[token + 1]["start_time"] - scheduled[token]["start_time"]) * 1000, 1) for token in (1, 2)],
        "rates": [scheduled[token]["rate"] for token in (1, 2, 3)],
        "adaptive_rates": [scheduled[token]["rate"] for token in (4, 5, 6)],
        "playback_finished": sum(event.get("type") == "done" for event in events),
    }))
finally:
    process.kill()
    process.wait()
