#!/usr/bin/env python3
import json
import math
from pathlib import Path
import select
import struct
import subprocess
import time
import wave

ROOT = Path(__file__).resolve().parents[2]
HOST = ROOT / "target/native/simulmt-tts-host"
CACHE = ROOT / "runtime/cache"
CACHE.mkdir(parents=True, exist_ok=True)
WAV = CACHE / "policy-one-second.wav"
with wave.open(str(WAV), "wb") as output:
    output.setnchannels(1)
    output.setsampwidth(2)
    output.setframerate(24_000)
    frames = (struct.pack("<h", int(80 * math.sin(n * 0.08))) for n in range(24_000))
    output.writeframes(b"".join(frames))


def start_player(mode):
    process = subprocess.Popen(
        [str(HOST)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True,
    )
    process.stdin.write(json.dumps({"cmd": "policy", "latency_mode": mode}) + "\n")
    return process


def play(process, token, source_duration_ms):
    process.stdin.write(json.dumps({
        "cmd": "play", "token": token, "path": str(WAV),
        "source_duration_ms": source_duration_ms,
    }) + "\n")


def read_until(process, events, predicate, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline and not predicate(events):
        ready, _, _ = select.select([process.stdout], [], [], 0.25)
        if ready:
            line = process.stdout.readline()
            if line:
                events.append(json.loads(line))
        if any(event.get("type") == "error" for event in events):
            break
    assert not any(event.get("type") == "error" for event in events), events


def verify_rates(mode, expected):
    process = start_player(mode)
    events = []
    try:
        for token, duration in enumerate((500, 2000, 1000), 1):
            play(process, token, duration)
        process.stdin.flush()
        read_until(process, events, lambda items: len([
            event for event in items if event.get("type") == "scheduled"
        ]) >= 3)
        scheduled = {event["token"]: event for event in events if event.get("type") == "scheduled"}
        assert set(scheduled) == {1, 2, 3}, (mode, events)
        actual = [scheduled[token]["rate"] for token in (1, 2, 3)]
        for value, target in zip(actual, expected):
            assert abs(value - target) < 0.01, (mode, actual, events)
        return actual
    finally:
        process.kill()
        process.wait()


def verify_backlog(mode, expected_depth, expected_cancelled):
    process = start_player(mode)
    events = []
    try:
        for token in range(1, 21):
            play(process, token, 1000)
        process.stdin.flush()
        read_until(process, events, lambda items: any(
            event.get("type") == "queue" and event.get("depth") == expected_depth
            for event in items
        ) and len([event for event in items if event.get("type") == "scheduled"]) >= 2
        and len([event for event in items if event.get("type") == "cancelled"]) >= expected_cancelled)
        cancelled = [event["token"] for event in events if event.get("type") == "cancelled"]
        assert len(cancelled) == expected_cancelled, (mode, events)
        return cancelled
    finally:
        process.kill()
        process.wait()


rates = {
    "low_latency": verify_rates("low_latency", [1.8, 0.75, 1.0]),
    "balanced": verify_rates("balanced", [1.25, 0.85, 1.0]),
    "complete": verify_rates("complete", [1.0, 1.0, 1.0]),
}
cancelled = {
    "low_latency": verify_backlog("low_latency", 8, 12),
    "balanced": verify_backlog("balanced", 15, 5),
    "complete": verify_backlog("complete", 20, 0),
}
print(json.dumps({
    "ok": True,
    "rates": rates,
    "cancelled_count": {mode: len(items) for mode, items in cancelled.items()},
    "fidelity_fixed_rate_and_never_skips": True,
}))
