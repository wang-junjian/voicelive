#!/usr/bin/env python3
"""Real-model soak test. Use --seconds 1800 or 3600 for M2 acceptance."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request
import wave

ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ROOT.parent
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def request(base, path, method="GET", body=None, timeout=30):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        base + path,
        data=data,
        method=method,
        headers={"content-type": "application/json"},
    )
    with OPENER.open(req, timeout=timeout) as response:
        return json.load(response)


def wait_for(predicate, timeout, description):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.25)
    raise AssertionError(f"timed out: {description}")


def model_pids(core_pid):
    output = subprocess.check_output(["ps", "-ax", "-o", "pid=,ppid=,command="], text=True)
    pids = []
    for line in output.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) == 3 and int(parts[1]) == core_pid and "llama-mtmd-cli" in parts[2]:
            pids.append(int(parts[0]))
    return pids


def rss_kb(pid):
    output = subprocess.check_output(["ps", "-p", str(pid), "-o", "rss="], text=True)
    return int(output.strip()) * 1024


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=1800)
    parser.add_argument("--port", type=int, default=8893)
    parser.add_argument("--context", type=int, default=1024)
    args = parser.parse_args()
    assert args.seconds >= 75, "at least 75 seconds of source audio is required"
    base = f"http://127.0.0.1:{args.port}"
    input_path = ROOT / f"runtime/long-session-{args.seconds}.wav"
    core_log_path = ROOT / "runtime/long-session-core.log"
    report_path = ROOT / f"runtime/long-session-{args.seconds}-report.json"
    runtime_dir = ROOT / f"runtime/long-session-{args.seconds}-{int(time.time())}"

    pcm_path = WORKSPACE / "live-cache/recv.pcm"
    with pcm_path.open("rb") as source:
        pcm = source.read(16_000 * 2 * 75)
    assert len(pcm) == 16_000 * 2 * 75, "need 75 seconds of PCM audio"
    with wave.open(str(input_path), "wb") as output:
        output.setnchannels(1)
        output.setsampwidth(2)
        output.setframerate(16_000)
        for offset in range(0, args.seconds, 75):
            duration = min(75, args.seconds - offset)
            output.writeframes(pcm[: duration * 16_000 * 2])

    env = os.environ.copy()
    env.update(
        SIMULMT_ROOT=str(ROOT),
        SIMULMT_ADDR=f"127.0.0.1:{args.port}",
        SIMULMT_UI_URL=base,
        SIMULMT_RUNTIME_DIR=str(runtime_dir),
    )
    core = None
    session_id = None
    try:
        with core_log_path.open("w") as log:
            core = subprocess.Popen(
                [str(ROOT / "target/debug/simulmt-app")],
                cwd=ROOT,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            wait_for(lambda: request(base, "/api/health", timeout=1).get("status") == "ok", 15, "core health")
            config = request(base, "/api/default-config")
            config["source"] = {"type": "file", "path": str(input_path), "realtime": True}
            config["translator"]["context"] = args.context
            config["translator"]["n_predict"] = 64
            config["output"] = {"type": "mock"}
            config["queue"]["max_frames"] = 256
            config["queue"]["max_buffered_duration_us"] = 25_600_000
            config["queue"]["max_model_backlog_us"] = 120_000_000
            session = request(base, "/api/sessions", "POST", config)
            session_id = session["session_id"]
            request(base, f"/api/sessions/{session_id}/prepare", "POST", {})
            wait_for(lambda: request(base, f"/api/sessions/{session_id}")["state"] == "ready", 90, "model ready")
            initial_pids = wait_for(lambda: model_pids(core.pid), 5, "model process")
            assert len(initial_pids) == 1, initial_pids
            model_pid = initial_pids[0]
            initial_rss = rss_kb(model_pid)

            started = time.monotonic()
            request(base, f"/api/sessions/{session_id}/start", "POST", {})
            snapshot = session
            backlog_samples = []
            seen_pids = {model_pid}
            deadline = started + args.seconds + 90
            target_us = args.seconds * 1_000_000
            while time.monotonic() < deadline:
                snapshot = request(base, f"/api/sessions/{session_id}")
                assert snapshot["state"] != "faulted", snapshot
                metrics = snapshot["metrics"]
                assert metrics["model_restarts"] == 0, snapshot
                current_pids = model_pids(core.pid)
                assert current_pids == [model_pid], (model_pid, current_pids)
                seen_pids.update(current_pids)
                backlog_samples.append((time.monotonic() - started, metrics["model_backlog_us"]))
                if metrics["sent_duration_us"] >= target_us and metrics["completed_duration_us"] >= target_us:
                    break
                time.sleep(1)
            else:
                raise AssertionError(f"long session did not drain: {snapshot}")

            final_rss = rss_kb(model_pid)
            request(base, f"/api/sessions/{session_id}/stop-model", "POST", {})
            session_id = None
            history = request(base, f"/api/sessions/{session['session_id']}/history")
            segment_ids = [segment for cue in history for segment in cue["segment_ids"]]
            assert len(segment_ids) == len(set(segment_ids)), "duplicate segment IDs"
            starts = [cue["source_start_ns"] for cue in history]
            ends = [cue["source_end_ns"] for cue in history]
            assert starts == sorted(starts), "subtitle start timestamps are not monotonic"
            assert ends == sorted(ends), "subtitle end timestamps are not monotonic"
            assert all(end >= start for start, end in zip(starts, ends)), "negative subtitle duration"
            metrics = snapshot["metrics"]
            assert metrics["context_rotations"] >= 1, snapshot
            assert metrics["dropped_frames"] == 0, snapshot
            assert metrics["model_backlog_us"] <= 1_600_000, snapshot
            recent = [value for elapsed, value in backlog_samples if elapsed >= args.seconds * 0.75]
            assert recent and recent[-1] <= max(1_600_000, recent[0]), "backlog keeps growing"

            log_text = ""
            for path in (runtime_dir / "model.log", runtime_dir / "model.log.1"):
                if path.exists():
                    log_text += path.read_text(errors="replace")
            for marker in ("failed to find a memory slot", "failed to decode audio turn", "model context is above"):
                assert marker not in log_text, marker
            report = {
                "ok": True,
                "requested_seconds": args.seconds,
                "wall_seconds": round(time.monotonic() - started, 1),
                "captured_seconds": metrics["captured_duration_us"] / 1_000_000,
                "completed_seconds": metrics["completed_duration_us"] / 1_000_000,
                "context_rotations": metrics["context_rotations"],
                "model_restarts": metrics["model_restarts"],
                "model_pid": model_pid,
                "unique_model_pids": sorted(seen_pids),
                "model_initial_rss_bytes": initial_rss,
                "model_final_rss_bytes": final_rss,
                "model_rss_delta_bytes": final_rss - initial_rss,
                "translations": metrics["translations"],
                "history_cues": len(history),
                "unique_segments": len(segment_ids),
                "dropped_frames": metrics["dropped_frames"],
                "final_model_backlog_us": metrics["model_backlog_us"],
                "max_model_backlog_us": max(value for _, value in backlog_samples),
            }
            report_path.write_text(json.dumps(report, indent=2) + "\n")
            print(json.dumps(report))
    finally:
        if session_id:
            try:
                request(base, f"/api/sessions/{session_id}/stop-model", "POST", {})
            except Exception:
                pass
        if core is not None:
            core.terminate()
            try:
                core.wait(timeout=10)
            except subprocess.TimeoutExpired:
                core.kill()
                core.wait(timeout=5)


if __name__ == "__main__":
    main()
