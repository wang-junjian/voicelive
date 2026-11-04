#!/usr/bin/env python3
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
BASE = "http://127.0.0.1:8892"
INPUT = ROOT / "runtime/hy-rotation-input.wav"
CORE_LOG = ROOT / "runtime/hy-rotation-core.log"
MODEL_LOG = ROOT / "runtime/model.log"
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def request(path, method="GET", body=None, timeout=30):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        BASE + path,
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


def main():
    pcm_path = WORKSPACE / "live-cache/recv.pcm"
    with pcm_path.open("rb") as source:
        pcm = source.read(16_000 * 2 * 75)
    assert len(pcm) == 16_000 * 2 * 75, "need 75 seconds of PCM audio"
    with wave.open(str(INPUT), "wb") as output:
        output.setnchannels(1)
        output.setsampwidth(2)
        output.setframerate(16_000)
        output.writeframes(pcm)

    model_log_offset = MODEL_LOG.stat().st_size if MODEL_LOG.exists() else 0
    env = os.environ.copy()
    env.update(
        SIMULMT_ROOT=str(ROOT),
        SIMULMT_ADDR="127.0.0.1:8892",
        SIMULMT_UI_URL=BASE,
    )
    with CORE_LOG.open("w") as log:
        core = subprocess.Popen(
            [str(ROOT / "target/debug/simulmt-app")],
            cwd=ROOT,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    session_id = None
    try:
        wait_for(
            lambda: request("/api/health", timeout=1).get("status") == "ok",
            15,
            "core health",
        )
        config = request("/api/default-config")
        config["source"] = {"type": "file", "path": str(INPUT), "realtime": False}
        config["translator"]["context"] = 1_024
        config["translator"]["n_predict"] = 64
        config["output"] = {"type": "mock"}
        config["queue"]["max_frames"] = 256
        config["queue"]["max_buffered_duration_us"] = 25_600_000
        config["queue"]["max_model_backlog_us"] = 120_000_000
        session = request("/api/sessions", "POST", config)
        session_id = session["session_id"]
        request(f"/api/sessions/{session_id}/prepare", "POST", {})
        wait_for(
            lambda: request(f"/api/sessions/{session_id}")["state"] == "ready",
            60,
            "real model ready",
        )
        request(f"/api/sessions/{session_id}/start", "POST", {})
        deadline = time.time() + 240
        snapshot = session
        while time.time() < deadline:
            snapshot = request(f"/api/sessions/{session_id}")
            assert snapshot["state"] != "faulted", snapshot
            with MODEL_LOG.open("rb") as model_log:
                model_log.seek(model_log_offset)
                current_log = model_log.read().decode(errors="replace")
            starts = current_log.count("I main: loading model:")
            metrics = snapshot["metrics"]
            if metrics["context_rotations"] >= 1 and metrics["completed_duration_us"] >= 45_000_000:
                break
            time.sleep(0.5)
        else:
            raise AssertionError(f"real model did not rotate: {snapshot}")

        request(f"/api/sessions/{session_id}/stop-model", "POST", {})
        session_id = None
        history = request(f"/api/sessions/{session['session_id']}/history")
        segment_ids = [segment for cue in history for segment in cue["segment_ids"]]
        assert starts == 1, f"model restarted during in-process rotation: {starts}"
        assert snapshot["metrics"]["model_restarts"] == 0, snapshot["metrics"]
        assert len(segment_ids) == len(set(segment_ids)), "duplicate segment IDs after rotation"
        assert not any(segment.startswith("hy-1-") for segment in segment_ids), segment_ids
        assert "failed to find a memory slot" not in current_log, current_log
        assert "failed to decode audio turn" not in current_log, current_log
        print(
            json.dumps(
                {
                    "ok": True,
                    "model_starts": starts,
                    "context_rotations": snapshot["metrics"]["context_rotations"],
                    "model_restarts": snapshot["metrics"]["model_restarts"],
                    "translations": snapshot["metrics"]["translations"],
                    "completed_seconds": snapshot["metrics"]["completed_duration_us"] / 1_000_000,
                    "dropped_frames": snapshot["metrics"]["dropped_frames"],
                    "history_cues": len(history),
                    "unique_segments": len(segment_ids),
                }
            )
        )
    finally:
        if session_id:
            try:
                request(f"/api/sessions/{session_id}/stop-model", "POST", {})
            except Exception:
                pass
        core.terminate()
        try:
            core.wait(timeout=10)
        except subprocess.TimeoutExpired:
            core.kill()
            core.wait(timeout=5)


if __name__ == "__main__":
    main()
