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
BASE = "http://127.0.0.1:8894"
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
TOKEN = None


def request(path, method="GET", body=None, timeout=10):
    data = None if body is None else json.dumps(body).encode()
    headers = {"content-type": "application/json"}
    if TOKEN:
        headers["authorization"] = f"Bearer {TOKEN}"
    req = urllib.request.Request(BASE + path, data=data, method=method, headers=headers)
    with OPENER.open(req, timeout=timeout) as response:
        return json.load(response)


def main():
    global TOKEN
    runtime = ROOT / f"runtime/export-stream-{int(time.time())}"
    cache = runtime / "cache"
    cache.mkdir(parents=True)
    input_path = cache / "input.wav"
    with wave.open(str(input_path), "wb") as output:
        output.setnchannels(1)
        output.setsampwidth(2)
        output.setframerate(24_000)
        output.writeframes(b"\0\0" * 24_000)

    env = os.environ.copy()
    env.update(
        SIMULMT_ROOT=str(ROOT),
        SIMULMT_ADDR="127.0.0.1:8894",
        SIMULMT_UI_URL=BASE,
        SIMULMT_RUNTIME_DIR=str(runtime),
    )
    with (runtime / "core.log").open("w") as log:
        core = subprocess.Popen(
            [str(ROOT / "target/debug/simulmt-app")],
            cwd=ROOT,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            for _ in range(60):
                try:
                    if request("/api/health", timeout=1)["status"] == "ok":
                        break
                except (OSError, urllib.error.URLError):
                    time.sleep(0.25)
            else:
                raise AssertionError("core did not start")
            TOKEN = json.loads((runtime / "bootstrap.json").read_text())["token"]
            session = request("/api/sessions", "POST", request("/api/default-config"))
            sid = session["session_id"]
            spool = cache / f"speech-{sid}-1.wav"
            spool.write_bytes(input_path.read_bytes())
            export_request = urllib.request.Request(
                f"{BASE}/api/sessions/{sid}/export.wav",
                headers={"authorization": f"Bearer {TOKEN}"},
            )
            with OPENER.open(export_request, timeout=10) as response:
                exported = response.read()
                content_length = int(response.headers["Content-Length"])
            assert content_length == len(exported), (content_length, len(exported))
            with wave.open(str(spool), "rb") as source:
                source_frames = source.readframes(source.getnframes())
            export_path = cache / f"simulmt-translation-{sid}.wav"
            with wave.open(str(export_path), "rb") as exported_wav:
                exported_frames = exported_wav.readframes(exported_wav.getnframes())
            assert exported.startswith(b"RIFF")
            assert exported_frames == source_frames
            print(json.dumps({"ok": True, "content_length": content_length, "wav_bytes": len(exported)}))
        finally:
            core.terminate()
            try:
                core.wait(timeout=10)
            except subprocess.TimeoutExpired:
                core.kill()
                core.wait(timeout=5)


if __name__ == "__main__":
    main()
