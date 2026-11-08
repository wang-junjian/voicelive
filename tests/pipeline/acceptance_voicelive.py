#!/usr/bin/env python3
"""Isolated HTTP acceptance for VoiceLive; uses mocks and never opens audio devices."""

import json
import math
import os
from pathlib import Path
import socket
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import wave
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
BINARY = ROOT / "target/debug/simulmt-app"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


PORT = free_port()
BASE = f"http://127.0.0.1:{PORT}"
TOKEN = None
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def request(path, method="GET", body=None, raw=None, timeout=20) -> Any:
    data = raw if raw is not None else (None if body is None else json.dumps(body).encode())
    content_type = "application/octet-stream" if raw is not None else "application/json"
    headers = {"content-type": content_type}
    if TOKEN:
        headers["authorization"] = f"Bearer {TOKEN}"
    req = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers=headers,
    )
    try:
        with OPENER.open(req, timeout=timeout) as response:
            payload = response.read()
            if not payload:
                return None
            if response.headers.get_content_type() == "application/json":
                return json.loads(payload)
            return payload
    except urllib.error.HTTPError as error:
        raise AssertionError(f"{method} {path}: {error.code} {error.read().decode()}") from error


class Core:
    def __init__(self, runtime):
        self.runtime = runtime
        self.process = None
        self.log = None

    def start(self):
        global TOKEN
        TOKEN = None
        log_path = self.runtime / "core.log"
        self.log = open(log_path, "ab")
        env = os.environ.copy()
        env.update(
            SIMULMT_ROOT=str(ROOT),
            SIMULMT_RUNTIME_DIR=str(self.runtime),
            SIMULMT_ADDR=f"127.0.0.1:{PORT}",
        )
        self.process = subprocess.Popen(
            [str(BINARY)], cwd=ROOT, env=env, stdout=self.log, stderr=subprocess.STDOUT
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError(f"Core exited early:\n{log_path.read_text()}")
            try:
                health = request("/api/health", timeout=1)
                if health and health["status"] == "ok":
                    TOKEN = json.loads((self.runtime / "bootstrap.json").read_text())["token"]
                    return
            except Exception:
                time.sleep(0.1)
        raise AssertionError(f"Core did not become healthy:\n{log_path.read_text()}")

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.log:
            self.log.close()
        self.process = None
        self.log = None


def wait_for(predicate, message, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    raise AssertionError(message)


def make_wav(path):
    rate = 22050
    with wave.open(str(path), "wb") as output:
        output.setnchannels(1)
        output.setsampwidth(2)
        output.setframerate(rate)
        samples = bytearray()
        for index in range(rate // 4):
            value = int(5000 * math.sin(2 * math.pi * 440 * index / rate))
            samples.extend(struct.pack("<h", value))
        output.writeframes(samples)


def mock_config():
    config = request("/api/default-config")
    config["source"] = {"type": "mock", "frames": 30}
    config["translator"] = {"type": "mock"}
    config["output"] = {"type": "mock"}
    return config


def main():
    assert BINARY.exists(), f"Build {BINARY} first"
    runtime_root = ROOT / "runtime"
    runtime_root.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="acceptance-", dir=runtime_root) as temporary:
        runtime = Path(temporary)
        core = Core(runtime)
        try:
            core.start()
            session = request("/api/sessions", "POST", mock_config())
            sid = session["session_id"]
            session = request(f"/api/sessions/{sid}/title", "PUT", {"title": "验收 · 声译"})
            assert session["title"] == "验收 · 声译"
            session = request(
                f"/api/sessions/{sid}/latency-mode", "PUT", {"latency_mode": "complete"}
            )
            assert session["config"]["latency_mode"] == "complete"
            request(f"/api/sessions/{sid}/prepare", "POST", {})
            wait_for(
                lambda: request(f"/api/sessions/{sid}")["state"] == "ready",
                "mock model did not become ready",
            )
            request(f"/api/sessions/{sid}/start", "POST", {})
            wait_for(
                lambda: request(f"/api/sessions/{sid}/history"),
                "mock pipeline produced no subtitles",
            )
            request(f"/api/sessions/{sid}/stop", "POST", {})
            history = request(f"/api/sessions/{sid}/history")
            assert request(f"/api/sessions/{sid}")["state"] == "ready"
            assert request(f"/api/sessions/{sid}")["started_at_ms"] is not None
            assert request(f"/api/sessions/{sid}/export.srt").find(b" --> ") >= 0
            assert request(f"/api/sessions/{sid}/export.vtt").startswith(b"WEBVTT")
            request(f"/api/sessions/{sid}/stop-model", "POST", {})
            assert request(f"/api/sessions/{sid}")["state"] == "idle"

            settings_config = mock_config()
            settings_config["translator"] = request("/api/default-config")["translator"]
            settings = request("/api/sessions", "POST", settings_config)
            settings_id = settings["session_id"]
            settings = request(
                f"/api/sessions/{settings_id}/direction", "PUT", {"direction": "zh-en"}
            )
            assert settings["config"]["translator"]["direction"] == "zh-en"

            core.stop()
            core.start()
            restored = request(f"/api/sessions/{sid}")
            assert restored["state"] == "idle", restored
            assert restored["title"] == "验收 · 声译"
            assert restored["config"]["latency_mode"] == "complete"
            assert restored["metrics"]["captured_duration_us"] > 0
            restored_settings = request(f"/api/sessions/{settings_id}")
            assert restored_settings["state"] == "idle"
            assert restored_settings["config"]["translator"]["direction"] == "zh-en"
            restored_history = request(f"/api/sessions/{sid}/history")
            assert restored_history == history, {"before": history, "after": restored_history}
            assert any(item["session_id"] == sid for item in request("/api/sessions"))

            request(f"/api/sessions/{sid}", "DELETE")
            request(f"/api/sessions/{settings_id}", "DELETE")
            assert not (runtime / "sessions" / f"{sid}.json").exists()
            assert not (runtime / "subtitles" / f"{sid}.jsonl").exists()
            assert all(item["session_id"] != sid for item in request("/api/sessions"))

            source_wav = runtime / "local.wav"
            make_wav(source_wav)
            imported = request(
                "/api/media?" + urllib.parse.urlencode({"name": "本地样本.wav"}),
                "POST",
                raw=source_wav.read_bytes(),
            )
            decoded = Path(imported["path"])
            assert decoded.exists() and decoded != source_wav
            with wave.open(str(decoded), "rb") as converted:
                assert converted.getframerate() == 16000
                assert converted.getnchannels() == 1
                assert converted.getsampwidth() == 2
                assert converted.getnframes() > 0

            media_config = mock_config()
            media_config["source"] = {
                "type": "file",
                "path": str(decoded),
                "realtime": False,
            }
            media_session = request("/api/sessions", "POST", media_config)
            assert media_session["config"]["source"]["path"] == str(decoded)

            export_config = mock_config()
            export_config["output"] = {
                "type": "macos_system",
                "voice": "Tingting",
                "speech_rate": 220,
            }
            export_session = request("/api/sessions", "POST", export_config)
            export_id = export_session["session_id"]
            core.stop()
            cache = runtime / "cache"
            cache.mkdir(exist_ok=True)
            # Simulate a crash after the native player wrote WAV files but before
            # Core persisted their speech rows. No legacy manifest is present.
            segment = cache / f"speech-{export_id}-1.wav"
            third_segment = cache / f"speech-{export_id}-3.wav"
            segment.write_bytes(decoded.read_bytes())
            third_segment.write_bytes(decoded.read_bytes())
            core.start()
            exported = request(f"/api/sessions/{export_id}/export.wav")
            assert exported.startswith(b"RIFF")
            assert len(exported) > len(decoded.read_bytes())
            request(f"/api/sessions/{export_id}", "DELETE")
            assert not segment.exists()
            assert not third_segment.exists()

            print(
                json.dumps(
                    {
                        "ok": True,
                        "http": True,
                        "restart_restore": True,
                        "subtitle_cues": len(history),
                        "media_decode": True,
                        "exports": ["srt", "vtt", "wav"],
                        "delete_cleanup": True,
                        "real_audio_or_model": False,
                    },
                    ensure_ascii=False,
                )
            )
        finally:
            core.stop()


if __name__ == "__main__":
    main()
