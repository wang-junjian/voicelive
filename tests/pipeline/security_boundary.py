#!/usr/bin/env python3
"""Negative security acceptance for the loopback-only EchoNote control plane."""

import base64
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import tempfile
import urllib.error
import urllib.request
from typing import Any

from harness import CoreProcess, OPENER, ensure_core, free_port

ROOT = Path(__file__).resolve().parents[2]
UNSET = object()


def response_body(payload):
    if not payload:
        return None
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return payload.decode(errors="replace")


def request(core, path, method="GET", body=None, token=UNSET, host=None, origin=None) -> tuple[int, Any]:
    headers = {"content-type": "application/json"}
    credential = core.token if token is UNSET else token
    if credential:
        headers["authorization"] = f"Bearer {credential}"
    if host:
        headers["host"] = host
    if origin:
        headers["origin"] = origin
    req = urllib.request.Request(
        core.base + path,
        data=body,
        method=method,
        headers=headers,
    )
    try:
        with OPENER.open(req, timeout=10) as response:
            payload = response.read()
            return response.status, response_body(payload)
    except urllib.error.HTTPError as error:
        payload = error.read()
        return error.code, response_body(payload)


def websocket_handshake(core, sid, origin=None):
    sock = socket.create_connection(("127.0.0.1", core.port), timeout=10)
    headers = [
        f"GET /api/sessions/{sid}/ws HTTP/1.1",
        f"Host: 127.0.0.1:{core.port}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {base64.b64encode(os.urandom(16)).decode()}",
        "Sec-WebSocket-Version: 13",
    ]
    if origin:
        headers.append(f"Origin: {origin}")
    sock.sendall(("\r\n".join(headers) + "\r\n\r\n").encode())
    response = b""
    while b"\r\n\r\n" not in response:
        response += sock.recv(4096)
    return sock, response


def send_masked_text(sock, text):
    payload = text.encode()
    mask = os.urandom(4)
    if len(payload) >= 126:
        raise AssertionError("test frame is unexpectedly large")
    header = bytes([0x81, 0x80 | len(payload)])
    masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    sock.sendall(header + mask + masked)


def assert_non_loopback_rejected(binary, runtime):
    port = free_port()
    rejected_runtime = runtime / "non-loopback"
    rejected_runtime.mkdir()
    env = os.environ.copy()
    env.update(
        SIMULMT_ROOT=str(ROOT),
        SIMULMT_RUNTIME_DIR=str(rejected_runtime),
        SIMULMT_ADDR=f"0.0.0.0:{port}",
    )
    log_path = rejected_runtime / "core.log"
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [str(binary)], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT
        )
        code = process.wait(timeout=10)
    assert code != 0, "non-loopback listener unexpectedly started"
    assert not (rejected_runtime / "bootstrap.json").exists()
    assert "must be loopback" in log_path.read_text()


def main():
    binary = ensure_core(ROOT)
    with tempfile.TemporaryDirectory(prefix="security-boundary-") as temporary:
        runtime = Path(temporary)
        core = CoreProcess(
            ROOT,
            runtime,
            binary,
            environment={
                "SIMULMT_MEDIA_MAX_BYTES": "1024",
                "SIMULMT_MEDIA_DISK_RESERVE_BYTES": "0",
            },
        )
        try:
            core.start()
            bootstrap_path = runtime / "bootstrap.json"
            bootstrap = json.loads(bootstrap_path.read_text())
            assert len(bootstrap["token"]) == 64
            assert bootstrap_path.stat().st_mode & 0o777 == 0o600
            assert bootstrap["token"] not in (runtime / "core.log").read_text()

            assert request(core, "/api/health", token=None)[0] == 200
            assert request(core, "/api/sessions", token=None)[0] == 401
            assert request(core, "/api/sessions", token="wrong")[0] == 401
            assert request(core, "/api/sessions")[0] == 200
            assert request(core, "/api/sessions", host="evil.example")[0] == 403
            assert request(core, "/api/sessions", origin="http://evil.example")[0] == 403
            assert request(core, "/api/sessions", origin=core.base)[0] == 200

            status, config = request(core, "/api/default-config")
            assert status == 200
            assert "executable" not in config["translator"]
            assert "model" not in config["translator"]
            assert "player_path" not in config["output"]
            assert "cache_dir" not in config["output"]
            config["source"] = {"type": "mock", "frames": 2}
            config["translator"] = {
                "type": "hy_simulmt",
                "direction": "en-zh",
                "context": 16384,
                "n_predict": 160,
                "executable": "/tmp/attacker-controlled",
            }
            status, error = request(
                core, "/api/sessions", "POST", json.dumps(config).encode()
            )
            assert status == 422, (status, error)

            config["translator"] = {"type": "mock"}
            config["output"] = {"type": "mock"}
            config["source"] = {"type": "file", "path": "/etc/passwd", "realtime": False}
            status, error = request(
                core, "/api/sessions", "POST", json.dumps(config).encode()
            )
            assert status == 400, (status, error)
            config["source"] = {"type": "mock", "frames": 2}
            status, session = request(
                core, "/api/sessions", "POST", json.dumps(config).encode()
            )
            assert status == 200, session
            sid = session["session_id"]

            sock, response = websocket_handshake(core, sid, "http://evil.example")
            sock.close()
            assert b" 403 " in response.split(b"\r\n", 1)[0], response
            sock, response = websocket_handshake(core, sid, core.base)
            assert b" 101 " in response.split(b"\r\n", 1)[0], response
            send_masked_text(sock, json.dumps({"type": "authenticate", "token": "wrong"}))
            close = sock.recv(4096)
            sock.close()
            assert close and close[0] & 0x0F == 8, close
            assert len(close) >= 4 and struct.unpack("!H", close[2:4])[0] == 1008, close

            status, error = request(
                core,
                "/api/media?name=oversize.wav",
                "POST",
                b"\0" * 1025,
            )
            assert status == 413, (status, error)
            media_dir = runtime / "media"
            assert not media_dir.exists() or not any(media_dir.iterdir())
            assert_non_loopback_rejected(binary, runtime)
            print(
                json.dumps(
                    {
                        "ok": True,
                        "bootstrap_0600": True,
                        "bearer_required": True,
                        "host_origin_allowlist": True,
                        "privileged_config_rejected": True,
                        "websocket_first_message_auth": True,
                        "non_loopback_rejected": True,
                        "streaming_upload_limit": True,
                    }
                )
            )
        finally:
            core.stop()


if __name__ == "__main__":
    main()
