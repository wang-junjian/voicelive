#!/usr/bin/env python3
"""Small, isolated Core process helpers for pipeline tests."""

import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request


OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for(predicate, description, timeout=25):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    raise AssertionError(f"timed out: {description}")


def ensure_executable(path, build_command, cwd):
    """Build incrementally so a test never accidentally exercises stale sources."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(build_command, cwd=cwd, check=True)
    if not os.access(path, os.X_OK):
        raise AssertionError(f"executable was not built: {path}")
    return path


def ensure_core(root):
    root = Path(root)
    return ensure_executable(
        root / "target/debug/simulmt-app",
        ["cargo", "build", "--package", "simulmt-app", "--locked"],
        root,
    )


def terminate_process(process):
    """Terminate only a process object owned by this test."""
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


class CoreProcess:
    """A Core listening on a random port in its own process group."""

    def __init__(self, root, runtime, binary, environment=None):
        self.root = Path(root)
        self.runtime = Path(runtime)
        self.binary = Path(binary)
        self.environment = environment or {}
        self.port = free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        self.process = None
        self.pgid = None
        self.log = None
        self.token = None

    @property
    def pid(self):
        if self.process is None:
            raise AssertionError("Core is not running")
        return self.process.pid

    def request(self, path, method="GET", body=None, timeout=20):
        data = None if body is None else json.dumps(body).encode()
        headers = {"content-type": "application/json"}
        if self.token:
            headers["authorization"] = f"Bearer {self.token}"
        req = urllib.request.Request(
            self.base + path,
            data=data,
            method=method,
            headers=headers,
        )
        try:
            with OPENER.open(req, timeout=timeout) as response:
                payload = response.read()
                return json.loads(payload) if payload else None
        except urllib.error.HTTPError as error:
            raise AssertionError(
                f"{method} {path}: {error.code} {error.read().decode()}"
            ) from error

    def start(self):
        if self.process is not None:
            raise AssertionError("Core was already started")
        if not os.access(self.binary, os.X_OK):
            raise AssertionError(f"Core executable is missing: {self.binary}")
        self.runtime.mkdir(parents=True, exist_ok=True)
        log_path = self.runtime / "core.log"
        self.log = log_path.open("ab")
        self.token = None
        env = os.environ.copy()
        env.update(
            SIMULMT_ROOT=str(self.root),
            SIMULMT_RUNTIME_DIR=str(self.runtime),
            SIMULMT_ADDR=f"127.0.0.1:{self.port}",
        )
        env.update(self.environment)
        try:
            self.process = subprocess.Popen(
                [str(self.binary)],
                cwd=self.root,
                env=env,
                stdout=self.log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except Exception:
            self.log.close()
            self.log = None
            raise
        self.pgid = self.process.pid
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError(f"Core exited early:\n{log_path.read_text()}")
            try:
                health = self.request("/api/health", timeout=1)
                if health is not None and health["status"] == "ok":
                    bootstrap = json.loads((self.runtime / "bootstrap.json").read_text())
                    self.token = bootstrap["token"]
                    return self
            except Exception:
                time.sleep(0.1)
        raise AssertionError(f"Core did not become healthy; see {log_path}")

    def _signal_group(self, sig):
        if self.pgid is None:
            return
        if self.pgid == os.getpgrp():
            raise AssertionError("refusing to signal the test's own process group")
        try:
            os.killpg(self.pgid, sig)
        except ProcessLookupError:
            pass

    def stop(self):
        process = self.process
        try:
            if process is not None:
                self._signal_group(signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self._signal_group(signal.SIGKILL)
                    process.wait(timeout=5)
        finally:
            if self.log is not None:
                self.log.close()
            self.process = None
            self.pgid = None
            self.log = None
            self.token = None
