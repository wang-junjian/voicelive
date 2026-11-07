import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const runtime = fs.mkdtempSync(path.join(os.tmpdir(), "echonote-ws-contract-"));
const binary = path.join(root, "target", "debug", "simulmt-app");
const logPath = path.join(runtime, "core.log");
let core = null;
let log = null;
let port = 0;
let token = null;
const streams = new Set();

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const waitFor = async (predicate, description, timeout = 12000) => {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const value = predicate();
    if (value) return value;
    await sleep(50);
  }
  throw new Error(`timed out: ${description}`);
};
const api = async (route, method = "GET", body, raw = false) => {
  const response = await fetch(`http://127.0.0.1:${port}${route}`, {
    method,
    headers: {
      ...(body === undefined ? {} : { "content-type": raw ? "application/json" : "application/json" }),
      ...(token ? { authorization: `Bearer ${token}` } : {}),
    },
    body: body === undefined ? undefined : raw ? body : JSON.stringify(body),
  });
  const text = await response.text();
  let payload = null;
  if (text) {
    try { payload = JSON.parse(text); } catch { payload = text; }
  }
  return { response, payload };
};
const request = async (...args) => {
  const { response, payload } = await api(...args);
  if (!response.ok) throw new Error(`${response.status} ${JSON.stringify(payload)}`);
  return payload;
};

async function waitReady() {
  const deadline = Date.now() + 12000;
  while (Date.now() < deadline) {
    if (core.exitCode !== null) throw new Error(fs.readFileSync(logPath, "utf8"));
    try {
      const result = await api("/api/health");
      if (result.response.ok) {
        token = JSON.parse(fs.readFileSync(path.join(runtime, "bootstrap.json"), "utf8")).token;
        return result.payload;
      }
    } catch {}
    await sleep(50);
  }
  throw new Error("Core did not become healthy");
}

async function launchCore() {
  log = fs.openSync(logPath, "a");
  core = spawn(binary, [], {
    cwd: root,
    env: { ...process.env, SIMULMT_ROOT: root, SIMULMT_RUNTIME_DIR: runtime, SIMULMT_ADDR: `127.0.0.1:${port}` },
    stdio: ["ignore", log, log],
  });
  return waitReady();
}

async function stopCore() {
  for (const stream of streams) stream.ws.close();
  streams.clear();
  if (core && core.exitCode === null) {
    const exited = new Promise(resolve => core.once("exit", resolve));
    core.kill("SIGTERM");
    await Promise.race([exited, sleep(5000)]);
    if (core.exitCode === null) core.kill("SIGKILL");
  }
  if (log !== null) fs.closeSync(log);
  log = null;
  core = null;
  token = null;
}

async function openStream(id) {
  const ws = new WebSocket(`ws://127.0.0.1:${port}/api/sessions/${id}/ws`);
  const stream = { ws, packets: [], baselines: [], events: [], lastSequence: null, instanceId: null };
  ws.onmessage = message => {
    const packet = JSON.parse(message.data);
    stream.packets.push(packet);
    if (packet.type === "baseline") {
      stream.baselines.push(packet.data);
      stream.instanceId = packet.data.instance_id;
      stream.lastSequence = packet.data.sequence;
    } else if (packet.type === "event") {
      assert.ok(stream.instanceId, "event arrived before baseline");
      assert.equal(packet.data.instance_id, stream.instanceId);
      assert.equal(packet.data.sequence, stream.lastSequence + 1, "event sequence must be contiguous");
      stream.lastSequence = packet.data.sequence;
      stream.events.push(packet.data);
    }
  };
  streams.add(stream);
  await new Promise((resolve, reject) => { ws.onopen = resolve; ws.onerror = reject; });
  ws.send(JSON.stringify({ type: "authenticate", token }));
  await waitFor(() => stream.baselines.length > 0, "initial baseline");
  return stream;
}

async function main() {
  const build = spawnSync("cargo", ["+stable", "build", "--package", "simulmt-app", "--locked"], { cwd: root, stdio: "inherit" });
  if (build.status !== 0) throw new Error("Core build failed");
  port = await new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const value = server.address().port;
      server.close(() => resolve(value));
    });
  });
  const firstHealth = await launchCore();
  const config = await request("/api/default-config");
  config.source = { type: "mock", frames: 12 };
  config.translator = { type: "mock" };
  config.output = { type: "mock" };
  const session = await request("/api/sessions", "POST", config);
  const id = session.session_id;
  const first = await openStream(id);
  const baseline = first.baselines[0];
  assert.equal(baseline.instance_id, firstHealth.instance_id);
  assert.equal(baseline.snapshot.event_sequence, baseline.sequence);
  assert.deepEqual(baseline.history, []);
  await request(`/api/sessions/${id}/prepare`, "POST", {});
  await waitFor(() => first.events.some(event => event.kind.type === "state" && event.kind.data.state === "ready"), "ready event");
  await request(`/api/sessions/${id}/start`, "POST", {});
  await waitFor(() => first.events.some(event => event.kind.type === "subtitle"), "subtitle event");
  await request(`/api/sessions/${id}/pause`, "POST", {});
  await request(`/api/sessions/${id}/stop-model`, "POST", {});
  await waitFor(() => first.events.some(event => event.kind.type === "state" && event.kind.data.state === "idle"), "idle event");
  const history = await request(`/api/sessions/${id}/history`);
  const cursor = first.lastSequence;

  const second = await openStream(id);
  assert.equal(second.baselines[0].instance_id, baseline.instance_id);
  assert.equal(second.baselines[0].sequence, cursor);
  assert.deepEqual(second.baselines[0].history, history);
  assert.equal(second.events.length, 0, "reconnect must not duplicate old events");

  await stopCore();
  const secondHealth = await launchCore();
  assert.notEqual(secondHealth.instance_id, firstHealth.instance_id);
  const third = await openStream(id);
  assert.equal(third.baselines[0].instance_id, secondHealth.instance_id);
  assert.equal(third.baselines[0].sequence, 0, "a new Core instance starts a new cursor domain");
  assert.deepEqual(third.baselines[0].history, history);

  const conflict = await api(`/api/sessions/${id}/pause`, "POST", {});
  assert.equal(conflict.response.status, 409);
  assert.equal(conflict.payload.code, "invalid_state");
  assert.equal(conflict.payload.retryable, false);
  assert.equal(conflict.payload.request_id, conflict.response.headers.get("x-request-id"));
  const malformed = await api("/api/sessions", "POST", "{", true);
  assert.equal(malformed.response.status, 400);
  assert.equal(malformed.payload.code, "invalid_request");
  assert.equal(malformed.payload.request_id, malformed.response.headers.get("x-request-id"));
  const unknownField = structuredClone(config);
  unknownField.executable = "/tmp/not-allowed";
  const rejected = await api("/api/sessions", "POST", unknownField);
  assert.equal(rejected.response.status, 422);
  assert.equal(rejected.payload.code, "invalid_json");
  assert.equal(rejected.payload.request_id, rejected.response.headers.get("x-request-id"));
  const missing = await api(`/api/sessions/${crypto.randomUUID()}`);
  assert.equal(missing.response.status, 404);
  assert.equal(missing.payload.code, "session_not_found");
  assert.equal(missing.payload.request_id, missing.response.headers.get("x-request-id"));
  console.log(JSON.stringify({ ok: true, baseline: true, contiguous_sequence: true, reconnect: true, instance_change: true, request_id: true, error_statuses: [400, 404, 409, 422] }));
}

try {
  await main();
} finally {
  await stopCore();
  fs.rmSync(runtime, { recursive: true, force: true });
}
