import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const token = process.env.SIMULMT_TEST_TOKEN || JSON.parse(fs.readFileSync(path.join(root, "runtime", "bootstrap.json"), "utf8")).token;
const base = "http://127.0.0.1:8780";
const request = async (path, method = "GET", body) => {
  const response = await fetch(base + path, {
    method,
    headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!response.ok) throw new Error(await response.text());
  return response.json();
};

const config = await request("/api/default-config");
config.source = { type: "mock", frames: 16 };
config.translator = { type: "mock" };
config.output = { type: "mock" };
const session = await request("/api/sessions", "POST", config);
const sid = session.session_id;
const ws = new WebSocket(`ws://127.0.0.1:8780/api/sessions/${sid}/ws`);
const events = [];
ws.onmessage = (message) => events.push(JSON.parse(message.data));
await new Promise((resolve, reject) => {
  ws.onopen = resolve;
  ws.onerror = reject;
});
try {
  ws.send(JSON.stringify({ type: "authenticate", token }));
  await request(`/api/sessions/${sid}/prepare`, "POST", {});
  await new Promise((resolve) => setTimeout(resolve, 100));
  await request(`/api/sessions/${sid}/start`, "POST", {});
  await new Promise((resolve) => setTimeout(resolve, 1100));
  await request(`/api/sessions/${sid}/stop`, "POST", {});
  await new Promise((resolve) => setTimeout(resolve, 100));
  const translation = events.find((event) => event.kind.type === "translation");
  if (!translation) throw new Error("no translation event received over WebSocket");
  if (translation.kind.data.epoch !== 1) throw new Error("unexpected translation epoch");
  console.log(JSON.stringify({ ok: true, events: events.length, segment: translation.kind.data.segment_id }));
} finally {
  ws.close();
  await request(`/api/sessions/${sid}/stop`, "POST", {});
}
process.exit(0);
