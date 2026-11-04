const assert = require("node:assert/strict");
const { spawn } = require("node:child_process");
const fs = require("node:fs");
const net = require("node:net");
const path = require("node:path");

const root = path.join(__dirname, "..", "..");
const runtime = fs.mkdtempSync(path.join(root, "runtime", "browser-acceptance-"));
const logPath = path.join(runtime, "core.log");
const log = fs.openSync(logPath, "a");
let core;
let port;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const api = async (route, method = "GET", body) => {
  const response = await fetch(`http://127.0.0.1:${port}${route}`, {
    method,
    headers: { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`${method} ${route}: ${response.status} ${text}`);
  return text ? JSON.parse(text) : null;
};
const waitFor = async (predicate, message, timeout = 12000) => {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const value = await predicate();
    if (value) return value;
    await sleep(100);
  }
  throw new Error(message);
};
const layout = async () => page.evaluate(() => ({
  width: innerWidth,
  height: innerHeight,
  documentWidth: document.documentElement.scrollWidth,
  documentHeight: document.documentElement.scrollHeight,
  bodyWidth: document.body.scrollWidth,
  bodyHeight: document.body.scrollHeight,
  timelineOverflow: getComputedStyle(document.getElementById("timelineScroll")).overflowY,
}));

try {
  port = await new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const value = server.address().port;
      server.close(() => resolve(value));
    });
  });
  core = spawn(path.join(root, "target/debug/simulmt-app"), [], {
    cwd: root,
    env: {
      ...process.env,
      SIMULMT_ROOT: root,
      SIMULMT_RUNTIME_DIR: runtime,
      SIMULMT_ADDR: `127.0.0.1:${port}`,
    },
    stdio: ["ignore", log, log],
  });
  await waitFor(async () => {
    if (core.exitCode !== null) throw new Error(fs.readFileSync(logPath, "utf8"));
    try { return (await api("/api/health")).status === "ok"; } catch { return false; }
  }, "Core did not become healthy");

  const config = await api("/api/default-config");
  config.source = { type: "mock", frames: 30 };
  config.translator = { type: "mock" };
  config.output = { type: "mock" };
  let session = await api("/api/sessions", "POST", config);
  const id = session.session_id;
  session = await api(`/api/sessions/${id}/title`, "PUT", { title: "秋季发布会 · 双语同传" });
  await api(`/api/sessions/${id}/prepare`, "POST", {});
  await waitFor(async () => (await api(`/api/sessions/${id}`)).state === "ready", "mock model not ready");

  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto(`http://127.0.0.1:${port}/`, { waitUntil: "domcontentloaded" });
  await page.waitForFunction(() => document.querySelector("#sessionTitle")?.value === "秋季发布会 · 双语同传");
  assert.equal(await page.locator(".brand-name").textContent(), "声译");
  assert.equal((await page.locator(".strategy-option").allTextContents()).join("/"), "实时/均衡/保真");
  assert.equal(await page.locator("#sessionTotal").textContent(), "01");
  assert.equal(await page.locator("#newSession").textContent(), "新同传");
  assert.equal(await page.getByText("PRIVATE WORKSPACE", { exact: true }).count(), 0);
  assert.match(await page.locator("#headerDuration").textContent(), /^音频 \d{2}:\d{2}/);
  assert.equal(await page.locator(".debug-metrics").count(), 0);
  assert.equal(await page.locator("text=实时区域").count(), 0);
  assert.equal(await page.locator("text=清空").count(), 0);

  await page.locator("#playToggle").click();
  await page.waitForSelector(".segment .source-line");
  await waitFor(async () => (await api(`/api/sessions/${id}`)).metrics.playback_started > 0, "no real mock playback event");
  assert.match(await page.locator(".segment .source-line").first().textContent(), /Mock source/);
  assert.match(await page.locator(".segment .translation-line").first().textContent(), /模拟译文/);
  await page.locator("#playToggle").click();
  await waitFor(async () => (await api(`/api/sessions/${id}`)).state === "paused", "UI did not pause");

  await page.locator("#sessionTitle").fill("产品验收 · 回响");
  await page.locator("#sessionTitle").press("Enter");
  await page.waitForFunction(() => document.querySelector(".session-item-title")?.textContent === "产品验收 · 回响");
  await page.locator('#strategyControl [data-policy="complete"]').click();
  await page.waitForFunction(() => localStorage.getItem("echonote.policy") === "complete");
  await page.locator("#openSettings").click();
  await page.waitForSelector('#panelContent .policy-card.selected[data-policy="complete"]');
  assert.equal(await page.locator("#panelTitle").textContent(), "设置");
  assert.equal(await page.locator("#panelContent").getByText("会话概览").count(), 0);
  await page.locator("#closePanel").click();

  await page.locator("#openInfo").click();
  await page.waitForSelector('[data-export="srt"]');
  assert.equal(await page.locator("#panelTitle").textContent(), "会话信息");
  assert.equal(await page.locator('[data-export]').count(), 3);
  const exportResponseEvent = page.waitForResponse(response => response.url().includes(`/api/sessions/${id}/export.srt`));
  await page.locator('[data-export="srt"]').click();
  const exportResponse = await exportResponseEvent;
  assert.equal(exportResponse.status(), 200);
  assert.match(await exportResponse.text(), /-->/);
  assert.equal(await page.locator("#toastRegion .toast").count(), 0);
  const audioSeconds = Math.floor((await api(`/api/sessions/${id}`)).metrics.captured_duration_us / 1e6);
  const expectedAudioDuration = [Math.floor(audioSeconds / 60), audioSeconds % 60].map(value => String(value).padStart(2, "0")).join(":");
  assert.equal(await page.locator("#panelAudioDuration").textContent(), expectedAudioDuration);
  await page.locator("#closePanel").click();

  await page.reload({ waitUntil: "domcontentloaded" });
  await page.waitForFunction(() => document.querySelector("#sessionTitle")?.value === "产品验收 · 回响");
  await page.waitForSelector(".segment");
  assert.equal(await page.locator("#sessionTotal").textContent(), "01");
  assert.equal(await page.locator('#strategyControl [data-policy="complete"]').getAttribute("aria-pressed"), "true");
  assert.equal(await page.evaluate(() => localStorage.getItem("echonote.session")), id);
  await page.screenshot({ path: path.join(root, "runtime", "echonote-desktop-acceptance.png"), fullPage: true });

  await page.goto(`http://127.0.0.1:${port}/?debug=1`, { waitUntil: "domcontentloaded" });
  await page.waitForSelector("#openSettings");
  await page.locator("#openSettings").click();
  await page.waitForSelector(".debug-metrics");
  await page.goto(`http://127.0.0.1:${port}/`, { waitUntil: "domcontentloaded" });
  await page.waitForSelector("#openSettings");
  await page.locator("#openSettings").click();
  await page.waitForSelector("#panelContent .policy-list");
  assert.equal(await page.locator(".debug-metrics").count(), 0);
  await page.locator("#closePanel").click();

  await page.locator("#newSession").click();
  await page.waitForFunction(() => document.querySelector("#sessionTotal")?.textContent === "02");
  await waitFor(async () => {
    const current = await page.evaluate(() => localStorage.getItem("echonote.session"));
    return current && (await api(`/api/sessions/${current}`)).state === "ready";
  }, "resident model was not moved to the new session");
  const secondId = await page.evaluate(() => localStorage.getItem("echonote.session"));
  assert.equal((await api(`/api/sessions/${id}`)).state, "idle");

  await page.locator("#playToggle").click();
  await waitFor(async () => (await api(`/api/sessions/${secondId}`)).state === "running", "new session did not continue");
  await page.locator("#sourceButton").click();
  await page.waitForSelector('[data-source-choice="system"]');
  assert.equal(await page.locator('[data-source-choice="microphone"]').count(), 1);
  assert.equal(await page.locator('[data-source-choice="media"]').count(), 1);
  assert.equal(await page.getByText("选取正在播放").count(), 0);
  await page.evaluate(() => {
    window.__nativeMediaRequests = 0;
    window.webkit = { messageHandlers: { selectMedia: { postMessage: () => { window.__nativeMediaRequests += 1; } } } };
  });
  await page.locator('[data-source-choice="media"]').click();
  assert.equal(await page.evaluate(() => window.__nativeMediaRequests), 1);
  await page.evaluate(() => { delete window.webkit; });
  await page.locator("#sourceButton").click();
  await page.waitForSelector('[data-source-choice="media"]');
  const wav = Buffer.alloc(44 + 320000);
  wav.write("RIFF", 0);
  wav.writeUInt32LE(wav.length - 8, 4);
  wav.write("WAVEfmt ", 8);
  wav.writeUInt32LE(16, 16);
  wav.writeUInt16LE(1, 20);
  wav.writeUInt16LE(1, 22);
  wav.writeUInt32LE(16000, 24);
  wav.writeUInt32LE(32000, 28);
  wav.writeUInt16LE(2, 32);
  wav.writeUInt16LE(16, 34);
  wav.write("data", 36);
  wav.writeUInt32LE(320000, 40);
  const fileChooserEvent = page.waitForEvent("filechooser");
  await page.locator('[data-source-choice="media"]').click();
  const fileChooser = await fileChooserEvent;
  await fileChooser.setFiles({ name: "本地验收.wav", mimeType: "audio/wav", buffer: wav });
  await page.waitForFunction(() => document.querySelector("#sessionTitle")?.value === "本地验收.wav");
  await page.waitForFunction(() => document.querySelector("#sessionTotal")?.textContent === "03");
  const mediaId = await page.evaluate(() => localStorage.getItem("echonote.session"));
  await waitFor(async () => {
    const snapshot = await api(`/api/sessions/${mediaId}`);
    return snapshot.state === "running" && snapshot.config.source.type === "file";
  }, "media import did not automatically continue interpretation");
  assert.equal((await api(`/api/sessions/${secondId}`)).state, "idle");
  assert.equal(await page.locator("#sourceName").textContent(), "本地验收.wav");
  await page.locator("#playToggle").click();
  await waitFor(async () => (await api(`/api/sessions/${mediaId}`)).state === "paused", "media session did not pause");
  assert.equal(await page.locator("#toastRegion .toast").count(), 0);

  for (const current of [mediaId, secondId]) {
    assert.equal(await page.evaluate(() => localStorage.getItem("echonote.session")), current);
    await page.locator("#openInfo").click();
    await page.locator("#deleteCurrent").click();
    await page.waitForSelector("#deleteModal:not([hidden])");
    await page.locator("#confirmDelete").click();
    await page.waitForFunction(() => document.querySelector("#deleteModal")?.hidden);
    await waitFor(async () => (await page.evaluate(() => localStorage.getItem("echonote.session"))) !== current, "active session did not change after deletion");
  }
  await page.waitForFunction(() => document.querySelector("#sessionTotal")?.textContent === "01");
  assert.equal(await page.locator("#sessionTitle").inputValue(), "产品验收 · 回响");

  const desktop = await layout();
  assert.ok(desktop.documentWidth <= desktop.width, JSON.stringify(desktop));
  assert.ok(desktop.documentHeight <= desktop.height, JSON.stringify(desktop));
  assert.equal(desktop.timelineOverflow, "auto");

  await page.setViewportSize({ width: 1024, height: 768 });
  const tablet = await layout();
  assert.ok(tablet.documentWidth <= tablet.width, JSON.stringify(tablet));
  assert.ok(tablet.documentHeight <= tablet.height, JSON.stringify(tablet));

  await page.setViewportSize({ width: 390, height: 844 });
  const mobile = await layout();
  assert.ok(mobile.documentWidth <= mobile.width, JSON.stringify(mobile));
  assert.ok(mobile.documentHeight <= mobile.height, JSON.stringify(mobile));
  assert.equal(mobile.bodyWidth, mobile.documentWidth);
  await page.reload({ waitUntil: "domcontentloaded" });
  await page.waitForFunction(() => document.querySelector("#sessionTitle")?.value === "产品验收 · 回响");
  await page.waitForSelector(".segment");
  await page.screenshot({ path: path.join(root, "runtime", "echonote-mobile-acceptance.png"), fullPage: true });
  assert.deepEqual(errors, []);

  await api(`/api/sessions/${id}/stop-model`, "POST", {});
  return {
    ok: true,
    ui: true,
    mock_pipeline: true,
    playback_events: true,
    download: "srt",
    refresh_restore: true,
    debug_not_sticky: true,
    create_delete: true,
    model_handoff: true,
    media_file_chooser: true,
    media_auto_resume: true,
    success_toasts_suppressed: true,
    layouts: { desktop, tablet, mobile },
    javascript_errors: errors,
    real_audio_or_model: false,
  };
} finally {
  if (core && core.exitCode === null) {
    core.kill("SIGTERM");
    await Promise.race([
      new Promise(resolve => core.once("exit", resolve)),
      sleep(5000).then(() => { if (core.exitCode === null) core.kill("SIGKILL"); }),
    ]);
  }
  fs.closeSync(log);
  fs.rmSync(runtime, { recursive: true, force: true });
}
