import { spawn, spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const args = process.argv.slice(2);
let host = "127.0.0.1";
let port = "7100";
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--host" && args[i + 1]) host = args[++i];
  else if (args[i].startsWith("--host=")) host = args[i].split("=")[1];
  else if (args[i] === "--port" && args[i + 1]) port = args[++i];
  else if (args[i].startsWith("--port=")) port = args[i].split("=")[1];
}
const built = spawnSync(path.join(root, "build.sh"), { cwd: root, stdio: "inherit" });
if (built.status !== 0) process.exit(built.status ?? 1);
const child = spawn(path.join(root, "target/debug/simulmt-app"), {
  cwd: root,
  stdio: "inherit",
  env: { ...process.env, SIMULMT_ROOT: root, SIMULMT_ADDR: `${host}:${port}` },
});
const stop = () => child.kill("SIGTERM");
process.on("SIGINT", stop);
process.on("SIGTERM", stop);
child.on("exit", (code, signal) => process.exit(code ?? (signal ? 1 : 0)));
