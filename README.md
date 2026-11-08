# 声译 VoiceLive

本地实时语音同传工作台：把系统／应用声音、麦克风或本地音视频变成一条可回读、可导出的双语会话时间线。当前实现 M0～M2 的核心链路，macOS 为首个完整支持平台。

## 产品界面

- **会话时间线**：原文与译文按句排列，真实 TTS 播放事件高亮当前句；不再拆分实时区域和完整记录，也不提供容易误触的清空按钮。
- **会话管理**：左侧新建、切换、重命名和删除会话。新建草稿不会打断当前会议；只有显式准备/启动新会话时才转移模型并停止其他会话的采集。每个音源、翻译方向和字幕时间轴独立保存；刷新页面或服务重启后恢复历史，运行状态回到 `idle`，不会伪造仍在采集或加载模型。
- **三类音源**：系统／应用音频、麦克风、本地音视频。选择新音源会创建独立会话，并停止其他会话的采集，避免多个 Tap 同时接管声音。
- **独立模型控制**：模型启动、停止与同传启停分离；全局只保留一个常驻模型服务。刷新页面可发现已启动的模型，不重复加载。
- **三档语音策略**：底部控制条和系统设置保持同步，运行中也可切换。
  - 跟得紧：短组句、低起播缓冲、积极追平，严重积压时允许跳过最旧语音，字幕仍保留。
  - 自然流畅：连续调度与温和追平，兼顾实时性和听感。
  - 完整优先：充分组句、固定原速、不跳句，允许更高延迟以保留全部译文语音。
- **会话信息与导出**：在会话信息中查看时间、方向和状态，下载双语 SRT、VTT 或译文 WAV，或删除整个会话。
- **持久化**：会话配置、名称、时间、字幕修订和语音清单位于 `runtime/voicelive.db`（SQLite WAL）；WAV 仍保存为文件。旧 `sessions/*.json`、`subtitles/*.jsonl` 与已有语音 WAV 在启动时只读迁移，原文件保留，重复迁移不会重复字幕。当前会话、界面偏好等保存在浏览器本地存储。窗口级页面不滚动，仅会话列表和时间线等局部区域滚动。

诊断面板默认隐藏，可通过 `./run.sh --debug`、URL `?debug=1` 或 `Ctrl + Shift + D` 打开。模拟链路、实时链路、音频账本和会话诊断位于该面板。

## 核心能力

- 版本化 Protobuf 契约与对应的 Rust API 类型。
- `Idle → Preparing → Ready → Running ↔ Paused → Stopping` 生命周期；`Ready` 表示模型常驻但未采集。每次开始/继续都创建新运行代际；暂停和停止都会释放采集与播放资源并恢复原声。
- `sessionId + epoch + sequence` 音频隔离，旧运行代际的翻译和播放结果会被丢弃。
- 100 ms 传输帧对齐为模型要求的 800 ms 块，有界队列、积压统计和丢弃指标。
- 模拟音源、翻译器与播放器；PCM16 WAV 文件音源。
- macOS Core Audio 进程／全局 Tap、AVAudioEngine 麦克风采集、原声静音与独立播放器排除。
- 本地媒体通过流式上传写入受控媒体目录，再由 `afconvert` 解码为 16 kHz 单声道 PCM16 WAV；支持 WAV、MP3、M4A、AAC、FLAC、MP4、MOV、M4V，上传上限 1 GiB。默认最多两个解码任务，单任务限时 120 秒、输出上限 4 GiB，并保留 512 MiB 磁盘余量；超限、超时和解码失败都会清理临时文件。文件音源只能引用通过媒体接口导入的 UUID WAV，不能提交任意本地路径。实际可解码范围仍受 macOS 解码器支持限制。
- 现有 Hy-SimulMT `llama-mtmd-cli --stream-v2` 常驻进程接入。
- 上下文达到 65% 后在空白回合轮换，达到 80% 时强制轮换；仅携带最近 4 轮双语短上下文。正常轮换在进程内完成，不支持进程内轮换时才重启；翻译方向切换会重启当前模型进程并重置语言上下文。
- 轮换与异常恢复会重放已写入但尚未确认的音频，进程内片段 ID 保持连续。
- 双语字幕事件、`AVSpeechSynthesizer` 合成与独立原生播放器；三档策略同时作用于组句和播放队列。
- 本地 Web 控制台和独立 macOS WKWebView 窗口。
- **一致的实时契约**：WebSocket 先发布原子 baseline，再按 `instance_id + sequence` 发布增量事件；客户端检测重复、序号空洞和 Core 重启，Lagged 后完整重同步。REST 错误统一包含稳定错误码、retryable 和 request ID。接口文档由 Rust 类型生成到 `api/openapi.yaml` 并由 CI 校验。
- **本机控制面安全**：Core 只接受数字 loopback 监听地址；每次启动生成 256-bit token 和 instance ID，写入权限为 `0600` 的 `runtime/bootstrap.json`。REST/下载/上传使用 Bearer，WebSocket 校验 Origin 并要求首条消息认证；Host/Origin 采用精确 allowlist，token 不进入 query string 或 Core 日志。模型、播放器、runtime 和缓存路径属于启动期部署配置，普通会话 API 无法指定。

## 构建与运行

```bash
./build.sh
cargo test --workspace
./run.sh
```

正式控制台默认位于 `http://127.0.0.1:8780`。`run.sh` 会把带 `#auth=` 的启动 URL 交给桌面壳；`--no-open` 和 `npm run dev` 会在终端输出该 URL。浏览器将 token 保存到当前标签页的 `sessionStorage` 后立即清除 fragment，刷新继续使用；Core 重启后需使用新的启动 URL。直接启动 Core 的自动化客户端可从其独立 runtime 的 `bootstrap.json` 读取 URL 和 token，不要把该文件或启动 URL 发到不可信环境。其他启动方式：

```bash
./run.sh --no-open
./run.sh --debug
npm run dev -- --host 127.0.0.1 --port 7100
```

## 使用

1. 新建会话，选择系统／应用音频、麦克风或本地音视频；选择新音源会建立新的独立会话。
2. 设置该会话的翻译方向，启动模型并等待就绪。模型加载与音源选择互不影响。
3. 点击“开始同传”。底部可随时切换语音策略。
4. 暂停或停止同传都会释放 Tap、停止译文播放并恢复原声，模型保持常驻；继续时会开启新的运行代际。不再需要模型时停止模型。
5. 打开“会话信息”下载字幕或译文声音，或删除不再需要的整个会话。

首次系统音频采集需要 macOS“屏幕与系统音频录制”权限，首次麦克风采集需要麦克风权限。授权对象应是启动 Core 的终端或宿主应用。真实音频接管只会在用户点击“开始同传”后发生。

## 默认外部依赖

项目默认使用项目内 `vendor/` 目录下的依赖（该目录已加入 `.gitignore`，不提交到仓库）：

- `vendor/llama.cpp-hyv3/build/bin/llama-mtmd-cli`（macOS arm64 二进制，含全部动态库，rpath 已设为 `@loader_path`，可随目录整体移动；需使用支持 Hy-SimulMT 架构的 llama.cpp 构建，见 [ggml-org/llama.cpp PR #31198](https://github.com/ggml-org/llama.cpp/pull/31198)）
- `vendor/models/Hy-SimulMT-30B-A3B-Interleave-Q4_K_M.gguf`（约 18.4 GiB，可从 [Hugging Face tencent/Hy-SimulMT-30B-A3B-Interleave-GGUF](https://huggingface.co/tencent/Hy-SimulMT-30B-A3B-Interleave-GGUF) 下载）
- `vendor/audio/`（测试用音频素材）

按以上布局放置文件后无需任何配置即可运行。也可用环境变量 `SIMULMT_DEMO_ROOT` 指向其他依赖位置；未设置时若 `vendor/models` 不存在，则回退到项目上一级目录。模型日志、会话、媒体和 TTS 缓存位于 `runtime/`（同样不提交）。

## 当前边界

- 完整音源与播放器目前只支持 macOS；Windows、Linux 和浏览器扩展保留适配器边界。
- 当前 TTS 使用 `AVSpeechSynthesizer` 与常驻原生播放器，不是 Qwen／MLX 合成器。
- 正常上下文轮换在进程内完成；运行中翻译方向切换通过重启模型进程实现。
- 进程采集无法区分同一浏览器中的多个标签页。
- 本地媒体按文件方式送入实时链路，不是带画面、可随意拖动的视频播放器。
- 导出音频只包含译文配音，不是嵌入原视频的混流成品；模拟播放器不会生成可导出的语音 WAV。三种导出都固定同一 SQLite 只读事务中的字幕 revision 和语音清单快照；VTT 正确转义文本实体，WAV 以 64 KiB 缓冲流式拼接 PCM16、校验后原子发布。缺失、失败、未完成、截断、格式不一致或导出期间被修改的片段会明确报错，不会静默生成不完整结果。
- HTTP 控制接口仅监听本机并实施启动 token、Host/Origin 校验；远程访问仍不支持，非 loopback 配置会直接拒绝启动。当前桌面壳通过环境变量接收启动 URL，后续签名应用将改用继承 pipe 握手。

## 验证

```bash
cargo test --workspace --locked
python3 tests/pipeline/acceptance_voicelive.py  # 自行启动隔离 Core，使用 mock 链路
python3 tests/pipeline/security_boundary.py   # 认证、来源、路径和非 loopback 负向验收
node tests/pipeline/ws_resync.mjs           # baseline、重连、序号与统一错误契约
npm install
npx playwright install --no-shell chromium
npm run test:browser                    # 独立 Core + Chromium 浏览器验收
python3 tests/pipeline/http_mock.py     # 需先启动本地 Core
python3 tests/pipeline/hy_rotation.py   # 独立端口，使用真实模型并自动清理进程
```

单元测试和 mock 链路不需要加载真实模型，也不会创建音频 Tap。真实系统音源、麦克风权限、媒体听感和长时模型稳定性仍需在目标机器上人工验收。
