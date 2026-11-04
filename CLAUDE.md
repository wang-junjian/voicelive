# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概览

回响 EchoNote 是本地实时语音同传工作台：Rust Core 负责会话、音频管线、模型与播放编排，Swift 原生进程负责 macOS 音频采集、TTS/播放和 WKWebView 桌面壳。完整音源与播放器目前以 macOS 为目标平台；默认真实模型是本地 Hy-SimulMT GGUF。

## 常用命令

```bash
# 完整构建：Rust 工作区 + 音频宿主 + TTS 宿主 + 桌面壳（需要 macOS/swiftc）
./build.sh

# 仅构建 Rust 工作区
cargo build --workspace

# 启动桌面应用；Core 默认监听 http://127.0.0.1:8780
./run.sh
./run.sh --no-open       # 仅启动 Core，用浏览器访问控制台
./run.sh --debug         # 打开诊断面板
./run.sh --port=8781 --no-open

# 开发模式：先完整构建，再启动 Core；不启动桌面壳
npm run dev -- --host 127.0.0.1 --port 7100

# 格式与静态检查
cargo fmt --all -- --check
cargo clippy --workspace --all-targets

# 全部 Rust 测试、单个 crate、单个测试
cargo test --workspace
cargo test -p simulmt-policy
cargo test -p simulmt-core mock_session_covers_complete_lifecycle -- --nocapture
```

前端是 [web/index.html](web/index.html) 中的原生 HTML/CSS/JavaScript，没有 npm 依赖或独立前端构建步骤；`npm run dev` 实际调用的是完整构建和 Core 启动脚本。

### 管道与验收测试

```bash
# 先构建；脚本自行在随机端口启动 Core，使用临时 runtime 和 mock 链路
python3 tests/pipeline/acceptance_echonote.py

# 以下两个脚本连接已经运行的 Core（默认 8780）
python3 tests/pipeline/http_mock.py
node tests/pipeline/ws_mock.mjs

# http_mock.py 可指向其他端口
SIMULMT_TEST_URL=http://127.0.0.1:8781 python3 tests/pipeline/http_mock.py

# 真实模型测试：需要下述 vendor 模型与可执行文件，脚本使用独立端口并清理进程
python3 tests/pipeline/hy_rotation.py
```

Rust 单元测试和 mock 管道不加载真实模型，也不创建系统音频 Tap。`tests/pipeline/` 中其余脚本覆盖真实模型长会话、原生 TTS/播放器、输出恢复或浏览器验收，运行前应检查脚本内的端口、外部依赖和前置条件；它们不属于 `cargo test --workspace`。

## 总体架构

依赖方向为 `simulmt-app` → `simulmt-core` → `simulmt-policy` / `simulmt-api`。Core 通过 trait 和子进程隔离具体音源、翻译器及播放器，而不是把 macOS 或模型细节放进 HTTP 层。

### 1. 契约与策略

- [crates/api/src/lib.rs](crates/api/src/lib.rs) 定义会话状态、配置、音频帧、翻译/字幕/播放事件、指标和快照等共享 serde 类型，版本为 `simulmt.api.v1`。[api/README.md](api/README.md) 还规定外部 Worker 的 Proto 契约与兼容约束。
- 核心隔离键是 `session_id + epoch + sequence`；旧 epoch 的帧和结果必须丢弃。音频归一化为 16 kHz、单声道、PCM S16LE；时间戳使用单调时钟纳秒，时长使用微秒。
- [crates/policy/src/lib.rs](crates/policy/src/lib.rs) 的 `AudioChunker` 将通常 100 ms 的传输帧拼成模型需要的 800 ms 块，并包含有界队列策略。
- [crates/policy/src/subtitle.rs](crates/policy/src/subtitle.rs) 负责字幕组句；[crates/core/src/output.rs](crates/core/src/output.rs) 的 `SpeechAssembler` 独立负责 TTS 组句。两者阈值和策略不同，不要让字幕边界隐式决定播放边界。
- [configs/](configs/) 下 YAML 是策略参考预设，当前代码不会加载它们。实际默认值来自 `/api/default-config` 和 API 类型默认值，运行中策略通过会话接口更新。

### 2. 会话与数据面

[crates/core/src/session.rs](crates/core/src/session.rs) 是编排中心：`SessionManager` 管理会话和全局模型互斥，`Session` 持有生命周期状态、运行资源、事件广播、指标、字幕和持久化。

```text
SourceConfig → RunningSource → AudioFrame（100 ms）
             → Session.forward_source → AudioChunker（800 ms）
             → RunningTranslator → TranslationEvent
                                  ├→ SubtitleAssembler → 历史/导出/WebSocket
                                  └→ SpeechAssembler → TTS/播放器 → 播放事件/指标/WebSocket
```

- 模型启动与采集启动分离：`prepare` 进入 `Ready`，`start` 才创建播放器和音源；`stop` 只停止采集/播放并保留模型，`stop_model` 才终止模型、递增 epoch 并回到 `Idle`。
- 全局只有一个常驻模型。`SessionManager::prepare` 会将其他会话的 `Ready` 模型 detach/adopt 到新会话；`RunningTranslator::reattach` 重定向事件并清理音频和语言上下文，避免重复加载权重。新建会话也会停止其他会话的活动采集。
- 字幕 JSONL 采用追加记录、读取时按 `cue_id` 折叠修订。内存历史最多 512 条，但磁盘历史完整保留；会话清单原子写入。重启只恢复配置、元数据和字幕，所有运行状态恢复为 `Idle`，不会恢复进程或 Tap。
- 默认持久化位置为 `runtime/sessions/`、`runtime/subtitles/`，媒体、模型日志和 TTS 缓存也在 `runtime/` 下。

### 3. 适配器与原生进程协议

- [crates/core/src/source.rs](crates/core/src/source.rs) 提供 mock、16 kHz 单声道 PCM16 WAV 文件和 macOS 音源。macOS 音源启动 [adapters/audio/macos/audio_host.swift](adapters/audio/macos/audio_host.swift)，从其 stdout 读取连续的 3200 字节 PCM 帧；宿主异常退出会重连，并用 `discontinuity` 标记时间线不连续。媒体上传由 HTTP 层调用 `afconvert` 转成上述 WAV 格式。
- [crates/core/src/translator.rs](crates/core/src/translator.rs) 提供 mock 和 Hy-SimulMT 翻译器。真实翻译器常驻 `llama-mtmd-cli --stream-v2`，stdin 使用带 `SMT2` 头的二进制帧，stdout 使用逐行 JSON。它跟踪已写入但未确认的音频，在进程恢复时重放；音源重置使用独立的 `source_generation`，模型进程内轮换失败时才回退为重启。
- [crates/core/src/output.rs](crates/core/src/output.rs) 启动 [adapters/sinks/native-player/tts_host.swift](adapters/sinks/native-player/tts_host.swift)，通过 stdin/stdout 的逐行 JSON 发送合成、播放、暂停、flush 和策略命令，并接收真实播放事件。TTS 使用 `AVSpeechSynthesizer` 生成 24 kHz PCM16 WAV，播放器 PID 会被排除在采集之外，避免译文回采。
- 修改任一进程协议时，必须同步修改 Rust 端与对应 Swift/模型端；音频 stdout 只能写 PCM，日志走 stderr。

### 4. HTTP、前端与桌面壳

[apps/core/src/main.rs](apps/core/src/main.rs) 是 Axum 应用入口：集中定义 REST 路由、每会话 WebSocket 事件流、媒体导入、SRT/VTT/WAV 导出、静态前端和运行路径配置。新增会话能力通常需要同时检查共享 API 类型、Core 方法、HTTP 路由和前端事件处理。

[web/index.html](web/index.html) 保存多会话快照、按 `cue_id` 索引的时间线和当前 WebSocket；后端负责权威会话数据，浏览器 localStorage 只保存界面偏好和当前选择。[apps/desktop/src/main.swift](apps/desktop/src/main.swift) 只是加载本地 URL 的 WKWebView 壳，并提供原生媒体选择桥接。

## 路径与外部依赖

- `SIMULMT_ROOT`：项目根目录，启动脚本会设置。
- `SIMULMT_ADDR`：Core 监听地址，默认 `127.0.0.1:8780`。
- `SIMULMT_RUNTIME_DIR`：运行数据目录，默认 `runtime/`。
- `SIMULMT_AUDIO_HOST`：覆盖原生音频宿主路径。
- `SIMULMT_DEMO_ROOT`：覆盖模型依赖根目录；默认使用存在 `models/` 的项目内 `vendor/`，否则回退到项目上一级目录。
- `SIMULMT_UI_URL`：桌面壳加载的 URL；`RUST_LOG` 可调整 Rust 日志。

真实模型默认需要 `vendor/llama.cpp-hyv3/build/bin/llama-mtmd-cli` 和支持 Hy-SimulMT 的 llama.cpp 构建，以及约 18.4 GiB 的 `vendor/models/Hy-SimulMT-30B-A3B-Interleave-Q4_K_M.gguf`。`vendor/`、`runtime/` 和 `target/` 均为本地生成或下载内容。
