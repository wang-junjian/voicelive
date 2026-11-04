# simulmt.api.v1

`proto/simulmt/v1/` 是外部 Worker 和客户端的稳定契约。M0/M1 内置适配器使用对应的 Rust 类型直接连接 Core；网络 Worker 可以按 Proto 通过 gRPC 接入，不需要修改会话与策略逻辑。

关键约束：

- 所有流式对象携带 `session_id` 和 `epoch`。
- 音频进入 Core 后归一化为 16 kHz、单声道、PCM S16LE。
- 时间戳使用单调时钟，时长使用微秒。
- 旧 epoch 的结果必须丢弃。
- 所有队列必须有界，丢弃必须产生指标或事件。
- Proto 的破坏性变更需要升级到 v2。
