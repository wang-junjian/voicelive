# simulmt.api.v1

当前实现的外部控制面契约由 Rust 类型生成到 `api/openapi.yaml`。修改 API 后运行：

```bash
cargo run -p simulmt-app --locked -- --print-openapi > api/openapi.yaml
```

CI 会重新生成并拒绝未提交的差异，Rust 单元测试也比对该文件。

REST 错误统一使用 `ErrorInfo`，包含稳定的 `code`、`retryable` 和与 `X-Request-Id` 响应头相同的 `request_id`。生命周期冲突返回 409，JSON/schema 拒绝返回 400 或 422，不存在的会话返回 404；`retryable` 只表示故障是否可能为暂时性，不代表非幂等命令可以自动重放。

WebSocket 在首条 `authenticate` 消息通过后收到 `baseline`，然后才是 `event`。baseline 在同一事件发布临界区内固定完整 snapshot、history 和 sequence；客户端随后只接受 `sequence + 1` 的事件。重复事件可忽略，序号空洞、未知消息或 `instance_id` 变化必须完整重连。服务端接收缓冲区 Lagged 时发送 `resync_required` 和新的完整 baseline。序号只在同一 Core instance 与 session 内单调，Core 重启后必须丢弃旧游标。

`proto/simulmt/v1/` 是未来外部 Worker 的**草案**，当前没有网络 gRPC Worker 服务。M0/M1 内置适配器使用对应的 Rust 类型直接连接 Core，不要把 Proto 草案描述为已实现的网络接口。

关键约束：

- 所有流式对象携带 `instance_id`、`session_id` 和 `epoch`；WS 增量事件还携带 `sequence`。
- 音频进入 Core 后归一化为 16 kHz、单声道、PCM S16LE。
- 时间戳使用单调时钟，时长使用微秒。
- 旧 epoch 的结果必须丢弃。
- 所有队列必须有界，丢弃必须产生指标或事件。
- Proto 的破坏性变更需要升级到 v2。
