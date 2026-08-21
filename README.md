# 抖音直播事件采集项目骨架

这是一个使用 Python、FastAPI、WebSocket、Pydantic 和 asyncio 构建的抖音直播 COMMENT 事件采集服务。默认使用 mock 流，也支持通过临时签名 WebSocket 地址接入真实直播间。

## 目录

```text
.
├── api/
│   ├── routes.py              # 健康检查与测试页面
│   └── websocket.py           # /ws/events 广播接口
├── collectors/
│   ├── base.py                # 采集器生命周期与重连/心跳接口
│   ├── douyin.py              # 抖音采集边界、真实 WS collector 与 COMMENT 解析器
│   ├── douyin_protocol.py     # PushFrame / gzip / Response / ChatMessage 解码
│   ├── signature.py           # 连接信息与短期签名 Provider
│   └── mock.py                # 本地 mock comment 流
├── config/
│   └── settings.py            # 环境变量配置
├── events/
│   ├── models.py              # 统一 Event / User / Room 模型
│   └── bus.py                 # asyncio 进程内 fan-out EventBus
├── services/
│   ├── dependencies.py        # 组装 collector、parser、bus
│   └── event_service.py       # 采集 → 解析 → 发布
├── web/static/index.html      # 最简 WebSocket 测试页面
├── main.py
└── tests/
```

业务层只接触 `events.models.Event` 和 `events.bus.EventBus`。抖音协议字段只在 `collectors/` 的采集与解析边界内被读取，后续扩展其他事件时无需改 WebSocket 和下游服务。

## 启动

需要 Python 3.11 或更高版本：

```bash
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\\Scripts\\activate
python -m pip install -r requirements.txt
cp .env.example .env
python -m uvicorn main:app --reload
```

打开 <http://127.0.0.1:8000/>，页面会自动连接 WebSocket，每两秒显示一条 mock comment，同时显示采集连接和健康状态。

## 接入真实直播间

当前真实 Collector 接收一条临时的、已签名的 WebSocket URL。签名 URL 会过期，不要提交到代码仓库；放在本地 `.env` 即可。

1. 在 Chrome 打开目标直播间，开发者工具的 Network 中筛选 `WS`。
2. 刷新页面，找到 `/webcast/im/push/v2/` 的 WebSocket 请求，复制完整 Request URL。
3. 在本地 `.env` 填入：

```dotenv
DOUYIN_COLLECTOR_MODE=douyin
DOUYIN_WS_URL=粘贴完整的临时 WebSocket URL
DOUYIN_ROOM_ID=真实room_id
DOUYIN_ROOM_TITLE=直播间标题
DOUYIN_TTWID=本地浏览器会话的ttwid（如果服务端要求）
DOUYIN_WS_EXPIRES_AT=2026-08-21T12:00:00+08:00  # 已知过期时间时填写
```

然后启动：

```bash
python -m uvicorn main:app --reload
```

页面收到的仍然是统一 Event，不会把抖音 protobuf 结构暴露给 API 或后续业务模块。当前已支持 `WebcastChatMessage`；连接确认、gzip 解压、protobuf 外层解码、ACK、心跳、指数退避重连和连接状态统计均已接通。

签名 URL 是短期凭证，可能在复制后很快失效。当前版本通过 `StaticSignedUrlProvider` 接收手工获取的地址；失效后服务会进入 `needs_refresh`，不会持续忙循环。更新 `.env` 后重启服务即可加载新的地址。自动生成签名属于后续 Provider 实现，不在核心采集器中处理。

也可以查看：

- 健康检查：<http://127.0.0.1:8000/health>
- 就绪检查：<http://127.0.0.1:8000/health/ready>
- OpenAPI：<http://127.0.0.1:8000/docs>
- WebSocket：`ws://127.0.0.1:8000/ws/events`

## 事件格式

```json
{
  "id": "uuid",
  "version": "1.0",
  "platform": "douyin",
  "event": "comment",
  "room": {"id": "mock-room-001", "title": "Mock 抖音直播间"},
  "timestamp": "2026-01-01T00:00:00Z",
  "user": {"id": "mock-user-0001", "nickname": "测试用户1"},
  "data": {"content": "这是第 1 条 mock comment"},
  "raw": {"method": "WebcastChatMessage", "content": "这是第 1 条 mock comment"}
}
```

`raw` 只保留必要的已解析字段，不包含签名 URL、Cookie 或完整二进制 payload。

## 当前阶段的预留点

- `Collector.connect()` / `disconnect()` / `reconnect()`：连接、断线重连和生命周期状态。
- `Collector.heartbeat()`：心跳保活，并记录最近心跳时间。
- `DouyinWebSocketCollector`：真实抖音 WebSocket 传输实现；连接地址由 `SignatureProvider` 提供。
- `StaticSignedUrlProvider`：当前的手工短期签名地址 Provider；未来可替换为自动刷新实现。
- `DouyinParser.parse()`：继续增加 `gift`、`like`、`follow` 等事件标准化。
- `EventBus`：当前是单进程内存 fan-out；未来可替换为 Redis Streams、NATS 或 Kafka，而不改变标准 Event 合约。

## 健康检查

- <http://127.0.0.1:8000/health>：服务、采集器、连接时间、最近消息、心跳、重连和错误计数。
- <http://127.0.0.1:8000/health/ready>：只有采集服务运行且 Collector 已连接时返回 `ready: true`。

健康接口不会返回完整 WebSocket 地址、签名参数或 Cookie。

## 测试

```bash
pytest
```
