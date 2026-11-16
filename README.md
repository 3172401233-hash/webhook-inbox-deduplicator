# webhook-inbox-deduplicator

Node.js Webhook 收件箱（inbox）组件。外部平台只保证**至少一次**投递，且到达顺序不等于业务发生顺序；本组件在接收侧把原始事件先落入临时收件箱，再按幂等键生成可消费事件，保证同一条有效事件只会进入下游投递流一次。

## 处理流程

```
ingest()                     claim()            ack()/fail()
原始事件 ──► 临时收件箱 ──► 幂等判定 ──► 有序投递 ──► 确认 / 回队重投
                │                │
                │                ├─ 重复 ID + 相同载荷 → duplicate（直接确认）
                │                ├─ 重复 ID + 不同载荷 → conflict（隔离，待对账）
                │                └─ 超过重试窗口        → late（迟到流）
                └─ 每次状态变更同步落 checkpoint（写临时文件 + 原子 rename）
```

## 快速开始

```js
import { WebhookInbox, FileStorage } from 'webhook-inbox-deduplicator';

const inbox = new WebhookInbox({
  storage: new FileStorage('./data/inbox-checkpoint.json'),
  retryWindowMs: 24 * 60 * 60 * 1000, // 去重窗口，默认 24h
});

// 接收回调：落临时收件箱 + 幂等判定
const { status } = inbox.ingest({
  eventId: 'evt_abc',          // 平台事件 ID（幂等键主体）
  businessKey: 'order-1001',   // 业务键，仅用于归属与排查，不参与去重
  eventTime: 1800000000000,    // 业务发生时间（ms）
  arrivalTime: Date.now(),     // 到达时间，缺省取当前时间
  payloadDigest: 'sha256:...', // 载荷摘要，缺省时由 payload 计算
  payload: { /* ... */ },
});
// status ∈ accepted | duplicate | conflict | late

// 消费：两阶段，处理成功才 ack
await inbox.consume(async (batch) => {
  for (const delivery of batch) {
    await processOrder(delivery.businessKey, delivery.payload);
  }
}, { batchSize: 100 });
```

## API

| 方法 | 说明 |
| --- | --- |
| `ingest(event)` | 落收件箱并做幂等判定，返回 `{ status, eventId }` |
| `claim(limit)` | 认领一批事件，按 `eventTime` 升序（与到达顺序无关） |
| `ack(deliveryIds)` | 下游处理成功后确认，推进 checkpoint |
| `fail(deliveryIds)` | 处理失败，事件回队等待重投 |
| `consume(handler, { batchSize })` | claim → handler → ack/fail 的便捷封装 |
| `lateEvents()` / `conflicts()` | 迟到流 / 冲突事件快照，供补偿与对账 |
| `checkpoint()` | 当前 checkpoint 摘要（pending/inflight/late/统计） |

投递对象中 `deliveryId` 恒等于 `eventId`，重投时不变，`attempt` 单调递增——下游可以用 `deliveryId` 做最后一道幂等。

## 去重窗口（retryWindowMs）

- 幂等键 = `eventId` + 载荷摘要。窗口内所有去重记录（含已 ack 的终态记录）都保留，重复 ID 直接确认。
- `arrivalTime - eventTime > retryWindowMs` 的事件判定为**迟到**，进入迟到流，不进入正常投递。
- 终态记录（acked/late）在 `firstSeenAt` 超过窗口后被清理；此后到达的同 ID 重复投递因必然满足迟到条件，仍会进入迟到流，不会再次投递给下游。
- 窗口长度应覆盖平台的最大重试跨度（多数平台为数小时到数天），默认 24 小时。

## exactly-once 的实际边界

本组件保证的是**组件内 exactly-once**：同一 `eventId` 的有效事件只会生成一条可消费事件、只会被 ack 一次，重复投递、乱序、并发重试都不会让下游看到第二条。

端到端 exactly-once 在分布式投递中不存在，以下边界需要知晓：

- **崩溃窗口**：下游处理成功但 `ack` 尚未落盘时进程崩溃，恢复后该事件会重投（inflight 回队）。这是 at-least-once 语义，下游应基于稳定的 `deliveryId` 做幂等，或把"处理 + ack"放进同一事务。
- **批量失败**：`consume` 的 handler 抛错时整批回队重投，批内已处理成功的事件也会被下游再次看到，同样依赖 `deliveryId` 幂等。
- **冲突**：同 ID 不同载荷说明平台侧数据不一致，组件选择隔离而非投递，需人工对账后补偿。
- **迟到流**：超过重试窗口的事件不保证按正常链路投递，需独立的补偿管道处理。
- **单写者假设**：checkpoint 文件不支持多进程并发写；水平扩容时请换实现 `load/save` 接口的共享存储（如 Redis/数据库）。

## 测试

```sh
node --test
```

覆盖：并发重复投递、乱序到达、同业务键不同事件、同 ID 不同载荷冲突、迟到流、checkpoint 崩溃恢复、批量消费失败重投、窗口过期清理，以及"下游不得看到重复有效事件"的端到端断言。
