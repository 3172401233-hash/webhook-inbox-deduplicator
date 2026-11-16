import { createHash } from 'node:crypto';
import { MemoryStorage } from './storage.js';

export const IngestStatus = Object.freeze({
  ACCEPTED: 'accepted',   // 新事件，已进入可消费队列
  DUPLICATE: 'duplicate', // 幂等键已存在且载荷一致，直接确认
  CONFLICT: 'conflict',   // 幂等键已存在但载荷摘要不同，隔离并标记冲突
  LATE: 'late',           // 超过重试窗口，进入迟到流
});

const DEFAULT_RETRY_WINDOW_MS = 24 * 60 * 60 * 1000;

function emptyState() {
  return {
    // eventId -> 记录（含去重指纹与生命周期状态）
    events: {},
    // 待投递 eventId 列表（claim 时按 eventTime 排序）
    pending: [],
    // 已认领未确认 eventId -> claimedAt
    inflight: {},
    // 迟到流 eventId 列表
    late: [],
    stats: {
      ingested: 0,
      duplicates: 0,
      conflicts: 0,
      late: 0,
      emitted: 0,
      acked: 0,
      redeliveries: 0,
    },
  };
}

/**
 * Webhook 收件箱。
 *
 * 职责边界：
 * - ingest(): 原始事件先落临时收件箱（持久化），按幂等键（eventId + 载荷摘要）去重；
 * - claim()/ack()/fail(): 两阶段消费，保证同一有效事件只进入投递流一次；
 * - 所有状态变更同步落 checkpoint，进程重启后从 checkpoint 恢复。
 */
export class WebhookInbox {
  constructor({ storage, retryWindowMs = DEFAULT_RETRY_WINDOW_MS, now } = {}) {
    if (retryWindowMs <= 0) throw new RangeError('retryWindowMs must be positive');
    this.storage = storage ?? new MemoryStorage();
    this.retryWindowMs = retryWindowMs;
    this.now = now ?? (() => Date.now());

    this.state = this.storage.load() ?? emptyState();
    this.#recoverInflight();
    this.#prune();
    this.#persist();
  }

  /**
   * 接收原始事件。
   * @param {object} event
   * @param {string} event.eventId      平台事件 ID（幂等键主体）
   * @param {string} event.businessKey  业务键（如订单号），仅用于归属与排查，不参与去重
   * @param {number} event.eventTime    业务发生时间（ms  epoch）
   * @param {number} [event.arrivalTime] 到达时间，默认取当前时间
   * @param {string} [event.payloadDigest] 载荷摘要；缺省时由 payload 计算 sha256
   * @param {*}      [event.payload]    原始载荷，随事件保存并投递给下游
   * @returns {{ status: string, eventId: string }}
   */
  ingest(event) {
    const { eventId, businessKey, eventTime } = event ?? {};
    if (!eventId) throw new TypeError('eventId is required');
    if (!businessKey) throw new TypeError('businessKey is required');
    if (!Number.isFinite(eventTime)) throw new TypeError('eventTime must be a finite number');

    const arrivalTime = event.arrivalTime ?? this.now();
    const digest = event.payloadDigest ?? this.#digestOf(event.payload);

    this.state.stats.ingested += 1;

    const existing = this.state.events[eventId];
    if (existing) {
      if (existing.digest === digest) {
        this.state.stats.duplicates += 1;
        this.#persist();
        return { status: IngestStatus.DUPLICATE, eventId };
      }
      // 同 ID 不同载荷：隔离原事件，标记冲突，等待人工对账
      this.#quarantine(eventId);
      existing.status = 'conflict';
      existing.conflictDigests = [...new Set([existing.digest, digest])];
      this.state.stats.conflicts += 1;
      this.#persist();
      return { status: IngestStatus.CONFLICT, eventId };
    }

    if (arrivalTime - eventTime > this.retryWindowMs) {
      this.state.events[eventId] = {
        eventId, businessKey, eventTime, arrivalTime, digest,
        payload: event.payload ?? null,
        status: 'late',
        attempts: 0,
        firstSeenAt: this.now(),
      };
      this.state.late.push(eventId);
      this.state.stats.late += 1;
      this.#persist();
      return { status: IngestStatus.LATE, eventId };
    }

    this.state.events[eventId] = {
      eventId, businessKey, eventTime, arrivalTime, digest,
      payload: event.payload ?? null,
      status: 'pending',
      attempts: 0,
      firstSeenAt: this.now(),
    };
    this.state.pending.push(eventId);
    this.#persist();
    return { status: IngestStatus.ACCEPTED, eventId };
  }

  /**
   * 认领一批待投递事件。按 eventTime（业务发生时间）升序投递，
   * 与到达顺序无关。deliveryId 即 eventId，重投时保持不变，
   * 下游可用它做最后一道幂等。
   */
  claim(limit = 100) {
    const ids = this.state.pending
      .slice()
      .sort((a, b) => {
        const ea = this.state.events[a];
        const eb = this.state.events[b];
        return ea.eventTime - eb.eventTime || (a < b ? -1 : a > b ? 1 : 0);
      })
      .slice(0, limit);

    const claimedAt = this.now();
    const deliveries = [];
    for (const id of ids) {
      const record = this.state.events[id];
      record.status = 'inflight';
      record.attempts += 1;
      if (record.attempts > 1) this.state.stats.redeliveries += 1;
      this.state.inflight[id] = claimedAt;
      this.state.stats.emitted += 1;
      deliveries.push({
        deliveryId: id,
        attempt: record.attempts,
        eventId: record.eventId,
        businessKey: record.businessKey,
        eventTime: record.eventTime,
        payload: record.payload,
      });
    }
    this.state.pending = this.state.pending.filter((id) => !this.state.inflight[id]);
    if (deliveries.length > 0) this.#persist();
    return deliveries;
  }

  /** 下游处理成功后确认，推进 checkpoint。 */
  ack(deliveryIds) {
    for (const id of deliveryIds) {
      if (!(id in this.state.inflight)) {
        throw new Error(`cannot ack ${id}: not inflight`);
      }
      delete this.state.inflight[id];
      this.state.events[id].status = 'acked';
      this.state.stats.acked += 1;
    }
    this.#persist();
  }

  /** 下游处理失败，事件回到待投递队列，等待重投。 */
  fail(deliveryIds) {
    for (const id of deliveryIds) {
      if (!(id in this.state.inflight)) continue;
      delete this.state.inflight[id];
      this.state.events[id].status = 'pending';
      this.state.pending.push(id);
    }
    this.#persist();
  }

  /**
   * 便捷消费：认领一批交给 handler，成功则 ack，异常则 fail 并重抛。
   */
  async consume(handler, { batchSize = 100 } = {}) {
    const batch = this.claim(batchSize);
    if (batch.length === 0) return [];
    try {
      await handler(batch);
    } catch (err) {
      this.fail(batch.map((d) => d.deliveryId));
      throw err;
    }
    this.ack(batch.map((d) => d.deliveryId));
    return batch;
  }

  /** 迟到流快照（只读），供独立的补偿/告警管道消费。 */
  lateEvents() {
    return this.state.late.map((id) => ({ ...this.state.events[id] }));
  }

  conflicts() {
    return Object.values(this.state.events)
      .filter((e) => e.status === 'conflict')
      .map((e) => ({ ...e }));
  }

  /** 当前 checkpoint 摘要。 */
  checkpoint() {
    return {
      pending: this.state.pending.length,
      inflight: Object.keys(this.state.inflight).length,
      late: this.state.late.length,
      tracked: Object.keys(this.state.events).length,
      stats: { ...this.state.stats },
    };
  }

  // ---- 内部 ----

  /** 崩溃恢复：已认领未确认的事件回到待投递队列（可能导致重投，见 README 边界说明）。 */
  #recoverInflight() {
    for (const id of Object.keys(this.state.inflight)) {
      const record = this.state.events[id];
      if (record && record.status === 'inflight') {
        record.status = 'pending';
        this.state.pending.push(id);
      }
      delete this.state.inflight[id];
    }
  }

  /** 清理超出重试窗口的终态去重记录；窗口外到达的重复事件会按“迟到”处理。 */
  #prune() {
    const horizon = this.now() - this.retryWindowMs;
    for (const [id, record] of Object.entries(this.state.events)) {
      if ((record.status === 'acked' || record.status === 'late') && record.firstSeenAt < horizon) {
        delete this.state.events[id];
      }
    }
    this.state.late = this.state.late.filter((id) => this.state.events[id]);
  }

  #quarantine(eventId) {
    this.state.pending = this.state.pending.filter((id) => id !== eventId);
    delete this.state.inflight[eventId];
  }

  #digestOf(payload) {
    if (payload === undefined || payload === null) return 'sha256:empty';
    const canonical = typeof payload === 'string' ? payload : JSON.stringify(payload);
    return `sha256:${createHash('sha256').update(canonical).digest('hex')}`;
  }

  #persist() {
    this.storage.save(this.state);
  }
}
