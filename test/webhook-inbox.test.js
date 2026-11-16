import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import { FileStorage, IngestStatus, MemoryStorage, WebhookInbox } from '../index.js';

const NOW = 1_800_000_000_000;

function makeEvent(overrides = {}) {
  return {
    eventId: 'evt-1',
    businessKey: 'order-1001',
    eventTime: NOW - 1_000,
    arrivalTime: NOW,
    payload: { orderId: 'order-1001', status: 'paid' },
    ...overrides,
  };
}

function makeInbox(options = {}) {
  let tick = NOW;
  return new WebhookInbox({ now: () => tick++, ...options });
}

test('并发重复投递：同一事件只进入可消费流一次，其余直接确认', async () => {
  const inbox = makeInbox();
  const event = makeEvent();

  const results = await Promise.all(
    Array.from({ length: 20 }, () => Promise.resolve().then(() => inbox.ingest(event))),
  );

  assert.equal(results.filter((r) => r.status === IngestStatus.ACCEPTED).length, 1);
  assert.equal(results.filter((r) => r.status === IngestStatus.DUPLICATE).length, 19);

  const seen = [];
  await inbox.consume((batch) => seen.push(...batch.map((d) => d.eventId)));
  assert.deepEqual(seen, ['evt-1']);
  assert.equal(inbox.claim().length, 0, '确认后不应再有可投递事件');
});

test('乱序到达：按业务发生时间而非到达顺序投递', async () => {
  const inbox = makeInbox();
  inbox.ingest(makeEvent({ eventId: 'evt-c', eventTime: NOW - 300 }));
  inbox.ingest(makeEvent({ eventId: 'evt-a', eventTime: NOW - 900 }));
  inbox.ingest(makeEvent({ eventId: 'evt-b', eventTime: NOW - 600 }));

  const batch = inbox.claim(10);
  assert.deepEqual(batch.map((d) => d.eventId), ['evt-a', 'evt-b', 'evt-c']);
});

test('同业务键不同事件：都视为有效事件，分别投递', async () => {
  const inbox = makeInbox();
  inbox.ingest(makeEvent({ eventId: 'evt-created', eventTime: NOW - 500, payload: { status: 'created' } }));
  inbox.ingest(makeEvent({ eventId: 'evt-paid', eventTime: NOW - 100, payload: { status: 'paid' } }));

  const batch = inbox.claim(10);
  assert.deepEqual(batch.map((d) => d.eventId), ['evt-created', 'evt-paid']);
});

test('同 ID 不同载荷：标记冲突并隔离，下游不可见', async () => {
  const inbox = makeInbox();
  assert.equal(inbox.ingest(makeEvent({ payload: { status: 'paid' } })).status, IngestStatus.ACCEPTED);
  assert.equal(inbox.ingest(makeEvent({ payload: { status: 'refunded' } })).status, IngestStatus.CONFLICT);

  assert.equal(inbox.claim(10).length, 0, '冲突事件应被隔离，不进入投递流');
  const conflicts = inbox.conflicts();
  assert.equal(conflicts.length, 1);
  assert.equal(conflicts[0].conflictDigests.length, 2);
});

test('超过重试窗口的事件进入迟到流，不进入正常投递', () => {
  const inbox = makeInbox({ retryWindowMs: 60_000 });
  const result = inbox.ingest(makeEvent({ eventTime: NOW - 120_000, arrivalTime: NOW }));

  assert.equal(result.status, IngestStatus.LATE);
  assert.equal(inbox.claim(10).length, 0);
  assert.equal(inbox.lateEvents().length, 1);
  assert.equal(inbox.lateEvents()[0].eventId, 'evt-1');
});

test('checkpoint 恢复：已确认事件不重投，未确认事件恢复后重投', () => {
  const dir = mkdtempSync(join(tmpdir(), 'inbox-'));
  try {
    const file = join(dir, 'checkpoint.json');
    const first = makeInbox({ storage: new FileStorage(file) });
    first.ingest(makeEvent({ eventId: 'evt-done', eventTime: NOW - 300 }));
    first.ingest(makeEvent({ eventId: 'evt-stuck', eventTime: NOW - 200 }));
    first.ingest(makeEvent({ eventId: 'evt-fresh', eventTime: NOW - 100 }));

    const batch = first.claim(2);
    first.ack([batch[0].deliveryId]);

    const recovered = makeInbox({ storage: new FileStorage(file) });
    const redelivered = recovered.claim(10);
    assert.deepEqual(
      redelivered.map((d) => d.eventId),
      ['evt-stuck', 'evt-fresh'],
      '已 ack 的事件不得重投，inflight 事件应回到队列',
    );
    assert.equal(redelivered[0].attempt, 2, '重投时 attempt 递增，deliveryId 不变');
    assert.equal(redelivered[0].deliveryId, 'evt-stuck');

    assert.equal(
      recovered.ingest(makeEvent({ eventId: 'evt-done', eventTime: NOW - 300 })).status,
      IngestStatus.DUPLICATE,
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('批量消费失败：整批回队重投，最终每条有效事件只被确认一次', async () => {
  const inbox = makeInbox();
  for (let i = 0; i < 5; i++) {
    inbox.ingest(makeEvent({ eventId: `evt-${i}`, eventTime: NOW - (5 - i) * 100 }));
  }

  const delivered = [];
  const acked = [];
  let failures = 2;
  const handler = async (batch) => {
    delivered.push(...batch.map((d) => `${d.deliveryId}#${d.attempt}`));
    if (failures > 0) {
      failures -= 1;
      throw new Error('downstream unavailable');
    }
    acked.push(...batch.map((d) => d.deliveryId));
  };

  await assert.rejects(() => inbox.consume(handler, { batchSize: 5 }), /unavailable/);
  await assert.rejects(() => inbox.consume(handler, { batchSize: 5 }), /unavailable/);
  await inbox.consume(handler, { batchSize: 5 });

  assert.deepEqual(delivered.slice(0, 5), delivered.slice(5, 10).map((s) => s.replace('#2', '#1')));
  assert.deepEqual(acked.sort(), ['evt-0', 'evt-1', 'evt-2', 'evt-3', 'evt-4']);
  assert.equal(new Set(acked).size, 5, '每条事件只被确认一次');

  const cp = inbox.checkpoint();
  assert.equal(cp.pending + cp.inflight, 0);
  assert.equal(cp.stats.acked, 5);
});

test('下游视角：网络重试 + 乱序混合场景下，有效事件恰好各出现一次', async () => {
  const inbox = makeInbox();
  const orders = ['order-1', 'order-2', 'order-3'];
  const events = orders.flatMap((key, i) => [
    makeEvent({ eventId: `${key}-created`, businessKey: key, eventTime: NOW - 3_000 + i * 100, payload: { s: 'created' } }),
    makeEvent({ eventId: `${key}-paid`, businessKey: key, eventTime: NOW - 2_000 + i * 100, payload: { s: 'paid' } }),
  ]);

  const shuffled = [...events].reverse();
  for (let round = 0; round < 3; round++) {
    for (const e of shuffled) inbox.ingest(e);
  }

  const seenByDownstream = [];
  let batch;
  while ((batch = await inbox.consume(async (b) => seenByDownstream.push(...b.map((d) => d.eventId)), { batchSize: 2 })).length > 0) {
    // 逐批消费直到清空
  }

  assert.equal(seenByDownstream.length, events.length);
  assert.equal(new Set(seenByDownstream).size, events.length, '下游不得看到重复事件');
  const times = seenByDownstream.map((id) => events.find((e) => e.eventId === id).eventTime);
  assert.deepEqual(times, [...times].sort((a, b) => a - b));
});

test('去重窗口：窗口内重复被去重，终态记录过期后被清理', () => {
  let tick = NOW;
  const storage = new MemoryStorage();
  const inbox = new WebhookInbox({ retryWindowMs: 60_000, now: () => tick, storage });
  inbox.ingest(makeEvent({ eventTime: NOW - 1_000, arrivalTime: NOW }));
  inbox.ack(inbox.claim(1).map((d) => d.deliveryId));
  assert.equal(inbox.checkpoint().tracked, 1, '窗口内保留去重记录');

  tick = NOW + 120_000;
  const inbox2 = new WebhookInbox({ retryWindowMs: 60_000, now: () => tick, storage });
  assert.equal(inbox2.checkpoint().tracked, 0, '窗口外终态记录被清理');
  assert.equal(
    inbox2.ingest(makeEvent({ eventTime: NOW - 1_000, arrivalTime: tick })).status,
    IngestStatus.LATE,
  );
});
