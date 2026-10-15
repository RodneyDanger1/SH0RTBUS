import assert from 'node:assert/strict';
import { test } from 'node:test';
import { BusValidationError, openBus, type Envelope, type HandlerErrorEvent } from '../src/bus.ts';
import { collector, logPath, sleep, tempProject, waitFor } from '../testing/harness.ts';
import * as fs from 'node:fs';

test('subscribe delivers exact, prefix and wildcard patterns', async (t) => {
  const dir = tempProject('subscribe-patterns');
  const bus = await openBus(dir);
  t.after(() => bus.close());

  const exact = collector();
  const prefix = collector();
  const all = collector();
  const offExact = bus.subscribe('build.unit', exact.push);
  const offPrefix = bus.subscribe('build.*', prefix.push);
  const offAll = bus.subscribe('*', all.push);
  t.after(() => {
    offExact();
    offPrefix();
    offAll();
  });

  await bus.post({ from: 'alice', topic: 'build.unit', kind: 'post', payload: { n: 1 } });
  await bus.post({ from: 'alice', topic: 'build.integration', kind: 'post', payload: { n: 2 } });
  await bus.post({ from: 'alice', topic: 'deploy', kind: 'post', payload: { n: 3 } });
  await bus.post({ from: 'alice', topic: 'build', kind: 'post', payload: { n: 4 } });

  await waitFor(() => all.seqs().length === 4, 'wildcard subscriber sees all four');
  assert.deepEqual(exact.seqs(), [1]);
  assert.deepEqual(prefix.seqs(), [1, 2]);
  assert.deepEqual(all.seqs(), [1, 2, 3, 4]);
});

test('a pattern must be * , prefix* or exact', async (t) => {
  const bus = await openBus(tempProject('subscribe-pattern-errors'));
  t.after(() => bus.close());
  const handler = () => undefined;
  assert.throws(() => bus.subscribe('a*b', handler), BusValidationError);
  assert.throws(() => bus.subscribe('**', handler), BusValidationError);
  assert.throws(() => bus.subscribe('', handler), BusValidationError);
  assert.throws(() => bus.subscribe('a b', handler), BusValidationError);
  assert.throws(() => bus.subscribe('*', null as never), BusValidationError);
});

test('deliveries arrive in seq order even with a slow handler and concurrent posts', async (t) => {
  const dir = tempProject('subscribe-order');
  const bus = await openBus(dir);
  t.after(() => bus.close());

  const got = collector();
  const order: number[] = [];
  const off = bus.subscribe('*', async (message) => {
    if (order.length > 0 && message.seq <= order[order.length - 1]!) assert.fail('out of order delivery');
    await sleep(5);
    order.push(message.seq);
    got.push(message);
  });
  t.after(off);

  await Promise.all(
    Array.from({ length: 10 }, (_unused, i) =>
      bus.post({ from: `agent-${String(i % 3)}`, topic: 'work', kind: 'note', payload: { i } }),
    ),
  );
  await waitFor(() => got.messages.length === 10, 'all ten delivered');
  assert.deepEqual(
    got.seqs(),
    Array.from({ length: 10 }, (_unused, i) => i + 1),
  );
});

test('unsubscribe stops delivery, and re-subscribing with a cursor sees history', async (t) => {
  const dir = tempProject('subscribe-unsub');
  const bus = await openBus(dir);
  t.after(() => bus.close());

  const first = collector();
  const off = bus.subscribe('*', first.push);
  await bus.post({ from: 'a', topic: 't', kind: 'post', payload: { n: 1 } });
  await waitFor(() => first.messages.length === 1, 'first message delivered');
  off();

  await bus.post({ from: 'a', topic: 't', kind: 'post', payload: { n: 2 } });
  await sleep(150);
  assert.equal(first.messages.length, 1, 'no delivery after unsubscribe');

  const history = collector();
  const off2 = bus.subscribe('*', history.push, { from: 'all' });
  t.after(off2);
  await waitFor(() => history.messages.length === 2, 'history is replayed to the new subscriber');
  assert.deepEqual(history.seqs(), [1, 2], 're-reading the log did not duplicate anything for others');

  const fromCursor = collector();
  const off3 = bus.subscribe('*', fromCursor.push, { from: 1 });
  t.after(off3);
  await waitFor(() => fromCursor.seqs().length === 1, 'from is exclusive: only seq 2');
  assert.deepEqual(fromCursor.seqs(), [2]);
});

test('a subscriber that asks for history does not overtake live messages', async (t) => {
  const dir = tempProject('subscribe-history-order');
  const bus = await openBus(dir);
  t.after(() => bus.close());

  for (let i = 0; i < 4; i += 1) {
    await bus.post({ from: 'seed', topic: 'work', kind: 'post', payload: { i } });
  }
  const seen: number[] = [];
  const late = bus.subscribe(
    '*',
    (message) => {
      seen.push(message.seq);
    },
    { from: 'all' },
  );
  t.after(late);
  // Live traffic immediately after subscribing: history and live must interleave in
  // seq order, never as "new message first, then the backfill".
  for (let i = 0; i < 4; i += 1) {
    await bus.post({ from: 'live', topic: 'work', kind: 'post', payload: { i } });
  }
  await waitFor(() => seen.length === 8, 'history plus live traffic all arrive');
  assert.deepEqual(seen, [1, 2, 3, 4, 5, 6, 7, 8]);
});

test('a throwing handler is reported and does not stop other subscribers', async (t) => {
  const dir = tempProject('subscribe-errors');
  const bus = await openBus(dir);
  t.after(() => bus.close());

  const errors: HandlerErrorEvent[] = [];
  bus.on('handler-error', (event) => errors.push(event));
  const healthy = collector();
  bus.subscribe('*', () => {
    throw new Error('boom');
  });
  const off = bus.subscribe('*', healthy.push);
  t.after(off);

  await bus.post({ from: 'a', topic: 't', kind: 'post', payload: { n: 1 } });
  await bus.post({ from: 'a', topic: 't', kind: 'post', payload: { n: 2 } });

  await waitFor(() => healthy.messages.length === 2, 'the healthy subscriber still gets both');
  await waitFor(() => errors.length === 2, 'each failure is reported');
  assert.equal(errors[0]!.message.seq, 1);
  assert.equal((errors[0]!.error as Error).message, 'boom');
  assert.equal(errors[1]!.message.seq, 2);
});

test('tail follows the log live, buffers a partial line and stops cleanly', async (t) => {
  const dir = tempProject('tail-live');
  const writer = await openBus(dir);
  const reader = await openBus(dir);
  t.after(async () => {
    await writer.close();
    await reader.close();
  });

  const issues: string[] = [];
  reader.on('issue', (issue) => issues.push(issue.kind));
  const got = collector();
  const stop = reader.tail(got.push);
  t.after(stop);

  await waitFor(() => fs.existsSync(logPath(dir)), 'the log exists');

  // A half-written record must never surface as a message.
  fs.appendFileSync(logPath(dir), '{"v":1,"id":"dead');
  await sleep(120);
  assert.deepEqual(got.messages.length, 0, 'a partial line is buffered, not delivered');

  const one = await writer.post({ from: 'writer', topic: 'work', kind: 'post', payload: { n: 1 } });
  await waitFor(() => got.messages.length === 1, 'the message written after the fragment arrives');
  assert.equal(got.messages[0]!.id, one.id);
  assert.equal(got.messages[0]!.seq, 1);
  assert.ok(issues.includes('corrupt-line'), `the fragment is reported (got ${issues.join(',')})`);

  stop();
  await writer.post({ from: 'writer', topic: 'work', kind: 'post', payload: { n: 2 } });
  await sleep(150);
  assert.equal(got.messages.length, 1, 'stop() ends live delivery');
});

test('tail resumes after a restart without replaying delivered messages', async (t) => {
  const dir = tempProject('tail-restart');
  const writer = await openBus(dir);
  t.after(() => writer.close());

  const first = collector();
  const firstStop = writer.tail(first.push);
  await writer.post({ from: 'a', topic: 't', kind: 'post', payload: { n: 1 } });
  await waitFor(() => first.messages.length === 1, 'delivered before the restart');
  firstStop();
  await writer.close();

  const reopened = await openBus(dir);
  t.after(() => reopened.close());
  const second = collector();
  const secondStop = reopened.tail(second.push);
  t.after(secondStop);
  const third = await reopened.post({ from: 'a', topic: 't', kind: 'post', payload: { n: 2 } });
  await waitFor(() => second.messages.length === 1, 'only the new message arrives after restart');
  assert.equal(second.messages[0]!.seq, third.seq);
  assert.deepEqual(second.seqs(), [2]);

  const all = collector();
  const allStop = reopened.tail(all.push, { from: 'all' });
  t.after(allStop);
  await waitFor(() => all.messages.length === 2, 'from:"all" replays the whole log');
  assert.deepEqual(all.seqs(), [1, 2]);
});

test('messages appended by another handle are delivered to live subscribers', async (t) => {
  const dir = tempProject('subscribe-multi-handle');
  const writer = await openBus(dir);
  const listener = await openBus(dir, { pollIntervalMs: 5 });
  t.after(async () => {
    await writer.close();
    await listener.close();
  });

  const got: Envelope[] = [];
  const off = listener.subscribe('work.*', (message) => {
    got.push(message);
  });
  t.after(off);

  await Promise.all([0, 1, 2, 3, 4].map((i) => writer.post({ from: 'writer', topic: 'work.item', kind: 'post', payload: { i } })));
  await waitFor(() => got.length === 5, 'all five cross-handle messages delivered');
  assert.deepEqual(
    got.map((m) => m.seq),
    [1, 2, 3, 4, 5],
  );
});
