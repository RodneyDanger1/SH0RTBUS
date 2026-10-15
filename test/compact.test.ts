import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import { test } from 'node:test';
import { BusValidationError, openBus } from '../src/bus.ts';
import { logPath, tempProject } from '../testing/harness.ts';

function lines(dir: string): string[] {
  const text = fs.readFileSync(logPath(dir), 'utf8');
  return text.length === 0 ? [] : text.split('\n').slice(0, -1);
}

test('compact({ keepLastN }) keeps the newest messages byte-identically and reports the rest', async (t) => {
  const dir = tempProject('compact-n');
  const bus = await openBus(dir);
  t.after(() => bus.close());

  for (let i = 1; i <= 6; i += 1) {
    await bus.post({ from: 'alice', topic: 'work', kind: 'post', payload: { i } });
  }
  const before = lines(dir);
  const beforeIds = before.map((line) => (JSON.parse(line) as { id: string }).id);
  const beforeBytes = fs.statSync(logPath(dir)).size;

  const report = await bus.compact({ keepLastN: 2 });
  assert.deepEqual(report, {
    kept: 2,
    dropped: 4,
    droppedCorrupt: 0,
    beforeBytes,
    afterBytes: fs.statSync(logPath(dir)).size,
    firstSeq: 5,
    lastSeq: 6,
  });
  assert.equal(report.afterBytes, Buffer.byteLength(`${before[4]!}\n${before[5]!}\n`, 'utf8'));

  const after = lines(dir);
  assert.deepEqual(after, [before[4]!, before[5]!], 'retained lines are the original bytes, in order');
  assert.deepEqual(
    after.map((line) => (JSON.parse(line) as { id: string }).id),
    beforeIds.slice(4),
    'every retained id survives',
  );

  const messages = await bus.read();
  assert.deepEqual(
    messages.map((m) => m.seq),
    [5, 6],
  );
  assert.deepEqual(messages.map((m) => m.payload), [{ i: 5 }, { i: 6 }]);
  const stats = await bus.stats();
  assert.equal(stats.firstSeq, 5);
  assert.equal(stats.messages, 2);
});

test('compact({ keepSince }) keeps a seq window and seq continuity is preserved by the next append', async (t) => {
  const dir = tempProject('compact-since');
  const bus = await openBus(dir);
  const other = await openBus(dir);
  t.after(async () => {
    await bus.close();
    await other.close();
  });

  for (let i = 1; i <= 5; i += 1) {
    await bus.post({ from: 'alice', topic: 'work', kind: 'post', payload: { i } });
  }

  const report = await bus.compact({ keepSince: 3 });
  assert.equal(report.kept, 3);
  assert.equal(report.dropped, 2);
  assert.equal(report.firstSeq, 3);
  assert.equal(report.lastSeq, 5);

  const next = await bus.post({ from: 'alice', topic: 'work', kind: 'post', payload: { i: 6 } });
  assert.equal(next.seq, 6, 'seq continues after the dropped prefix; it is never renumbered or reused');
  assert.deepEqual(
    (await other.read()).map((m) => m.seq),
    [3, 4, 5, 6],
    'another handle sees the compacted log and the new message',
  );

  const keepNothing = await bus.compact({ keepSince: 99 });
  assert.equal(keepNothing.kept, 0);
  assert.equal(keepNothing.dropped, 4);
  assert.equal(keepNothing.afterBytes, 0);
  assert.deepEqual(await bus.read(), []);
  const afterEmpty = await bus.post({ from: 'alice', topic: 'work', kind: 'post', payload: { i: 7 } });
  assert.equal(afterEmpty.seq, 7, 'seq still continues from the highest ever assigned');
});

test('compact drops corrupt lines too, and says how many', async (t) => {
  const dir = tempProject('compact-corrupt');
  const bus = await openBus(dir);
  t.after(() => bus.close());

  await bus.post({ from: 'alice', topic: 'work', kind: 'post', payload: { i: 1 } });
  fs.appendFileSync(logPath(dir), 'not json\n');
  fs.appendFileSync(logPath(dir), '{"v":1}\n');
  fs.appendFileSync(logPath(dir), 'half-written'); // truncated, no newline
  const report = await bus.compact({ keepLastN: 1 });

  assert.equal(report.kept, 1);
  assert.equal(report.dropped, 0);
  assert.equal(report.droppedCorrupt, 3, 'two corrupt lines plus the truncated fragment');
  assert.equal(lines(dir).length, 1);
  assert.deepEqual((await bus.read()).map((m) => m.seq), [1]);
  assert.equal((await bus.stats()).corruptLines, 0, 'the log is clean after compaction');
});

test('compact validates its arguments and refuses an ambiguous request', async (t) => {
  const bus = await openBus(tempProject('compact-args'));
  t.after(() => bus.close());
  await assert.rejects(() => bus.compact({}), /exactly one of/);
  await assert.rejects(() => bus.compact({ keepLastN: 1, keepSince: 1 }), /exactly one of/);
  await assert.rejects(() => bus.compact({ keepLastN: -1 }), BusValidationError);
  await assert.rejects(() => bus.compact({ keepSince: 0 }), /keepSince must be/);
  await assert.rejects(() => bus.compact({ keepLastN: 1.5 }), /keepLastN must be/);
});

test('a reader never sees a half-written log while another handle compacts repeatedly', async (t) => {
  const dir = tempProject('compact-atomic');
  const writer = await openBus(dir);
  const reader = await openBus(dir);
  t.after(async () => {
    await writer.close();
    await reader.close();
  });

  const posted = new Set<string>();
  for (let i = 0; i < 30; i += 1) {
    const envelope = await writer.post({ from: 'writer', topic: 'work', kind: 'post', payload: { i } });
    posted.add(envelope.id);
  }

  const compactions = (async () => {
    for (let i = 0; i < 6; i += 1) await writer.compact({ keepLastN: 12 });
  })();
  const reads = (async () => {
    for (let i = 0; i < 12; i += 1) {
      const messages = await reader.read();
      const seqs = messages.map((m) => m.seq);
      assert.deepEqual(
        seqs,
        [...seqs].sort((a, b) => a - b),
        'read never returns a half-renamed or reordered log',
      );
      assert.equal(reader.issues().length, 0, `no issues while compacting: ${JSON.stringify(reader.issues())}`);
      for (const message of messages) assert.ok(posted.has(message.id), 'every read message was really posted');
    }
  })();
  await Promise.all([compactions, reads]);

  const final = await reader.read();
  assert.equal(final.length, 12);
  assert.deepEqual(
    final.map((m) => m.seq),
    Array.from({ length: 12 }, (_unused, i) => 19 + i),
    'keepLastN:12 retains the last twelve messages, whatever the interleaving',
  );
});
