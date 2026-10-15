import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import { test } from 'node:test';
import { BusClosedError, openBus, type Issue } from '../src/bus.ts';
import { logPath, tempProject, waitFor } from '../testing/harness.ts';

async function seed(bus: Awaited<ReturnType<typeof openBus>>): Promise<void> {
  await bus.post({ from: 'alice', topic: 'build.unit', kind: 'request', payload: { n: 1 } });
  await bus.post({ from: 'bob', topic: 'build.unit', kind: 'response', payload: { n: 2 } });
  await bus.post({ from: 'alice', topic: 'build.integration', kind: 'post', payload: { n: 3 } });
  await bus.post({ from: 'carol', topic: 'deploy', kind: 'claim', payload: { n: 4 } });
  await bus.post({ from: 'bob', topic: 'deploy', kind: 'release', payload: { n: 5 } });
}

test('read returns everything oldest first', async (t) => {
  const bus = await openBus(tempProject('read-all'));
  t.after(() => bus.close());
  await seed(bus);

  const all = await bus.read();
  assert.deepEqual(
    all.map((m) => m.seq),
    [1, 2, 3, 4, 5],
  );
  assert.deepEqual(
    all.map((m) => m.topic),
    ['build.unit', 'build.unit', 'build.integration', 'deploy', 'deploy'],
  );
});

test('read filters: since is exclusive, topic/from/kind/limit apply in order', async (t) => {
  const bus = await openBus(tempProject('read-filters'));
  t.after(() => bus.close());
  await seed(bus);

  assert.deepEqual(
    (await bus.read({ since: 3 })).map((m) => m.seq),
    [4, 5],
  );
  assert.deepEqual(
    (await bus.read({ since: 5 })).map((m) => m.seq),
    [],
  );
  assert.deepEqual(
    (await bus.read({ topic: 'build.unit' })).map((m) => m.seq),
    [1, 2],
  );
  assert.deepEqual(
    (await bus.read({ topic: 'build.*' })).map((m) => m.seq),
    [1, 2, 3],
  );
  assert.deepEqual(
    (await bus.read({ topic: '*' })).map((m) => m.seq),
    [1, 2, 3, 4, 5],
  );
  assert.deepEqual(
    (await bus.read({ from: 'alice' })).map((m) => m.seq),
    [1, 3],
  );
  assert.deepEqual(
    (await bus.read({ kind: 'claim' })).map((m) => m.seq),
    [4],
  );
  assert.deepEqual(
    (await bus.read({ limit: 2 })).map((m) => m.seq),
    [1, 2],
  );
  assert.deepEqual(
    (await bus.read({ from: 'alice', topic: 'build.*', since: 1, limit: 1 })).map((m) => m.seq),
    [3],
  );
  assert.deepEqual(await bus.read({ topic: 'nope' }), []);
  await assert.rejects(() => bus.read({ since: -1 }), /since must be/);
  await assert.rejects(() => bus.read({ limit: 1.5 }), /limit must be/);
  await assert.rejects(() => bus.read({ topic: 'a*b' }), /unsupported pattern/);
});

test('read and stats on an empty bus', async (t) => {
  const bus = await openBus(tempProject('read-empty'));
  t.after(() => bus.close());

  assert.deepEqual(await bus.read(), []);
  const stats = await bus.stats();
  assert.equal(stats.messages, 0);
  assert.equal(stats.firstSeq, null);
  assert.equal(stats.lastSeq, null);
  assert.equal(stats.logBytes, 0);
  assert.equal(stats.corruptLines, 0);
  assert.deepEqual(stats.byTopic, {});
});

test('stats counts messages by topic, kind and agent', async (t) => {
  const bus = await openBus(tempProject('stats'));
  t.after(() => bus.close());
  await seed(bus);

  const stats = await bus.stats();
  assert.equal(stats.messages, 5);
  assert.equal(stats.firstSeq, 1);
  assert.equal(stats.lastSeq, 5);
  assert.deepEqual(stats.byTopic, { 'build.unit': 2, 'build.integration': 1, deploy: 2 });
  assert.deepEqual(stats.byKind, { request: 1, response: 1, post: 1, claim: 1, release: 1 });
  assert.deepEqual(stats.byAgent, { alice: 2, bob: 2, carol: 1 });
  assert.equal(stats.corruptLines, 0);
  assert.equal(stats.appends, 5);
  assert.ok(stats.logBytes > 0);
  assert.equal(stats.logBytes, fs.statSync(logPath(bus.projectDir)).size);
  assert.ok(stats.oldestAt !== null && stats.newestAt !== null && stats.oldestAt <= stats.newestAt);
});

test('a half-written trailing line is skipped, reported, and does not break the next append', async (t) => {
  const dir = tempProject('truncated');
  const bus = await openBus(dir);
  t.after(() => bus.close());
  await seed(bus);

  // Simulate a writer that died mid-append: bytes with no newline terminator.
  const fragment = '{"v":1,"id":"aaaa';
  fs.appendFileSync(logPath(dir), fragment);

  const issues: Issue[] = [];
  const observer = await openBus(dir, { onIssue: (issue) => issues.push(issue) });
  t.after(() => observer.close());

  const messages = await observer.read();
  assert.deepEqual(
    messages.map((m) => m.seq),
    [1, 2, 3, 4, 5],
    'every good message survives the fragment',
  );
  const truncated = observer.issues().find((i) => i.kind === 'truncated-line');
  assert.ok(truncated, 'the truncated line is reported');
  assert.equal(truncated.offset, fs.statSync(logPath(dir)).size - fragment.length);
  assert.equal((await observer.stats()).corruptLines, 1);
  assert.equal(issues.length, 1, 'onIssue fires once per distinct problem');

  // The next append terminates the fragment and starts on a clean line.
  const next = await bus.post({ from: 'carol', topic: 'deploy', kind: 'note', payload: { n: 6 } });
  assert.equal(next.seq, 6, 'seq continues from the last complete record');
  const after = await observer.read();
  assert.deepEqual(
    after.map((m) => m.seq),
    [1, 2, 3, 4, 5, 6],
  );
  assert.deepEqual(after[5]!.payload, { n: 6 }, 'the new record is intact and was not glued to the fragment');
  assert.equal((await observer.stats()).corruptLines, 1, 'the fragment is one corrupt line, not a broken new one');
});

test('a corrupt complete line in the middle is skipped and reported, never reordered', async (t) => {
  const dir = tempProject('corrupt');
  const bus = await openBus(dir);
  t.after(() => bus.close());
  await seed(bus);

  // A syntactically valid JSON object that is not a valid envelope, plus a
  // completely unparsable line, both newline terminated.
  fs.appendFileSync(logPath(dir), '{"v":1,"id":"not-an-envelope"}\n');
  fs.appendFileSync(logPath(dir), 'garbage\n');
  const sixth = await bus.post({ from: 'carol', topic: 'deploy', kind: 'note', payload: { n: 6 } });
  assert.equal(sixth.seq, 6);

  const messages = await bus.read();
  assert.deepEqual(
    messages.map((m) => m.seq),
    [1, 2, 3, 4, 5, 6],
    'good messages keep their seq order',
  );
  const issues = bus.issues();
  assert.equal(issues.filter((i) => i.kind === 'corrupt-line').length, 2);
  assert.deepEqual(
    issues.map((i) => i.offset),
    [...issues.map((i) => i.offset)].sort((a, b) => a - b),
    'issues are reported in file order',
  );
  assert.equal((await bus.stats()).corruptLines, 2);
  assert.equal(await bus.replay(() => undefined), 6, 'replay feeds exactly the valid messages');
});

test('a line whose id does not match its content is reported but still readable', async (t) => {
  const dir = tempProject('idmismatch');
  const bus = await openBus(dir);
  await bus.post({ from: 'alice', topic: 'build', kind: 'post', payload: { n: 1 } });
  await bus.close();

  const raw = fs.readFileSync(logPath(dir), 'utf8');
  fs.writeFileSync(logPath(dir), raw.replace(/"id":"[0-9a-f]{32}"/, `"id":"${'a'.repeat(32)}"`));

  const reopened = await openBus(dir);
  t.after(() => reopened.close());
  const messages = await reopened.read();
  assert.equal(messages.length, 1);
  const issue = reopened.issues().find((i) => i.kind === 'id-mismatch');
  assert.ok(issue, 'an id-mismatch is reported');
  assert.match(issue.detail, /content-addressed id/);
});

test('read() sees bytes written by another process, and closed handles refuse work', async (t) => {
  const dir = tempProject('cross-read');
  const writer = await openBus(dir);
  const reader = await openBus(dir);
  await writer.post({ from: 'alice', topic: 'build', kind: 'post', payload: { n: 1 } });
  await writer.post({ from: 'alice', topic: 'build', kind: 'post', payload: { n: 2 } });

  await waitFor(async () => (await reader.read()).length === 2, 'reader sees both messages');
  assert.equal(reader.lastSeq(), 2);

  await reader.close();
  await assert.rejects(() => reader.read(), BusClosedError);
  await assert.rejects(() => reader.post({ from: 'a', topic: 't', kind: 'post', payload: null }), BusClosedError);
  await assert.rejects(() => reader.stats(), BusClosedError);
  await reader.close(); // idempotent
  await writer.close();
});
