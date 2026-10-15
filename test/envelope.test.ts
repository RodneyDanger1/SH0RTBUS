import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { test } from 'node:test';
import {
  BusValidationError,
  KINDS,
  computeId,
  openBus,
  parseEnvelope,
  serializeEnvelope,
  type Envelope,
  type Kind,
} from '../src/bus.ts';
import { tempProject } from '../testing/harness.ts';

const base = { from: 'alice', topic: 'build', kind: 'post' as Kind, payload: { b: 2, a: 1 }, refs: [], causationId: null };

test('id is content-addressed: identical logical messages agree across buses', async (t) => {
  const one = await openBus(tempProject('id-a'));
  const two = await openBus(tempProject('id-b'));
  t.after(async () => {
    await one.close();
    await two.close();
  });

  const first = await one.post({ ...base, payload: { b: [1, 2], a: { y: true, x: null } } });
  const second = await two.post({ ...base, payload: { a: { x: null, y: true }, b: [1, 2] } });

  assert.match(first.id, /^[0-9a-f]{32}$/);
  assert.equal(first.id, second.id, 'same logical message must produce the same id');
  assert.equal(first.seq, 1);
  assert.equal(second.seq, 1);
  const later = await one.post({ from: 'alice', topic: 'build', kind: 'post', payload: {} });
  assert.equal(later.seq, 2, 'seq is per-log: the same id can carry a different seq in another log');
});

test('id changes when any single field changes', () => {
  const source = {
    from: 'alice',
    topic: 'build',
    kind: 'post' as Kind,
    payload: { n: 1, tags: ['x'] },
    refs: ['r1'],
    causationId: 'deadbeef',
  };
  const id = computeId(source);
  const variants: Array<[string, Parameters<typeof computeId>[0]]> = [
    ['from', { ...source, from: 'bob' }],
    ['topic', { ...source, topic: 'build.x' }],
    ['kind', { ...source, kind: 'note' }],
    ['payload value', { ...source, payload: { n: 2, tags: ['x'] } }],
    ['payload shape', { ...source, payload: { n: 1, tags: ['x', 'y'] } }],
    ['refs', { ...source, refs: ['r2'] }],
    ['refs order', { ...source, refs: ['r2', 'r1'] }],
    ['causationId', { ...source, causationId: 'feedface' }],
  ];
  const seen = new Set([id]);
  for (const [label, variant] of variants) {
    const next = computeId(variant);
    assert.notEqual(next, id, `changing ${label} must change the id`);
    assert.equal(seen.has(next), false, `${label} collided with another variant`);
    seen.add(next);
  }
});

test('id ignores payload key order but not payload types', () => {
  const a = computeId({ ...base, payload: { z: 1, a: { m: [1, { q: 2, p: 3 }] }, k: 'v' } });
  const b = computeId({ ...base, payload: { k: 'v', a: { m: [1, { p: 3, q: 2 }] }, z: 1 } });
  assert.equal(a, b);
  assert.notEqual(a, computeId({ ...base, payload: { z: '1', a: { m: [1, { q: 2, p: 3 }] }, k: 'v' } }));
});

test('post stamps a monotonic seq, an ISO-8601 UTC timestamp and defaults', async (t) => {
  const dir = tempProject('stamp');
  const bus = await openBus(dir);
  t.after(() => bus.close());

  const before = Date.now();
  const first = await bus.post({ from: 'alice', topic: 'build', kind: 'post', payload: null });
  const second = await bus.post({ from: 'alice', topic: 'build.x', kind: 'note', payload: { i: 2 } });
  const after = Date.now();

  assert.equal(first.seq, 1);
  assert.equal(second.seq, 2);
  assert.deepEqual(first.refs, []);
  assert.equal(first.causationId, null);
  assert.equal(first.v, 1);
  assert.match(first.at, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
  const at = Date.parse(first.at);
  assert.ok(at >= before - 1 && at <= after + 1, `${first.at} must be stamped at append time`);
  assert.deepEqual(first.payload, null);
});

test('post preserves refs and causationId, and every documented kind is accepted', async (t) => {
  const bus = await openBus(tempProject('kinds'));
  t.after(() => bus.close());

  for (const [index, kind] of KINDS.entries()) {
    const envelope = await bus.post({
      from: 'alice',
      topic: 'build',
      kind,
      payload: { kind },
      refs: ['a', 'b'],
      causationId: 'abc123',
    });
    assert.equal(envelope.kind, kind);
    assert.deepEqual(envelope.refs, ['a', 'b']);
    assert.equal(envelope.causationId, 'abc123');
    assert.equal(envelope.seq, index + 1);
  }
  const wrong = bus.post.bind(bus);
  await assert.rejects(
    () => wrong({ from: 'alice', topic: 'build', kind: 'shrug' as Kind, payload: null }),
    BusValidationError,
  );
});

test('post rejects values that cannot be canonicalized', async (t) => {
  const bus = await openBus(tempProject('reject'));
  t.after(() => bus.close());
  const post = (payload: unknown) => bus.post({ from: 'a', topic: 't', kind: 'post', payload: payload as never });

  await assert.rejects(() => bus.post({ from: '', topic: 't', kind: 'post', payload: null }), BusValidationError);
  await assert.rejects(() => post(undefined), /payload is required/);
  await assert.rejects(() => post({ n: Number.NaN }), /finite/);
  await assert.rejects(() => post({ n: Number.POSITIVE_INFINITY }), /finite/);
  await assert.rejects(() => post({ fn: () => 1 }), /unsupported value/);
  await assert.rejects(() => post({ big: 1n }), /unsupported value/);
  await assert.rejects(() => post(new Date()), /plain objects/);
  const cyclic: Record<string, unknown> = { name: 'loop' };
  cyclic.self = cyclic;
  await assert.rejects(() => post(cyclic), /cyclic/);
  await assert.rejects(() => bus.post({ from: 'a b', topic: 't', kind: 'post', payload: null }), /whitespace/);
  await assert.rejects(() => bus.post({ from: 'a', topic: 't\n2', kind: 'post', payload: null }), /whitespace|control/);
  await assert.rejects(() => bus.post({ from: 'a', topic: 't', kind: 'post', payload: null, refs: [''] }), /ref must be/);

  const log = fs.readFileSync(path.join(bus.dir, 'bus.ndjson'), 'utf8');
  assert.equal(log, '', 'a rejected post must not write a line');
});

test('a stored line round-trips through serialize/parse and is rejected when tampered', async (t) => {
  const bus = await openBus(tempProject('roundtrip'));
  t.after(() => bus.close());
  const stored = await bus.post({ from: 'alice', topic: 'build', kind: 'post', payload: { n: 1 } });

  const line = serializeEnvelope(stored);
  const parsed = parseEnvelope(line);
  assert.equal(parsed.ok, true);
  if (!parsed.ok) return;
  assert.deepEqual(parsed.envelope, stored);

  assert.equal(parseEnvelope('{').ok, false);
  assert.equal(parseEnvelope('{"v":2}').ok, false);
  assert.equal(parseEnvelope(serializeEnvelope({ ...stored, id: 'XYZ' })).ok, false);
  assert.equal(parseEnvelope(serializeEnvelope({ ...stored, seq: 0 })).ok, false);
  assert.equal(parseEnvelope(serializeEnvelope({ ...stored, at: '2026-01-01' })).ok, false);
  assert.equal(parseEnvelope(serializeEnvelope({ ...stored, kind: 'shrug' as Kind })).ok, false);
  assert.equal(parseEnvelope('[]').ok, false);
  const withoutPayload = serializeEnvelope(stored).replace('"payload":{"n":1},', '');
  assert.equal(parseEnvelope(withoutPayload).ok, false);
});

test('openBus with create:false refuses to create a bus, and openBus creates the layout otherwise', async () => {
  const dir = tempProject('create');
  await assert.rejects(() => openBus(dir, { create: false }), /no SH0RTBUS log/);
  assert.equal(fs.existsSync(path.join(dir, '.shortbus')), false, 'create:false must not touch the filesystem');

  const bus = await openBus(dir);
  const files = fs.readdirSync(path.join(dir, '.shortbus')).sort();
  assert.deepEqual(files, ['bus.ndjson', 'meta.json']);
  const meta = JSON.parse(fs.readFileSync(bus.metaPath, 'utf8')) as Record<string, unknown>;
  assert.equal(meta.v, 1);
  assert.equal(meta.project, path.resolve(dir));
  assert.equal(meta.lastSeq, 0);
  await bus.close();
});

test('the log line is NDJSON with the documented key order', async (t) => {
  const bus = await openBus(tempProject('keys'));
  t.after(() => bus.close());
  const stored: Envelope = await bus.post({ from: 'alice', topic: 'build', kind: 'post', payload: { n: 1 } });
  const line = fs.readFileSync(path.join(bus.dir, 'bus.ndjson'), 'utf8');
  assert.ok(line.endsWith('\n'), 'every record is newline terminated');
  assert.deepEqual(Object.keys(JSON.parse(line) as object), [
    'v',
    'id',
    'at',
    'from',
    'topic',
    'kind',
    'payload',
    'refs',
    'causationId',
    'seq',
  ]);
  assert.equal(line, `${serializeEnvelope(stored)}\n`);
});
