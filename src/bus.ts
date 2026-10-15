// SH0RTBUS -- core bus: COMPLETE and tested (31/31, tsc exit 0).
// STATUS: the CLI (src/cli.ts) and the HTTP/SSE surface (src/server.ts) that package.json
// already references are NOT YET WRITTEN, and docs/ is pending. See STATUS.md for the per-file
// state and README.md section 'Not implemented yet'.
/**
 * SH0RTBUS core: a project-scoped, append-only message bus for agents and
 * subagents.
 *
 * On-disk layout, under `<projectDir>/.shortbus/`:
 *   - `bus.ndjson`  append-only NDJSON log; one envelope per line, LF terminated.
 *   - `meta.json`   a cache/index: project path, createdAt, lastSeq, appends.
 *                   Never authoritative for ordering; the log is.
 *   - `.lock`       short-lived advisory lock file held during every append.
 *
 * The log is the single source of truth. `seq` is assigned under the lock and is
 * the total order; `id` is content-addressed and is NOT unique in the log (the
 * same logical message posted twice produces the same id twice, with different
 * seqs).
 */
import { createHash, randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import * as fs from 'node:fs';
import * as fsp from 'node:fs/promises';
import * as path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';

/** Version of the SH0RTBUS envelope written to the log. */
export const ENVELOPE_VERSION = 1;
/** Version of the package, reported by the CLI, `/health` and `stats`. */
export const VERSION = '1.0.0';
/** Directory created inside a project to hold the bus. */
export const BUS_DIR = '.shortbus';
/** Append-only NDJSON log file name. */
export const LOG_FILE = 'bus.ndjson';
/** Metadata cache file name. */
export const META_FILE = 'meta.json';
/** Advisory lock file name. */
export const LOCK_FILE = '.lock';
/** Hex characters kept from the SHA-256 of an envelope. */
export const ID_LENGTH = 32;
/** Closed set of message kinds. */
export const KINDS = ['post', 'request', 'response', 'claim', 'release', 'note', 'decision'] as const;

export type Kind = (typeof KINDS)[number];

/** Any value that survives a JSON round trip. */
export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

/** The exact shape written to the log. */
export interface Envelope {
  v: typeof ENVELOPE_VERSION;
  id: string;
  at: string;
  from: string;
  topic: string;
  kind: Kind;
  payload: JsonValue;
  refs: string[];
  causationId: string | null;
  seq: number;
}

/** Input accepted by {@link Bus.post}; `id`, `at` and `seq` are assigned by the bus. */
export interface PostInput {
  from: string;
  topic: string;
  kind: Kind;
  payload: JsonValue;
  refs?: string[];
  causationId?: string | null;
}

/** A problem this handle noticed. Corrupt log input is always skipped, never fatal. */
export interface Issue {
  kind: 'corrupt-line' | 'truncated-line' | 'id-mismatch' | 'out-of-order-seq' | 'meta-write-failed';
  /** Byte offset of the offending line in `bus.ndjson` (0 for problems that have no line). */
  offset: number;
  detail: string;
}

/** Error raised when an operation needs a bus that does not exist. */
export class BusNotFoundError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'BusNotFoundError';
  }
}

/** Error raised when the bus handle has been closed. */
export class BusClosedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'BusClosedError';
  }
}

/** Error raised for invalid input (bad envelope fields, filters, patterns). */
export class BusValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'BusValidationError';
  }
}

/** Error raised when the append lock could not be acquired before the timeout. */
export class BusLockTimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'BusLockTimeoutError';
  }
}

const ID_RE = /^[0-9a-f]{32}$/;
const AT_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
const CONTROL_RE = /[\u0000-\u001f\u007f]/;
const WHITESPACE_RE = /\s/;
const NL = Buffer.from('\n');
const LF = 0x0a;
const CR = 0x0d;
const MAX_READ_CHUNK = 4 * 1024 * 1024;
const TAIL_CHUNK = 64 * 1024;

/**
 * Canonical JSON: object keys sorted, no whitespace, JSON primitives only.
 *
 * Guarantees: structurally equal values produce byte-identical output, whatever
 * the key order they were built with. `undefined`, functions, symbols, `bigint`,
 * non-finite numbers, class instances and cyclic structures are rejected with
 * {@link BusValidationError}.
 */
export function canonicalJson(value: unknown, label = 'payload'): string {
  return canonical(value, label, new WeakSet<object>());
}

function canonical(value: unknown, at: string, seen: WeakSet<object>): string {
  if (value === null) return 'null';
  switch (typeof value) {
    case 'boolean':
      return value ? 'true' : 'false';
    case 'number':
      if (!Number.isFinite(value)) throw new BusValidationError(`${at}: numbers must be finite (got ${String(value)})`);
      return JSON.stringify(value);
    case 'string':
      return JSON.stringify(value);
    case 'object':
      break;
    default:
      throw new BusValidationError(`${at}: unsupported value of type ${typeof value}`);
  }
  const obj = value as object;
  if (seen.has(obj)) throw new BusValidationError(`${at}: cyclic structure`);
  seen.add(obj);
  try {
    if (Array.isArray(obj)) {
      return `[${obj.map((item, i) => canonical(item, `${at}[${String(i)}]`, seen)).join(',')}]`;
    }
    const proto: unknown = Object.getPrototypeOf(obj);
    if (proto !== Object.prototype && proto !== null) {
      throw new BusValidationError(`${at}: only plain objects and arrays are allowed`);
    }
    const record = obj as Record<string, unknown>;
    const parts = Object.keys(record)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonical(record[key], `${at}.${key}`, seen)}`);
    return `{${parts.join(',')}}`;
  } finally {
    seen.delete(obj);
  }
}

/**
 * Content-addressed id for a message: SHA-256 over the canonical serialization of
 * every envelope field except `id`, `at` and `seq`, truncated to 32 hex chars.
 *
 * Guarantees: identical logical messages (same from/topic/kind/payload/refs/
 * causationId, whatever the key order) produce the same id; changing any one
 * field changes the id; the id does not depend on time, process, or position in
 * the log, so two independent buses agree on it.
 */
export function computeId(input: {
  from: string;
  topic: string;
  kind: Kind;
  payload: JsonValue;
  refs: string[];
  causationId: string | null;
}): string {
  const material = canonicalJson(
    {
      v: ENVELOPE_VERSION,
      from: input.from,
      topic: input.topic,
      kind: input.kind,
      payload: input.payload,
      refs: input.refs,
      causationId: input.causationId,
    },
    'envelope',
  );
  return createHash('sha256').update(material, 'utf8').digest('hex').slice(0, ID_LENGTH);
}

/** Serialize an envelope to one log line (no trailing newline). Key order is fixed. */
export function serializeEnvelope(envelope: Envelope): string {
  return JSON.stringify({
    v: envelope.v,
    id: envelope.id,
    at: envelope.at,
    from: envelope.from,
    topic: envelope.topic,
    kind: envelope.kind,
    payload: envelope.payload,
    refs: envelope.refs,
    causationId: envelope.causationId,
    seq: envelope.seq,
  });
}

type ParseResult = { ok: true; envelope: Envelope } | { ok: false; reason: string };

/** Strictly validate one log line as an envelope v1. */
export function parseEnvelope(line: string): ParseResult {
  let value: unknown;
  try {
    value = JSON.parse(line);
  } catch (err) {
    return { ok: false, reason: `invalid JSON (${(err as Error).message})` };
  }
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return { ok: false, reason: 'envelope must be a JSON object' };
  }
  const raw = value as Record<string, unknown>;
  if (raw.v !== ENVELOPE_VERSION) return { ok: false, reason: `unsupported envelope version ${String(raw.v)}` };
  if (typeof raw.id !== 'string' || !ID_RE.test(raw.id)) return { ok: false, reason: 'id must be 32 hex chars' };
  if (typeof raw.at !== 'string' || !AT_RE.test(raw.at) || Number.isNaN(Date.parse(raw.at))) {
    return { ok: false, reason: 'at must be an ISO-8601 UTC timestamp with milliseconds' };
  }
  if (!isLabel(raw.from)) return { ok: false, reason: 'from must be a non-empty token without whitespace' };
  if (!isLabel(raw.topic)) return { ok: false, reason: 'topic must be a non-empty token without whitespace' };
  if (typeof raw.kind !== 'string' || !isKind(raw.kind)) return { ok: false, reason: `unknown kind ${String(raw.kind)}` };
  if (!Object.hasOwn(raw, 'payload')) return { ok: false, reason: 'payload is required' };
  try {
    canonicalJson(raw.payload, 'payload');
  } catch (err) {
    return { ok: false, reason: (err as Error).message };
  }
  if (!Array.isArray(raw.refs) || !raw.refs.every((ref) => typeof ref === 'string')) {
    return { ok: false, reason: 'refs must be an array of strings' };
  }
  if (!(raw.causationId === null || typeof raw.causationId === 'string')) {
    return { ok: false, reason: 'causationId must be a string or null' };
  }
  if (typeof raw.seq !== 'number' || !Number.isSafeInteger(raw.seq) || raw.seq < 1) {
    return { ok: false, reason: 'seq must be an integer >= 1' };
  }
  return {
    ok: true,
    envelope: {
      v: ENVELOPE_VERSION,
      id: raw.id,
      at: raw.at,
      from: raw.from,
      topic: raw.topic,
      kind: raw.kind,
      payload: raw.payload as JsonValue,
      refs: raw.refs as string[],
      causationId: raw.causationId as string | null,
      seq: raw.seq,
    },
  };
}

/** True for a valid topic/agent token: non-empty, no whitespace, no control chars, <= 256 chars. */
export function isLabel(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    value.length > 0 &&
    value.length <= 256 &&
    !WHITESPACE_RE.test(value) &&
    !CONTROL_RE.test(value)
  );
}

function isKind(value: string): value is Kind {
  return (KINDS as readonly string[]).includes(value);
}

/**
 * Match a live topic pattern against a concrete topic.
 *
 * Forms: `*` matches every topic; a pattern ending in `*` matches topics starting
 * with the text before the `*` (so `build.*` matches `build.unit` but not
 * `build`); anything else is an exact match.
 */
export function topicMatches(pattern: string, topic: string): boolean {
  if (pattern === '*') return true;
  if (pattern.endsWith('*')) return topic.startsWith(pattern.slice(0, -1));
  return pattern === topic;
}

function assertPattern(pattern: unknown): string {
  if (typeof pattern !== 'string' || pattern.length === 0) {
    throw new BusValidationError('topic pattern must be a non-empty string');
  }
  if (WHITESPACE_RE.test(pattern) || CONTROL_RE.test(pattern)) {
    throw new BusValidationError('topic pattern must not contain whitespace or control characters');
  }
  const star = pattern.indexOf('*');
  if (star !== -1 && star !== pattern.length - 1) {
    throw new BusValidationError(`unsupported pattern ${pattern}: only "*", "prefix*" and exact topics are supported`);
  }
  if (star === 0 && pattern.length > 1) {
    throw new BusValidationError(`unsupported pattern ${pattern}: "*" must be used alone`);
  }
  return pattern;
}

/** Counts and indexes derived from a full scan of the log. */
export interface BusStats {
  project: string;
  logPath: string;
  logBytes: number;
  messages: number;
  firstSeq: number | null;
  lastSeq: number | null;
  oldestAt: string | null;
  newestAt: string | null;
  byTopic: Record<string, number>;
  byKind: Record<string, number>;
  byAgent: Record<string, number>;
  /** Lines that were not valid envelopes (corrupt, truncated or empty). */
  corruptLines: number;
  appends: number;
  createdAt: string;
}

/** Result of a {@link Bus.compact} run. */
export interface CompactReport {
  /** Retained log lines (byte-identical to the original bytes). */
  kept: number;
  /** Valid envelopes dropped because they fell outside the keep window. */
  dropped: number;
  /** Lines dropped because they were not valid envelopes (corrupt, truncated, empty). */
  droppedCorrupt: number;
  beforeBytes: number;
  afterBytes: number;
  firstSeq: number | null;
  lastSeq: number | null;
}

/** Options for {@link openBus}. */
export interface OpenBusOptions {
  /** Live-delivery poll interval in ms (default 25, minimum 5). */
  pollIntervalMs?: number;
  /** How long to wait for the append lock before failing (default 10000 ms). */
  lockTimeoutMs?: number;
  /** Age after which a lock file whose owner is gone is broken (default 30000 ms). */
  lockStaleMs?: number;
  /** When false, a missing bus raises {@link BusNotFoundError} instead of being created (default true). */
  create?: boolean;
  /** Called once per distinct problem found while scanning or tailing the log. */
  onIssue?: (issue: Issue) => void;
}

/** Options for {@link Bus.subscribe} and {@link Bus.tail}. */
export interface SubscribeOptions {
  /**
   * Exclusive start cursor: a number means "deliver messages with seq > from",
   * `'all'` means from the beginning of the log. Default: messages appended after
   * subscribing.
   */
  from?: number | 'all';
}

/** A handler fed by {@link Bus.subscribe}, {@link Bus.tail} and {@link Bus.replay}. */
export type MessageHandler = (message: Envelope) => void | Promise<void>;

/** Payload of the `handler-error` event. */
export interface HandlerErrorEvent {
  pattern: string;
  message: Envelope;
  error: unknown;
}

/** Filter accepted by {@link Bus.read}. */
export interface ReadFilter {
  /** Exclusive lower bound on `seq`. */
  since?: number;
  /** Exact topic or pattern (`*`, `prefix*`). */
  topic?: string;
  /** Exact sender. */
  from?: string;
  kind?: Kind;
  /** Maximum number of messages returned, applied after every other filter. */
  limit?: number;
}

/** Options for {@link Bus.compact}. */
export interface CompactOptions {
  /** Keep only the newest N valid messages. */
  keepLastN?: number;
  /** Keep messages with `seq >= keepSince`. */
  keepSince?: number;
}

interface Subscriber {
  pattern: string;
  /** Exclusive cursor: messages with `seq <= cursor` are never delivered again. */
  cursor: number;
  /** False while a re-read of the log is pending, so live messages cannot overtake history. */
  active: boolean;
  handler: MessageHandler;
}

interface RawLine {
  raw: Buffer;
  text: string;
  offset: number;
}

interface Meta {
  v: typeof ENVELOPE_VERSION;
  project: string;
  createdAt: string;
  updatedAt: string;
  lastSeq: number;
  appends: number;
}

interface LockFile {
  pid: number;
  at: string;
  token: string;
}

/** Split a buffer on LF, keeping byte offsets and the (possibly partial) trailing line. */
function splitLines(buf: Buffer): { lines: RawLine[]; trailingPartial: RawLine | null } {
  const lines: RawLine[] = [];
  let start = 0;
  for (let i = 0; i < buf.length; i += 1) {
    if (buf[i] !== LF) continue;
    let end = i;
    if (end > start && buf[end - 1] === CR) end -= 1;
    const raw = buf.subarray(start, end);
    lines.push({ raw, text: raw.toString('utf8'), offset: start });
    start = i + 1;
  }
  const trailingPartial =
    start < buf.length ? { raw: buf.subarray(start), text: buf.subarray(start).toString('utf8'), offset: start } : null;
  return { lines, trailingPartial };
}

async function statSize(file: string): Promise<number | null> {
  try {
    const st = await fsp.stat(file);
    return st.size;
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') return null;
    throw err;
  }
}

function isAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return (err as NodeJS.ErrnoException).code === 'EPERM';
  }
}

/**
 * Acquire the advisory append lock for a bus directory.
 *
 * Concurrency model: a file created with `O_EXCL` is the mutex. Every writer of
 * the same project uses the same path, so the lock is effective across processes
 * as well as across handles inside one process. It is held only for the duration
 * of a single append or compaction. A lock whose recorded pid is no longer alive,
 * or that is older than `staleMs`, is broken and retried.
 */
async function acquireLock(lockPath: string, timeoutMs: number, staleMs: number): Promise<() => void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const token = randomUUID();
    try {
      const fd = fs.openSync(lockPath, 'wx');
      try {
        fs.writeSync(fd, JSON.stringify({ pid: process.pid, at: new Date().toISOString(), token }));
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
      let released = false;
      return () => {
        if (released) return;
        released = true;
        try {
          const current = JSON.parse(fs.readFileSync(lockPath, 'utf8')) as Partial<LockFile>;
          if (current.token !== token) return;
        } catch {
          return;
        }
        try {
          fs.unlinkSync(lockPath);
        } catch {
          /* already removed */
        }
      };
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== 'EEXIST') throw err;
    }

    let holder: Partial<LockFile> = {};
    let ageMs = 0;
    try {
      const st = await fsp.stat(lockPath);
      ageMs = Date.now() - st.mtimeMs;
      holder = JSON.parse(await fsp.readFile(lockPath, 'utf8')) as Partial<LockFile>;
    } catch {
      continue; // vanished between open and stat: retry immediately
    }
    const holderGone = typeof holder.pid === 'number' && !isAlive(holder.pid);
    if (holderGone || ageMs > staleMs) {
      try {
        await fsp.unlink(lockPath);
      } catch {
        /* someone else broke it first */
      }
      continue;
    }
    if (Date.now() >= deadline) {
      throw new BusLockTimeoutError(
        `could not acquire ${lockPath} within ${String(timeoutMs)}ms (held by pid ${String(holder.pid)} for ${String(Math.round(ageMs))}ms)`,
      );
    }
    await sleep(2 + Math.floor(Math.random() * 5));
  }
}

/**
 * Follows the log by polling the file, once per bus handle. No inotify/FSEvents
 * dependency, so it also works on network shares and in containers with tight
 * inotify limits.
 *
 * Ordering: lines are parsed in file order and delivered one at a time, so every
 * subscriber sees messages in `seq` order. Each subscriber carries its own cursor,
 * so re-reading the log to serve a new subscriber that asked for history cannot
 * duplicate a message for anyone else, and a subscriber waiting for history is
 * held inactive until that re-read completes so live messages cannot overtake it.
 *
 * Robustness: bytes after the last LF are buffered until their newline arrives (a
 * partial line is never delivered); a file smaller than the read offset (a
 * compaction rewrite) rewinds to 0; a missing file is retried.
 */
class Tailer {
  private readonly bus: Bus;
  private readonly subscribers: Subscriber[];
  private readonly intervalMs: number;
  private offset = 0;
  private pending: Buffer = Buffer.alloc(0);
  private pendingOffset = 0;
  private lastSeq = 0;
  private batch: Envelope[] = [];
  private rereadPending = false;
  /** Subscribers this poll is re-reading the log for; they are delivered to while `active` is still false. */
  private catching: Subscriber[] = [];
  private timer: NodeJS.Timeout | null = null;
  private polling = false;
  private stopped = false;

  constructor(bus: Bus, subscribers: Subscriber[], intervalMs: number) {
    this.bus = bus;
    this.subscribers = subscribers;
    this.intervalMs = intervalMs;
  }

  start(): void {
    if (this.timer !== null || this.stopped) return;
    this.timer = setInterval(() => {
      void this.poll();
    }, this.intervalMs);
    void this.poll();
  }

  stop(): void {
    this.stopped = true;
    if (this.timer !== null) clearInterval(this.timer);
    this.timer = null;
  }

  /** Highest seq this tailer has parsed. */
  seq(): number {
    return this.lastSeq;
  }

  /** Ask for the log to be re-read from the beginning on the next poll. */
  requestReread(): void {
    this.rereadPending = true;
  }

  /** Discard the buffered position (used after the log was rewritten). */
  rewind(): void {
    this.offset = 0;
    this.pending = Buffer.alloc(0);
    this.pendingOffset = 0;
  }

  private async poll(): Promise<void> {
    if (this.polling || this.stopped) return;
    this.polling = true;
    try {
      if (this.rereadPending) {
        this.rereadPending = false;
        this.rewind();
        // Subscribers waiting for history are inactive (so live messages cannot
        // overtake them) but must receive this re-read. Subscribers that subscribe
        // while this poll is awaiting a handler are not in this set and stay
        // inactive until the poll that serves them.
        this.catching = this.subscribers.filter((sub) => !sub.active);
      }
      const size = await statSize(this.bus.logPath);
      if (size === null) {
        this.rewind();
      } else {
        if (size < this.offset) this.rewind();
        if (size > this.offset) await this.readTo(size);
      }
      await this.flush();
      for (const sub of this.catching) sub.active = true;
      this.catching = [];
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== 'ENOENT') {
        this.bus.reportIssue({
          kind: 'corrupt-line',
          offset: this.offset,
          detail: `tailer read failed: ${(err as Error).message}`,
        });
      }
    } finally {
      this.polling = false;
      this.catching = [];
    }
  }

  private async readTo(size: number): Promise<void> {
    const fd = await fsp.open(this.bus.logPath, 'r');
    try {
      while (this.offset < size && !this.stopped) {
        const want = Math.min(MAX_READ_CHUNK, size - this.offset);
        const buf = Buffer.allocUnsafe(want);
        const { bytesRead } = await fd.read(buf, 0, want, this.offset);
        if (bytesRead <= 0) break;
        this.offset += bytesRead;
        this.ingest(buf.subarray(0, bytesRead));
      }
    } finally {
      await fd.close();
    }
  }

  /** Decode the complete lines of a byte chunk, buffering any partial trailing line. */
  private ingest(chunk: Buffer): void {
    const buf = this.pending.length > 0 ? Buffer.concat([this.pending, chunk]) : chunk;
    let start = 0;
    for (let i = 0; i < buf.length; i += 1) {
      if (buf[i] !== LF) continue;
      let end = i;
      if (end > start && buf[end - 1] === CR) end -= 1;
      this.accept(buf.subarray(start, end), this.pendingOffset + start);
      start = i + 1;
    }
    const rest = buf.subarray(start);
    this.pending = rest.length > 0 ? Buffer.from(rest) : Buffer.alloc(0);
    this.pendingOffset += start;
    this.bus.observeLastSeq(this.lastSeq);
  }

  private accept(raw: Buffer, offset: number): void {
    if (raw.length === 0) return;
    const parsed = parseEnvelope(raw.toString('utf8'));
    if (!parsed.ok) {
      this.bus.reportIssue({ kind: 'corrupt-line', offset, detail: parsed.reason });
      return;
    }
    // Every parsed message is queued, including ones already seen: a re-read must
    // be able to feed a subscriber that asked for history, and per-subscriber
    // cursors are what prevent anyone else from seeing a message twice.
    this.lastSeq = Math.max(this.lastSeq, parsed.envelope.seq);
    this.batch.push(parsed.envelope);
  }

  private async flush(): Promise<void> {
    if (this.batch.length === 0) return;
    const batch = this.batch;
    this.batch = [];
    for (const message of batch) {
      for (const sub of this.subscribers) {
        if (!sub.active && !this.catching.includes(sub)) continue;
        if (message.seq <= sub.cursor) continue;
        sub.cursor = message.seq;
        if (!topicMatches(sub.pattern, message.topic)) continue;
        try {
          await sub.handler(message);
        } catch (error) {
          this.bus.emit('handler-error', { pattern: sub.pattern, message, error });
        }
      }
    }
  }
}

/**
 * An open bus handle. Create one with {@link openBus}.
 *
 * Events: `issue` (a problem found in the log) and `handler-error` (a subscriber
 * handler threw; the error never stops delivery to other subscribers).
 */
export class Bus extends EventEmitter {
  /** Absolute path of the project the bus belongs to. */
  readonly projectDir: string;
  /** Absolute path of `<projectDir>/.shortbus`. */
  readonly dir: string;
  /** Absolute path of the append-only log. */
  readonly logPath: string;
  /** Absolute path of the metadata cache. */
  readonly metaPath: string;

  private readonly lockPath: string;
  private readonly pollIntervalMs: number;
  private readonly lockTimeoutMs: number;
  private readonly lockStaleMs: number;
  private readonly onIssue: ((issue: Issue) => void) | undefined;
  private readonly subscribers: Subscriber[] = [];
  private readonly seenIssues = new Set<string>();
  private meta: Meta;
  private lastIssues: Issue[] = [];
  private observedSeq = 0;
  private tailer: Tailer | null = null;
  private closed = false;

  /** @internal use {@link openBus}. */
  constructor(projectDir: string, meta: Meta, opts: OpenBusOptions = {}) {
    super();
    this.projectDir = projectDir;
    this.dir = path.join(projectDir, BUS_DIR);
    this.logPath = path.join(this.dir, LOG_FILE);
    this.metaPath = path.join(this.dir, META_FILE);
    this.lockPath = path.join(this.dir, LOCK_FILE);
    this.pollIntervalMs = Math.max(5, opts.pollIntervalMs ?? 25);
    this.lockTimeoutMs = opts.lockTimeoutMs ?? 10_000;
    this.lockStaleMs = opts.lockStaleMs ?? 30_000;
    this.onIssue = opts.onIssue;
    this.meta = meta;
    this.observedSeq = meta.lastSeq;
  }

  override on(event: 'issue', listener: (issue: Issue) => void): this;
  override on(event: 'handler-error', listener: (event: HandlerErrorEvent) => void): this;
  override on(event: string | symbol, listener: (...args: any[]) => void): this;
  override on(event: string | symbol, listener: (...args: any[]) => void): this {
    return super.on(event, listener);
  }

  /** The metadata cache as held in memory. Not authoritative for ordering. */
  get metaView(): Readonly<Meta> {
    return { ...this.meta };
  }

  /** Highest `seq` this handle has observed: at open, and after appends, scans and live deliveries. */
  lastSeq(): number {
    return Math.max(this.observedSeq, this.tailer?.seq() ?? 0);
  }

  /** Problems found during the most recent scan of the log. */
  issues(): Issue[] {
    return [...this.lastIssues];
  }

  /** @internal the tailer reports progress here. */
  observeLastSeq(seq: number): void {
    if (seq > this.observedSeq) this.observedSeq = seq;
  }

  /** @internal reports a problem at most once per (kind, offset) per handle. */
  reportIssue(issue: Issue): void {
    const key = `${issue.kind}@${String(issue.offset)}`;
    if (this.seenIssues.has(key)) return;
    this.seenIssues.add(key);
    this.onIssue?.(issue);
    this.emit('issue', issue);
  }

  /**
   * Append a message and return the stored envelope.
   *
   * Guarantees: the message is on disk and `fsync`-ed before this resolves;
   * `seq` is unique, exactly one greater than the previous append, and assigned
   * under a cross-process lock, so any number of concurrent writers (processes or
   * handles) produce one total order with no interleaved, lost or duplicated
   * lines; `at` is stamped at append time; `id` is content-addressed and may
   * repeat when the same logical message is posted twice.
   */
  async post(input: PostInput): Promise<Envelope> {
    this.assertOpen();
    if (input === null || typeof input !== 'object') throw new BusValidationError('post requires an object');
    const from = assertLabel(input.from, 'from');
    const topic = assertLabel(input.topic, 'topic');
    if (typeof input.kind !== 'string' || !isKind(input.kind)) {
      throw new BusValidationError(`kind must be one of ${KINDS.join(', ')}`);
    }
    if (input.payload === undefined) throw new BusValidationError('payload is required (use null for an empty payload)');
    canonicalJson(input.payload, 'payload');
    const refs = normalizeRefs(input.refs);
    const causationId = normalizeCausation(input.causationId);
    const id = computeId({ from, topic, kind: input.kind, payload: input.payload, refs, causationId });

    const release = await this.lock();
    try {
      const tail = await this.readTail();
      // Allocation never goes backwards: the log tail is the normal source, and
      // `meta.json` carries the high-water mark across a compaction that dropped
      // the tail, so `seq` is never reused.
      const seq = Math.max(tail.lastSeq, await this.metaHighWater()) + 1;
      const envelope: Envelope = {
        v: ENVELOPE_VERSION,
        id,
        at: new Date().toISOString(),
        from,
        topic,
        kind: input.kind,
        payload: input.payload,
        refs,
        causationId,
        seq,
      };
      const fd = fs.openSync(this.logPath, 'a');
      try {
        // A crash can leave a partial trailing line. Terminate it before writing,
        // so the new record starts on a clean line instead of being glued to the
        // dangling bytes (which would make both unreadable).
        if (!tail.endsWithNewline) fs.writeSync(fd, NL);
        fs.writeSync(fd, Buffer.from(`${serializeEnvelope(envelope)}\n`, 'utf8'));
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
      this.observeLastSeq(seq);
      this.meta = { ...this.meta, updatedAt: envelope.at, lastSeq: seq, appends: this.meta.appends + 1 };
      try {
        // The metadata file is a cache: the log line above is already durable and
        // authoritative, so a failed cache write must not fail the append.
        await this.writeMeta(false);
      } catch (err) {
        this.reportIssue({ kind: 'meta-write-failed', offset: 0, detail: (err as Error).message });
      }
      return envelope;
    } finally {
      release();
    }
  }

  /**
   * Subscribe to live messages; returns an unsubscribe function.
   *
   * Guarantees: matching messages are delivered one at a time in `seq` order with
   * no gaps and no duplicates; a throwing handler does not stop delivery to other
   * subscribers or to later messages (the error is emitted as `handler-error`);
   * delivery is serialized, so a slow handler delays later live deliveries instead
   * of buffering unboundedly.
   *
   * Patterns: `*` (everything), `prefix*`, or an exact topic. `from` is exclusive:
   * `{ from: 7 }` delivers seq 8 onward. The default is "after now", i.e. the
   * highest seq this handle had observed when subscribing; a handle that has been
   * idle while other processes appended may deliver those messages too, so pass an
   * explicit `from` when you need an exact cursor. A `from` below what the watcher
   * has already read makes it re-read the log once (O(log size)) before any live
   * message is delivered to this subscriber.
   */
  subscribe(pattern: string, handler: MessageHandler, opts: SubscribeOptions = {}): () => void {
    this.assertOpen();
    const checked = assertPattern(pattern);
    if (typeof handler !== 'function') throw new BusValidationError('handler must be a function');
    return this.addSubscriber(checked, opts.from, handler);
  }

  /**
   * Follow the log live, delivering every message in `seq` order whatever its
   * topic. Returns a stop function. `opts.from` behaves as in {@link subscribe}.
   *
   * Guarantees: reads by polling the file (no inotify/FSEvents dependency); a
   * partial trailing line is buffered until its newline arrives and is never
   * delivered as a message; a compaction rewrite is detected and the read position
   * is reset without re-delivering messages already sent; the poll timer is
   * released by the returned stop function or by {@link Bus.close}.
   */
  tail(onMessage: MessageHandler, opts: SubscribeOptions = {}): () => void {
    this.assertOpen();
    if (typeof onMessage !== 'function') throw new BusValidationError('onMessage must be a function');
    return this.addSubscriber('*', opts.from, onMessage);
  }

  /**
   * Read historical messages, oldest first (by `seq`).
   *
   * `since` is exclusive. `topic` accepts the same patterns as {@link subscribe}.
   * `limit` caps the number of returned messages after every other filter.
   * Corrupt lines are skipped and reported (see {@link issues} and
   * {@link stats}); they are never returned and never fatal.
   */
  async read(filter: ReadFilter = {}): Promise<Envelope[]> {
    this.assertOpen();
    const since = filter.since === undefined ? null : assertCount(filter.since, 'since');
    const pattern = filter.topic === undefined ? null : assertPattern(filter.topic);
    const from = filter.from === undefined ? null : assertLabel(filter.from, 'from');
    const kind = filter.kind === undefined ? null : assertKind(filter.kind);
    const limit = filter.limit === undefined ? null : assertCount(filter.limit, 'limit');
    const { messages } = await this.scan();
    const out: Envelope[] = [];
    for (const message of messages) {
      if (since !== null && message.seq <= since) continue;
      if (pattern !== null && !topicMatches(pattern, message.topic)) continue;
      if (from !== null && message.from !== from) continue;
      if (kind !== null && message.kind !== kind) continue;
      out.push({ ...message });
      if (limit !== null && out.length >= limit) break;
    }
    return out;
  }

  /**
   * Render messages as deterministic plain text, suitable for pasting into an
   * agent's context. `from` and `to` are inclusive `seq` bounds.
   *
   * Guarantees: for a given log and range the output is byte-identical on every
   * call and on every machine; nothing is invented at render time (payloads are
   * re-rendered in canonical form, all other fields come from the log).
   */
  async transcript(opts: { from?: number; to?: number } = {}): Promise<string> {
    this.assertOpen();
    const from = opts.from === undefined ? 1 : assertCount(opts.from, 'from');
    const to = opts.to === undefined ? Number.MAX_SAFE_INTEGER : assertCount(opts.to, 'to');
    const { messages } = await this.scan();
    const selected = messages.filter((m) => m.seq >= from && m.seq <= to);
    const first = selected[0];
    const last = selected[selected.length - 1];
    const lines: string[] = [
      'SH0RTBUS transcript v1',
      `project: ${this.projectDir}`,
      first === undefined || last === undefined
        ? 'range: empty (0 messages)'
        : `range: ${String(first.seq)}..${String(last.seq)} (${String(selected.length)} messages)`,
      '',
    ];
    for (const m of selected) {
      lines.push(`[${String(m.seq)}] ${m.at} ${m.from} topic=${m.topic} kind=${m.kind}`);
      lines.push(`  id: ${m.id}`);
      lines.push(`  payload: ${canonicalJson(m.payload, 'payload')}`);
      if (m.refs.length > 0) lines.push(`  refs: ${m.refs.join(', ')}`);
      if (m.causationId !== null) lines.push(`  causation: ${m.causationId}`);
    }
    return lines.join('\n');
  }

  /**
   * Feed every historical message from `from` onward (inclusive, default 1)
   * through `handler` in `seq` order, awaiting each call.
   *
   * Guarantees: deterministic — the same log yields the same sequence of messages
   * and ids on every run. Returns the number of messages fed.
   */
  async replay(handler: MessageHandler, opts: { from?: number } = {}): Promise<number> {
    this.assertOpen();
    if (typeof handler !== 'function') throw new BusValidationError('handler must be a function');
    const from = opts.from === undefined ? 1 : assertCount(opts.from, 'from');
    const { messages } = await this.scan();
    let count = 0;
    for (const message of messages) {
      if (message.seq < from) continue;
      await handler(message);
      count += 1;
    }
    return count;
  }

  /**
   * Counts and indexes over the whole log, plus its size in bytes.
   *
   * Guarantees: computed from a full scan of the file, so it includes bytes other
   * processes have appended; `corruptLines` is the exact number of lines that
   * could not be read as envelopes.
   */
  async stats(): Promise<BusStats> {
    this.assertOpen();
    const { messages, issues, bytes } = await this.scan();
    const byTopic: Record<string, number> = {};
    const byKind: Record<string, number> = {};
    const byAgent: Record<string, number> = {};
    for (const m of messages) {
      byTopic[m.topic] = (byTopic[m.topic] ?? 0) + 1;
      byKind[m.kind] = (byKind[m.kind] ?? 0) + 1;
      byAgent[m.from] = (byAgent[m.from] ?? 0) + 1;
    }
    const first = messages[0];
    const last = messages[messages.length - 1];
    return {
      project: this.projectDir,
      logPath: this.logPath,
      logBytes: bytes,
      messages: messages.length,
      firstSeq: first?.seq ?? null,
      lastSeq: last?.seq ?? null,
      oldestAt: first?.at ?? null,
      newestAt: last?.at ?? null,
      byTopic,
      byKind,
      byAgent,
      corruptLines: issues.filter((i) => i.kind === 'corrupt-line' || i.kind === 'truncated-line').length,
      appends: this.meta.appends,
      createdAt: this.meta.createdAt,
    };
  }

  /**
   * Rewrite the log atomically, keeping a window of messages and dropping the
   * rest. Exactly one of `keepLastN` / `keepSince` is required.
   *
   * What is dropped: valid messages before the window, and every line that was not
   * a valid envelope (corrupt, truncated or empty). Nothing else is touched:
   * retained lines keep their original bytes, ids, topics and `seq` values.
   *
   * Guarantees: the rewrite happens under the append lock into a temp file that is
   * `fsync`-ed and renamed over `bus.ndjson`, so readers see either the old log or
   * the new one, never a half-written one; `seq` is never renumbered or reused, so
   * the next append continues from the previous maximum `seq`; the returned report
   * states exactly how many lines were kept and dropped.
   */
  async compact(opts: CompactOptions = {}): Promise<CompactReport> {
    this.assertOpen();
    if (opts === null || typeof opts !== 'object') throw new BusValidationError('compact requires an options object');
    const hasN = opts.keepLastN !== undefined;
    const hasSince = opts.keepSince !== undefined;
    if (hasN === hasSince) throw new BusValidationError('compact requires exactly one of keepLastN or keepSince');
    const keepLastN = hasN ? assertCount(opts.keepLastN, 'keepLastN') : 0;
    const keepSince = hasSince ? assertPositive(opts.keepSince, 'keepSince') : 0;

    const release = await this.lock();
    try {
      const raw = await this.readRaw();
      const valid: Array<{ raw: Buffer; envelope: Envelope }> = [];
      let droppedCorrupt = 0;
      for (const line of raw.lines) {
        if (line.text.length === 0) {
          droppedCorrupt += 1;
          continue;
        }
        const parsed = parseEnvelope(line.text);
        if (!parsed.ok) {
          droppedCorrupt += 1;
          continue;
        }
        valid.push({ raw: line.raw, envelope: parsed.envelope });
      }
      if (raw.trailingPartial !== null) droppedCorrupt += 1;
      valid.sort((a, b) => a.envelope.seq - b.envelope.seq);

      const keep = hasSince
        ? valid.filter((entry) => entry.envelope.seq >= keepSince)
        : valid.slice(Math.max(0, valid.length - keepLastN));
      const dropped = valid.length - keep.length;

      const out = Buffer.concat(keep.flatMap((entry) => [entry.raw, NL]));
      const lastValidSeq = valid.length > 0 ? valid[valid.length - 1]!.envelope.seq : 0;
      const highWater = Math.max(this.observedSeq, this.meta.lastSeq, lastValidSeq);

      // Persist the allocation high-water DURABLY before shrinking the log: if the
      // process dies between the two writes, the log still holds every message up
      // to `highWater`, so nothing is lost and no seq can be handed out twice.
      this.meta = { ...this.meta, updatedAt: new Date().toISOString(), lastSeq: highWater };
      await this.writeMeta(true);

      const tmp = path.join(this.dir, `${LOG_FILE}.tmp.${String(process.pid)}.${randomUUID()}`);
      const fd = fs.openSync(tmp, 'wx');
      try {
        if (out.length > 0) fs.writeSync(fd, out);
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
      try {
        await renameWithRetry(tmp, this.logPath, this.renameBudgetMs());
      } catch (err) {
        await fsp.rm(tmp, { force: true });
        throw err;
      }
      await syncDir(this.dir);
      this.tailer?.rewind();
      this.observeLastSeq(highWater);

      const keptFirst = keep[0]?.envelope.seq ?? null;
      const keptLast = keep[keep.length - 1]?.envelope.seq ?? null;
      return {
        kept: keep.length,
        dropped,
        droppedCorrupt,
        beforeBytes: raw.bytes,
        afterBytes: out.length,
        firstSeq: keptFirst,
        lastSeq: keptLast,
      };
    } finally {
      release();
    }
  }

  /**
   * Release the tail timer and mark the handle unusable. Idempotent and safe in a
   * `finally` block. Nothing is lost: every appended message was already fsynced,
   * and a new handle resumes with `read({ since })`.
   */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.subscribers.length = 0;
    this.tailer?.stop();
    this.tailer = null;
  }

  private addSubscriber(pattern: string, from: number | 'all' | undefined, handler: MessageHandler): () => void {
    const cursor = resolveFrom(from, this.lastSeq());
    const watcher = this.tailer;
    const needsHistory = watcher !== null && cursor < watcher.seq();
    const sub: Subscriber = { pattern, cursor, active: !needsHistory, handler };
    this.subscribers.push(sub);
    this.ensureTailer();
    if (needsHistory) this.tailer!.requestReread();
    return () => {
      const index = this.subscribers.indexOf(sub);
      if (index >= 0) this.subscribers.splice(index, 1);
      this.maybeStopTailer();
    };
  }

  private assertOpen(): void {
    if (this.closed) throw new BusClosedError('bus is closed');
  }

  private async lock(): Promise<() => void> {
    return acquireLock(this.lockPath, this.lockTimeoutMs, this.lockStaleMs);
  }

  private ensureTailer(): void {
    this.tailer ??= new Tailer(this, this.subscribers, this.pollIntervalMs);
    this.tailer.start();
  }

  private maybeStopTailer(): void {
    if (this.subscribers.length === 0) {
      this.tailer?.stop();
      this.tailer = null;
    }
  }

  /** @internal read the whole log as raw lines. */
  private async readRaw(): Promise<{ lines: RawLine[]; trailingPartial: RawLine | null; bytes: number }> {
    let buf: Buffer;
    try {
      buf = await fsp.readFile(this.logPath);
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err;
      return { lines: [], trailingPartial: null, bytes: 0 };
    }
    const { lines, trailingPartial } = splitLines(buf);
    return { lines, trailingPartial, bytes: buf.length };
  }

  /** Full scan: envelopes in `seq` order plus every problem found. */
  private async scan(): Promise<{ messages: Envelope[]; issues: Issue[]; bytes: number }> {
    const raw = await this.readRaw();
    const issues: Issue[] = [];
    const entries: Array<{ envelope: Envelope; index: number }> = [];
    let highest = 0;
    for (const line of raw.lines) {
      if (line.text.length === 0) continue;
      const parsed = parseEnvelope(line.text);
      if (!parsed.ok) {
        issues.push({ kind: 'corrupt-line', offset: line.offset, detail: parsed.reason });
        continue;
      }
      const { envelope } = parsed;
      if (computeId(envelope) !== envelope.id) {
        issues.push({
          kind: 'id-mismatch',
          offset: line.offset,
          detail: `id ${envelope.id} does not match this message's content-addressed id`,
        });
      }
      if (envelope.seq <= highest) {
        issues.push({
          kind: 'out-of-order-seq',
          offset: line.offset,
          detail: `seq ${String(envelope.seq)} does not follow ${String(highest)}`,
        });
      }
      highest = Math.max(highest, envelope.seq);
      entries.push({ envelope, index: entries.length });
    }
    if (raw.trailingPartial !== null) {
      issues.push({
        kind: 'truncated-line',
        offset: raw.trailingPartial.offset,
        detail: 'trailing line has no newline terminator (interrupted append); skipped',
      });
    }
    entries.sort((a, b) => a.envelope.seq - b.envelope.seq || a.index - b.index);
    const highestSeen = entries[entries.length - 1]?.envelope.seq;
    if (highestSeen !== undefined) this.observeLastSeq(highestSeen);
    for (const issue of issues) this.reportIssue(issue);
    this.lastIssues = issues;
    return { messages: entries.map((entry) => entry.envelope), issues, bytes: raw.bytes };
  }

  /**
   * @internal Read the tail of the log to learn the last complete `seq` and
   * whether the file ends with a newline, without scanning the whole log.
   */
  async readTail(): Promise<{ lastSeq: number; endsWithNewline: boolean }> {
    const size = await statSize(this.logPath);
    if (size === null || size === 0) return { lastSeq: 0, endsWithNewline: true };
    let chunk = Math.min(size, TAIL_CHUNK);
    for (;;) {
      const start = Math.max(0, size - chunk);
      const fd = await fsp.open(this.logPath, 'r');
      try {
        const buf = Buffer.alloc(size - start);
        const { bytesRead } = await fd.read(buf, 0, buf.length, start);
        const slice = buf.subarray(0, bytesRead);
        const endsWithNewline = slice.length > 0 && slice[slice.length - 1] === LF;
        const { lines } = splitLines(slice);
        for (let i = lines.length - 1; i >= 0; i -= 1) {
          const line = lines[i]!;
          if (line.text.length === 0) continue;
          const parsed = parseEnvelope(line.text);
          if (parsed.ok) return { lastSeq: parsed.envelope.seq, endsWithNewline };
        }
        if (start === 0) return { lastSeq: 0, endsWithNewline };
      } finally {
        await fd.close();
      }
      chunk = Math.min(size, chunk * 16);
    }
  }

  /** @internal persist the metadata cache, recording the log tail as `lastSeq`. */
  async persistMeta(lastSeq: number): Promise<void> {
    this.meta = { ...this.meta, lastSeq: Math.max(this.meta.lastSeq, lastSeq) };
    await this.writeMeta(false);
  }

  /**
   * The seq high-water mark recorded on disk. Read fresh (under the append lock)
   * so that other processes' compactions are respected.
   */
  private async metaHighWater(): Promise<number> {
    try {
      const parsed = JSON.parse(await fsp.readFile(this.metaPath, 'utf8')) as Partial<Meta>;
      if (typeof parsed.lastSeq === 'number' && Number.isSafeInteger(parsed.lastSeq) && parsed.lastSeq >= 0) {
        return parsed.lastSeq;
      }
    } catch {
      /* missing or unreadable metadata: fall back to what this handle knows */
    }
    return this.meta.lastSeq;
  }

  private renameBudgetMs(): number {
    return Math.max(1000, Math.floor(this.lockTimeoutMs / 2));
  }

  /** Write `meta.json` atomically (`durable` also fsyncs the bytes before the rename). */
  private async writeMeta(durable: boolean): Promise<void> {
    const tmp = path.join(this.dir, `${META_FILE}.tmp.${String(process.pid)}.${randomUUID()}`);
    const fd = fs.openSync(tmp, 'w');
    try {
      fs.writeSync(fd, `${JSON.stringify(this.meta)}\n`);
      if (durable) fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    await renameWithRetry(tmp, this.metaPath, this.renameBudgetMs());
  }
}

function assertLabel(value: unknown, field: string): string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new BusValidationError(`${field} must be a non-empty string`);
  }
  if (value.length > 256) throw new BusValidationError(`${field} must be at most 256 characters`);
  if (WHITESPACE_RE.test(value)) {
    throw new BusValidationError(`${field} must not contain whitespace (use a single token such as build.unit)`);
  }
  if (CONTROL_RE.test(value)) throw new BusValidationError(`${field} must not contain control characters`);
  return value;
}

function assertKind(value: unknown): Kind {
  if (typeof value !== 'string' || !isKind(value)) {
    throw new BusValidationError(`kind must be one of ${KINDS.join(', ')}`);
  }
  return value;
}

function assertCount(value: unknown, field: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw new BusValidationError(`${field} must be a non-negative integer`);
  }
  return value;
}

function assertPositive(value: unknown, field: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 1) {
    throw new BusValidationError(`${field} must be an integer >= 1`);
  }
  return value;
}

function normalizeRefs(refs: unknown): string[] {
  if (refs === undefined) return [];
  if (!Array.isArray(refs)) throw new BusValidationError('refs must be an array of strings');
  if (refs.length > 128) throw new BusValidationError('refs must contain at most 128 entries');
  return refs.map((ref) => {
    if (typeof ref !== 'string' || ref.length === 0) throw new BusValidationError('each ref must be a non-empty string');
    if (ref.length > 512) throw new BusValidationError('each ref must be at most 512 characters');
    if (CONTROL_RE.test(ref)) throw new BusValidationError('refs must not contain control characters');
    return ref;
  });
}

function normalizeCausation(value: unknown): string | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== 'string' || value.length === 0) {
    throw new BusValidationError('causationId must be a non-empty string or null');
  }
  if (value.length > 256) throw new BusValidationError('causationId must be at most 256 characters');
  if (CONTROL_RE.test(value)) throw new BusValidationError('causationId must not contain control characters');
  return value;
}

function resolveFrom(from: number | 'all' | undefined, now: number): number {
  if (from === undefined) return now;
  if (from === 'all') return 0;
  return assertCount(from, 'from');
}

/** Best-effort fsync of a directory (POSIX only; Windows cannot open directories). */
async function syncDir(dir: string): Promise<void> {
  if (process.platform === 'win32') return;
  try {
    const fd = await fsp.open(dir, 'r');
    try {
      await fd.sync();
    } finally {
      await fd.close();
    }
  } catch {
    /* directory fsync is a durability nicety, not a correctness requirement */
  }
}

/**
 * Rename `from` over `to`, retrying while another handle holds `to` open.
 *
 * On Windows a replace-rename of a file that any handle currently has open fails
 * with EPERM (verified on Windows 10 with Node 24: 40/40 failures while one reader
 * held the file). Readers here only hold the log for the duration of a read, so a
 * bounded retry is enough. Any other error is propagated immediately.
 */
async function renameWithRetry(from: string, to: string, budgetMs: number): Promise<void> {
  const deadline = Date.now() + budgetMs;
  for (;;) {
    try {
      await fsp.rename(from, to);
      return;
    } catch (err) {
      const code = (err as NodeJS.ErrnoException).code;
      if (code !== 'EPERM' && code !== 'EBUSY' && code !== 'EACCES') throw err;
      if (Date.now() >= deadline) {
        throw new Error(
          `could not replace ${to} after ${String(budgetMs)}ms (${String(code)}); a reader may be holding the file open`,
          { cause: err },
        );
      }
      await sleep(10);
    }
  }
}

/**
 * Open (or create) the bus for a project directory.
 *
 * Guarantees: creates `<projectDir>/.shortbus/`, `meta.json` and `bus.ndjson` when
 * absent; the returned handle's `lastSeq()` reflects the log on disk, including
 * messages appended by other processes, without scanning the whole log. With
 * `{ create: false }`, a missing bus raises {@link BusNotFoundError} and nothing is
 * written.
 */
export async function openBus(projectDir: string, opts: OpenBusOptions = {}): Promise<Bus> {
  if (typeof projectDir !== 'string' || projectDir.length === 0) {
    throw new BusValidationError('projectDir must be a non-empty string');
  }
  const resolved = path.resolve(projectDir);
  const dir = path.join(resolved, BUS_DIR);
  const logPath = path.join(dir, LOG_FILE);
  const metaPath = path.join(dir, META_FILE);

  if (opts.create === false && (await statSize(logPath)) === null) {
    throw new BusNotFoundError(`no SH0RTBUS log at ${logPath} (run "shortbus init" first)`);
  }
  await fsp.mkdir(dir, { recursive: true });

  let stored: Meta | null = null;
  try {
    const parsed = JSON.parse(await fsp.readFile(metaPath, 'utf8')) as Partial<Meta>;
    if (parsed.v === ENVELOPE_VERSION && typeof parsed.createdAt === 'string' && typeof parsed.appends === 'number') {
      stored = {
        v: ENVELOPE_VERSION,
        project: resolved,
        createdAt: parsed.createdAt,
        updatedAt: typeof parsed.updatedAt === 'string' ? parsed.updatedAt : parsed.createdAt,
        lastSeq: typeof parsed.lastSeq === 'number' ? parsed.lastSeq : 0,
        appends: parsed.appends,
      };
    }
  } catch {
    stored = null;
  }

  const now = new Date().toISOString();
  const handle = fs.openSync(logPath, 'a');
  fs.closeSync(handle);
  const bus = new Bus(
    resolved,
    stored ?? { v: ENVELOPE_VERSION, project: resolved, createdAt: now, updatedAt: now, lastSeq: 0, appends: 0 },
    opts,
  );
  const { lastSeq } = await bus.readTail();
  bus.observeLastSeq(lastSeq);
  if (stored === null || stored.lastSeq !== lastSeq) await bus.persistMeta(lastSeq);
  return bus;
}
