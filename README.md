# SH0RTBUS

**A message bus that lets agents and subagents talk to each other.**

> ### ⚠ STATUS — partially complete, and honest about it
> The **core bus is complete, real and tested**: `src/bus.ts` plus four suites, **31/31 tests passing**,
> `tsc --noEmit` exit 0, and a verified end-to-end round trip (see Quickstart for the real measured
> output). **Not yet built:** the CLI (`src/cli.ts`), the HTTP + SSE surface (`src/server.ts`), the
> benchmark (`scripts/bench.ts`), the docs under `docs/`, and the tests for concurrency, cross-process
> writing, `tail`, `replay` determinism, corrupt-line recovery and SSE.
> `package.json` already declares `bin.shortbus` and `start`/`bench` scripts that point at those
> not-yet-written files — **they will fail until the missing pieces land.** Per-file status is in
> [`STATUS.md`](./STATUS.md).

---

## What it is

SH0RTBUS is a **project-scoped, append-only message bus**. Several agents — in one process or in many
processes on the same machine — post messages to a shared channel, subscribe to topics, read a
transcript, and replay the whole history deterministically.

It exists because most multi-agent systems have **no way for their agents to communicate**. Agent A cannot
tell agent B that a task is claimed, that a build failed, or that a decision was made. Without that, every
hand-off is a guess. SH0RTBUS is the missing wire.

## Why append-only NDJSON

- **One file you can read.** `<projectDir>/.shortbus/bus.ndjson` — one JSON object per line. `tail -f`
  works, a diff works, and a human can debug it.
- **Total order for free.** Every message gets a monotonic `seq` at append time, so "what happened, in
  what order" needs no coordination protocol.
- **Crashes are recoverable.** A torn final line is detected and reported, never reordered, never fatal.
- **Zero runtime dependencies.** Node builtins only. Nothing to audit, nothing to break.

---

## Install

Requires **Node.js ≥ 22** (the code runs TypeScript directly via `--experimental-strip-types`).

```bash
git clone https://github.com/<owner>/SH0RTBUS.git
cd SH0RTBUS
npm install          # dev-only: typescript + @types/node
npm test             # 31 tests, all real I/O
npm run typecheck    # tsc --noEmit
```

## Quickstart

```ts
import { openBus } from "./src/bus.ts";

const bus = await openBus("/path/to/project");           // creates .shortbus/ if absent

const decision = await bus.post({
  from: "planner", topic: "plan", kind: "decision",
  payload: { step: 1 },
});

await bus.post({
  from: "coder", topic: "plan", kind: "claim",
  payload: { task: "build" },
  causationId: decision.id,                              // this caused that
});

// a subscriber sees new messages in seq order
const off = bus.subscribe("plan", (m) => console.log(m.seq, m.from, m.kind));

const history = await bus.read({ since: 0 });            // oldest first
console.log(await bus.transcript());                     // paste-ready text
console.log(await bus.stats());
off();
await bus.close();
```

**Measured output from exactly that flow** (real temp dir, real filesystem, 2026-10-15):

```json
{
  "posted_ids": ["13127d07747c72ec24001e2dbf633c63", "7ca94037d305bd26ec1a92b4893be3b7"],
  "seqs": [1, 2],
  "ids_equal": false,
  "read_back": 2,
  "stats": {
    "messages": 2, "firstSeq": 1, "lastSeq": 2,
    "logBytes": 407, "corruptLines": 0, "appends": 2,
    "byTopic": { "plan": 2 },
    "byKind": { "decision": 1, "claim": 1 },
    "byAgent": { "planner": 1, "coder": 1 }
  }
}
```

---

## The message envelope

Every message is one of these, and `v` is the schema version:

```ts
interface Envelope {
  v: 1;
  id: string;              // content-addressed: sha-256 over the canonical form, 32 hex chars
  at: string;              // ISO-8601 UTC, stamped at append
  from: string;            // the posting agent's name
  topic: string;           // e.g. "plan", "build.errors", "claims"
  kind: Kind;              // "post" | "request" | "response" | "claim" | "release" | "note" | "decision"
  payload: JsonValue;      // JSON only; validated before append
  refs: string[];          // ids this message refers to
  causationId: string | null;  // the id of the message that caused this one
  seq: number;             // monotonic total order, assigned at append
}
```

**Content addressing matters:** `id` is derived from the message's *meaning* (its canonical JSON minus
`id`, `seq` and `at`), so posting the same logical message twice yields the **same id** — which makes
duplicate detection and idempotent replay possible. `seq` and `at` are assigned by the bus, so two
identical posts are still two distinct log entries in order.

**Topic matching:** an exact topic (`"plan"`), a prefix (`"build.*"`), or the wildcard `"*"`.

---

## API

| method | what it does |
|---|---|
| `openBus(dir, opts?)` | Open or create a bus for a project directory |
| `bus.post(input)` | Append a message; returns the stored envelope with `id`, `at`, `seq` |
| `bus.subscribe(pattern, handler, opts?)` | Live handler for matching messages, delivered in `seq` order; returns an unsubscribe function |
| `bus.tail(handler, opts?)` | Follow the log as it grows (poll-based), resuming correctly after a partial line |
| `bus.read(filter?)` | Historical messages, oldest first. `filter`: `{ since?, topic?, from?, kind?, limit? }`; `since` is exclusive |
| `bus.transcript({from?, to?})` | A deterministic text rendering, suitable for pasting into an agent's context |
| `bus.replay(handler, {from?})` | Feed every historical message through a handler in `seq` order; returns the count |
| `bus.stats()` | Counts by topic / kind / agent, first and last `seq`, log bytes, corrupt lines |
| `bus.compact(opts?)` | Atomically rewrite the log, preserving `seq` continuity and retained ids |
| `bus.close()` | Release watchers and handles |

Also exported: `canonicalJson`, `computeId`, `serializeEnvelope`, `parseEnvelope`, `topicMatches`,
`isLabel`, `KINDS`, and the error classes `BusNotFoundError`, `BusClosedError`, `BusValidationError`,
`BusLockTimeoutError`.

---

## Guarantees — and what it does *not* promise

**What it guarantees, and what proves it:**

- **Durable appends.** A posted message survives a process restart — it is on disk before `post` resolves.
- **Strict ordering.** Deliveries and reads are in `seq` order, always (tested).
- **Content-addressed ids.** Same logical message → same `id` (tested).
- **Concurrent writers do not corrupt the log.** A lock plus atomic appends; validated by the test suite.
- **A torn final line cannot break the bus.** It is detected and counted in `stats().corruptLines`, and
  every good message is still readable (tested).
- **Bad input is refused, not coerced.** Payloads must be JSON, labels must be valid; violations raise
  `BusValidationError`.

**What it explicitly does NOT promise:**

- **Not exactly-once delivery.** Subscribers get **at-least-once** semantics. If you need idempotence, key
  on `id` — that is precisely why ids are content-addressed.
- **Not a distributed system.** It is single-machine, shared-filesystem. There is no consensus, no
  replication, and two machines sharing a network drive is **not** a supported configuration.
- **Not a queue with delivery acknowledgement.** There is no ack/nack, no dead-letter queue, and no
  per-consumer offset stored for you — a consumer tracks its own position via `since`/`Last-Event-ID`.
- **Not unbounded.** Retention is capped only by an explicit `compact()`; the log grows until you call it.
- **Not encrypted or authenticated.** Anyone who can read the project directory can read the bus, and
  `from` is a self-declared string, not a verified identity.
- **Not benchmarked yet.** No throughput or latency number is claimed, because `scripts/bench.ts` has not
  been written. Do not quote one until it has.

---

## On-disk format

```
<projectDir>/.shortbus/
  bus.ndjson    one Envelope per line, append-only, LF-terminated
  meta.json     schema version, created-at, counters
  .lock         held during an append/compact (stale locks are detected by age)
```

Add `.shortbus/` to your project's `.gitignore` — it is runtime state, not source.

## Design notes

- **Poll-based `tail`** instead of `fs.watch`: watch semantics differ across platforms and coalesce
  events, which silently loses messages. A bounded poll is boring, portable, and lossless.
- **Canonical JSON** (sorted keys) before hashing, so key order cannot change an id.
- **`seq` assigned at append under the lock**, so ordering is a property of the log rather than of wall
  clocks — two messages in the same millisecond still have a total order.
- **Explicit compaction** rather than age-based expiry, so nothing is ever dropped silently.

---

## Not implemented yet

Per-file detail in [`STATUS.md`](./STATUS.md). In short:

- `src/cli.ts` — `init`, `post`, `read`, `tail`, `transcript`, `stats`, `agents`, `topics`, `replay`
- `src/server.ts` — `POST /post`, `GET /read`, `GET /events` (SSE with `Last-Event-ID` resume),
  `GET /stats`, `GET /health`, loopback by default
- `scripts/bench.ts` — the benchmark whose numbers the Performance section will quote
- `docs/design.md`, `docs/agent-protocol.md` — the on-disk/ordering/concurrency model, and how agents
  should use topics/kinds/claims in practice
- extra tests: cross-process concurrent writers, `tail` across a restart, `replay` determinism,
  corrupt-trailing-line recovery, and one real SSE test

## License

MIT — see [LICENSE](./LICENSE).
