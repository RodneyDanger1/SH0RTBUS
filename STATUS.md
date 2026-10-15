# SH0RTBUS — STATUS (what is done, what is half-done, what is left)

**Recorded: 2026-10-15.** The build was stopped mid-flight by the owner. This file is the per-file state so
nothing is ambiguous, and it is the companion to the STATUS banner and the "Not implemented yet" section in
[`README.md`](./README.md).

**Verified at the pause** (commands actually run in the project directory):

```
npx tsc --noEmit      -> exit 0        (no error TS lines)
node --test           -> # tests 31 · # pass 31 · # fail 0 · # skipped 0
a real end-to-end run -> posted 2 messages, read 2 back, seq 1..2,
                         content-addressed ids (32 hex), corruptLines 0
```

So **what exists is real and works**. Nothing here is a mock, a stub, a demo or a placeholder.

---

## Per-file state

| path | state | notes |
|---|---|---|
| `package.json` | **done, but it references files that do not exist yet** | `bin.shortbus` → `./src/cli.ts` and `scripts.start` → `node src/server.ts`, `scripts.bench` → `node scripts/bench.ts`. **Those three are missing**, so `npm start`, `npm run bench` and the `shortbus` bin will fail until they land. devDeps: `typescript`, `@types/node` only — **zero runtime dependencies**. |
| `tsconfig.json` | done | Node ≥ 22, ESM, strict. |
| `.gitignore` | done | ignores `node_modules/`, `*.log`, `.shortbus/`. |
| `src/bus.ts` | **COMPLETE** (53.8 KB) | `openBus`, the `Bus` class with all nine methods (`post`, `subscribe`, `tail`, `read`, `transcript`, `replay`, `stats`, `compact`, `close`), the `Envelope`/`PostInput`/`ReadFilter`/`CompactOptions` types, `canonicalJson`, `computeId`, `serializeEnvelope`, `parseEnvelope`, `topicMatches`, `isLabel`, `KINDS`, and five error classes. |
| `test/envelope.test.ts` · `test/read.test.ts` · `test/subscribe.test.ts` · `test/compact.test.ts` | **done** | 31 tests total, all passing, all real I/O. |
| `testing/harness.ts` | done | shared **test** helper (temp-dir bootstrapping). Deliberately not under `test/` so the runner does not treat it as a test file; it contains no production code and no mock of the bus. |
| `src/cli.ts` | **NOT WRITTEN** | was chartered as: `init`, `post --from --topic --kind --payload`, `read [--since|--topic|--limit|--json]`, `tail`, `transcript`, `stats`, `agents`, `topics`, `replay`; exit codes 0 ok / 1 usage / 2 not-found. |
| `src/server.ts` | **NOT WRITTEN** | was chartered as: `POST /post`, `GET /read?since=`, `GET /events` (SSE, `id:` = `seq`, resume via `Last-Event-ID`), `GET /stats`, `GET /health`; **loopback by default**, explicit flag to bind all interfaces, bounded request body, no leaked connections on disconnect. |
| `scripts/bench.ts` | **NOT WRITTEN** | the benchmark. **Because of this, the README's Performance section deliberately claims no throughput or latency number.** |
| `README.md` | done | what it is, why, install, a quickstart with **measured** output, the envelope spec, the full API table, guarantees **and explicit non-guarantees**, the on-disk format, design notes, and the honest "Not implemented yet" list. |
| `docs/design.md` | **NOT WRITTEN** | the on-disk format, the ordering model, the concurrency model, failure modes and recovery, and the exact crash-resume recipe. |
| `docs/agent-protocol.md` | **NOT WRITTEN** | how agents should use topics/kinds, `claim`/`release` for work ownership, request/response correlation via `causationId`, with a worked example transcript. |
| `LICENSE` | written | MIT. |

---

## What is left, in the order that makes sense

1. **`scripts/bench.ts`** first: it is small, and it unblocks the one README number that is currently
   withheld honestly.
2. **`src/server.ts`** (HTTP + SSE): the highest-value missing capability — it is what lets agents in
   *separate processes* use the bus over a socket, and it makes `package.json`'s `start` script true.
3. **`src/cli.ts`**: makes `bin.shortbus` true and gives agents a shell-level interface.
4. **The missing tests**: cross-process concurrent writers (spawn two real `node` children), `tail` across
   a restart, `replay` determinism (run twice → identical id sequence), corrupt-trailing-line recovery
   (append a half-written line, prove every good message survives and the corruption is reported), and one
   real SSE test **including** a `Last-Event-ID` resume.
5. **`docs/design.md`** and **`docs/agent-protocol.md`**.
6. Optional, worth considering once the above lands: a `Bus`-level **idempotency helper** (post-or-return-
   existing by content id), a **consumer-offset file** so a subscriber can crash-resume without tracking
   `since` itself, and a size-based `compact` trigger.

## Rules this project was built under (keep them)

- **No mock, stub, fake, demo or placeholder code on any production path.** Test doubles are permitted
  only inside a test seam, and this project prefers **real I/O** (real temp dirs, real files, real
  loopback sockets, real child processes) over doubles — the 31 passing tests already do.
- **No runtime dependency** was added, and none should be: Node builtins only.
- **Every README claim must be demonstrated by a test or a measured command.** If it cannot be measured,
  say it is not measured (as the Performance section does today).
- **State non-guarantees as loudly as guarantees.** The README's list (no exactly-once, single-machine, no
  ack, no encryption, `from` unverified) is the model to keep.
