/**
 * Shared test helpers.
 *
 * This directory is deliberately NOT named `test/`: the Node test runner treats
 * every file under a `test/` directory as a test file, and this module contains no
 * tests. The real test files live in `test/` and import from `../testing/harness.ts`.
 */
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import type { Envelope } from '../src/bus.ts';

const roots: string[] = [];

/** Create a real temporary project directory that is removed when the process exits. */
export function tempProject(label: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `shortbus-${label}-`));
  roots.push(dir);
  return dir;
}

process.on('exit', () => {
  for (const root of roots) {
    try {
      fs.rmSync(root, { recursive: true, force: true });
    } catch {
      /* best effort */
    }
  }
});

/**
 * Poll a predicate until it returns true, or fail the test after `timeoutMs`.
 *
 * Used instead of a fake timer: live delivery really is asynchronous and really
 * does depend on polling the file. The predicate may be async.
 */
export async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  message: string,
  timeoutMs = 5000,
  intervalMs = 10,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await predicate()) return;
    if (Date.now() >= deadline) assert.fail(`timed out after ${timeoutMs}ms waiting for: ${message}`);
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
}

/** `setTimeout` as a promise, for tests that need to prove something does NOT happen. */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Collect messages into an array, preserving delivery order. */
export function collector(): { messages: Envelope[]; push: (m: Envelope) => void; seqs: () => number[] } {
  const messages: Envelope[] = [];
  return {
    messages,
    push: (m: Envelope) => {
      messages.push(m);
    },
    seqs: () => messages.map((m) => m.seq),
  };
}

/** Read the raw bytes of a project's log. */
export function readLog(projectDir: string): string {
  return fs.readFileSync(path.join(projectDir, '.shortbus', 'bus.ndjson'), 'utf8');
}

/** Absolute path of a project's log. */
export function logPath(projectDir: string): string {
  return path.join(projectDir, '.shortbus', 'bus.ndjson');
}
