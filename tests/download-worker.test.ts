// tests/download-worker.test.ts — the download child's lifecycle contract.
//
// runSpawnedDownload owns the yt-dlp process from spawn to exit. The invariant
// under test: no matter how the function leaves (success, failure, or a throw
// from inside the progress loop), the child is dead and gone from
// `activeProcs`, and the on-disk partial survives for the next attempt.
// Before 1.3 a throw in the loop skipped the cleanup and the orphan kept
// writing to a .part another worker could claim.

import { afterAll, afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { db, initDatabase, type Job } from "../src/db";
import { activeProcs, setConfig } from "../src/state";
import { DEFAULT_CONFIG } from "../src/config";
import { runSpawnedDownload } from "../src/workers/download";

const MOCK_YTDLP = resolve(import.meta.dir, "mocks", "yt-dlp");
const tmpDirs: string[] = [];

afterAll(async () => {
  for (const d of tmpDirs) await rm(d, { recursive: true, force: true });
});

beforeEach(() => {
  initDatabase(":memory:");
  setConfig({ ...DEFAULT_CONFIG });
  activeProcs.clear();
});

afterEach(() => {
  delete process.env.FAKE_PROGRESS_THEN_HANG;
  for (const [, p] of activeProcs) {
    try {
      p.kill("SIGKILL");
    } catch {}
  }
  activeProcs.clear();
});

function jobRow(dir: string): Job {
  db.run(
    `INSERT INTO jobs (id, url, title, output_directory, target_format, download_status, download_claimed_by)
     VALUES ('leak01', 'https://www.youtube.com/watch?v=leak01', 'Leak test', ?, 'mp4', 'downloading', 'dl-1')`,
    [dir],
  );
  return db.query("SELECT * FROM jobs WHERE id = 'leak01'").get() as Job;
}

const isAlive = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

describe("runSpawnedDownload reaps its child on every exit path (1.3)", () => {
  // The mock's orphan watchdog is POSIX-only (AGENTS.md 9.3); the kill in
  // the finally block is what this test proves, and it is platform-neutral,
  // but the extensionless mock cannot be launched on win32 at all.
  const run = process.platform === "win32" ? test.skip : test;

  run("a throw inside the progress loop kills yt-dlp, clears activeProcs, and keeps the .part", async () => {
    const dir = await mkdtemp(join(tmpdir(), "yta-leak-"));
    tmpDirs.push(dir);
    const job = jobRow(dir);
    const outTemplate = join(dir, "001 - Leak test.%(ext)s");
    process.env.FAKE_PROGRESS_THEN_HANG = "1";

    // Observe the child while it runs so we can check it afterwards.
    let child: Bun.Subprocess | undefined;
    const spy = setInterval(() => {
      child = activeProcs.get(1) ?? child;
      // The first progress UPDATE will hit a closed database and throw — the
      // same shape as SQLITE_BUSY after the busy timeout, which is the real
      // production trigger.
      if (child && !closed) {
        closed = true;
        db.close();
      }
    }, 5);
    let closed = false;

    let thrown: unknown = null;
    try {
      await runSpawnedDownload(
        1,
        job,
        [MOCK_YTDLP, job.url, "-o", outTemplate, "--newline"],
        60_000,
        DEFAULT_CONFIG,
      );
    } catch (e) {
      thrown = e;
    } finally {
      clearInterval(spy);
    }

    expect(thrown).toBeTruthy(); // the loop really did throw
    expect(child).toBeTruthy(); // and the child was registered while running
    expect(activeProcs.has(1)).toBe(false); // … and unregistered on the way out
    // Give the kernel a tick to reap, then the pid must be gone.
    await child!.exited;
    expect(child!.exitCode !== null || child!.signalCode !== null).toBe(true);
    expect(isAlive(child!.pid)).toBe(false);
    // The partial survives for the next attempt (never throw away work).
    expect(existsSync(join(dir, "001 - Leak test.f137.mp4.part"))).toBe(true);
  }, 20_000);

  run("a normal run leaves no child behind either", async () => {
    const dir = await mkdtemp(join(tmpdir(), "yta-leak-"));
    tmpDirs.push(dir);
    const job = jobRow(dir);
    const outTemplate = join(dir, "001 - Leak test.%(ext)s");
    const res = await runSpawnedDownload(
      1,
      job,
      [MOCK_YTDLP, job.url, "-o", outTemplate, "--newline"],
      60_000,
      DEFAULT_CONFIG,
    );
    expect(res.code).toBe(0);
    expect(res.timedOut).toBe(false);
    expect(activeProcs.has(1)).toBe(false);
    // The final path is reported as a candidate, resolved by the caller.
    expect(res.pathCandidates.some((p) => p.endsWith("001 - Leak test.mp4"))).toBe(true);
    // Progress reached the row (and heart-beat the claim).
    const row = db.query("SELECT progress, download_claimed_at FROM jobs WHERE id = 'leak01'").get() as any;
    expect(row.progress).toBeGreaterThan(0);
    expect(row.download_claimed_at).toBeTruthy();
  }, 20_000);
});
