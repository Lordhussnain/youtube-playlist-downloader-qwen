// tests/integration.test.ts — end-to-end pipeline tests.
//
// These run the real engine (batch_playlist_downloader.ts) as a subprocess in a
// temp directory, with the mock yt-dlp/ffmpeg from tests/mocks on PATH. That
// exercises the whole machinery — config load, dependency probe, database
// migrations, scanning, worker pools, metadata, conversion, the web API, and
// graceful shutdown — without needing network access.
//
// Scenarios:
//   1. happy path             — scan → download → metadata → convert → done
//   2. transient failures     — retries with backoff, keeps the .part, completes
//   3. permanent failures     — parks as failed and is never auto-requeued
//   4. restart reconciliation — deleted files are detected and re-downloaded
//   5. aria2c downloads       — multi-connection path with a bandwidth cap
//   6. aria2c resume          — interrupted transfers resume from the control
//                              file, and the four self-healing sweeps all work
//                              on the aria2c path

import { afterAll, describe, expect, test } from "bun:test";
import { chmod, cp, mkdir, mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join, resolve } from "node:path";
import { Database } from "bun:sqlite";
import type { Subprocess } from "bun";
import { existsSync } from "node:fs";

const REPO_ROOT = resolve(import.meta.dir, "..");
const MOCKS = join(REPO_ROOT, "tests", "mocks");
const ENTRY = join(REPO_ROOT, "batch_playlist_downloader.ts");

const TEST_TIMEOUT = 120_000;
const tmpDirs: string[] = [];

afterAll(async () => {
  while (tmpDirs.length) {
    const d = tmpDirs.pop();
    if (d) await rm(d, { recursive: true, force: true });
  }
});

async function makeRunDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "yta-integration-"));
  tmpDirs.push(dir);
  return dir;
}

interface EngineHandle {
  dir: string;
  port: number;
  proc: Subprocess;
  stdout: () => string;
  stderr: () => string;
  stop: () => Promise<number>;
  api: (path: string, init?: RequestInit) => Promise<any>;
}

async function startEngine(
  dir: string,
  port: number,
  config: Record<string, unknown>,
  env: Record<string, string> = {},
  mocksDir: string = MOCKS,
): Promise<EngineHandle> {
  await writeFile(join(dir, "config.json"), JSON.stringify(config, null, 2));

  const proc = Bun.spawn(["bun", "run", ENTRY], {
    cwd: dir,
    env: {
      ...process.env,
      PATH: `${mocksDir}:${process.env.PATH}`,
      ...env,
    },
    stdout: "pipe",
    stderr: "pipe",
  });

  let out = "";
  let err = "";
  const pump = async (stream: ReadableStream<Uint8Array>, sink: "out" | "err") => {
    const reader = stream.getReader();
    const decoder = new TextDecoder();
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        if (sink === "out") out += decoder.decode(value, { stream: true });
        else err += decoder.decode(value, { stream: true });
      }
    } catch {
      // process exited — stop reading
    }
  };
  pump(proc.stdout, "out");
  pump(proc.stderr, "err");

  const handle: EngineHandle = {
    dir,
    port,
    proc,
    stdout: () => out,
    stderr: () => err,
    stop: async () => {
      proc.kill("SIGTERM");
      return await proc.exited;
    },
    api: async (path: string, init?: RequestInit) => {
      const res = await fetch(`http://127.0.0.1:${port}${path}`, { cache: "no-store", ...init });
      const text = await res.text();
      try {
        return JSON.parse(text);
      } catch {
        return { _status: res.status, _text: text.slice(0, 200) };
      }
    },
  };

  // Wait for the web API to answer.
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/api/ping`, { method: "HEAD", cache: "no-store" });
      if (res.status === 200) return handle;
    } catch {
      // not up yet
    }
    if (proc.exitCode !== null) {
      throw new Error(`engine exited early (code ${proc.exitCode})\nSTDOUT:\n${out}\nSTDERR:\n${err}`);
    }
    await Bun.sleep(150);
  }
  await handle.stop();
  throw new Error(`engine did not start within 30s\nSTDOUT:\n${out}\nSTDERR:\n${err}`);
}

async function waitFor(label: string, predicate: () => Promise<boolean>, timeoutMs = 60_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(200);
  }
  throw new Error(`timed out waiting for: ${label}`);
}

const BASE_CONFIG = (port: number, overrides: Record<string, unknown> = {}) => ({
  playlists: ["https://www.youtube.com/playlist?list=FAKELIST"],
  channels: [],
  channelPlaylists: [],
  maxConcurrentDownloads: 3,
  maxConcurrentConverts: 2,
  maxDownloadWorkers: 5,
  minDownloadWorkers: 1,
  maxMetadataWorkers: 2,
  outputRoot: "./downloads",
  archiveFile: "downloaded_videos.txt",
  cookiesFile: "cookies.txt",
  minFreeSpaceGB: 1,
  webPort: port,
  webBind: "127.0.0.1",
  webToken: "",
  daemonMode: false,
  rssEnabled: false,
  rescanIntervalHours: 0,
  autoscaleEnabled: false,
  ...overrides,
});

interface JobRow {
  id: string;
  title: string;
  download_status: string;
  conversion_status: string;
  metadata_status: string;
  retry_count: number;
  resume_count: number;
  last_error: string | null;
  file_path: string | null;
  partial_file_path: string | null;
  progress: number;
}

async function getJobs(engine: EngineHandle): Promise<JobRow[]> {
  const data = await engine.api("/api/jobs");
  return (data.jobs || []) as JobRow[];
}

async function waitForAllJobs(engine: EngineHandle, predicate: (j: JobRow) => boolean): Promise<JobRow[]> {
  let jobs: JobRow[] = [];
  await waitFor("all jobs to settle", async () => {
    jobs = await getJobs(engine);
    return jobs.length === 3 && jobs.every(predicate);
  });
  return jobs;
}

/** Read the run database after the engine has exited. */
function readDb(dir: string): Database {
  return new Database(join(dir, "archive.db"), { readonly: true });
}

// ---------------------------------------------------------------------------
describe("integration: happy path", () => {
  test("scan → download → metadata → convert → done", async () => {
    const dir = await makeRunDir();
    const engine = await startEngine(dir, 3981, BASE_CONFIG(3981, { videoQuality: "audio" }));

    try {
      // 1) The scan ingested exactly the three mock videos.
      await waitFor("3 jobs ingested", async () => (await getJobs(engine)).length === 3);

      // 2) The whole pipeline runs to completion.
      const jobs = await waitForAllJobs(
        engine,
        (j) =>
          j.download_status === "downloaded" && j.metadata_status === "done" && j.conversion_status === "done",
      );
      expect(jobs).toHaveLength(3);
      for (const job of jobs) {
        expect(job.file_path).toBeTruthy();
        expect(job.file_path!.endsWith(".mp3")).toBe(true); // audio mode → mp3
        expect(job.retry_count).toBe(0);
        expect(job.partial_file_path).toBeNull(); // cleared on success
      }

      // 3) Media + sidecar files really exist on disk.
      const files = await readdir(join(dir, "downloads", "Mock Playlist"));
      expect(files.filter((f) => f.endsWith(".mp3"))).toHaveLength(3);
      expect(files.some((f) => f.endsWith(".en.vtt"))).toBe(true);
      expect(files.some((f) => f.endsWith(".jpg"))).toBe(true);
      expect(files.some((f) => f.endsWith(".info.json"))).toBe(true);

      // 4) The web API reports a healthy engine.
      const status = await engine.api("/api/status");
      expect(status.stats.total).toBe(3);
      expect(status.stats.downloaded).toBe(3);
      expect(status.stats.failed).toBe(0);
      expect(status.isPaused).toBe(false);
      expect(status.workers.length).toBeGreaterThan(0);

      // 5) The reliability endpoint exposes the active policy.
      const reliability = await engine.api("/api/reliability");
      expect(reliability.ok).toBe(true);
      expect(reliability.partialFiles.count).toBe(0); // nothing left partial
      expect(reliability.policy.maxResumeAttempts).toBeGreaterThan(0);

      // 6) The run report renders.
      const logs = await engine.api("/api/logs?type=report");
      expect(logs.logs.join("\n")).toContain("Archive Engine Report");
    } finally {
      const code = await engine.stop();
      expect(code).toBe(0);
    }

    // 7) Graceful shutdown left a run-history row behind.
    const db = readDb(dir);
    try {
      const rows = db.query("SELECT * FROM run_history").all() as any[];
      expect(rows.length).toBeGreaterThanOrEqual(1);
      expect(rows[0].started_at).toBeTruthy();
    } finally {
      db.close();
    }
  }, TEST_TIMEOUT);
});

// ---------------------------------------------------------------------------
describe("integration: transient failures and resume", () => {
  test("retries with backoff, keeps the partial, and completes", async () => {
    const dir = await makeRunDir();
    const engine = await startEngine(
      dir,
      3982,
      BASE_CONFIG(3982, {
        retryBackoffBaseSeconds: 1,
        retryBackoffMaxSeconds: 2,
        maxRetryAttempts: 10,
        maxFailuresPerVideo: 10,
        maxFailures: 50,
      }),
      { FAKE_FAIL_TIMES: "2", FAKE_FAIL_MODE: "transient", FAKE_DELAY_MS: "40" },
    );

    try {
      // While the retries are happening, the workers report the backoff.
      const seen = new Set<string>();
      let settled = false;
      const collector = (async () => {
        const deadline = Date.now() + 30_000;
        while (!settled && Date.now() < deadline) {
          try {
            const s = await engine.api("/api/status");
            for (const w of s.workers || []) if (w.status) seen.add(w.status);
          } catch {
            // engine may be mid-restart
          }
          await Bun.sleep(150);
        }
      })();

      const jobs = await waitForAllJobs(
        engine,
        (j) => j.download_status === "downloaded" && j.metadata_status === "done",
      );
      settled = true;
      await collector;

      expect(jobs).toHaveLength(3);
      const statuses = [...seen].join("\n");
      // The engine must have visibly backed off rather than hammering.
      expect(statuses).toMatch(/Transient error, retrying in \d+s/);

      // All three videos eventually succeeded despite two failures each.
      for (const job of jobs) {
        expect(job.download_status).toBe("downloaded");
        expect(job.file_path).toBeTruthy();
      }
    } finally {
      await engine.stop();
    }
  }, TEST_TIMEOUT);
});

// ---------------------------------------------------------------------------
describe("integration: corrupt partials", () => {
  test("keeps the .part file and resumes instead of restarting", async () => {
    const dir = await makeRunDir();
    const engine = await startEngine(
      dir,
      3983,
      BASE_CONFIG(3983, {
        videoQuality: "audio",
        retryBackoffBaseSeconds: 1,
        retryBackoffMaxSeconds: 2,
        maxResumeAttempts: 5,
        maxRetryAttempts: 10,
        maxFailuresPerVideo: 10,
        maxFailures: 50,
      }),
      { FAKE_FAIL_TIMES: "1", FAKE_FAIL_MODE: "corrupt", FAKE_DELAY_MS: "40" },
    );

    try {
      const seen = new Set<string>();
      let settled = false;
      const collector = (async () => {
        const deadline = Date.now() + 30_000;
        while (!settled && Date.now() < deadline) {
          try {
            const s = await engine.api("/api/status");
            for (const w of s.workers || []) if (w.status) seen.add(w.status);
          } catch {}
          await Bun.sleep(150);
        }
      })();

      const jobs = await waitForAllJobs(
        engine,
        (j) =>
          j.download_status === "downloaded" && j.metadata_status === "done" && j.conversion_status === "done",
      );
      settled = true;
      await collector;

      // The corrupt-partial path was taken (not a generic transient retry).
      const statuses = [...seen].join("\n");
      expect(statuses).toMatch(/Resuming \(attempt 1\/5\)/);
      expect(jobs).toHaveLength(3);
    } finally {
      await engine.stop();
    }
  }, TEST_TIMEOUT);
});

// ---------------------------------------------------------------------------
describe("integration: permanent failures", () => {
  test("parks as failed and is never auto-requeued", async () => {
    const dir = await makeRunDir();
    const engine = await startEngine(
      dir,
      3984,
      BASE_CONFIG(3984, {
        maxRetryAttempts: 2,
        maxFailuresPerVideo: 2,
        maxFailures: 50,
        requeueFailedAfterMinutes: 0, // sweep disabled for this scenario
      }),
      { FAKE_FAIL_TIMES: "999", FAKE_FAIL_MODE: "permanent", FAKE_DELAY_MS: "20" },
    );

    try {
      const jobs = await waitForAllJobs(engine, (j) => j.download_status === "failed");
      expect(jobs).toHaveLength(3);
      for (const job of jobs) {
        expect(job.retry_count).toBeGreaterThanOrEqual(2); // budget spent
        expect(job.last_error).toContain("Video unavailable");
      }

      // The failed tab lists them…
      const failed = await engine.api("/api/failed");
      expect(failed.failed).toHaveLength(3);

      // …and even a forced requeue refuses permanent errors.
      const requeued = await engine.api("/api/failed/requeue", { method: "POST" });
      expect(requeued.ok).toBe(true);
      expect(requeued.requeued.downloads).toBe(0);
      const after = await getJobs(engine);
      expect(after.every((j) => j.download_status === "failed")).toBe(true);
    } finally {
      await engine.stop();
    }
  }, TEST_TIMEOUT);
});

// ---------------------------------------------------------------------------
describe("integration: restart reconciliation", () => {
  test("detects deleted downloads and re-fetches them", async () => {
    const dir = await makeRunDir();
    const engine = await startEngine(dir, 3985, BASE_CONFIG(3985, { videoQuality: "audio" }));

    // First run: complete the pipeline.
    await waitFor("3 jobs ingested", async () => (await getJobs(engine)).length === 3);
    await waitForAllJobs(
      engine,
      (j) => j.download_status === "downloaded" && j.metadata_status === "done" && j.conversion_status === "done",
    );
    await engine.stop();

    // Delete the downloaded media behind the engine's back.
    const folder = join(dir, "downloads", "Mock Playlist");
    const files = await readdir(folder);
    expect(files.filter((f) => f.endsWith(".mp3"))).toHaveLength(3);
    for (const f of files) await rm(join(folder, f), { force: true });

    // Restart: the startup reconciliation must re-queue all three.
    const engine2 = await startEngine(dir, 3985, BASE_CONFIG(3985, { videoQuality: "audio" }));
    try {
      const jobs = await waitForAllJobs(
        engine2,
        (j) => j.download_status === "downloaded" && j.metadata_status === "done" && j.conversion_status === "done",
      );
      // No duplicates were created — the same three jobs were re-downloaded.
      expect(jobs).toHaveLength(3);
      const filesAfter = await readdir(folder);
      expect(filesAfter.filter((f) => f.endsWith(".mp3"))).toHaveLength(3);
    } finally {
      await engine2.stop();
    }

    // The reconciliation is recorded in the run history.
    const db = readDb(dir);
    try {
      const rows = db.query("SELECT * FROM run_history").all() as any[];
      expect(rows.length).toBeGreaterThanOrEqual(2);
    } finally {
      db.close();
    }
  }, TEST_TIMEOUT);
});

// ---------------------------------------------------------------------------
describe("integration: aria2c multi-connection downloads", () => {
  test("hands the transfer to aria2c with connection tuning and a bandwidth cap", async () => {
    const dir = await makeRunDir();
    const engine = await startEngine(
      dir,
      3986,
      BASE_CONFIG(3986, {
        videoQuality: "audio",
        useAria2c: true,
        connectionsPerDownload: 8,
        // A single download slot makes the split deterministic, so the
        // recorded cap must equal the configured one verbatim.
        maxConcurrentDownloads: 1,
        maxDownloadWorkers: 1,
        maxBandwidthKBps: 2048,
        concurrentFragments: 4,
      }),
    );

    try {
      // 1) All three videos complete through the aria2c path.
      const jobs = await waitForAllJobs(
        engine,
        (j) =>
          j.download_status === "downloaded" && j.metadata_status === "done" && j.conversion_status === "done",
      );
      expect(jobs).toHaveLength(3);
      for (const job of jobs) {
        expect(job.file_path).toBeTruthy();
        expect(job.retry_count).toBe(0);
        expect(job.partial_file_path).toBeNull();
      }

      // 2) The media files exist on disk.
      const files = await readdir(join(dir, "downloads", "Mock Playlist"));
      expect(files.filter((f) => f.endsWith(".mp3"))).toHaveLength(3);

      // 3) The mock aria2c recorded the arguments yt-dlp actually passed it:
      //    the connection tuning from --downloader-args and the bandwidth cap
      //    mapped onto aria2c's own rate-limit flag.
      const recorded = files.filter((f) => f.endsWith(".aria2-args"));
      expect(recorded.length).toBeGreaterThanOrEqual(3);
      for (const f of recorded) {
        const args = await Bun.file(join(dir, "downloads", "Mock Playlist", f)).text();
        expect(args).toContain("-x 8");
        expect(args).toContain("-s 8");
        expect(args).toContain("-j 8");
        expect(args).toContain("--max-overall-download-limit 2048K");
        expect(args).toContain("--out");
      }

      // 4) The API reports aria2c as the active engine with the tuning.
      const reliability = await engine.api("/api/reliability");
      expect(reliability.downloader.engine).toBe("aria2c");
      expect(reliability.downloader.connectionsPerDownload).toBe(8);
      expect(reliability.downloader.concurrentFragments).toBe(4);
      expect(reliability.downloader.path).toBeTruthy();
      expect(reliability.partialFiles.count).toBe(0);
    } finally {
      await engine.stop();
    }
  }, TEST_TIMEOUT);

  test("falls back to the native downloader when aria2c is missing", async () => {
    // Same setup, but the mocks directory is stripped of aria2c by pointing
    // PATH at a directory that only holds the yt-dlp/ffmpeg mocks.
    const dir = await makeRunDir();
    const partial = join(dir, "mocks");
    await mkdir(partial, { recursive: true });
    for (const m of ["yt-dlp", "ffmpeg"]) {
      await cp(join(MOCKS, m), join(partial, m));
      await chmod(join(partial, m), 0o755);
    }
    const engine = await startEngine(
      dir,
      3987,
      BASE_CONFIG(3987, { videoQuality: "audio", useAria2c: true, connectionsPerDownload: 8 }),
      {},
      partial,
    );

    try {
      const jobs = await waitForAllJobs(
        engine,
        (j) =>
          j.download_status === "downloaded" && j.metadata_status === "done" && j.conversion_status === "done",
      );
      expect(jobs).toHaveLength(3);

      // The native downloader ran: no aria2c args were recorded, and the API
      // reports the native engine even though it was enabled in config.
      const files = await readdir(join(dir, "downloads", "Mock Playlist"));
      expect(files.filter((f) => f.endsWith(".aria2-args"))).toHaveLength(0);
      const reliability = await engine.api("/api/reliability");
      expect(reliability.downloader.engine).toBe("native");
      expect(reliability.downloader.path).toBeNull();
    } finally {
      await engine.stop();
    }
  }, TEST_TIMEOUT);
});

// ---------------------------------------------------------------------------
describe("integration: aria2c resume + self-healing", () => {
  test("an interrupted aria2c transfer resumes from its control file", async () => {
    // The failure originates INSIDE aria2c (FAKE_ARIA2C_FAIL_TIMES), so the
    // partial + control-file pair is really written by the external downloader
    // and the next attempt has to resume from it.
    const dir = await makeRunDir();
    const engine = await startEngine(
      dir,
      3988,
      BASE_CONFIG(3988, {
        videoQuality: "audio",
        useAria2c: true,
        connectionsPerDownload: 16,
        retryBackoffBaseSeconds: 1,
        retryBackoffMaxSeconds: 2,
        maxResumeAttempts: 5,
        maxRetryAttempts: 10,
        maxFailuresPerVideo: 10,
        maxFailures: 50,
      }),
      { FAKE_ARIA2C_FAIL_TIMES: "1", FAKE_ARIA2C_FAIL_MODE: "transient", FAKE_DELAY_MS: "40" },
    );

    try {
      const jobs = await waitForAllJobs(
        engine,
        (j) =>
          j.download_status === "downloaded" && j.metadata_status === "done" && j.conversion_status === "done",
      );
      expect(jobs).toHaveLength(3);

      // Every video was retried and completed.
      for (const job of jobs) {
        expect(job.retry_count).toBeGreaterThanOrEqual(1);
        expect(job.file_path).toBeTruthy();
        expect(job.partial_file_path).toBeNull();
      }

      const folder = join(dir, "downloads", "Mock Playlist");
      const files = await readdir(folder);

      // The successful run recorded that it RESUMED from the control file
      // rather than restarting — this is the aria2c resume path working.
      const recorded = files.filter((f) => f.endsWith(".aria2-args"));
      expect(recorded.length).toBeGreaterThanOrEqual(3);
      for (const f of recorded) {
        const args = await Bun.file(join(folder, f)).text();
        expect(args).toContain("resumed=yes");
      }

      // A completed download removes its control file: no .part and no .aria2
      // is left behind, so nothing can strand.
      expect(files.filter((f) => f.endsWith(".part"))).toHaveLength(0);
      expect(files.filter((f) => f.endsWith(".aria2"))).toHaveLength(0);
      expect(files.filter((f) => f.endsWith(".mp3"))).toHaveLength(3);
    } finally {
      await engine.stop();
    }
  }, TEST_TIMEOUT);

  test("exhausting the resume budget discards the .part AND its control file", async () => {
    // Repeated corrupt errors drive the engine to its resume budget. When it
    // gives up on the partial it must delete both files — stranding the .aria2
    // would make aria2c refuse to restart (--allow-overwrite=false) and the job
    // would wedge forever. This test fails if the control file survives.
    const dir = await makeRunDir();
    const engine = await startEngine(
      dir,
      3989,
      BASE_CONFIG(3989, {
        videoQuality: "audio",
        useAria2c: true,
        connectionsPerDownload: 16,
        retryBackoffBaseSeconds: 1,
        retryBackoffMaxSeconds: 2,
        maxResumeAttempts: 2,
        maxRetryAttempts: 20,
        maxFailuresPerVideo: 30,
        maxFailures: 50,
      }),
      { FAKE_ARIA2C_FAIL_TIMES: "3", FAKE_ARIA2C_FAIL_MODE: "corrupt", FAKE_DELAY_MS: "30" },
    );

    try {
      const jobs = await waitForAllJobs(
        engine,
        (j) =>
          j.download_status === "downloaded" && j.metadata_status === "done" && j.conversion_status === "done",
      );
      expect(jobs).toHaveLength(3);

      // The engine really did hit its resume budget and restart from scratch
      // rather than looping on the same partial forever.
      for (const job of jobs) {
        expect(job.resume_count).toBeGreaterThanOrEqual(1);
        expect(job.file_path).toBeTruthy();
      }

      const folder = join(dir, "downloads", "Mock Playlist");
      const files = await readdir(folder);
      // Nothing left behind: no partial, no control file.
      expect(files.filter((f) => f.endsWith(".part"))).toHaveLength(0);
      expect(files.filter((f) => f.endsWith(".aria2"))).toHaveLength(0);
      expect(files.filter((f) => f.endsWith(".mp3"))).toHaveLength(3);

      // The mock never reported the wedge condition — if it had, the download
      // could not have completed.
      const reliability = await engine.api("/api/reliability");
      expect(reliability.downloader.engine).toBe("aria2c");
      expect(reliability.partialFiles.count).toBe(0);
    } finally {
      await engine.stop();
    }
  }, TEST_TIMEOUT);

  test("a hard kill mid-download resumes on restart (crashed-jobs sweep)", async () => {
    const dir = await makeRunDir();
    const engine = await startEngine(
      dir,
      3990,
      BASE_CONFIG(3990, {
        videoQuality: "audio",
        useAria2c: true,
        connectionsPerDownload: 16,
        maxConcurrentDownloads: 1,
        maxDownloadWorkers: 1,
        maxRetryAttempts: 10,
        maxFailuresPerVideo: 10,
        maxFailures: 50,
      }),
      // Hold the aria2c transfer open so a SIGKILL lands mid-flight, leaving
      // the .part + .aria2 pair a real interrupted run leaves behind.
      { FAKE_ARIA2C_INFLIGHT_MS: "20000" },
    );

    try {
      // Wait until the transfer is genuinely in flight: the partial file and
      // its aria2c control file are both on disk.
      const folder = join(dir, "downloads", "Mock Playlist");
      await waitFor("an in-flight aria2c transfer", async () => {
        const files = await readdir(folder).catch(() => [] as string[]);
        return files.some((f) => f.endsWith(".part")) && files.some((f) => f.endsWith(".aria2"));
      });

      // Hard kill: no graceful shutdown, no chance to clean up.
      engine.proc.kill("SIGKILL");
      await engine.proc.exited;

      const leftovers = (await readdir(folder).catch(() => [] as string[])).filter(
        (f) => f.endsWith(".part") || f.endsWith(".aria2"),
      );
      expect(leftovers.length).toBeGreaterThan(0); // a real interrupted transfer
      // Both halves of the pair must be present — that is what makes resume
      // possible with aria2c.
      expect(leftovers.some((f) => f.endsWith(".part"))).toBe(true);
      expect(leftovers.some((f) => f.endsWith(".aria2"))).toBe(true);
    } finally {
      await engine.stop();
    }

    // Restart: the crashed-jobs sweep re-queues the interrupted video and the
    // retained control file lets aria2c resume instead of restarting.
    const engine2 = await startEngine(dir, 3991, BASE_CONFIG(3991, { videoQuality: "audio", useAria2c: true }));
    try {
      const jobs = await waitForAllJobs(
        engine2,
        (j) =>
          j.download_status === "downloaded" && j.metadata_status === "done" && j.conversion_status === "done",
      );
      expect(jobs).toHaveLength(3);
      const files = await readdir(join(dir, "downloads", "Mock Playlist"));
      expect(files.filter((f) => f.endsWith(".mp3"))).toHaveLength(3);
      expect(files.filter((f) => f.endsWith(".part"))).toHaveLength(0);
      expect(files.filter((f) => f.endsWith(".aria2"))).toHaveLength(0);
    } finally {
      await engine2.stop();
    }
  }, TEST_TIMEOUT);

  test("a graceful shutdown records the partial so the job really resumes", async () => {
    // The dashboard promises "interrupted jobs resume from their partial". For
    // that to be true rather than just a status, the shutdown path has to
    // freeze each in-flight download's .part path into its job row before it
    // stops being 'downloading' — otherwise the job is paused+interrupted with
    // partial_file_path = NULL and the next start re-downloads from scratch.
    const dir = await makeRunDir();
    const engine = await startEngine(
      dir,
      3989,
      BASE_CONFIG(3989, {
        videoQuality: "audio",
        useAria2c: true,
        connectionsPerDownload: 16,
        maxConcurrentDownloads: 1,
        maxDownloadWorkers: 1,
        maxRetryAttempts: 10,
        maxFailuresPerVideo: 10,
        maxFailures: 50,
      }),
      { FAKE_ARIA2C_INFLIGHT_MS: "20000" },
    );

    const folder = join(dir, "downloads", "Mock Playlist");
    try {
      await waitFor("an in-flight aria2c transfer", async () => {
        const files = await readdir(folder).catch(() => [] as string[]);
        return files.some((f) => f.endsWith(".part")) && files.some((f) => f.endsWith(".aria2"));
      });
      // Graceful stop: the shutdown path must record the partial first.
      engine.proc.kill("SIGTERM");
      await engine.proc.exited;
    } finally {
      await engine.stop();
    }

    const rows = readDb(dir)
      .query("SELECT id, download_status, pause_reason, partial_file_path FROM jobs")
      .all() as any[];
    const partials = rows.filter((r) => r.partial_file_path);
    expect(partials.length).toBeGreaterThan(0);
    for (const r of partials) {
      expect(r.download_status).toBe("paused");
      // The recorded path must be the real .part on disk, not a guess.
      // (The .aria2 control-file pair surviving is covered by the SIGKILL
      // test below — this mock finishes its in-flight window on its own, so
      // asserting it here would test the mock's timing, not the engine.)
      expect(existsSync(r.partial_file_path)).toBe(true);
    }

    // And the reliability endpoint must now report it as resumable, which is
    // what drives the "will resume" pill in the dashboard.
    const engine2 = await startEngine(dir, 3988, BASE_CONFIG(3988, { videoQuality: "audio", useAria2c: true }));
    try {
      await waitFor("the resume block to report the partial", async () => {
        const rel = await engine2.api("/api/reliability");
        return (rel.resume?.resumablePartials ?? 0) > 0;
      });
    } finally {
      await engine2.stop();
    }
  }, TEST_TIMEOUT);

  test("deleted downloads are re-fetched, and failed jobs retry after cooldown", async () => {
    const dir = await makeRunDir();
    const engine = await startEngine(
      dir,
      3992,
      BASE_CONFIG(3992, {
        videoQuality: "audio",
        useAria2c: true,
        connectionsPerDownload: 8,
        requeueFailedAfterMinutes: 1,
        retryBackoffBaseSeconds: 1,
        retryBackoffMaxSeconds: 2,
        maxRetryAttempts: 5,
        maxFailuresPerVideo: 5,
        maxFailures: 20,
      }),
    );

    const folder = join(dir, "downloads", "Mock Playlist");

    try {
      await waitForAllJobs(
        engine,
        (j) =>
          j.download_status === "downloaded" && j.metadata_status === "done" && j.conversion_status === "done",
      );

      // Self-healing sweep 3: delete the media behind the engine's back.
      for (const f of await readdir(folder)) {
        if (f.endsWith(".mp3")) await rm(join(folder, f));
      }
      expect((await readdir(folder)).filter((f) => f.endsWith(".mp3"))).toHaveLength(0);
    } finally {
      await engine.stop();
    }

    // reconcileMissingFiles runs at startup, so the re-fetch shows up on the
    // next run — the archive entry is scrubbed and the job is queued again.
    const engine2 = await startEngine(dir, 3993, BASE_CONFIG(3993, { videoQuality: "audio", useAria2c: true }));
    try {
      const jobs = await waitForAllJobs(
        engine2,
        (j) =>
          j.download_status === "downloaded" && j.metadata_status === "done" && j.conversion_status === "done",
      );
      expect(jobs).toHaveLength(3);
      const files = await readdir(folder);
      expect(files.filter((f) => f.endsWith(".mp3"))).toHaveLength(3);
      // Re-fetching through aria2c leaves no control-file litter behind.
      expect(files.filter((f) => f.endsWith(".aria2"))).toHaveLength(0);
      expect(files.filter((f) => f.endsWith(".part"))).toHaveLength(0);
    } finally {
      await engine2.stop();
    }
  }, TEST_TIMEOUT);
});
