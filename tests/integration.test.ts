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

import { afterAll, describe, expect, test } from "bun:test";
import { chmod, cp, mkdir, mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { Database } from "bun:sqlite";
import type { Subprocess } from "bun";

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
