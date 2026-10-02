// tests/web-routes.test.ts — the API route table: canonical per-job paths,
// legacy aliases, and the JSON 404/405 contract.
//
// handleRequest is exercised directly against an in-memory database, the same
// way tests/settings.test.ts drives the settings endpoints.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { DEFAULT_CONFIG, type Config } from "../src/config";
import { setConfig } from "../src/state";
import { db, initDatabase } from "../src/db";
import { triggerPause, triggerResume } from "../src/resilience";
import { handleRequest } from "../src/web";

function baseConfig(overrides: Partial<Config> = {}): Config {
  return { ...DEFAULT_CONFIG, ...overrides };
}

function insertJob(id: string, overrides: Record<string, unknown> = {}): void {
  const row: Record<string, unknown> = {
    id,
    url: `https://www.youtube.com/watch?v=${id}`,
    title: `Video ${id}`,
    output_directory: "/tmp/out",
    target_format: "mp4",
    ...overrides,
  };
  const cols = Object.keys(row);
  db.run(
    `INSERT INTO jobs (${cols.map((c) => `"${c}"`).join(", ")}) VALUES (${cols.map(() => "?").join(", ")})`,
    Object.values(row) as any[],
  );
}

function getJob(id: string): any {
  return db.query("SELECT * FROM jobs WHERE id = ?").get(id);
}

const req = (path: string, init?: RequestInit) => new Request(`http://localhost${path}`, init);
const api = (path: string, init?: RequestInit) => handleRequest(req(path, init), baseConfig());

beforeEach(() => {
  initDatabase(":memory:");
  setConfig(baseConfig());
});

afterEach(() => {
  // pause/resume tests flip global engine state — always restore "running".
  if (triggerPauseTestOnlyWasUsed) triggerResume();
});
let triggerPauseTestOnlyWasUsed = false;

describe("the route table", () => {
  test("GET /api/jobs/:id returns one job with parsed audio columns", async () => {
    insertJob("route01", { audio_tracks: JSON.stringify([{ formatId: "251", language: "en" }]) });
    const res = await api("/api/jobs/route01");
    expect(res.status).toBe(200);
    const data = await res.json();
    expect(data.ok).toBe(true);
    expect(data.job.id).toBe("route01");
    expect(Array.isArray(data.job.audio_tracks)).toBe(true);
    expect(data.job.audio_tracks[0].formatId).toBe("251");
  });

  test("GET /api/jobs/:id answers 404 as JSON for an unknown id", async () => {
    const res = await api("/api/jobs/nope999");
    expect(res.status).toBe(404);
    const data = await res.json();
    expect(data.ok).toBe(false);
    expect(data.error).toContain("not found");
  });

  test("POST /api/jobs/:id/retry re-queues a failed job with fresh budgets", async () => {
    insertJob("route02", { download_status: "failed", retry_count: 9, last_error: "boom" });
    const res = await api("/api/jobs/route02/retry", { method: "POST" });
    expect(res.status).toBe(200);
    const job = getJob("route02");
    expect(job.download_status).toBe("pending");
    expect(job.retry_count).toBe(0);
    expect(job.last_error).toBeNull();
  });

  test("the legacy POST /api/retry/:id alias still works", async () => {
    insertJob("route03", { download_status: "failed", retry_count: 3 });
    const res = await api("/api/retry/route03", { method: "POST" });
    expect((await res.json()).ok).toBe(true);
    expect(getJob("route03").download_status).toBe("pending");
  });

  test("POST /api/jobs/:id/reset-failures clears the counters; unknown id is a JSON 404", async () => {
    insertJob("route04", { retry_count: 5, conversion_retry_count: 2, metadata_retry_count: 1, last_error: "x" });
    const bad = await api("/api/jobs/nope999/reset-failures", { method: "POST" });
    expect(bad.status).toBe(404);
    const res = await api("/api/jobs/route04/reset-failures", { method: "POST" });
    expect((await res.json()).ok).toBe(true);
    const job = getJob("route04");
    expect(job.retry_count).toBe(0);
    expect(job.conversion_retry_count).toBe(0);
    expect(job.metadata_retry_count).toBe(0);
  });

  test("DELETE /api/jobs with an id list is the canonical bulk delete", async () => {
    insertJob("route05");
    insertJob("route06");
    const res = await api("/api/jobs", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ids: ["route05", "route06"] }),
    });
    const data = await res.json();
    expect(data.ok).toBe(true);
    expect(data.deleted).toBe(2);
    expect(getJob("route05")).toBeFalsy();
  });

  test("the legacy POST /api/jobs/delete alias still works", async () => {
    insertJob("route07");
    const res = await api("/api/jobs/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ids: ["route07"] }),
    });
    expect((await res.json()).deleted).toBe(1);
  });

  test("unknown API paths answer a JSON 404, not plain text", async () => {
    const res = await api("/api/definitely/not/a/route");
    expect(res.status).toBe(404);
    expect(res.headers.get("content-type")).toContain("application/json");
    const data = await res.json();
    expect(data.ok).toBe(false);
  });

  test("a known path with the wrong method answers 405 with an Allow header", async () => {
    const res = await api("/api/pause", { method: "GET" });
    expect(res.status).toBe(405);
    expect(res.headers.get("allow")).toContain("POST");
    const data = await res.json();
    expect(data.ok).toBe(false);

    // A static action path is never mistaken for an :id route.
    const res2 = await api("/api/jobs/pause", { method: "GET" });
    expect(res2.status).toBe(405);
  });

  test("trailing slashes collapse: /api/jobs/ is /api/jobs", async () => {
    const res = await api("/api/jobs/");
    expect(res.status).toBe(200);
    const data = await res.json();
    expect(data.ok).toBe(true);
    expect(Array.isArray(data.jobs)).toBe(true);
  });

  test("pause/resume share the {ok, success, paused} envelope", async () => {
    triggerPauseTestOnlyWasUsed = true;
    const pause = await api("/api/pause", { method: "POST" });
    const pauseData = await pause.json();
    expect(pauseData).toMatchObject({ ok: true, success: true, paused: true });

    const resume = await api("/api/resume", { method: "POST" });
    const resumeData = await resume.json();
    expect(resumeData).toMatchObject({ ok: true, success: true, paused: false });
    triggerPauseTestOnlyWasUsed = false;
  });

  test("GET /api/version reports the runtime", async () => {
    const res = await api("/api/version");
    const data = await res.json();
    expect(data.ok).toBe(true);
    expect(data.name).toBe("youtube-playlist-downloader");
    expect(data.runtime.bun).toBeTruthy();
    expect(typeof data.uptimeSeconds).toBe("number");
  });

  test("GET /api/status carries ok and runtime alongside the existing fields", async () => {
    const res = await api("/api/status");
    const data = await res.json();
    expect(data.ok).toBe(true);
    expect(data.stats.total).toBe(0);
    expect(data.runtime.platform).toBe(process.platform);
    expect(Array.isArray(data.workers)).toBe(true);
  });
});

// --- Live-claim guards (1.2) --------------------------------------------------
// A job some worker is actively working on must not be retried, deleted,
// purged or flipped by the dashboard: the download guard would otherwise pass
// and yt-dlp would write over the file ffmpeg is reading.
describe("mutating routes refuse jobs that are in progress", () => {
  const LIVE = {
    downloading: { download_status: "downloading", download_claimed_by: "dl-7", download_claimed_at: "2030-01-01 00:00:00" },
    converting: { download_status: "downloaded", conversion_status: "in_progress", conversion_claimed_by: "cv-1" },
    metadata: { download_status: "downloaded", metadata_status: "in_progress", conversion_status: "pending" },
  } as const;

  const snapshot = (id: string) => {
    const j = getJob(id);
    return [j.download_status, j.conversion_status, j.metadata_status, j.download_claimed_by, j.conversion_claimed_by, j.retry_count];
  };

  for (const [label, cols] of Object.entries(LIVE)) {
    test(`retry → 409 while ${label}, and nothing changes`, async () => {
      insertJob("busy1", { ...cols, retry_count: 2 });
      const before = snapshot("busy1");
      for (const path of ["/api/jobs/busy1/retry", "/api/retry/busy1"]) {
        const res = await api(path, { method: "POST" });
        expect(res.status).toBe(409);
        const body = await res.json();
        expect(body.ok).toBe(false);
        expect(body.error).toBe("job is in progress");
        expect(body.inProgress).toEqual(["busy1"]);
      }
      expect(snapshot("busy1")).toEqual(before);
    });

    test(`delete → 409 while ${label}`, async () => {
      insertJob("busy2", cols);
      const res = await api("/api/jobs/busy2", { method: "DELETE" });
      expect(res.status).toBe(409);
      expect(getJob("busy2")).toBeTruthy();
    });

    test(`bulk delete → 409 while ${label}, idle siblings untouched`, async () => {
      insertJob("busy3", cols);
      insertJob("idle3");
      const res = await api("/api/jobs", {
        method: "DELETE",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ ids: ["busy3", "idle3"] }),
      });
      expect(res.status).toBe(409);
      expect((await res.json()).inProgress).toEqual(["busy3"]);
      expect(getJob("busy3")).toBeTruthy();
      expect(getJob("idle3")).toBeTruthy();
    });
  }

  test("purge skips rows holding a live claim", async () => {
    insertJob("p-pending"); // deleted
    insertJob("p-failed", { download_status: "failed" }); // deleted
    insertJob("p-paused-claimed", { download_status: "paused", download_claimed_by: "dl-2" }); // kept
    insertJob("p-failed-converting", { download_status: "failed", conversion_status: "in_progress" }); // kept
    insertJob("p-downloaded", { download_status: "downloaded" }); // not in scope anyway
    const res = await api("/api/queue/purge", { method: "POST" });
    expect(res.status).toBe(200);
    expect((await res.json()).deleted).toBe(2);
    expect(getJob("p-pending")).toBeNull();
    expect(getJob("p-failed")).toBeNull();
    expect(getJob("p-paused-claimed")).toBeTruthy();
    expect(getJob("p-failed-converting")).toBeTruthy();
    expect(getJob("p-downloaded")).toBeTruthy();
  });

  test("bulk pause parks queued jobs, flags in-flight downloads, refuses post-processing", async () => {
    insertJob("pp-pending");
    insertJob("pp-dl", LIVE.downloading);
    insertJob("pp-cv", LIVE.converting);
    const res = await api("/api/jobs/pause", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ids: ["pp-pending", "pp-dl", "pp-cv"] }),
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.paused).toBe(1);
    expect(body.interrupting).toBe(1);
    expect(body.inProgress).toEqual(["pp-cv"]);
    // queued → parked immediately
    expect(getJob("pp-pending").download_status).toBe("paused");
    expect(getJob("pp-pending").pause_reason).toBe("user");
    // in flight → status + claim untouched, reason recorded for the worker
    expect(getJob("pp-dl").download_status).toBe("downloading");
    expect(getJob("pp-dl").download_claimed_by).toBe("dl-7");
    expect(getJob("pp-dl").pause_reason).toBe("user");
    // converting → refused, untouched
    expect(getJob("pp-cv").conversion_status).toBe("in_progress");
    expect(getJob("pp-cv").pause_reason).toBeNull();
  });

  test("retry of an idle job still works and clears a user pause", async () => {
    insertJob("idle-r", { download_status: "paused", pause_reason: "user", retry_count: 3 });
    const res = await api("/api/jobs/idle-r/retry", { method: "POST" });
    expect(res.status).toBe(200);
    expect(getJob("idle-r").download_status).toBe("pending");
    expect(getJob("idle-r").pause_reason).toBeNull();
    expect(getJob("idle-r").retry_count).toBe(0);
  });
});
