// tests/stats.test.ts — the shared aggregate snapshot (plan 2.3): one query
// feeds /api/status, /api/reliability and the TUI; memoised for a second;
// never served across a re-opened database.

import { beforeEach, describe, expect, test } from "bun:test";
import { db, initDatabase } from "../src/db";
import { DEFAULT_CONFIG, type Config } from "../src/config";
import { getStatsSnapshot, invalidateStats, STATS_TTL_MS } from "../src/stats";

const cfg = (o: Partial<Config> = {}): Config => ({ ...DEFAULT_CONFIG, ...o });

function insertJob(id: string, fields: Record<string, unknown> = {}) {
  const row = {
    id,
    url: `https://www.youtube.com/watch?v=${id}`,
    title: id,
    output_directory: "/tmp/out",
    target_format: "mp4",
    folder: "F",
    download_status: "pending",
    conversion_status: "not_needed",
    metadata_status: "not_needed",
    ...fields,
  };
  const cols = Object.keys(row);
  db.run(
    `INSERT INTO jobs (${cols.map((c) => `"${c}"`).join(",")}) VALUES (${cols.map(() => "?").join(",")})`,
    Object.values(row) as any[],
  );
}

beforeEach(() => {
  initDatabase(":memory:");
  invalidateStats();
});

describe("getStatsSnapshot", () => {
  test("counts every bucket the three consumers need from one scan", () => {
    const now = new Date().toISOString().slice(0, 19).replace("T", " ");
    insertJob("p1");
    insertJob("p2", { download_status: "paused", pause_reason: "interrupted" });
    insertJob("d1", { download_status: "downloading", download_claimed_by: "w", download_claimed_at: now, file_size: 1000, progress: 25 });
    insertJob("d2", { download_status: "downloading", download_claimed_by: "w", download_claimed_at: "2000-01-01 00:00:00", partial_file_path: "/tmp/x.part", file_size: 500 });
    insertJob("ok1", { download_status: "downloaded", conversion_status: "in_progress", conversion_claimed_by: "c", conversion_claimed_at: now });
    insertJob("ok2", { download_status: "downloaded", metadata_status: "pending" });
    insertJob("f1", { download_status: "failed", last_error: "HTTP Error 503", retry_count: 1 });
    insertJob("f2", { download_status: "failed", last_error: "Video unavailable", retry_count: 0 });
    insertJob("f3", { download_status: "failed", last_error: "timeout", retry_count: 99 });
    insertJob("cf", { download_status: "downloaded", conversion_status: "failed" });
    insertJob("live", { download_status: "waiting_live" });

    const s = getStatsSnapshot(cfg({ maxRetryAttempts: 3, maxFailuresPerVideo: 3 }));
    expect(s.total).toBe(11);
    expect(s.queued).toBe(4); // p1 p2 d1 d2
    expect(s.downloading).toBe(2);
    expect(s.downloaded).toBe(3); // ok1 ok2 cf
    expect(s.failedDownloads).toBe(3);
    expect(s.failedAny).toBe(4); // f1 f2 f3 + cf
    expect(s.metadataPending).toBe(1);
    expect(s.converting).toBe(1);
    expect(s.waitingLive).toBe(1);
    expect(s.partialCount).toBe(1);
    expect(s.partialBytes).toBe(500);
    expect(s.resumablePartials).toBe(1);
    expect(s.interrupted).toBe(1);
    expect(s.staleClaims).toBe(1); // d2 only — d1 and ok1 are fresh
    expect(s.resumableFailed).toBe(1); // f1: f2 is permanent, f3 is over the cap
    expect(s.remainingBytes).toBeCloseTo(750 + 500, 0);
  });

  test("an empty table is all zeros, never null/NaN", () => {
    const s = getStatsSnapshot(cfg());
    for (const [k, v] of Object.entries(s)) {
      expect(typeof v).toBe("number");
      if (k !== "at") expect(v).toBe(0);
    }
  });

  test("memoises within the TTL, invalidateStats() busts it", () => {
    insertJob("a");
    const first = getStatsSnapshot(cfg());
    expect(first.total).toBe(1);
    insertJob("b");
    expect(getStatsSnapshot(cfg()).total).toBe(1); // cached
    expect(getStatsSnapshot(cfg())).toBe(first); // same object
    invalidateStats();
    expect(getStatsSnapshot(cfg()).total).toBe(2);
    expect(STATS_TTL_MS).toBeLessThanOrEqual(2000); // must stay under the UI poll
  });

  test("a config change that moves the stale window or retry cap recomputes", () => {
    insertJob("d", { download_status: "downloading", download_claimed_by: "w", download_claimed_at: "2000-01-01 00:00:00" });
    insertJob("f", { download_status: "failed", last_error: "timeout", retry_count: 2 });
    const a = getStatsSnapshot(cfg({ maxRetryAttempts: 3, maxFailuresPerVideo: 3 }));
    expect(a.resumableFailed).toBe(1);
    const b = getStatsSnapshot(cfg({ maxRetryAttempts: 2, maxFailuresPerVideo: 3 }));
    expect(b).not.toBe(a);
    expect(b.resumableFailed).toBe(0);
  });

  test("a re-opened database never sees the previous one's counts", () => {
    insertJob("a");
    expect(getStatsSnapshot(cfg()).total).toBe(1);
    initDatabase(":memory:");
    expect(getStatsSnapshot(cfg()).total).toBe(0);
  });
});
