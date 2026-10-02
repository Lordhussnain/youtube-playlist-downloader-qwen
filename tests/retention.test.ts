// tests/retention.test.ts — retention policies (plan 5.5): the pure orphan
// detector, run_history age prune, media prune (+ the 'pruned' job state
// that keeps the scanner and the missing-files sweep away), and the sweep's
// off-by-default contract.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp, mkdir, readdir, rm, utimes, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DEFAULT_CONFIG, type Config } from "../src/config";
import { db, initDatabase } from "../src/db";
import { reconcileMissingFiles, sweepError } from "../src/reconcile";
import {
  DAY_MS,
  expiredBefore,
  orphanSidecars,
  pruneExpiredMedia,
  pruneOrphanSidecars,
  pruneRunHistoryByAge,
  retentionEnabled,
  retentionSweep,
  selectExpiredMedia,
  sidecarStems,
} from "../src/retention";

let dir: string;
beforeEach(async () => {
  initDatabase(":memory:");
  dir = await mkdtemp(join(tmpdir(), "yta-retention-"));
});
afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

const NOW = new Date("2026-10-02T12:00:00Z");
const daysAgo = (n: number) => expiredBefore(n, NOW);

async function touch(path: string, ageDays = 0): Promise<void> {
  await writeFile(path, "x");
  if (ageDays > 0) {
    const t = new Date(NOW.getTime() - ageDays * DAY_MS);
    await utimes(path, t, t);
  }
}

describe("orphanSidecars (pure)", () => {
  test("stems: language tags and .orig are stripped as alternatives", () => {
    expect(sidecarStems("001 - Title.en.srt")).toEqual(["001 - Title.en", "001 - Title"]);
    expect(sidecarStems("001 - Title.en-US.orig.vtt")).toEqual(["001 - Title.en-US.orig", "001 - Title"]);
    expect(sidecarStems("001 - Title.info.json")).toEqual(["001 - Title"]);
    expect(sidecarStems("001 - Title.jpg")).toEqual(["001 - Title"]);
    expect(sidecarStems("001 - Title.mp4")).toEqual([]);
  });

  test("a sidecar is orphaned only when nothing non-sidecar shares its stem", () => {
    const names = [
      "001 - A.mp4", "001 - A.en.srt", "001 - A.jpg", "001 - A.description", // kept: media present
      "002 - B.en.srt", "002 - B.jpg", "002 - B.info.json", // orphans: no media
      "003 - C.mp4.part", "003 - C.en.srt", // kept: a partial shares the stem
      "004 - D.f137.mp4", "004 - D.jpg", // kept: an intermediate shares the stem
      "cover.jpg", // orphan by the rule (no sibling) — the grace period is the only guard
    ];
    expect(orphanSidecars(names).sort()).toEqual(["002 - B.en.srt", "002 - B.info.json", "002 - B.jpg", "cover.jpg"]);
  });
});

describe("pruneRunHistoryByAge", () => {
  test("deletes only rows that ended before the cut-off; 0 disables", () => {
    const ins = db.prepare("INSERT INTO run_history (started_at, ended_at) VALUES (?, ?)");
    ins.run(daysAgo(40), daysAgo(40));
    ins.run(daysAgo(10), daysAgo(10));
    ins.run(daysAgo(50), null); // still running (never ended): kept
    expect(pruneRunHistoryByAge(0, NOW)).toBe(0);
    expect(pruneRunHistoryByAge(30, NOW)).toBe(1);
    expect((db.query("SELECT COUNT(*) AS n FROM run_history").get() as any).n).toBe(2);
  });
});

describe("media retention", () => {
  function insertJob(id: string, file: string, updatedAt: string, overrides: Record<string, unknown> = {}) {
    const row: Record<string, unknown> = {
      id, url: `https://www.youtube.com/watch?v=${id}`, title: id, output_directory: dir, target_format: "mp4",
      download_status: "downloaded", conversion_status: "not_needed", metadata_status: "done",
      file_path: file, updated_at: updatedAt, ...overrides,
    };
    const cols = Object.keys(row);
    db.run(`INSERT INTO jobs (${cols.map((c) => `"${c}"`).join(",")}) VALUES (${cols.map(() => "?").join(",")})`, Object.values(row) as any[]);
  }

  test("only settled, old jobs are selected", async () => {
    insertJob("old00000001", join(dir, "old.mp4"), daysAgo(100));
    insertJob("new00000001", join(dir, "new.mp4"), daysAgo(5));
    insertJob("meta0000001", join(dir, "meta.mp4"), daysAgo(100), { metadata_status: "pending" });
    insertJob("conv0000001", join(dir, "conv.mp4"), daysAgo(100), { conversion_status: "pending" });
    insertJob("fail0000001", join(dir, "fail.mp4"), daysAgo(100), { download_status: "failed" });
    expect(selectExpiredMedia(30, NOW).map((r) => r.id)).toEqual(["old00000001"]);
    expect(selectExpiredMedia(0, NOW)).toEqual([]);
  });

  test("prunes the media + its sidecars, marks the job pruned, and the sweeps leave it alone", async () => {
    const media = join(dir, "001 - Old.mp4");
    await touch(media);
    await touch(join(dir, "001 - Old.en.srt"));
    await touch(join(dir, "001 - Old.jpg"));
    await touch(join(dir, "001 - Older brother.mp4")); // shares a prefix, not the stem
    insertJob("old00000001", media, daysAgo(100));

    expect(await pruneExpiredMedia(30, NOW)).toBe(1);
    expect((await readdir(dir)).sort()).toEqual(["001 - Older brother.mp4"]);
    const j = db.query("SELECT * FROM jobs WHERE id = 'old00000001'").get() as any;
    expect(j.download_status).toBe("pruned");
    expect(j.file_path).toBeNull();
    expect(j.last_error).toContain("retention");

    // The missing-files sweep must not re-queue a pruned job.
    const cfg: Config = { ...DEFAULT_CONFIG, verifyExistingFiles: true, archiveFile: join(dir, "archive.txt") };
    expect(reconcileMissingFiles(cfg)).toBe(0);
    expect((db.query("SELECT download_status FROM jobs WHERE id = 'old00000001'").get() as any).download_status).toBe("pruned");
    // Idempotent.
    expect(await pruneExpiredMedia(30, NOW)).toBe(0);
  });

  test("a media file that cannot be deleted leaves the row untouched", async () => {
    // A directory where the file path is expected: unlink fails with EISDIR/EPERM, not ENOENT.
    const blocker = join(dir, "blocked.mp4");
    await mkdir(blocker);
    await touch(join(blocker, "inner"));
    insertJob("blk00000001", blocker, daysAgo(100));
    expect(await pruneExpiredMedia(30, NOW)).toBe(0);
    expect((db.query("SELECT download_status FROM jobs WHERE id = 'blk00000001'").get() as any).download_status).toBe("downloaded");
  });
});

describe("pruneOrphanSidecars", () => {
  test("removes old orphans across subdirectories, keeps fresh ones and anything with a sibling", async () => {
    const sub = join(dir, "Playlist A");
    await mkdir(sub);
    await touch(join(sub, "001 - Kept.mp4"));
    await touch(join(sub, "001 - Kept.en.srt"), 10);
    await touch(join(sub, "002 - Gone.en.srt"), 10);
    await touch(join(sub, "002 - Gone.jpg"), 10);
    await touch(join(sub, "003 - Fresh.jpg"), 0); // inside the grace period
    await touch(join(dir, "004 - Root.description"), 3);
    expect(await pruneOrphanSidecars(dir, NOW)).toBe(3);
    expect(existsSync(join(sub, "001 - Kept.en.srt"))).toBe(true);
    expect(existsSync(join(sub, "002 - Gone.en.srt"))).toBe(false);
    expect(existsSync(join(sub, "002 - Gone.jpg"))).toBe(false);
    expect(existsSync(join(sub, "003 - Fresh.jpg"))).toBe(true);
    expect(existsSync(join(dir, "004 - Root.description"))).toBe(false);
  });
});

describe("retentionSweep", () => {
  test("off by default: nothing runs, nothing is touched", async () => {
    expect(retentionEnabled(DEFAULT_CONFIG)).toBe(false);
    await touch(join(dir, "orphan.jpg"), 10);
    const r = await retentionSweep({ ...DEFAULT_CONFIG, outputRoot: dir }, NOW);
    expect(r).toEqual({ historyRows: 0, mediaPruned: 0, sidecarsRemoved: 0 });
    expect(existsSync(join(dir, "orphan.jpg"))).toBe(true);
  });

  test("an unreadable output root lands on the sweep-error registry instead of throwing", async () => {
    const cfg = { ...DEFAULT_CONFIG, outputRoot: join(dir, "does-not-exist"), pruneOrphanSidecars: true };
    const r = await retentionSweep(cfg, NOW);
    expect(r.sidecarsRemoved).toBe(0);
    expect(sweepError("retention")?.message).toMatch(/ENOENT|no such file/i);
    const ok = await retentionSweep({ ...cfg, outputRoot: dir }, NOW);
    expect(ok.sidecarsRemoved).toBe(0);
    expect(sweepError("retention")).toBeNull();
  });
});
