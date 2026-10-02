// src/stats.ts — one aggregate snapshot of the jobs table, shared by
// /api/status, /api/reliability and the TUI.
//
// Each of those used to run its own full-table SUM(CASE …) scan — eight
// aggregates ×2 per /api/status poll, seven more queries (one unbounded
// `.all()`) per reliability poll, and another for the TUI header every 2 s.
// At 10k+ jobs that is a lot of event-loop time spent recounting the same
// rows. One query, memoised for a second, serves them all; the mutating web
// routes call `invalidateStats()` so the UI sees its own actions immediately.

import { db, perVideoCap } from "./db";
import { isPermanentDownloadError } from "./retry";
import { STALE_CLAIM_THRESHOLDS } from "./reconcile";
import type { Config } from "./config";

export interface StatsSnapshot {
  /** pending + paused + downloading */
  queued: number;
  downloading: number;
  downloaded: number;
  /** any stage failed (what the dashboard's "failed" tile counts) */
  failedAny: number;
  /** download_status = 'failed' only (TUI header) */
  failedDownloads: number;
  metadataPending: number;
  converting: number;
  waitingLive: number;
  total: number;
  /** rows holding a partial_file_path, and the bytes they account for */
  partialCount: number;
  partialBytes: number;
  /** partials whose job is still in play (pending/paused/downloading) */
  resumablePartials: number;
  /** paused + interrupted (crashed-jobs sweep scope) */
  interrupted: number;
  /** what reapStaleClaims would reclaim right now */
  staleClaims: number;
  /** failed downloads the requeue sweep would still retry */
  resumableFailed: number;
  /** bytes left on in-flight downloads (global ETA numerator) */
  remainingBytes: number;
  /** when this snapshot was computed (epoch ms) */
  at: number;
}

export const STATS_TTL_MS = 1000;

// `handle` pins the memo to the Database instance it was computed from, so a
// re-opened database (tests call initDatabase(":memory:") per case) can never
// be served counts from the previous one.
let cached: { key: string; handle: unknown; snap: StatsSnapshot } | null = null;

/** Drop the memoised snapshot (after a state change the UI should see at once). */
export function invalidateStats(): void {
  cached = null;
}

/**
 * The current aggregate picture. `config` only affects the stale-claim window
 * and the retry cap; a change to either busts the memo.
 */
export function getStatsSnapshot(config: Config): StatsSnapshot {
  const t = STALE_CLAIM_THRESHOLDS(config);
  const cap = perVideoCap(config);
  const key = `${t.download}|${cap}`;
  const now = Date.now();
  if (cached && cached.handle === db && cached.key === key && now - cached.snap.at < STATS_TTL_MS) return cached.snap;

  const row = db
    .query(
      `SELECT
         SUM(download_status IN ('pending', 'paused', 'downloading')) AS queued,
         SUM(download_status = 'downloading') AS downloading,
         SUM(download_status = 'downloaded') AS downloaded,
         SUM(download_status = 'failed' OR conversion_status = 'failed' OR metadata_status = 'failed') AS failed_any,
         SUM(download_status = 'failed') AS failed_dl,
         SUM(metadata_status IN ('pending', 'in_progress')) AS metadata_pending,
         SUM(conversion_status IN ('pending', 'in_progress')) AS converting,
         SUM(download_status = 'waiting_live') AS waiting_live,
         COUNT(*) AS total,
         SUM(partial_file_path IS NOT NULL) AS partial_count,
         SUM(CASE WHEN partial_file_path IS NOT NULL THEN COALESCE(file_size, 0) ELSE 0 END) AS partial_bytes,
         SUM(partial_file_path IS NOT NULL AND download_status IN ('pending', 'paused', 'downloading')) AS resumable,
         SUM(download_status = 'paused' AND pause_reason = 'interrupted') AS interrupted,
         SUM(download_status = 'downloading'
             AND (download_claimed_at IS NULL OR download_claimed_at < datetime('now', '${t.download}'))) AS stale_dl,
         SUM(conversion_status = 'in_progress'
             AND (conversion_claimed_at IS NULL OR conversion_claimed_at < datetime('now', '${t.conversion}'))) AS stale_cv,
         SUM(metadata_status = 'in_progress' AND updated_at < datetime('now', '${t.metadata}')) AS stale_md,
         SUM(CASE WHEN download_status = 'downloading'
                  THEN COALESCE(file_size, 0) * (1 - COALESCE(progress, 0) / 100.0) ELSE 0 END) AS remaining
       FROM jobs`,
    )
    .get() as Record<string, number | null>;

  // Same eligibility rules as the sweep itself: retry budget remaining and a
  // non-permanent last error. Bounded by the failed rows under the cap (uses
  // the download-status index), classified with the shared predicate so the
  // dashboard and the sweep can never disagree about what is retryable.
  const failedRows = db
    .query(`SELECT last_error FROM jobs WHERE download_status = 'failed' AND retry_count < ?`)
    .all(cap) as { last_error: string | null }[];
  let resumableFailed = 0;
  for (const r of failedRows) if (!isPermanentDownloadError(r.last_error)) resumableFailed++;

  const n = (v: number | null | undefined): number => (typeof v === "number" && Number.isFinite(v) ? v : 0);
  const snap: StatsSnapshot = {
    queued: n(row.queued),
    downloading: n(row.downloading),
    downloaded: n(row.downloaded),
    failedAny: n(row.failed_any),
    failedDownloads: n(row.failed_dl),
    metadataPending: n(row.metadata_pending),
    converting: n(row.converting),
    waitingLive: n(row.waiting_live),
    total: n(row.total),
    partialCount: n(row.partial_count),
    partialBytes: n(row.partial_bytes),
    resumablePartials: n(row.resumable),
    interrupted: n(row.interrupted),
    staleClaims: n(row.stale_dl) + n(row.stale_cv) + n(row.stale_md),
    resumableFailed,
    remainingBytes: n(row.remaining),
    at: now,
  };
  cached = { key, handle: db, snap };
  return snap;
}
