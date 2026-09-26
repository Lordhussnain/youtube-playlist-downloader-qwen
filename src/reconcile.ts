// src/reconcile.ts — startup & periodic self-healing for the job database.
//
// Four sweeps keep the pipeline honest across crashes, hard kills, and files
// moved or deleted behind the engine's back:
//
//   reconcileCrashedJobs   — jobs interrupted mid-flight resume automatically
//   reapStaleClaims        — claims orphaned by a dead worker are re-queued
//   reconcileMissingFiles  — "downloaded" files that vanished are re-queued
//                            (and scrubbed from the yt-dlp archive)
//   requeueFailedJobs      — failed jobs are retried after a cooldown, with
//                            permanent errors (private/removed videos) skipped

import { existsSync } from "node:fs";
import { readdir, stat, unlink } from "node:fs/promises";
import { join } from "node:path";
import { db, perVideoCap } from "./db";
import { removeFromArchive } from "./archive";
import { logError } from "./logger";
import { isPermanentDownloadError } from "./retry";
import type { Config } from "./config";

/**
 * Interrupted mid-download jobs become 'paused' + 'interrupted' so they are
 * visible as paused AND automatically re-claimed (resuming where they left
 * off via yt-dlp --continue). User-paused jobs stay held.
 */
export function reconcileCrashedJobs(): void {
  const stmt = db.run(
    `UPDATE jobs SET
       download_status = CASE WHEN download_status = 'downloading' THEN 'paused' ELSE download_status END,
       pause_reason = CASE WHEN download_status = 'downloading' OR (download_status = 'paused' AND pause_reason IS NULL) THEN 'interrupted' ELSE pause_reason END,
       conversion_status = CASE WHEN conversion_status = 'in_progress' THEN 'pending' ELSE conversion_status END,
       metadata_status = CASE WHEN metadata_status = 'in_progress' THEN 'pending' ELSE metadata_status END,
       download_claimed_by = NULL, download_claimed_at = NULL,
       conversion_claimed_by = NULL, conversion_claimed_at = NULL,
       updated_at = CURRENT_TIMESTAMP
     WHERE download_status = 'downloading'
        OR (download_status = 'paused' AND pause_reason IS NULL)
        OR conversion_status = 'in_progress'
        OR metadata_status = 'in_progress'`,
  );
  if (stmt.changes > 0) {
    console.log(`🔄 Reconciled ${stmt.changes} interrupted job(s) — paused/interrupted jobs will resume automatically.`);
  }
}

/**
 * Periodic safety net: if a worker process/thread dies mid-job the claim can
 * be left behind. Downloads have a duration-aware watchdog, so any claim older
 * than 20 minutes is definitely dead → mark paused+interrupted for
 * auto-resume. Conversion claims older than 3h and metadata older than 15m are
 * re-queued.
 */
export function reapStaleClaims(): void {
  try {
    const dl = db.run(
      `UPDATE jobs SET download_status = 'paused', pause_reason = 'interrupted',
         download_claimed_by = NULL, download_claimed_at = NULL, updated_at = CURRENT_TIMESTAMP
       WHERE download_status = 'downloading'
         AND (download_claimed_at IS NULL OR download_claimed_at < datetime('now', '-20 minutes'))`,
    );
    const cv = db.run(
      `UPDATE jobs SET conversion_status = 'pending', conversion_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP
       WHERE conversion_status = 'in_progress'
         AND (conversion_claimed_at IS NULL OR conversion_claimed_at < datetime('now', '-3 hours'))`,
    );
    const md = db.run(
      `UPDATE jobs SET metadata_status = 'pending', updated_at = CURRENT_TIMESTAMP
       WHERE metadata_status = 'in_progress' AND updated_at < datetime('now', '-15 minutes')`,
    );
    const total = dl.changes + cv.changes + md.changes;
    if (total > 0) {
      console.log(`🧟 Reclaimed ${dl.changes} stale download(s), ${cv.changes} conversion(s), ${md.changes} metadata job(s).`);
      logError("reaper", `reclaimed stale claims: downloads=${dl.changes} conversions=${cv.changes} metadata=${md.changes}`);
    }
  } catch (e: any) {
    logError("reaper", String(e?.message || e));
  }
}

/**
 * Startup reconciliation: the database says a video is downloaded, but the
 * file is not on disk any more (moved, renamed, or deleted by hand). Scrub the
 * id from the yt-dlp archive and re-queue the job so the next run fetches it
 * again — otherwise the archive entry would make yt-dlp skip it forever.
 *
 * Returns the number of jobs re-queued.
 */
export function reconcileMissingFiles(config: Config): number {
  if (!config.verifyExistingFiles) return 0;
  let fixed = 0;
  try {
    const rows = db
      .query(
        `SELECT id, file_path, download_status, conversion_status, metadata_status,
                want_subtitles, want_thumbnail, want_description
         FROM jobs
         WHERE file_path IS NOT NULL
           AND (download_status = 'downloaded' OR conversion_status = 'done')`,
      )
      .all() as any[];
    for (const row of rows) {
      if (row.file_path && existsSync(row.file_path)) continue;
      removeFromArchive(config.archiveFile, row.id);
      db.run(
        `UPDATE jobs SET
           download_status = 'pending', pause_reason = NULL,
           retry_count = 0, resume_count = 0, best_progress = 0, progress = 0,
           file_path = NULL, file_size = 0, integrity = NULL, partial_file_path = NULL,
           conversion_status = CASE WHEN conversion_status = 'not_needed' THEN 'not_needed' ELSE 'pending' END,
           metadata_status = CASE
             WHEN COALESCE(want_subtitles,0) + COALESCE(want_thumbnail,0) + COALESCE(want_description,0) > 0 THEN 'pending'
             ELSE metadata_status END,
           download_claimed_by = NULL, conversion_claimed_by = NULL,
           last_error = 'file missing on startup — re-queued',
           updated_at = CURRENT_TIMESTAMP
         WHERE id = ?`,
        [row.id],
      );
      fixed++;
      logError("reconcile", `${row.id}: file gone (${row.file_path}) — re-queued`);
    }
    if (fixed > 0) {
      console.log(`🔍 Startup check: ${fixed} downloaded file(s) missing — re-queued for download.`);
    }
  } catch (e: any) {
    logError("reconcile", String(e?.message || e));
  }
  return fixed;
}

export interface RequeueResult {
  downloads: number;
  conversions: number;
  metadata: number;
}

/**
 * Re-queue failed jobs whose failure was transient, once they have cooled down
 * for `requeueFailedAfterMinutes`. Permanent failures (private, removed,
 * age-gated, geo-blocked, dead URLs) are never retried, and every stage keeps
 * its own retry counter so the sweep is bounded by the per-video cap.
 *
 * Pass `ignoreCooldown: true` (used by the "Requeue all failed" web button) to
 * retry everything eligible immediately.
 */
export function requeueFailedJobs(config: Config, opts: { ignoreCooldown?: boolean } = {}): RequeueResult {
  const result: RequeueResult = { downloads: 0, conversions: 0, metadata: 0 };
  const ignoreCooldown = !!opts.ignoreCooldown;
  if (!ignoreCooldown && config.requeueFailedAfterMinutes <= 0) return result;
  const cap = perVideoCap(config);
  // SQLite modifier built only from a validated integer — never user text.
  // With ignoreCooldown there is no age filter at all (a `-0 minutes` modifier
  // would still exclude rows whose updated_at falls in the current second).
  const modifier = ignoreCooldown ? "" : `AND updated_at < datetime('now', '-${Math.floor(config.requeueFailedAfterMinutes)} minutes')`;

  try {
    // --- Downloads -----------------------------------------------------------
    const failedDownloads = db
      .query(
        `SELECT id, retry_count, last_error FROM jobs
         WHERE download_status = 'failed' AND retry_count < ? ${modifier}`,
      )
      .all(cap) as any[];
    for (const row of failedDownloads) {
      if (isPermanentDownloadError(row.last_error)) continue;
      db.run(
        `UPDATE jobs SET download_status = 'pending', retry_count = retry_count + 1,
           download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
        [row.id],
      );
      result.downloads++;
    }

    // --- Conversions -----------------------------------------------------------
    const failedConversions = db
      .query(
        `SELECT id, conversion_retry_count FROM jobs
         WHERE conversion_status = 'failed' AND conversion_retry_count < ? ${modifier}`,
      )
      .all(cap) as any[];
    for (const row of failedConversions) {
      db.run(
        `UPDATE jobs SET conversion_status = 'pending', conversion_retry_count = conversion_retry_count + 1,
           conversion_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
        [row.id],
      );
      result.conversions++;
    }

    // --- Metadata -------------------------------------------------------------
    const failedMetadata = db
      .query(
        `SELECT id, metadata_retry_count, file_path FROM jobs
         WHERE metadata_status = 'failed' AND metadata_retry_count < ? ${modifier}`,
      )
      .all(cap) as any[];
    for (const row of failedMetadata) {
      // Without the media file the metadata fetch can never succeed.
      if (!row.file_path || !existsSync(row.file_path)) continue;
      db.run(
        `UPDATE jobs SET metadata_status = 'pending', metadata_retry_count = metadata_retry_count + 1,
           updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
        [row.id],
      );
      result.metadata++;
    }

    const total = result.downloads + result.conversions + result.metadata;
    if (total > 0) {
      console.log(
        `♻️ Re-queued ${result.downloads} download(s), ${result.conversions} conversion(s), ${result.metadata} metadata job(s) after cooldown.`,
      );
    }
  } catch (e: any) {
    logError("requeue", String(e?.message || e));
  }
  return result;
}

/**
 * Locate the in-progress download for a base filename: the `.part` file
 * (progressive and DASH both land here) or the `.ytdl` fragment directory.
 * Returns the newest match, or "" when there is nothing to resume.
 */
export async function findPartialFile(dir: string, baseFilename: string): Promise<string> {
  try {
    const files = await readdir(dir);
    const matches: { path: string; mtime: number }[] = [];
    for (const f of files) {
      if (!f.startsWith(baseFilename + ".")) continue;
      if (!f.endsWith(".part") && !f.endsWith(".ytdl")) continue;
      const s = await stat(join(dir, f)).catch(() => null);
      if (s) matches.push({ path: join(dir, f), mtime: s.mtimeMs });
    }
    matches.sort((a, b) => b.mtime - a.mtime);
    return matches[0]?.path || "";
  } catch {
    return "";
  }
}

/**
 * Housekeeping for leftover partial downloads at startup:
 *   • a .part belonging to a FAILED job whose retry budget is exhausted, or
 *     one older than a week, is deleted (it can never complete)
 *   • orphan .part files with no matching job (DB reset, manual cleanup) are
 *     deleted once they are a day old
 *   • everything else — including in-flight downloads from a previous run and
 *     resume-able partials of failed-but-retryable jobs — is kept so
 *     `--continue` can pick up exactly where the download stopped
 */
export async function cleanOrphanedFiles(rootDir: string, config?: Config): Promise<void> {
  try {
    const cap = config ? perVideoCap(config) : 0;
    const rows = db
      .query("SELECT partial_file_path, download_status, retry_count FROM jobs WHERE partial_file_path IS NOT NULL")
      .all() as any[];
    const owners = new Map<string, { status: string; retries: number }>();
    for (const r of rows) {
      if (r.partial_file_path) {
        owners.set(r.partial_file_path, { status: r.download_status, retries: r.retry_count || 0 });
      }
    }

    const files = await readdir(rootDir, { recursive: true });
    let removed = 0;
    for (const file of files) {
      if (!file.endsWith(".part") && !file.endsWith(".ytdl")) continue;
      const fullPath = join(rootDir, file);
      const s = await stat(fullPath).catch(() => null);
      if (!s) continue;
      const ageMs = Date.now() - s.mtimeMs;
      const owner = owners.get(fullPath);
      if (owner) {
        const exhausted = owner.status === "failed" && cap > 0 && owner.retries >= cap;
        const ancient = ageMs > 7 * 24 * 60 * 60 * 1000;
        if (exhausted || ancient) {
          await unlink(fullPath).catch(() => {});
          removed++;
        }
      } else if (ageMs > 24 * 60 * 60 * 1000) {
        // Orphan: no job claims it — safe to clean once it is clearly stale.
        await unlink(fullPath).catch(() => {});
        removed++;
      }
    }
    if (removed > 0) console.log(`🧹 Cleaned ${removed} stale partial file(s).`);
  } catch {}
}
