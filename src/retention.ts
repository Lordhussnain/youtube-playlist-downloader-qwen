// src/retention.ts — retention policies (plan 5.5).
//
// Three independent, individually-switchable prunes, all off by default:
//
//   runHistoryDays      delete run_history rows that ended more than N days ago
//                       (the count-based prune in db.ts still applies)
//   mediaRetentionDays  delete finished media (+ its sidecars) untouched for
//                       N days and mark the job 'pruned' — the row stays so the
//                       scanner never re-adds it and the missing-files sweep
//                       never re-fetches it; "Retry job" brings it back
//   pruneOrphanSidecars delete subtitle/thumbnail/description/info.json files
//                       whose media file is gone (a day's grace, and never
//                       when *anything* non-sidecar still shares the stem)
//
// `orphanSidecars(names)` and `expiredBefore()` are pure; the sweep is the
// only part that touches the filesystem, and it reports through the same
// sweep-error registry as the other self-healing sweeps.

import { readdir, stat, unlink } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import type { Config } from "./config";
import { db } from "./db";
import { logError } from "./logger";
import { clearSweepError, recordSweepError } from "./reconcile";
import { SIDECAR_SUFFIXES } from "./util";

export const DAY_MS = 24 * 60 * 60 * 1000;
/** A sidecar younger than this is never considered orphaned (metadata may still be arriving). */
export const SIDECAR_GRACE_MS = DAY_MS;

export interface RetentionResult {
  historyRows: number;
  mediaPruned: number;
  sidecarsRemoved: number;
}

/** ISO timestamp `days` before `now` — the cut-off every age rule compares against. */
export function expiredBefore(days: number, now: Date = new Date()): string {
  return new Date(now.getTime() - days * DAY_MS).toISOString().replace("T", " ").slice(0, 19);
}

// --- run_history --------------------------------------------------------------

export function pruneRunHistoryByAge(days: number, now: Date = new Date()): number {
  if (!(days > 0)) return 0;
  return db.run(`DELETE FROM run_history WHERE ended_at IS NOT NULL AND ended_at < ?`, [expiredBefore(days, now)]).changes;
}

// --- media ---------------------------------------------------------------------

interface ExpiredRow {
  id: string;
  title: string;
  file_path: string;
}

/** Finished jobs (every stage settled) whose row has not changed for `days`. */
export function selectExpiredMedia(days: number, now: Date = new Date()): ExpiredRow[] {
  if (!(days > 0)) return [];
  return db
    .query(
      `SELECT id, title, file_path FROM jobs
       WHERE file_path IS NOT NULL
         AND download_status = 'downloaded'
         AND conversion_status IN ('done', 'not_needed')
         AND metadata_status NOT IN ('pending', 'in_progress')
         AND updated_at < ?`,
    )
    .all(expiredBefore(days, now)) as ExpiredRow[];
}

/** Delete `mediaPath` and every sidecar sharing its stem. Returns files removed. */
async function removeMediaWithSidecars(mediaPath: string): Promise<number> {
  const dir = dirname(mediaPath);
  const stem = basename(mediaPath).replace(/\.[^.]+$/, "");
  let removed = 0;
  try {
    await unlink(mediaPath);
    removed++;
  } catch (e: any) {
    if (e?.code !== "ENOENT") throw e;
  }
  const names = await readdir(dir).catch(() => [] as string[]);
  for (const f of names) {
    if (!f.startsWith(stem + ".") || !SIDECAR_SUFFIXES.some((s) => f.endsWith(s))) continue;
    try {
      await unlink(join(dir, f));
      removed++;
    } catch {}
  }
  return removed;
}

export async function pruneExpiredMedia(days: number, now: Date = new Date()): Promise<number> {
  const rows = selectExpiredMedia(days, now);
  let pruned = 0;
  for (const row of rows) {
    try {
      await removeMediaWithSidecars(row.file_path);
    } catch (e: any) {
      logError("retention", `${row.id} ${row.title}: could not delete ${row.file_path}: ${e?.code || e?.message || e}`);
      continue; // the row stays as-is; the next sweep tries again
    }
    db.run(
      `UPDATE jobs SET download_status = 'pruned', file_path = NULL, file_size = 0, integrity = NULL,
         partial_file_path = NULL, metadata_files = NULL,
         last_error = ?, updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [`pruned by retention policy (${days} day(s))`, row.id],
    );
    pruned++;
  }
  return pruned;
}

// --- orphan sidecars ------------------------------------------------------------

const LANG_TAG_RE = /\.[a-zA-Z]{2,3}(?:-[a-zA-Z0-9]{2,8})?(?:\.orig)?$/;

function isSidecar(name: string): boolean {
  return SIDECAR_SUFFIXES.some((s) => name.endsWith(s));
}

/** The media stems a sidecar name could belong to ("Title.en.srt" → "Title.en", "Title"). */
export function sidecarStems(name: string): string[] {
  const suffix = SIDECAR_SUFFIXES.filter((s) => name.endsWith(s)).sort((a, b) => b.length - a.length)[0];
  if (!suffix) return [];
  const bare = name.slice(0, -suffix.length);
  const stems = [bare];
  const noLang = bare.replace(LANG_TAG_RE, "");
  if (noLang && noLang !== bare) stems.push(noLang);
  return stems;
}

/**
 * Of one directory's entries, the sidecars with no non-sidecar sibling that
 * shares a stem — i.e. no media file, no partial, no anything else. Pure.
 */
export function orphanSidecars(names: readonly string[]): string[] {
  const others = names.filter((n) => !isSidecar(n));
  const hasSibling = (stem: string) => others.some((o) => o === stem || o.startsWith(stem + "."));
  return names.filter((n) => isSidecar(n) && !sidecarStems(n).some(hasSibling));
}

async function* walkDirs(root: string): AsyncGenerator<{ dir: string; names: string[] }> {
  const pending = [root];
  while (pending.length > 0) {
    const dir = pending.pop()!;
    let entries: import("node:fs").Dirent[];
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch (e) {
      if (dir === root) throw e;
      continue;
    }
    const names: string[] = [];
    for (const e of entries) {
      if (e.isDirectory()) pending.push(join(dir, e.name));
      else names.push(e.name);
    }
    yield { dir, names };
  }
}

export async function pruneOrphanSidecars(rootDir: string, now: Date = new Date(), graceMs: number = SIDECAR_GRACE_MS): Promise<number> {
  let removed = 0;
  for await (const { dir, names } of walkDirs(rootDir)) {
    for (const name of orphanSidecars(names)) {
      const full = join(dir, name);
      const s = await stat(full).catch(() => null);
      if (!s || now.getTime() - s.mtimeMs < graceMs) continue;
      try {
        await unlink(full);
        removed++;
      } catch {}
    }
  }
  return removed;
}

// --- the sweep -------------------------------------------------------------------

export function retentionEnabled(config: Pick<Config, "runHistoryDays" | "mediaRetentionDays" | "pruneOrphanSidecars">): boolean {
  return config.runHistoryDays > 0 || config.mediaRetentionDays > 0 || config.pruneOrphanSidecars;
}

/** Run every enabled prune once. Never throws; failures land on the reliability panel. */
export async function retentionSweep(config: Config, now: Date = new Date()): Promise<RetentionResult> {
  const result: RetentionResult = { historyRows: 0, mediaPruned: 0, sidecarsRemoved: 0 };
  if (!retentionEnabled(config)) return result;
  try {
    result.historyRows = pruneRunHistoryByAge(config.runHistoryDays, now);
    result.mediaPruned = await pruneExpiredMedia(config.mediaRetentionDays, now);
    if (config.pruneOrphanSidecars) {
      result.sidecarsRemoved = await pruneOrphanSidecars(config.outputRoot, now);
      if (config.secondaryStoragePath) {
        result.sidecarsRemoved += await pruneOrphanSidecars(config.secondaryStoragePath, now).catch(() => 0);
      }
    }
    const total = result.historyRows + result.mediaPruned + result.sidecarsRemoved;
    if (total > 0) {
      console.log(
        `🗑️ Retention: ${result.historyRows} history row(s), ${result.mediaPruned} media file(s), ${result.sidecarsRemoved} orphan sidecar(s) removed.`,
      );
    }
    clearSweepError("retention");
  } catch (e) {
    recordSweepError("retention", e);
  }
  return result;
}
