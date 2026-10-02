// src/dedupe.ts — content-hash dedupe across playlists (plan 5.2).
//
// The same upload often appears under several video ids (re-uploads, mirrors,
// a channel's own "best of" playlist). Their content is only known once the
// bytes are on disk, so dedupe runs *after* the SHA-256 is computed: when a
// finished file's hash matches an earlier job's, the new file is replaced by a
// hard link to the original and the job is marked `duplicate_of`. Every row
// keeps a valid `file_path` (the sweeps, the dashboard and the player all
// still work), the archive just stops paying for the bytes twice.
//
// Hard links need the same filesystem; anything else (EXDEV, a filesystem
// without link support, the original gone) leaves the new file alone — dedupe
// is an optimisation, never a reason to fail a job.

import { existsSync } from "node:fs";
import { link, rename, stat, unlink } from "node:fs/promises";
import { db } from "./db";
import { logError } from "./logger";

export interface DuplicateCandidate {
  id: string;
  file_path: string;
}

/**
 * Atomically decide whether `callerId` is a duplicate and of whom. Runs in one
 * synchronous SQLite transaction, so two workers finishing identical files in
 * the same instant cannot both pick each other: the first to claim becomes
 * the duplicate, the second sees "someone already points at me" and stays
 * the original. Candidates are other finished rows with the same hash that
 * are not themselves duplicates, oldest first, whose file still exists.
 * Returns the original and has already written `duplicate_of` when non-null.
 */
export function claimDuplicate(hash: string, callerId: string): DuplicateCandidate | null {
  if (!hash) return null;
  const tx = db.transaction((): DuplicateCandidate | null => {
    const pointedAt = db.query(`SELECT 1 FROM jobs WHERE duplicate_of = ? LIMIT 1`).get(callerId);
    if (pointedAt) return null;
    const rows = db
      .query(
        `SELECT id, file_path FROM jobs
         WHERE integrity = ? AND id != ? AND file_path IS NOT NULL
           AND download_status = 'downloaded' AND duplicate_of IS NULL
         ORDER BY created_at ASC, id ASC LIMIT 10`,
      )
      .all(hash, callerId) as DuplicateCandidate[];
    const original = rows.find((r) => existsSync(r.file_path));
    if (!original) return null;
    db.run(`UPDATE jobs SET duplicate_of = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [original.id, callerId]);
    return original;
  });
  return tx();
}

/** Read-only view of what claimDuplicate would pick (dashboard / tests). */
export function findDuplicateByHash(hash: string, excludeId: string): DuplicateCandidate | null {
  if (!hash) return null;
  const rows = db
    .query(
      `SELECT id, file_path FROM jobs
       WHERE integrity = ? AND id != ? AND file_path IS NOT NULL
         AND download_status = 'downloaded' AND duplicate_of IS NULL
       ORDER BY created_at ASC, id ASC LIMIT 10`,
    )
    .all(hash, excludeId) as DuplicateCandidate[];
  return rows.find((r) => existsSync(r.file_path)) ?? null;
}

export type LinkOutcome = "hardlinked" | "already-linked" | "skipped";

/**
 * Replace `duplicatePath` with a hard link to `originalPath`. Atomic from the
 * reader's point of view: the link is created under a temp name and renamed
 * over the duplicate, so there is never a moment without a file.
 */
export async function linkDuplicate(duplicatePath: string, originalPath: string): Promise<LinkOutcome> {
  if (duplicatePath === originalPath) return "already-linked";
  const [a, b] = await Promise.all([stat(originalPath).catch(() => null), stat(duplicatePath).catch(() => null)]);
  if (!a || !b) return "skipped";
  if (a.dev === b.dev && a.ino === b.ino && a.ino !== 0) return "already-linked";
  if (a.size !== b.size) return "skipped"; // same hash, different size: do not trust it
  const tmp = `${duplicatePath}.dedupe-tmp`;
  try {
    await unlink(tmp).catch(() => {});
    await link(originalPath, tmp);
    await rename(tmp, duplicatePath);
    return "hardlinked";
  } catch {
    await unlink(tmp).catch(() => {});
    return "skipped";
  }
}

/**
 * Called right after a job's hash is recorded. Returns the original's id when
 * the file was deduplicated (and `jobs.duplicate_of` was set), else null.
 */
export async function dedupeAfterHash(
  job: { id: string; title: string },
  filePath: string,
  hash: string | null,
  enabled: boolean,
): Promise<string | null> {
  if (!enabled || !hash) return null;
  const original = claimDuplicate(hash, job.id);
  if (!original) return null;
  const release = () =>
    db.run(`UPDATE jobs SET duplicate_of = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND duplicate_of = ?`, [job.id, original.id]);
  try {
    const outcome = await linkDuplicate(filePath, original.file_path);
    if (outcome === "skipped") {
      release();
      return null;
    }
    if (outcome === "hardlinked") console.log(`🔗 ${job.title}: identical to ${original.id} — hard-linked, bytes stored once.`);
    return original.id;
  } catch (e: any) {
    release();
    logError("dedupe", `${job.id} ${job.title}: ${e?.message || e}`);
    return null;
  }
}
