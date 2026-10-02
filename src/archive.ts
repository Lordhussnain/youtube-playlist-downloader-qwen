// src/archive.ts — yt-dlp --download-archive helpers.
//
// The archive file is yt-dlp's own idempotence layer: one video id per line
// (optionally prefixed by the extractor). When our copy of a file disappears
// we must scrub the id, otherwise yt-dlp will skip the video forever.

import { existsSync, readFileSync, writeFileSync } from "node:fs";

/**
 * Remove one or many video ids from the archive file (the id is the last
 * field of each line) so lost or moved files can be downloaded again.
 *
 * Batched on purpose: the archive is rewritten at most ONCE per call. The
 * startup missing-files sweep used to call the single-id form inside its loop
 * — 100 missing files on a 500k-line archive meant 100 full read/filter/write
 * passes of a ~20 MB file on the startup path. Returns how many lines were
 * removed.
 */
export function removeFromArchive(archiveFile: string, videoIds: string | Iterable<string>): number {
  try {
    if (!archiveFile || !existsSync(archiveFile)) return 0;
    const wanted = new Set(typeof videoIds === "string" ? [videoIds] : videoIds);
    wanted.delete("");
    if (wanted.size === 0) return 0;
    const text = readFileSync(archiveFile, "utf-8");
    const lines = text.split("\n");
    const kept: string[] = [];
    let removed = 0;
    for (const l of lines) {
      const trimmed = l.trim();
      const id = trimmed ? trimmed.slice(trimmed.lastIndexOf(" ") + 1) : "";
      if (id && wanted.has(id)) {
        removed++;
        continue;
      }
      kept.push(l);
    }
    if (removed > 0) writeFileSync(archiveFile, kept.join("\n"));
    return removed;
  } catch {
    // Best-effort: a failed scrub is surfaced on the next attempt instead.
    return 0;
  }
}

/** All video ids currently present in the archive file. */
export function readArchiveIds(archiveFile: string): Set<string> {
  const ids = new Set<string>();
  try {
    if (!existsSync(archiveFile)) return ids;
    for (const line of readFileSync(archiveFile, "utf-8").split("\n")) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      ids.add(trimmed.slice(trimmed.lastIndexOf(" ") + 1));
    }
  } catch {
    // ignore
  }
  return ids;
}
