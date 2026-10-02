// src/scanner.ts — playlist/channel scanning and job ingestion.
//
// Two sources feed the same ingest path: the full yt-dlp flat-playlist scan
// (slow, authoritative) and the cheap per-channel RSS poller (fast, recent
// uploads only). Both dedupe on the YouTube video id, so the same video found
// by a playlist, a channel, and a watch URL is only ever queued once.

import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { db, existingJobIds, peekNextIndex, setNextIndex } from "./db";
import { cookiesArgs, ytDlp } from "./tools";
import { sanitizeFolderName } from "./util";
import { spawnBounded, tailLines } from "./spawn";
import { abortController, stats } from "./state";
import { logError } from "./logger";
import type { Config } from "./config";

export interface ListingItem {
  id: string;
  title: string;
  playlist: string;
  duration: number;
}

/**
 * Canonical form of a video URL: strip tracking/timestamp junk and fold
 * youtu.be short links into watch URLs, so the same video always produces the
 * same stored job URL regardless of how it was discovered.
 */
export function normalizeVideoUrl(url: string): string {
  const trimmed = (url || "").trim();
  if (!trimmed) return trimmed;
  try {
    const u = new URL(trimmed);
    const shortMatch = u.hostname === "youtu.be" ? u.pathname.slice(1).split("/")[0] : null;
    const v = shortMatch || u.searchParams.get("v");
    if (v && /^[\w-]{6,}$/.test(v)) return `https://www.youtube.com/watch?v=${v}`;
    return trimmed;
  } catch {
    return trimmed;
  }
}

/** Fetch a flat listing of a playlist/channel URL via yt-dlp. */
export async function getPlaylistItems(url: string, config: Config): Promise<ListingItem[]> {
  const args = [
    ytDlp(),
    ...cookiesArgs(config),
    "--flat-playlist",
    "--print",
    "%(playlist_title)s|||%(id)s|||%(title)s|||%(duration)s",
    url,
  ];
  // Bounded: a flat listing of even a very large channel finishes in minutes;
  // one stuck on a dead socket used to hang the scan request forever.
  const { stdout: out, stderr, code, timedOut } = await spawnBounded(args, {
    timeoutMs: LISTING_TIMEOUT_MS,
    signal: abortController.signal,
  });
  if (timedOut) {
    logError("scan", `${url}: listing timed out after ${LISTING_TIMEOUT_MS / 60000} min`);
    return [];
  }
  if (code !== 0) {
    logError("scan", `${url}: yt-dlp exited ${code}: ${tailLines(stderr)}`);
    return [];
  }
  return parseListing(out);
}

/**
 * Longest folder name we will create. A 1,000-character playlist title made
 * `mkdir` throw ENAMETOOLONG and abort the WHOLE ingest, not one item; 120 is
 * well inside every filesystem's 255-byte component limit even for
 * multi-byte scripts, and leaves room under Windows' MAX_PATH for the files.
 */
export const MAX_FOLDER_CHARS = 120;
/** Stored title cap; filenames are budgeted separately by fitBaseFilename. */
export const MAX_TITLE_CHARS = 300;

/** 15 minutes: generous for a 10k-video flat listing, finite for a hang. */
export const LISTING_TIMEOUT_MS = 15 * 60 * 1000;

const LISTING_SEP = "|||";
/** What a YouTube video id looks like; anything else is a parse artefact. */
export const VIDEO_ID_RE = /^[\w-]{6,}$/;
/** Id characters only (any length) — the ingest-time guard. */
const SAFE_ID_RE = /^[\w-]+$/;

/**
 * Parse the `--print "%(playlist_title)s|||%(id)s|||%(title)s|||%(duration)s"`
 * output. Field-count aware: a title containing the separator used to shift
 * every field, so the TITLE fragment became the primary key and a
 * permanently-failing job was inserted under a bogus id. The id is the only
 * field with a fixed shape, so it anchors the split: playlist is everything
 * before it, duration is the last field, title is whatever sits between.
 * Lines whose id does not validate are skipped and logged.
 */
export function parseListing(out: string): ListingItem[] {
  const items: ListingItem[] = [];
  for (const line of out.split("\n")) {
    if (!line.trim()) continue;
    const parts = line.split(LISTING_SEP);
    let item: ListingItem | null = null;
    if (parts.length === 4 && VIDEO_ID_RE.test(parts[1].trim())) {
      item = {
        playlist: parts[0].trim() || "playlist",
        id: parts[1].trim(),
        title: (parts[2].trim() || "video").slice(0, MAX_TITLE_CHARS),
        duration: parseFloat(parts[3] ?? "NaN"),
      };
    } else if (parts.length > 4) {
      // A separator inside the playlist title or the video title. The id is
      // the first field (after the first) that validates and is followed by
      // at least two more fields.
      const idx = parts.findIndex((p, i) => i >= 1 && i <= parts.length - 3 && VIDEO_ID_RE.test(p.trim()));
      if (idx > 0) {
        item = {
          playlist: parts.slice(0, idx).join(LISTING_SEP).trim() || "playlist",
          id: parts[idx].trim(),
          title: (parts.slice(idx + 1, -1).join(LISTING_SEP).trim() || "video").slice(0, MAX_TITLE_CHARS),
          duration: parseFloat(parts[parts.length - 1] ?? "NaN"),
        };
      }
    }
    if (!item) {
      logError("scan", `skipped unparseable listing line: ${line.slice(0, 200)}`);
      continue;
    }
    items.push(item);
  }
  return items;
}

/**
 * Insert (or skip) a batch of listing items into the jobs table. Shared by the
 * full scanner (yt-dlp flat listing) and the cheap RSS poller.
 */
export async function ingestItems(
  items: ListingItem[],
  config: Config,
  overrideFolderName?: string,
): Promise<{ found: number; added: number; skipped: number }> {
  if (items.length === 0) return { found: 0, added: 0, skipped: 0 };
  const folder = sanitizeFolderName(overrideFolderName || items[0].playlist || "Single Videos").slice(0, MAX_FOLDER_CHARS).trim() || "playlist";
  const outputDir = join(config.outputRoot, folder);
  await mkdir(outputDir, { recursive: true });
  const targetFormat = config.videoQuality === "audio" ? "mp3" : (config.targetFormat || "mp4");
  const wantSubs = config.downloadSubtitles ? 1 : 0;
  const wantThumb = config.writeThumbnail ? 1 : 0;
  const wantDesc = config.writeDescription ? 1 : 0;
  const conversionStatus = config.videoQuality === "audio" || targetFormat !== "mp4" ? "pending" : "not_needed";
  // Sidecar metadata (subs/thumbnail/description/info.json) is fetched by the
  // metadata worker after the download completes.
  const metadataStatus = wantSubs || wantThumb || wantDesc || config.writeInfoJson ? "pending" : "not_needed";

  let added = 0;
  let skipped = 0;
  const insertTransaction = db.transaction((batch: ListingItem[]) => {
    // Prefetch once instead of 3 statements per item inside the write
    // transaction (a 2,000-video playlist used to issue ~6,000 statements
    // while holding the write lock): the set of ids already known, and the
    // folder's index high-water mark.
    const known = existingJobIds(batch.map((i) => i.id));
    let nextIndex = peekNextIndex(folder);
    const stmt = db.prepare(
      `INSERT OR IGNORE INTO jobs
         (id, url, title, output_directory, target_format, want_subtitles, want_thumbnail, want_description, folder, "index", duration, download_status, conversion_status, metadata_status)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)`,
    );
    const requeueLive = db.prepare(
      `UPDATE jobs SET download_status = 'pending', updated_at = CURRENT_TIMESTAMP WHERE id = ? AND download_status = 'waiting_live'`,
    );
    const seenInBatch = new Set<string>();
    for (const item of batch) {
      if (!SAFE_ID_RE.test(item.id)) {
        // Defence in depth for the RSS path and any future lister: a bogus id
        // (a title fragment with spaces, an empty string) becomes a
        // permanently-failing row, so refuse it here too. Shape only — the
        // length check lives in parseListing, where yt-dlp's output is parsed.
        logError("ingest", `skipped item with invalid video id ${JSON.stringify(item.id).slice(0, 80)}`);
        skipped++;
        stats.skipped++;
        continue;
      }
      if (known.has(item.id) || seenInBatch.has(item.id)) {
        // A job parked as waiting_live (stream was live at download time) may
        // have ended by now — any fresh listing that still contains it requeues
        // it; the !is_live filter drops it again if it is somehow still live.
        requeueLive.run(item.id);
        skipped++;
        stats.skipped++;
        continue;
      }
      if (
        config.skipShorts &&
        !config.downloadShorts &&
        Number.isFinite(item.duration) &&
        item.duration > 0 &&
        item.duration < 60
      ) {
        skipped++;
        stats.skipped++;
        continue;
      }
      const index = ++nextIndex;
      const res = stmt.run(
        item.id,
        normalizeVideoUrl(`https://www.youtube.com/watch?v=${item.id}`),
        item.title,
        outputDir,
        targetFormat,
        wantSubs,
        wantThumb,
        wantDesc,
        folder,
        index,
        Number.isFinite(item.duration) && item.duration > 0 ? item.duration : null,
        conversionStatus,
        metadataStatus,
      );
      seenInBatch.add(item.id);
      if (res.changes === 0) {
        // Raced by another ingester (RSS + scan on the same id): not ours.
        nextIndex--;
        skipped++;
        stats.skipped++;
        continue;
      }
      added++;
      stats.totalQueued++;
    }
    if (added > 0) setNextIndex(folder, nextIndex);
  });
  insertTransaction(items);
  return { found: items.length, added, skipped };
}

/** Full scan of one playlist/channel URL, then ingest. */
export async function scanAndIngest(
  url: string,
  config: Config,
  overrideFolderName?: string,
): Promise<{ found: number; added: number; skipped: number }> {
  const items = await getPlaylistItems(url, config);
  return ingestItems(items, config, overrideFolderName);
}
