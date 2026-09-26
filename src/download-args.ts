// src/download-args.ts — yt-dlp command construction (pure, unit-tested).
//
// Everything the download worker needs to know about HOW to fetch a video is
// decided here: which downloader engine (aria2c multi-connection vs yt-dlp's
// native downloader), how many connections/fragments to use, how the bandwidth
// cap is split across active slots, and the watchdog timeout for this video.
//
// yt-dlp reference behaviour this encodes (verified against yt-dlp source):
//   • `--downloader aria2c` makes yt-dlp hand the transfer to aria2c with
//     `-x16 -s16 -j16 --min-split-size 1M` by default; extra args arrive via
//     `--downloader-args aria2c:"…"` (shlex-parsed, so quote the whole list).
//   • yt-dlp maps `--limit-rate` to aria2c's `--max-overall-download-limit`,
//     so the global bandwidth cap keeps working on both engines.
//   • aria2c only speaks http/https/ftp — for HLS/live streams yt-dlp silently
//     falls back to its native downloader, so no special-casing is needed.
//   • External downloads still land in yt-dlp's `<name>.part` temp file, so
//     partial-file tracking and `--continue` resume behave identically.

import { join } from "node:path";
import { cookiesArgs } from "./tools";
import { fitBaseFilename, sanitizeFileName } from "./util";
import { computeDownloadTimeoutMs } from "./retry";
import { QUALITY_FORMATS, type Config } from "./config";
import type { Job } from "./db";

export type DownloaderEngine = "aria2c" | "native";

/** Which engine a download should use, given availability and config. */
export function resolveDownloaderEngine(config: Config, aria2cAvailable: boolean): DownloaderEngine {
  return config.useAria2c && aria2cAvailable ? "aria2c" : "native";
}

/**
 * The value for yt-dlp's `--downloader-args aria2c:"…"`.
 *
 * yt-dlp already defaults aria2c to `-x16 -s16 -j16 --min-split-size 1M`, so we
 * only emit what differs from that baseline — fewer moving parts, and an
 * explicit `-k` only when the operator changed the split threshold.
 */
export function buildAria2cArgs(config: Config): string {
  const n = Math.max(1, Math.floor(config.connectionsPerDownload));
  const parts = [`-x ${n}`, `-s ${n}`, `-j ${n}`];
  const split = (config.minSplitSize || "").trim();
  if (split && split !== "1M") parts.push(`--min-split-size ${split}`);
  return parts.join(" ");
}

/**
 * The per-process bandwidth cap in KB/s.
 *
 * yt-dlp's `--limit-rate` is per process, so the configured global cap is
 * divided across the slots that are actually allowed to claim work. Floored at
 * 64 KB/s so a large worker count can never throttle a stream to a trickle.
 * Returns null when no cap is configured.
 */
export function computePerWorkerLimitKBps(config: Config, activeSlots: number): number | null {
  if (config.maxBandwidthKBps <= 0) return null;
  const slots = Math.max(1, Math.floor(activeSlots));
  return Math.max(64, Math.floor(config.maxBandwidthKBps / slots));
}

/** The on-disk base name for a job's files (no extension). */
export function jobBaseFilename(job: Pick<Job, "index" | "title" | "id">): string {
  return `${String(job.index).padStart(3, "0")} - ${sanitizeFileName(job.title)}`;
}

export interface DownloadPlan {
  engine: DownloaderEngine;
  /** Full argv for the yt-dlp process. */
  args: string[];
  /** Duration-aware watchdog for this specific video. */
  timeoutMs: number;
  /** Applied `--limit-rate` value in KB/s, or null when uncapped. */
  perWorkerLimitKBps: number | null;
  /** Output template (`…/base.%(ext)s`) and the base name without extension. */
  baseFilename: string;
  outTemplate: string;
}

export interface BuildDownloadPlanOptions {
  job: Pick<Job, "id" | "url" | "title" | "index" | "output_directory" | "duration">;
  config: Config;
  /** Slots currently allowed to claim work (drives the bandwidth split). */
  activeSlots: number;
  /** Whether aria2c was found on this machine. */
  aria2cAvailable: boolean;
}

/** Build the complete yt-dlp invocation for one download attempt. */
export function buildDownloadPlan(opts: BuildDownloadPlanOptions): DownloadPlan {
  const { job, config, activeSlots, aria2cAvailable } = opts;

  const engine = resolveDownloaderEngine(config, aria2cAvailable);
  const format = QUALITY_FORMATS[config.videoQuality] || QUALITY_FORMATS["1080p"];
  const baseFilename = fitBaseFilename(
    job.output_directory,
    jobBaseFilename(job),
    job.id,
  );
  const outTemplate = join(job.output_directory, `${baseFilename}.%(ext)s`);

  const args: string[] = [
    // argv[0] is filled in by the caller (the resolved yt-dlp path).
    job.url,
    ...cookiesArgs(config),
    "--format",
    format,
    // Parallel fragments for DASH/HLS (native path). Ignored when aria2c is
    // handling a whole-file transfer, which splits internally instead.
    "--concurrent-fragments",
    String(Math.max(1, Math.floor(config.concurrentFragments))),
    "-o",
    outTemplate,
    // --newline/--no-colors keep progress lines parseable from a pipe.
    "--progress",
    "--newline",
    "--no-colors",
    "--progress-template",
    "download:PROGRESS:%(progress.percent).1f|%(progress.speed)f|%(progress.eta)f|%(progress.total_bytes)s|%(progress.downloaded_bytes)s",
    // Print the final path after all post-processing so we can record it.
    // --print implies --simulate, so --no-simulate is required to actually write files.
    "--print",
    "after_move:%(filepath)s",
    "--no-simulate",
    "--socket-timeout",
    "15",
    "--retries",
    "10",
    "--retry-sleep",
    "5",
    "--fragment-retries",
    String(Math.max(1, Math.floor(config.fragmentRetries))),
    "--extractor-retries",
    "5",
    // --continue is what makes retries cheap: yt-dlp (or aria2c) picks the
    // existing .part file up instead of starting the transfer over.
    "--continue",
    "--no-overwrites",
  ];

  // Real bandwidth cap. yt-dlp translates --limit-rate into the external
  // downloader's own rate limit, so this works for both engines.
  const perWorkerLimitKBps = computePerWorkerLimitKBps(config, activeSlots);
  if (perWorkerLimitKBps) {
    args.push("--limit-rate", `${perWorkerLimitKBps}K`);
  }

  // yt-dlp's own idempotence layer: ids already in the archive file are
  // never re-downloaded, even if a job is re-queued after a DB reset.
  if (config.archiveFile) args.push("--download-archive", config.archiveFile);

  // "Wait for VOD" mode: never grab a stream while it is still live — the
  // job is parked as waiting_live and re-queued by the next full scan.
  if (config.archiveLiveStreams) args.push("--match-filters", "!is_live");

  // Sidecar files (subs/thumbnail/description/info.json) are fetched by the
  // metadata worker once the download completes; the download phase only
  // enriches the container itself (embedded art/metadata/chapters).
  if (config.embedMetadata) args.push("--embed-thumbnail", "--embed-metadata", "--embed-chapters");

  // Multi-connection downloader. HLS/live streams fall back to the native
  // downloader inside yt-dlp automatically.
  if (engine === "aria2c") {
    args.push("--downloader", "aria2c");
    args.push("--downloader-args", `aria2c:"${buildAria2cArgs(config)}"`);
  }

  // Native-downloader tuning. Range-based chunking can dramatically improve
  // throughput on the native path, but some CDNs misbehave with Range
  // requests — hence opt-in (empty = yt-dlp's default behaviour).
  const chunk = (config.httpChunkSize || "").trim();
  if (chunk) args.push("--http-chunk-size", chunk);
  const buffer = (config.bufferSize || "").trim();
  if (buffer) args.push("--buffer-size", buffer);

  return {
    engine,
    args,
    timeoutMs: computeDownloadTimeoutMs(job.duration, {
      minMinutes: config.downloadTimeoutMinutes,
      maxMinutes: config.maxDownloadMinutes,
    }),
    perWorkerLimitKBps,
    baseFilename,
    outTemplate,
  };
}
