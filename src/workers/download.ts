// src/workers/download.ts — the resilient download worker.
//
// Pulls videos with yt-dlp and records the result in the job database.
// Everything failure-related is designed around one idea: never lose work
// that has already been done. A failed attempt keeps its .part file, the next
// attempt resumes from it with --continue, and the per-video retry budget only
// shrinks while the video is making no forward progress.

import { stat, unlink } from "node:fs/promises";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { claimDownloadJob, db, perVideoCap, type Job } from "../db";
import { activeDlSlots, autoscaler } from "../autoscale";
import { cookiesArgs, ytDlp } from "../tools";
import { checkDiskSpace, notePipelineFailure, notePipelineSuccess, triggerPause } from "../resilience";
import { findPartialFile } from "../reconcile";
import { removeFromArchive } from "../archive";
import { computeBackoffMs, computeDownloadTimeoutMs, isTransientDownloadError } from "../retry";
import {
  findDownloadedFile,
  fitBaseFilename,
  formatBytesPerSec,
  parseSpeedToBytesPerSec,
  sanitizeFileName,
} from "../util";
import { updateAbsoluteLine } from "../dashboard";
import { abortController, activeProcs, isPaused, stats, workerStatuses } from "../state";
import { logError } from "../logger";
import { QUALITY_FORMATS, type Config } from "../config";

export const aliveDownloadWorkers = new Set<number>();

export async function downloadWorker(id: number, config: Config): Promise<void> {
  const workerId = `dl-${id}`;
  aliveDownloadWorkers.add(id);
  while (!abortController.signal.aborted) {
    if (isPaused()) {
      await Bun.sleep(2000);
      continue;
    }
    // Autoscaling gate: a scaled-down slot's worker idles here instead of
    // claiming work (a job already in flight always runs to completion).
    if (!activeDlSlots.has(id)) {
      await Bun.sleep(1000);
      continue;
    }

    const disk = await checkDiskSpace(config.outputRoot, config.minFreeSpaceGB);
    if (!disk.ok) {
      triggerPause(`LOW_DISK_SPACE (${disk.free.toFixed(1)}GB < ${config.minFreeSpaceGB}GB)`);
      await Bun.sleep(10000);
      continue;
    }

    const job = claimDownloadJob(workerId);
    if (!job) {
      await Bun.sleep(500);
      continue;
    }

    try {
      await runDownload(id, job, config);
    } catch (err: any) {
      await handleDownloadFailure(id, job, config, err);
    } finally {
      activeProcs.delete(id);
      autoscaler.clearWorker(id);
    }
  }
  // Loop exited (shutdown): this worker is no longer alive.
  aliveDownloadWorkers.delete(id);
}

/** One download attempt for `job`. Throws on any failure. */
async function runDownload(id: number, job: Job, config: Config): Promise<void> {
  updateWorkerLine(id, `⬇️ Starting... | ${job.title}`, config);
  const format = QUALITY_FORMATS[config.videoQuality] || QUALITY_FORMATS["1080p"];
  const baseFilename = fitBaseFilename(
    job.output_directory,
    `${String(job.index).padStart(3, "0")} - ${sanitizeFileName(job.title)}`,
    job.id,
  );
  const outTemplate = join(job.output_directory, `${baseFilename}.%(ext)s`);

  const args = [
    ytDlp(),
    job.url,
    ...cookiesArgs(config),
    "--format",
    format,
    "--concurrent-fragments",
    "16",
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
    "10",
    "--extractor-retries",
    "5",
    // --continue is what makes retries cheap: yt-dlp picks the existing .part
    // file up instead of starting the transfer over.
    "--continue",
    "--no-overwrites",
  ];

  // Real bandwidth cap: --limit-rate applies per yt-dlp process, so the
  // configured global cap is split across the currently active slots.
  if (config.maxBandwidthKBps > 0) {
    const perWorkerKBps = Math.max(64, Math.floor(config.maxBandwidthKBps / Math.max(1, activeDlSlots.size)));
    args.push("--limit-rate", `${perWorkerKBps}K`);
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

  // Duration-aware watchdog: long videos legitimately take a long time on a
  // slow connection, so the timeout scales with the real duration (3×
  // realtime + 5 min slack) between the configured floor and ceiling.
  const timeoutMs = computeDownloadTimeoutMs(job.duration, {
    minMinutes: config.downloadTimeoutMinutes,
    maxMinutes: config.maxDownloadMinutes,
  });
  let timedOut = false;
  const downloadCtl = new AbortController();
  const downloadTimer = setTimeout(() => {
    timedOut = true;
    downloadCtl.abort();
  }, timeoutMs);
  const proc = Bun.spawn(args, { stdout: "pipe", stderr: "pipe", signal: downloadCtl.signal });
  activeProcs.set(id, proc);
  // Drain stderr immediately so a chatty yt-dlp cannot deadlock on a full pipe buffer.
  const stderrPromise = new Response(proc.stderr).text();

  let buffer = "";
  let finalFilePath: string | null = null;
  let lastProgressUpdate = 0;

  const reader = proc.stdout.getReader();
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += new TextDecoder().decode(value);
    const lines = buffer.split("\n");
    buffer = lines.pop() || "";

    for (const line of lines) {
      if (line.startsWith("PROGRESS:")) {
        const parts = line.replace("PROGRESS:", "").split("|");
        const bps = parseSpeedToBytesPerSec(parts[1]);
        if (bps > 0) autoscaler.recordSpeed(id, bps);
        const sizeNum = parseInt(parts[3], 10);
        const dlNum = parseInt(parts[4], 10);
        let pctNum = parseFloat(parts[0]);
        if (Number.isNaN(pctNum) && dlNum > 0 && sizeNum > 0) pctNum = (dlNum / sizeNum) * 100;
        if (!Number.isNaN(pctNum) && pctNum >= 0 && Date.now() - lastProgressUpdate > 500) {
          // Backfill file_size from progress so the global ETA has a total to work with.
          const totalBytes = Number.isFinite(sizeNum) && sizeNum > 0 ? sizeNum : null;
          // best_progress is the high-water mark of this job's attempts: it is
          // what lets the retry budget forgive repeated failures at increasing
          // completion percentages (see handleDownloadFailure).
          updateJobProgress(job.id, pctNum, bps, parseFloat(parts[2]) || 0, totalBytes);
          const speedTxt = bps > 0 ? formatBytesPerSec(bps) : "Calculating...";
          const etaNum = parseFloat(parts[2]);
          const etaTxt = Number.isFinite(etaNum) && etaNum > 0 ? `, ETA ${Math.round(etaNum)}s` : "";
          updateWorkerLine(id, `⬇️ ${pctNum.toFixed(1)}% @ ${speedTxt}${etaTxt} | ${job.title}`, config);
          lastProgressUpdate = Date.now();
        }
      } else {
        const trimmed = line.trim();
        if (trimmed && existsSync(trimmed)) {
          finalFilePath = trimmed;
        } else {
          // Capture paths embedded in yt-dlp status lines (merger output, etc.).
          const m = trimmed.match(/Merged formats into "(.+)"$/) || trimmed.match(/Destination: (.+)$/);
          if (m && existsSync(m[1])) finalFilePath = m[1];
        }
      }
    }
  }

  const [stderrText, code] = await Promise.all([stderrPromise, proc.exited]);
  clearTimeout(downloadTimer);
  activeProcs.delete(id);

  if (isPaused()) {
    parkPaused(job.id);
    updateWorkerLine(id, `⏸️ Paused | ${job.title}`, config);
    return;
  }

  if (timedOut) throw new Error(`Process timed out (${Math.round(timeoutMs / 60000)}m)`);

  if (code === 0) {
    let filePath =
      finalFilePath ||
      buffer
        .split("\n")
        .reverse()
        .find((l) => l.trim() && existsSync(l.trim()))
        ?.trim() ||
      "";
    if (!filePath) filePath = await findDownloadedFile(job.output_directory, baseFilename);
    if (!filePath) {
      logError("download", `${job.id} exited 0 but the output file could not be located: ${job.title}`);
      throw new Error("Download finished but output file could not be located");
    }
    const fileSize = (await stat(filePath)).size;
    recordSuccess(job.id, filePath, fileSize);
    stats.downloaded++;
    notePipelineSuccess("dl");
    updateWorkerLine(id, `✅ Downloaded | ${job.title}`, config);
  } else {
    const tail = [stderrText, buffer]
      .filter(Boolean)
      .join("\n")
      .split("\n")
      .filter((l) => l.trim())
      .slice(-4)
      .join(" ");
    throw new Error(tail || `yt-dlp exited with code ${code}`);
  }
}

/**
 * Central failure handler: classifies the error, decides whether the partial
 * file survives, and computes the next state. The guiding rules:
 *
 *   • transient (network/timeout/5xx)  → requeue with exponential backoff
 *   • corrupt/incomplete               → delete the partial and resume (bounded
 *                                        by maxResumeAttempts, then restart)
 *   • live stream in "wait for VOD"    → park as waiting_live
 *   • permanent (private/removed/…)    → fail fast, never auto-requeued
 *   • retry budget spent               → park as failed for the sweep
 */
async function handleDownloadFailure(id: number, job: Job, config: Config, err: any): Promise<void> {
  if (isPaused()) {
    parkPaused(job.id);
    updateWorkerLine(id, `⏸️ Paused | ${job.title}`, config);
    return;
  }

  const errMsg = String(err?.message || err);
  const lower = errMsg.toLowerCase();
  const base = baseNameOf(job);

  // Signature challenge broke (yt-dlp extractor changed) — self-heal by
  // updating yt-dlp, then retry immediately with a clean budget.
  if (lower.includes("signature") || lower.includes("unable to extract")) {
    console.warn("⚠️ Signature challenge failed. Auto-updating yt-dlp...");
    const updateProc = Bun.spawn([ytDlp(), "-U"], { stdout: "pipe", stderr: "pipe" });
    await updateProc.exited;
    resetForRetry(job.id);
    updateWorkerLine(id, `🔄 Auto-updated yt-dlp, retrying... | ${job.title}`, config);
    return;
  }

  // Corrupt/incomplete partial: delete the .part file so yt-dlp restarts that
  // transfer — but only after the resume budget is spent. Until then we keep
  // the partial and let --continue resume from it.
  if (lower.includes("unable to resume") || lower.includes("incomplete") || lower.includes("corrupt")) {
    const resumeCount = (job.resume_count || 0) + 1;
    const partial =
      job.partial_file_path && existsSync(job.partial_file_path)
        ? job.partial_file_path
        : await findPartialFile(job.output_directory, base);
    if (resumeCount >= Math.max(1, config.maxResumeAttempts) || !partial) {
      // Budget spent (or nothing to resume): throw the partial away and
      // restart this video from scratch.
      if (partial) await unlink(partial).catch(() => {});
      resetForRetry(job.id, { incrementRetry: true, clearPartial: true });
      updateWorkerLine(id, `🗑️ Restarting from scratch | ${job.title}`, config);
      return;
    }
    // Keep the partial, count the resume attempt, and try again shortly.
    db.run(
      `UPDATE jobs SET download_status = 'pending', resume_count = ?, download_claimed_by = NULL, last_error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
      [resumeCount, errMsg.slice(0, 500), job.id],
    );
    updateWorkerLine(id, `⏳ Resuming (attempt ${resumeCount}/${config.maxResumeAttempts}) | ${job.title}`, config);
    await Bun.sleep(computeBackoffMs(resumeCount, config.retryBackoffBaseSeconds, config.retryBackoffMaxSeconds));
    return;
  }

  // --download-archive recorded the id but our copy is gone (deleted by
  // hand, moved, or the folder was cleaned). Scrub the id from the archive
  // so yt-dlp will actually download it on the retry.
  if (lower.includes("output file could not be located")) {
    removeFromArchive(config.archiveFile, job.id);
    resetForRetry(job.id, { incrementRetry: true, clearPartial: true });
    updateWorkerLine(id, `Re-downloading (archive entry scrubbed) | ${job.title}`, config);
    return;
  }

  // archiveLiveStreams mode: yt-dlp refused the job because the stream is
  // live right now. Park it until the next full rescan or a manual retry —
  // scans flip waiting_live jobs back to pending once a VOD exists.
  if (lower.includes("does not pass filter") || lower.includes("is live") || lower.includes("live event")) {
    db.run(
      `UPDATE jobs SET download_status = 'waiting_live', download_claimed_by = NULL, last_error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
      [errMsg.slice(0, 500), job.id],
    );
    updateWorkerLine(id, `Live now — waiting for VOD | ${job.title}`, config);
    return;
  }

  // Transient failure: remember where the .part file is so the next attempt
  // can resume from it, then requeue with exponential backoff.
  if (isTransientDownloadError(errMsg)) {
    const partial = await findPartialFile(job.output_directory, base);
    const { retryCount, bestProgress, progress } = readProgressState(job.id);
    // The retry budget only shrinks when the video makes no forward progress:
    // a flaky connection that keeps advancing is forgiven, a video stuck at
    // the same percentage eventually exhausts its budget.
    const nextRetry = progress > bestProgress ? retryCount : retryCount + 1;
    db.run(
      `UPDATE jobs SET download_status = 'pending', retry_count = ?, best_progress = ?, partial_file_path = ?,
         download_claimed_by = NULL, last_error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
      [nextRetry, Math.max(bestProgress, progress), partial || null, errMsg.slice(0, 500), job.id],
    );
    const backoff = computeBackoffMs(nextRetry, config.retryBackoffBaseSeconds, config.retryBackoffMaxSeconds);
    updateWorkerLine(id, `🌐 Transient error, retrying in ${Math.round(backoff / 1000)}s | ${job.title}`, config);
    await Bun.sleep(backoff);
    return;
  }

  // Permanent or unknown error: spend the retry budget, then park as failed
  // for the periodic sweep (which skips permanent errors entirely).
  const { retryCount } = readProgressState(job.id);
  const cap = perVideoCap(config);
  const newStatus = retryCount + 1 >= cap ? "failed" : "pending";
  const partial = newStatus === "failed" ? null : await findPartialFile(job.output_directory, base);
  db.run(
    `UPDATE jobs SET download_status = ?, retry_count = ?, partial_file_path = ?, download_claimed_by = NULL, last_error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
    [newStatus, retryCount + 1, partial, errMsg.slice(0, 500), job.id],
  );
  if (newStatus === "failed") {
    stats.failed++;
    logError("download", `${job.id} ${job.title}: ${errMsg.slice(0, 500)}`);
    notePipelineFailure("dl", config);
  }
  updateWorkerLine(id, `❌ Failed | ${job.title}`, config);
}

// --- small DB helpers --------------------------------------------------------

/** The on-disk base name used for a job's files (no extension). */
function baseNameOf(job: Job): string {
  return `${String(job.index).padStart(3, "0")} - ${sanitizeFileName(job.title)}`;
}

/** Record progress, keeping best_progress as the high-water mark. */
function updateJobProgress(
  id: string,
  pct: number,
  bps: number,
  eta: number,
  totalBytes: number | null,
): void {
  db.run(
    `UPDATE jobs SET progress = ?, speed = ?, eta = ?,
       best_progress = MAX(COALESCE(best_progress, 0), ?),
       file_size = COALESCE(?, file_size)
     WHERE id = ?`,
    [pct, bps, eta, pct, totalBytes, id],
  );
}

function readProgressState(id: string): { retryCount: number; bestProgress: number; progress: number } {
  const row = db
    .query("SELECT retry_count, best_progress, progress FROM jobs WHERE id = ?")
    .get(id) as any;
  return {
    retryCount: row?.retry_count || 0,
    bestProgress: row?.best_progress || 0,
    progress: row?.progress || 0,
  };
}

function parkPaused(id: string): void {
  db.run(
    `UPDATE jobs SET download_status = 'paused', download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
    [id],
  );
}

function resetForRetry(id: string, opts: { incrementRetry?: boolean; clearPartial?: boolean } = {}): void {
  const clearPartial = opts.clearPartial ? 1 : 0;
  if (opts.incrementRetry) {
    db.run(
      `UPDATE jobs SET download_status = 'pending', retry_count = retry_count + 1, download_claimed_by = NULL,
         partial_file_path = CASE WHEN ? THEN NULL ELSE partial_file_path END, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
      [clearPartial, id],
    );
  } else {
    db.run(
      `UPDATE jobs SET download_status = 'pending', retry_count = 0, download_claimed_by = NULL,
         partial_file_path = CASE WHEN ? THEN NULL ELSE partial_file_path END, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
      [clearPartial, id],
    );
  }
}

function recordSuccess(id: string, filePath: string, fileSize: number): void {
  db.run(
    `UPDATE jobs SET download_status = 'downloaded', file_path = ?, file_size = ?, partial_file_path = NULL,
       progress = 100, download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
    [filePath, fileSize, id],
  );
}

function updateWorkerLine(id: number, text: string, _config: Config): void {
  workerStatuses.set(`DL${id}`, text);
  updateAbsoluteLine(2 + id, `[DL${id}] ${text}`);
}
