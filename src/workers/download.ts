// src/workers/download.ts — the resilient download worker.
//
// Pulls videos with yt-dlp and records the result in the job database.
// Everything failure-related is designed around one idea: never lose work
// that has already been done. A failed attempt keeps its .part file, the next
// attempt resumes from it with --continue, and the per-video retry budget only
// shrinks while the video is making no forward progress.

import { stat } from "node:fs/promises";
import { existsSync } from "node:fs";
import { claimDownloadJob, db, perVideoCap, type Job } from "../db";
import { activeDlSlots, autoscaler } from "../autoscale";
import { aria2cPath, ytDlp } from "../tools";
import { checkDiskSpace, notePipelineFailure, notePipelineSuccess, triggerPause } from "../resilience";
import { queueFailureNotification } from "../notify";
import { findPartialFile, recordJobPartial, removePartialFiles } from "../reconcile";
import { removeFromArchive } from "../archive";
import { computeBackoffMs, decideFailureOutcome } from "../retry";
import { buildDownloadPlan, jobBaseFilename } from "../download-args";
import {
  parseSelectionJson,
  parseTracksJson,
  probeAudioTracks,
  selectAudioTracks,
  type AudioTrack,
} from "../audio-tracks";
import { findDownloadedFile, formatBytesPerSec, parseSpeedToBytesPerSec } from "../util";
import { updateAbsoluteLine } from "../dashboard";
import { abortController, activeProcs, getConfig, isPaused, stats, workerStatuses } from "../state";
import { logError } from "../logger";
import type { Config } from "../config";

export const aliveDownloadWorkers = new Set<number>();

export async function downloadWorker(id: number, config: Config): Promise<void> {
  const workerId = `dl-${id}`;
  aliveDownloadWorkers.add(id);
  while (!abortController.signal.aborted) {
    // Re-read the config every iteration so settings changed from the dashboard
    // (POST /api/settings) take effect on the next job without a restart. The
    // parameter is only the initial value.
    config = getConfig();
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

    // Everything before the per-job try/catch used to be unguarded: a
    // SQLITE_BUSY from the claim (or a statfs error) threw out of the loop,
    // the supervisor restarted the worker 5 s later, and in the meantime the
    // slot sat idle. Treat these as transient and keep looping.
    let job: ReturnType<typeof claimDownloadJob>;
    try {
      const disk = await checkDiskSpace(config.outputRoot, config.minFreeSpaceGB);
      if (!disk.ok) {
        triggerPause(`LOW_DISK_SPACE (${disk.free.toFixed(1)}GB < ${config.minFreeSpaceGB}GB)`);
        await Bun.sleep(10000);
        continue;
      }
      job = claimDownloadJob(workerId);
    } catch (e: any) {
      logError("download", `${workerId}: claim failed (${e?.code || e?.message || e}) — retrying`);
      await Bun.sleep(1000);
      continue;
    }
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
  // Multi-audio support: know what the video offers (original + auto-dubbed
  // tracks) and which tracks this job wants, then hand the selection to the
  // plan so every wanted language is downloaded into one switchable file.
  const discovered = await resolveJobAudioTracks(job, config);
  const audioTracks = discovered
    ? selectAudioTracks(
        discovered,
        config.multiAudioMode,
        config.audioTrackLanguages,
        parseSelectionJson(job.audio_selection),
      )
    : [];
  // One plan per attempt: downloader engine, connection/fragment tuning,
  // bandwidth split across the currently active slots, and the watchdog.
  const plan = buildDownloadPlan({
    job,
    config,
    activeSlots: activeDlSlots.size,
    aria2cAvailable: !!aria2cPath(),
    aria2cBinary: aria2cPath(),
    audioTracks,
  });
  const { baseFilename, outTemplate, timeoutMs } = plan;
  const engineTag = plan.engine === "aria2c" ? `aria2c×${config.connectionsPerDownload}` : "native";
  const audioTag = audioTracks.length > 0 ? `, ${audioTracks.length} audio track(s)` : "";

  updateWorkerLine(id, `⬇️ Starting [${engineTag}${audioTag}]... | ${job.title}`, config);

  const run = await runSpawnedDownload(id, job, [ytDlp(), ...plan.args], timeoutMs, config);
  const { code, stderrText, tail: buffer, timedOut } = run;

  if (isPaused()) {
    parkPaused(job);
    updateWorkerLine(id, `⏸️ Paused | ${job.title}`, config);
    return;
  }
  // Per-job user pause (POST /api/jobs/pause while this job was downloading):
  // the route recorded pause_reason='user' and SIGINTed yt-dlp. An interrupted
  // transfer is parked with its .part frozen; one that finished anyway is
  // recorded as downloaded — work already done is never thrown away — and
  // keeps pause_reason='user' so the metadata/convert claims hold it until an
  // explicit Resume.
  if (code !== 0 && userPauseRequested(job.id)) {
    parkPaused(job, "user");
    updateWorkerLine(id, `⏸️ Paused by user | ${job.title}`, config);
    return;
  }

  if (timedOut) throw new Error(`Process timed out (${Math.round(timeoutMs / 60000)}m)`);

  if (code === 0) {
    // Resolve the output path now, once, instead of stat-ing every stdout
    // line while the transfer was running (see runSpawnedDownload).
    let filePath = "";
    for (const candidate of run.pathCandidates) {
      if (candidate && existsSync(candidate)) filePath = candidate; // last existing wins
    }
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

/** What one yt-dlp run produced, independent of how it is classified. */
interface SpawnedDownloadResult {
  code: number;
  stderrText: string;
  /** Unconsumed stdout remainder (no trailing newline) — error context. */
  tail: string;
  /** Lines that might be the output path, in order seen; resolved after exit. */
  pathCandidates: string[];
  timedOut: boolean;
}

/** Stdout we keep in memory between newlines; a chatty tool cannot grow it unbounded. */
const MAX_LINE_BUFFER = 64 * 1024;
/** How many non-progress lines to remember as output-path candidates. */
const MAX_PATH_CANDIDATES = 32;

/**
 * Spawn yt-dlp, stream its progress into the job row, and ALWAYS reap it.
 *
 * Everything between spawn and exit sits inside one try/finally: the watchdog
 * timer is cleared, the child is removed from `activeProcs`, and a child that
 * is somehow still alive is killed — on success, on a classified failure, and
 * on an unexpected throw (SQLITE_BUSY from the progress UPDATE, a stat on a
 * vanished directory). Before this, a throw in the loop exited past the
 * cleanup, the worker's own finally removed the id, and the orphaned yt-dlp
 * kept writing to a .part another worker could claim — invisible to
 * killActiveChildren() and alive after shutdown.
 *
 * The loop does no filesystem work per line: path-looking lines are collected
 * and resolved once by the caller after exit (a DASH download prints hundreds
 * of lines, and a synchronous stat per line stalled the event loop that also
 * serves /api/status).
 */
export async function runSpawnedDownload(
  id: number,
  job: Job,
  args: string[],
  timeoutMs: number,
  config: Config,
): Promise<SpawnedDownloadResult> {
  let timedOut = false;
  const downloadCtl = new AbortController();
  const downloadTimer = setTimeout(() => {
    timedOut = true;
    downloadCtl.abort();
  }, timeoutMs);
  // `env: process.env` on purpose: Bun's default is a snapshot taken at
  // startup, so the live environment (what tests and operators mutate) would
  // never reach yt-dlp.
  const proc = Bun.spawn(args, { stdout: "pipe", stderr: "pipe", signal: downloadCtl.signal, env: process.env });
  activeProcs.set(id, proc);
  const decoder = new TextDecoder();
  const pathCandidates: string[] = [];
  let buffer = "";
  try {
    // Drain stderr immediately so a chatty yt-dlp cannot deadlock on a full pipe buffer.
    const stderrPromise = new Response(proc.stderr).text().catch(() => "");
    let lastProgressUpdate = 0;
    const reader = proc.stdout.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() || "";
      // A pathological line with no newline must not grow memory forever.
      if (buffer.length > MAX_LINE_BUFFER) buffer = buffer.slice(-MAX_LINE_BUFFER);

      for (const line of lines) {
        if (line.startsWith("PROGRESS:")) {
          // percent|speed|eta|total_bytes|downloaded_bytes — any field may be
          // "NA" or missing on the first lines; every parse below tolerates that.
          const [pctRaw = "", speedRaw = "", etaRaw = "", sizeRaw = "", dlRaw = ""] = line
            .slice("PROGRESS:".length)
            .split("|");
          const bps = parseSpeedToBytesPerSec(speedRaw);
          if (bps > 0) autoscaler.recordSpeed(id, bps);
          const sizeNum = parseInt(sizeRaw, 10);
          const dlNum = parseInt(dlRaw, 10);
          let pctNum = parseFloat(pctRaw);
          if (Number.isNaN(pctNum) && dlNum > 0 && sizeNum > 0) pctNum = (dlNum / sizeNum) * 100;
          if (!Number.isNaN(pctNum) && pctNum >= 0 && Date.now() - lastProgressUpdate > 500) {
            // Backfill file_size from progress so the global ETA has a total to work with.
            const totalBytes = Number.isFinite(sizeNum) && sizeNum > 0 ? sizeNum : null;
            // best_progress is the high-water mark of this job's attempts: it is
            // what lets the retry budget forgive repeated failures at increasing
            // completion percentages (see handleDownloadFailure).
            updateJobProgress(job.id, pctNum, bps, parseFloat(etaRaw) || 0, totalBytes);
            const speedTxt = bps > 0 ? formatBytesPerSec(bps) : "Calculating...";
            const etaNum = parseFloat(etaRaw);
            const etaTxt = Number.isFinite(etaNum) && etaNum > 0 ? `, ETA ${Math.round(etaNum)}s` : "";
            updateWorkerLine(id, `⬇️ ${pctNum.toFixed(1)}% @ ${speedTxt}${etaTxt} | ${job.title}`, config);
            lastProgressUpdate = Date.now();
          }
        } else {
          const trimmed = line.trim();
          if (!trimmed) continue;
          // Capture paths embedded in yt-dlp status lines (merger output,
          // etc.) and the bare `--print after_move:filepath` line.
          const m = trimmed.match(/Merged formats into "(.+)"$/) || trimmed.match(/Destination: (.+)$/);
          pathCandidates.push(m?.[1] ?? trimmed);
          if (pathCandidates.length > MAX_PATH_CANDIDATES) pathCandidates.shift();
        }
      }
    }
    const [stderrText, code] = await Promise.all([stderrPromise, proc.exited]);
    const trailing = buffer.trim();
    if (trailing) pathCandidates.push(trailing);
    return { code, stderrText, tail: buffer, pathCandidates, timedOut };
  } finally {
    clearTimeout(downloadTimer);
    activeProcs.delete(id);
    if (proc.exitCode === null && !proc.killed) {
      try {
        proc.kill("SIGKILL");
      } catch {}
    }
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
  // Thin applier: gather context → decideFailureOutcome() (pure, table-tested
  // in tests/retry.test.ts) → perform the I/O the outcome asks for. No
  // policy lives here; add new failure classes in retry.ts.
  const errMsg = String(err?.message || err);
  const base = baseNameOf(job);
  const { retryCount, bestProgress, progress } = readProgressState(job.id);
  const partial =
    job.partial_file_path && existsSync(job.partial_file_path)
      ? job.partial_file_path
      : await findPartialFile(job.output_directory, base);

  const outcome = decideFailureOutcome({
    error: errMsg,
    isPaused: isPaused(),
    userPaused: userPauseRequested(job.id),
    retryCount,
    retryCap: perVideoCap(config),
    resumeCount: job.resume_count || 0,
    maxResume: config.maxResumeAttempts,
    progress,
    bestProgress,
    hasPartial: !!partial,
  });

  switch (outcome.kind) {
    case "park": {
      parkPaused(job, outcome.pauseReason);
      updateWorkerLine(id, `⏸️ ${outcome.pauseReason === "user" ? "Paused by user" : "Paused"} | ${job.title}`, config);
      return;
    }

    case "self-update": {
      // Signature challenge broke (yt-dlp extractor changed) — self-heal by
      // updating yt-dlp, then retry immediately with a clean budget.
      console.warn("⚠️ Signature challenge failed. Auto-updating yt-dlp...");
      const result = await selfUpdateYtDlp(id);
      resetForRetry(job.id);
      updateWorkerLine(id, `🔄 yt-dlp self-update ${result}, retrying... | ${job.title}`, config);
      return;
    }

    case "bad-args": {
      // aria2c rejected the command line: a global misconfiguration, not a
      // video problem. Park this job and pause the engine with an actionable
      // reason instead of burning every retry budget in the playlist.
      logError(
        "download",
        `${job.id} ${job.title}: aria2c rejected the downloader arguments (exit 28). ` +
          `Check connectionsPerDownload/minSplitSize. ${errMsg.slice(0, 300)}`,
      );
      parkPaused(job);
      triggerPause(`BAD_DOWNLOADER_ARGS (${errMsg.slice(0, 120)})`);
      updateWorkerLine(id, `⚙️ aria2c rejected downloader args — paused | ${job.title}`, config);
      return;
    }

    case "resume": {
      // Keep the partial, count the resume attempt, and try again shortly.
      db.run(
        `UPDATE jobs SET download_status = 'pending', resume_count = ?, download_claimed_by = NULL, last_error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
        [outcome.resumeCount, errMsg.slice(0, 500), job.id],
      );
      updateWorkerLine(id, `⏳ Resuming (attempt ${outcome.resumeCount}/${config.maxResumeAttempts}) | ${job.title}`, config);
      await Bun.sleep(computeBackoffMs(outcome.backoffAttempt, config.retryBackoffBaseSeconds, config.retryBackoffMaxSeconds));
      return;
    }

    case "restart-fresh": {
      // Budget spent (or nothing to resume): throw the partial away and
      // restart this video from scratch. The aria2c control file goes first —
      // stranding it makes aria2c refuse to restart (see removePartialFiles).
      if (partial) {
        const removal = await removePartialFiles(partial);
        if (removal.fatal) {
          // The control file is locked (orphaned aria2c, an antivirus scan).
          // Deleting only the data file now would wedge this video forever,
          // so keep both files, surface exactly what is blocking, and let a
          // later attempt retry once the handle is released.
          const msg = `partial file locked, cannot restart cleanly (${removal.error}). Close the program holding it — usually an orphaned aria2c/ffmpeg or antivirus scanning the download folder.`;
          db.run(
            `UPDATE jobs SET download_status = 'pending', download_claimed_by = NULL, last_error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
            [msg.slice(0, 500), job.id],
          );
          logError("download", `${job.id} ${job.title}: ${msg}`);
          updateWorkerLine(id, `🔒 Partial locked — will retry | ${job.title}`, config);
          await Bun.sleep(computeBackoffMs(2, config.retryBackoffBaseSeconds, config.retryBackoffMaxSeconds));
          return;
        }
      }
      resetForRetry(job.id, { incrementRetry: true, clearPartial: true });
      updateWorkerLine(id, `🗑️ Restarting from scratch | ${job.title}`, config);
      return;
    }

    case "archive-scrub": {
      // --download-archive recorded the id but our copy is gone. Scrub the id
      // so yt-dlp will actually download it on the retry.
      removeFromArchive(config.archiveFile, job.id);
      resetForRetry(job.id, { incrementRetry: true, clearPartial: true });
      updateWorkerLine(id, `Re-downloading (archive entry scrubbed) | ${job.title}`, config);
      return;
    }

    case "wait-live": {
      // Park until the next full rescan or a manual retry — scans flip
      // waiting_live jobs back to pending once a VOD exists.
      db.run(
        `UPDATE jobs SET download_status = 'waiting_live', download_claimed_by = NULL, last_error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
        [errMsg.slice(0, 500), job.id],
      );
      updateWorkerLine(id, `Live now — waiting for VOD | ${job.title}`, config);
      return;
    }

    case "transient": {
      // Remember where the .part is so the next attempt resumes from it.
      db.run(
        `UPDATE jobs SET download_status = 'pending', retry_count = ?, best_progress = ?, partial_file_path = ?,
           download_claimed_by = NULL, last_error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
        [outcome.retryCount, outcome.bestProgress, partial || null, errMsg.slice(0, 500), job.id],
      );
      const backoff = computeBackoffMs(outcome.backoffAttempt, config.retryBackoffBaseSeconds, config.retryBackoffMaxSeconds);
      updateWorkerLine(id, `🌐 Transient error, retrying in ${Math.round(backoff / 1000)}s | ${job.title}`, config);
      await Bun.sleep(backoff);
      return;
    }

    case "permanent": {
      // Spend the retry budget, then park as failed for the periodic sweep
      // (which skips permanent errors entirely).
      db.run(
        `UPDATE jobs SET download_status = ?, retry_count = ?, partial_file_path = ?, download_claimed_by = NULL, last_error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
        [outcome.status, outcome.retryCount, outcome.status === "failed" ? null : partial || null, errMsg.slice(0, 500), job.id],
      );
      if (outcome.tripBreaker) {
        stats.failed++;
        logError("download", `${job.id} ${job.title}: ${errMsg.slice(0, 500)}`);
        notePipelineFailure("dl", config);
        queueFailureNotification({ id: job.id, title: job.title, stage: "download", error: errMsg });
      }
      updateWorkerLine(id, `❌ Failed | ${job.title}`, config);
      return;
    }
  }
}

// --- yt-dlp self-update -------------------------------------------------------

/** Hard ceiling on `yt-dlp -U`: a hung updater must never pin a worker. */
const YTDLP_UPDATE_TIMEOUT_MS = 120_000;
let ytDlpUpdateInFlight: Promise<string> | null = null;

/**
 * Run `yt-dlp -U` once, bounded and observable.
 *
 * Previously spawned with both pipes unread and no abort: past the pipe buffer
 * the updater blocked forever, holding the job claim with nothing able to kill
 * it. Now both pipes are drained, a 120 s timeout aborts it, it is registered
 * in `activeProcs` under a reserved negative key so killActiveChildren() reaches
 * it on shutdown, and concurrent workers hitting the same extractor break share
 * ONE update instead of racing several.
 */
async function selfUpdateYtDlp(workerId: number): Promise<string> {
  if (!ytDlpUpdateInFlight) {
    ytDlpUpdateInFlight = (async () => {
      const key = -Math.abs(workerId || 1); // never collides with a worker slot
      const ctl = new AbortController();
      const timer = setTimeout(() => ctl.abort(), YTDLP_UPDATE_TIMEOUT_MS);
      let proc: Bun.Subprocess<"ignore", "pipe", "pipe"> | null = null;
      try {
        proc = Bun.spawn([ytDlp(), "-U"], { stdout: "pipe", stderr: "pipe", signal: ctl.signal });
        activeProcs.set(key, proc);
        const [out, err, code] = await Promise.all([
          new Response(proc.stdout).text().catch(() => ""),
          new Response(proc.stderr).text().catch(() => ""),
          proc.exited,
        ]);
        if (ctl.signal.aborted) {
          logError("download", `yt-dlp -U timed out after ${YTDLP_UPDATE_TIMEOUT_MS / 1000}s`);
          return "timed out";
        }
        const summary = `${out}\n${err}`.split("\n").map((l) => l.trim()).filter(Boolean).slice(-2).join(" ");
        if (code !== 0) {
          logError("download", `yt-dlp -U exited ${code}: ${summary.slice(0, 300)}`);
          return `failed (exit ${code})`;
        }
        return "done";
      } catch (e: any) {
        logError("download", `yt-dlp -U could not run: ${e?.message || e}`);
        return "unavailable";
      } finally {
        clearTimeout(timer);
        activeProcs.delete(key);
        if (proc && proc.exitCode === null && !proc.killed) {
          try {
            proc.kill("SIGKILL");
          } catch {}
        }
        ytDlpUpdateInFlight = null;
      }
    })();
  }
  return ytDlpUpdateInFlight;
}

// --- small DB helpers --------------------------------------------------------

/**
 * Discover the video's audio tracks once per job (persisted in
 * `jobs.audio_tracks`) so multi-audio selection and the dashboard's track
 * picker know what YouTube offers. Only runs when something will consume the
 * result, and a probe failure never fails the download — we just fall back to
 * the classic single-track plan.
 */
async function resolveJobAudioTracks(job: Job, config: Config): Promise<AudioTrack[] | null> {
  if (config.videoQuality === "audio") return null;
  const known = parseTracksJson(job.audio_tracks);
  if (known) return known;
  const selection = parseSelectionJson(job.audio_selection) || [];
  if (config.multiAudioMode === "off" && selection.length === 0) return null;
  try {
    const tracks = await probeAudioTracks(job.url, config);
    db.run(`UPDATE jobs SET audio_tracks = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [
      JSON.stringify(tracks),
      job.id,
    ]);
    return tracks;
  } catch (err: any) {
    logError(
      "download",
      `${job.id} audio-track probe failed (falling back to single audio): ${String(err?.message || err).slice(0, 200)}`,
    );
    return null;
  }
}

/** The on-disk base name used for a job's files (no extension). */
function baseNameOf(job: Job): string {
  return jobBaseFilename(job);
}

/**
 * Record progress, keeping best_progress as the high-water mark.
 *
 * Also heartbeats the claim: `download_claimed_at` rides along in the same
 * (500 ms-throttled) UPDATE, so the stale-claim reaper measures "no progress
 * for N minutes" rather than "claimed N minutes ago" — a legitimate long
 * transfer is never stolen from under a live yt-dlp.
 */
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
       file_size = COALESCE(?, file_size),
       download_claimed_at = CURRENT_TIMESTAMP
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

/** True when the dashboard asked for THIS job to pause while it was in flight. */
function userPauseRequested(id: string): boolean {
  const row = db.query("SELECT pause_reason FROM jobs WHERE id = ?").get(id) as { pause_reason: string | null } | null;
  return row?.pause_reason === "user";
}

/**
 * Park an in-flight job as paused. With no reason the row is auto-resumable
 * (global pause / shutdown); `"user"` holds it until an explicit Resume.
 */
function parkPaused(job: Job, reason: "user" | null = null): void {
  db.run(
    `UPDATE jobs SET download_status = 'paused', pause_reason = ?, download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
    [reason, job.id],
  );
  // Freeze the resume point: the .part is on disk, and without recording it the
  // job is paused with no resumable partial, so the next attempt restarts the
  // video from zero instead of continuing.
  recordJobPartial(job);
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
