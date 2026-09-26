// src/workers/convert.ts — the conversion worker.
//
// Runs after a download (and after metadata work is terminal). Transcodes
// audio-only archives to mp3 and remuxes everything else into the target mp4
// container, then optionally moves the file plus its sidecars to a secondary
// storage path and records a SHA-256 integrity hash.

import { cp, mkdir, readdir, rename, unlink } from "node:fs/promises";
import { existsSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import { claimConvertJob, db, perVideoCap, type Job } from "../db";
import { computeBackoffMs } from "../retry";
import { SIDECAR_SUFFIXES, hashFile } from "../util";
import { updateAbsoluteLine } from "../dashboard";
import { abortController, isPaused, stats, workerStatuses } from "../state";
import { notePipelineFailure, notePipelineSuccess } from "../resilience";
import { logError } from "../logger";
import { ffmpeg } from "../tools";
import type { Config } from "../config";

/** Run ffmpeg with a hard timeout so a wedged encode can never pin a worker. */
export async function runFfmpeg(
  args: string[],
  timeoutMs: number,
): Promise<{ code: number; stderr: string; timedOut: boolean }> {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), timeoutMs);
  try {
    const proc = Bun.spawn([ffmpeg(), ...args], { stdout: "ignore", stderr: "pipe", signal: ctl.signal });
    const [stderr, code] = await Promise.all([new Response(proc.stderr).text(), proc.exited]);
    return { code, stderr, timedOut: ctl.signal.aborted };
  } finally {
    clearTimeout(timer);
  }
}

export async function converterWorker(id: number, config: Config): Promise<void> {
  const workerId = `cv-${id}`;
  while (!abortController.signal.aborted) {
    if (isPaused()) {
      await Bun.sleep(2000);
      continue;
    }
    const job = claimConvertJob(workerId);
    if (!job) {
      await Bun.sleep(2000);
      continue;
    }
    try {
      await convertJob(job, config, id);
    } catch (err: any) {
      await handleConvertFailure(job, config, err, id);
    }
  }
}

async function convertJob(job: Job, config: Config, id: number): Promise<void> {
  updateConvertWorkerLine(id, `🔄 Converting | ${job.title}`, config);
  const sourcePath = job.file_path!;
  if (!sourcePath || !existsSync(sourcePath)) throw new Error("Source file missing");
  let finalPath = sourcePath;
  const wantsMp3 = job.target_format === "mp3" || config.videoQuality === "audio";
  if (wantsMp3 && !sourcePath.endsWith(".mp3")) {
    // Audio archive: encode to the target .mp3 instead of leaving the
    // source container (webm/m4a) untouched.
    const mp3Path = sourcePath.replace(/\.[^.]+$/, ".mp3");
    const res = await runFfmpeg(
      ["-y", "-i", sourcePath, "-vn", "-map", "0:a:0", "-c:a", "libmp3lame", "-q:a", "2", mp3Path],
      60 * 60 * 1000,
    );
    if (res.code !== 0) {
      throw new Error(
        `FFmpeg mp3 encode ${res.timedOut ? "timed out" : "failed"}: ${res.stderr.split("\n").filter((l) => l.trim()).slice(-2).join(" ")}`,
      );
    }
    if (config.deleteSourceAfterConvert) await unlink(sourcePath).catch(() => {});
    finalPath = mp3Path;
  } else if (!wantsMp3 && !sourcePath.endsWith(".mp4")) {
    const mp4Path = sourcePath.replace(/\.[^.]+$/, ".mp4");
    const res = await runFfmpeg(
      ["-y", "-i", sourcePath, "-map", "0:v:0", "-map", "0:a?", "-c:v", "copy", "-c:a", "aac", mp4Path],
      30 * 60 * 1000,
    );
    if (res.code !== 0) {
      throw new Error(
        `FFmpeg remux ${res.timedOut ? "timed out" : "failed"}: ${res.stderr.split("\n").filter((l) => l.trim()).slice(-2).join(" ")}`,
      );
    }
    if (config.deleteSourceAfterConvert) await unlink(sourcePath).catch(() => {});
    finalPath = mp4Path;
  }
  if (config.secondaryStoragePath) {
    const destDir = join(config.secondaryStoragePath, job.folder);
    await mkdir(destDir, { recursive: true });
    const srcDir = dirname(finalPath);
    const srcBase = basename(finalPath).replace(/\.[^.]+$/, "");
    // Move matching sidecar files (subs/thumbs/description/info.json) with
    // the media so everything stays together in the final location.
    const entries = await readdir(srcDir).catch(() => [] as string[]);
    for (const f of entries) {
      if (!f.startsWith(srcBase + ".")) continue;
      if (!SIDECAR_SUFFIXES.some((sfx) => f.endsWith(sfx))) continue;
      const sideSrc = join(srcDir, f);
      const sideDest = join(destDir, f);
      await rename(sideSrc, sideDest).catch(async () => {
        await cp(sideSrc, sideDest, { force: true }).catch(() => {});
        await unlink(sideSrc).catch(() => {});
      });
    }
    const destPath = join(destDir, basename(finalPath));
    await rename(finalPath, destPath).catch(async () => {
      await cp(finalPath, destPath, { force: true });
      await unlink(finalPath);
    });
    finalPath = destPath;
  }
  let integrity: string | null = null;
  if (config.verifyIntegrity) {
    integrity = await hashFile(finalPath);
  }
  db.run(
    `UPDATE jobs SET conversion_status = 'done', file_path = ?, integrity = ?, conversion_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
    [finalPath, integrity, job.id],
  );
  stats.converted++;
  notePipelineSuccess("post");
  updateConvertWorkerLine(id, `✅ Done | ${job.title}`, config);
}

/**
 * Conversion failures are usually transient (a wedged ffmpeg, a full disk), so
 * they get their own retry budget with exponential backoff before the job is
 * parked as failed for the periodic sweep.
 */
async function handleConvertFailure(job: Job, config: Config, err: any, id: number): Promise<void> {
  const errMsg = String(err?.message || err).slice(0, 500);
  const attempts = (job.conversion_retry_count || 0) + 1;
  const cap = perVideoCap(config);
  const newStatus = attempts >= cap ? "failed" : "pending";
  db.run(
    `UPDATE jobs SET conversion_status = ?, conversion_retry_count = ?, last_error = ?, conversion_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
    [newStatus, attempts, errMsg, job.id],
  );
  if (newStatus === "failed") {
    stats.failed++;
    logError("conversion", `${job.id} ${job.title}: ${errMsg}`);
    notePipelineFailure("post", config);
    updateConvertWorkerLine(id, `❌ Failed | ${job.title}`, config);
  } else {
    const backoff = computeBackoffMs(attempts, config.retryBackoffBaseSeconds, config.retryBackoffMaxSeconds);
    updateConvertWorkerLine(id, `🔁 Retrying in ${Math.round(backoff / 1000)}s | ${job.title}`, config);
    await Bun.sleep(backoff);
  }
}

function updateConvertWorkerLine(id: number, text: string, config: Config): void {
  workerStatuses.set(`CV${id}`, text);
  updateAbsoluteLine(2 + config.maxDownloadWorkers + id, `[CV${id}] ${text}`);
}
