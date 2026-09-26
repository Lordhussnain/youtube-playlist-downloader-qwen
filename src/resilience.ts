// src/resilience.ts — pause/resume, circuit breaker, network monitor, disk guard.
//
// The engine pauses (rather than dies) whenever something systemic goes
// wrong: the network drops, the disk fills, cookies expire overnight. Every
// in-flight yt-dlp is interrupted, its job is parked as
// paused+interrupted, and the job resumes from its .part file on the next
// attempt — nothing already downloaded is thrown away.

import { statfs } from "node:fs/promises";
import { resolve } from "node:path";
import { db } from "./db";
import { logError } from "./logger";
import { activeMetadataProcs, activeProcs, abortController, isPaused, getPauseReason, setPaused } from "./state";
import type { Config } from "./config";

export function triggerPause(reason: string): void {
  if (isPaused() && getPauseReason() === reason) return;
  setPaused(true, reason);
  console.log(`⏸️ Triggering pause: ${reason}`);
  for (const [, proc] of activeProcs.entries()) {
    try {
      proc.kill("SIGINT");
    } catch {}
  }
}

export function triggerResume(): void {
  setPaused(false, null);
  try {
    // Re-queue ALL paused jobs (global + user-paused) on an explicit Resume All.
    // In-flight jobs still holding a claim finish naturally in their worker.
    const stmt = db.run(
      `UPDATE jobs SET download_status = 'pending', pause_reason = NULL, download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP
       WHERE download_status = 'paused' AND download_claimed_by IS NULL`,
    );
    if (stmt.changes > 0) console.log(`▶️ Re-queued ${stmt.changes} paused job(s).`);
  } catch {}
  // A human resumed the engine — give the failure circuit a clean slate.
  failureCircuit.dl = 0;
  failureCircuit.post = 0;
}

// --- Circuit breaker ---------------------------------------------------------
// maxFailures is a consecutive-failure tripwire per pipeline stage: a run of
// hard failures with no successes in between (expired cookies overnight, a
// YouTube outage, a broken ffmpeg) pauses the whole engine instead of letting
// it burn through the queue one video at a time.
const failureCircuit = { dl: 0, post: 0 };

export function notePipelineSuccess(stage: "dl" | "post"): void {
  if (stage === "dl") failureCircuit.dl = 0;
  else failureCircuit.post = 0;
}

export function notePipelineFailure(stage: "dl" | "post", config: Config): void {
  if (stage === "dl") failureCircuit.dl++;
  else failureCircuit.post++;
  const worst = Math.max(failureCircuit.dl, failureCircuit.post);
  if (worst >= config.maxFailures) {
    const detail = `TOO_MANY_FAILURES (${failureCircuit.dl} consecutive download / ${failureCircuit.post} consecutive post-processing failures, limit ${config.maxFailures})`;
    failureCircuit.dl = 0;
    failureCircuit.post = 0;
    logError("circuit", `pausing engine: ${detail}`);
    triggerPause(detail);
  }
}

// --- Network monitor ---------------------------------------------------------
// Tried in order: the first reachable host means "online". Multiple endpoints
// keep the monitor honest on networks that block one host but not another.
const NETWORK_PROBES = [
  "https://www.youtube.com/favicon.ico",
  "https://youtu.be/favicon.ico",
  "https://manifest.googlevideo.com/favicon.ico",
];

export async function checkInternet(): Promise<boolean> {
  for (const url of NETWORK_PROBES) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 5000);
      await fetch(url, { signal: controller.signal, method: "HEAD", redirect: "follow" });
      clearTimeout(timeout);
      return true;
    } catch {
      // try the next probe
    }
  }
  return false;
}

export async function networkMonitor(): Promise<void> {
  let consecutiveFails = 0;
  console.log("🌐 Network monitor started.");
  while (!abortController.signal.aborted) {
    const isUp = await checkInternet();
    if (!isUp) {
      consecutiveFails++;
      if (consecutiveFails >= 2 && !isPaused()) {
        triggerPause("NETWORK_DISCONNECTED");
        console.log("🌐 Network down detected. Pausing engine gracefully.");
      }
    } else {
      if (consecutiveFails > 0) {
        console.log("🌐 Network restored!");
        consecutiveFails = 0;
        if (getPauseReason() === "NETWORK_DISCONNECTED") triggerResume();
      }
    }
    await Bun.sleep(15000);
  }
}

// --- Disk space guard --------------------------------------------------------
let diskCheckWarned = false;

export async function checkDiskSpace(
  path: string,
  minGB: number,
): Promise<{ free: number; ok: boolean }> {
  try {
    const stats = await statfs(path);
    const freeGB = (stats.bavail * stats.bsize) / 1024 ** 3;
    return { free: freeGB, ok: freeGB > minGB };
  } catch {
    // Windows fallback: some Bun builds lack statfs — ask PowerShell instead.
    try {
      if (process.platform === "win32") {
        const root = resolve(path); // e.g. D:\Downloads\YT
        const drive = root.slice(0, 1); // "D"
        if (/^[A-Za-z]$/.test(drive)) {
          const proc = Bun.spawn(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", `(Get-PSDrive -Name '${drive}').Free`],
            { stdout: "pipe", stderr: "pipe" },
          );
          const [out, code] = await Promise.all([new Response(proc.stdout).text(), proc.exited]);
          const freeGB = parseFloat(out.trim()) / 1024 ** 3;
          if (code === 0 && Number.isFinite(freeGB)) return { free: freeGB, ok: freeGB > minGB };
        }
      }
    } catch {}
    // Degraded mode: never permanently brick the engine over a failed probe —
    // log once and allow (yt-dlp will still surface a real disk-full error).
    if (!diskCheckWarned) {
      diskCheckWarned = true;
      console.warn("⚠️ Could not determine free disk space — continuing without the low-disk guard.");
      logError("disk", `statfs/PowerShell probe failed for ${path}; low-disk guard disabled for this run`);
    }
    return { free: -1, ok: true };
  }
}

// --- Child-process bookkeeping ----------------------------------------------
// Kill in-flight children on shutdown; called by the lifecycle module.
export function killActiveChildren(): void {
  for (const [, proc] of activeProcs) {
    try {
      proc.kill("SIGINT");
    } catch {}
  }
  for (const [, proc] of activeMetadataProcs) {
    try {
      proc.kill("SIGINT");
    } catch {}
  }
}
