// src/web.ts — dashboard web server + JSON API.
//
// Security model: loopback-only by default (webBind), and every request — UI
// and API — is gated behind the optional shared-secret token (cookie,
// Authorization: Bearer, X-Web-Token header, or ?token= query param), with
// timing-safe comparison. The destructive routes (purge, delete) are behind
// the same gate.

import { existsSync, readFileSync } from "node:fs";
import { statfs } from "node:fs/promises";
import { timingSafeEqual } from "node:crypto";
import os from "node:os";
import { db } from "./db";
import { autoscaler, activeDlSlots } from "./autoscale";
import { getConfig, getPauseReason, isPaused, workerStatuses } from "./state";
import { scanAndIngest } from "./scanner";
import { triggerPause, triggerResume } from "./resilience";
import { requeueFailedJobs } from "./reconcile";
import { buildRunReport } from "./report";
import { isPermanentDownloadError } from "./retry";
import { aria2cPath } from "./tools";
import { resolveDownloaderEngine } from "./download-args";
import { formatBytesPerSec, formatDuration } from "./util";
import { logError } from "./logger";
import type { Config } from "./config";

// --- Web UI auth (optional shared-secret token) ------------------------------
// When webToken is set, every request must present it — as a cookie (set after
// the first successful sign-in), an Authorization: Bearer header, an
// X-Web-Token header, or a ?token= query parameter. Comparison is timing-safe.
export function extractWebToken(req: Request, url: URL): string | null {
  const auth = req.headers.get("authorization") || "";
  if (auth.toLowerCase().startsWith("bearer ")) return auth.slice(7).trim();
  const header = req.headers.get("x-web-token");
  if (header) return header.trim();
  const query = url.searchParams.get("token");
  if (query) return query.trim();
  const cookie = req.headers.get("cookie") || "";
  const match = cookie.match(/(?:^|;\s*)yta_token=([^;]+)/);
  if (match) return decodeURIComponent(match[1]).trim();
  return null;
}

export function timingSafeEq(a: string, b: string): boolean {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  if (ab.length !== bb.length) return false;
  return timingSafeEqual(ab, bb);
}

export function isAuthorized(req: Request, url: URL, config: Config): boolean {
  if (!config.webToken) return true;
  const presented = extractWebToken(req, url);
  return !!presented && timingSafeEq(presented, config.webToken);
}

// Minimal sign-in page: submits the token as ?token=..., the server validates
// it, sets an HttpOnly cookie, and serves the real UI — so the stock dashboard
// JS (plain fetch, no token logic) keeps working unchanged.
const LOGIN_PAGE = `<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Archive — Sign in</title>
<style>
  body { font-family: system-ui, sans-serif; background: #0f172a; color: #e2e8f0; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; }
  .card { background: #1e293b; padding: 2rem; border-radius: 12px; width: min(360px, 90vw); box-shadow: 0 10px 40px rgba(0,0,0,.4); }
  h1 { font-size: 1.1rem; margin: 0 0 1rem; }
  input { width: 100%; box-sizing: border-box; padding: .6rem .8rem; border-radius: 8px; border: 1px solid #334155; background: #0f172a; color: #e2e8f0; font-size: 1rem; }
  button { margin-top: .8rem; width: 100%; padding: .6rem; border: 0; border-radius: 8px; background: #3b82f6; color: white; font-size: 1rem; cursor: pointer; }
  .err { color: #f87171; font-size: .85rem; margin-top: .6rem; min-height: 1.2em; }
</style></head>
<body><div class="card">
  <h1>Archive Web UI — sign in</h1>
  <form onsubmit="return go()">
    <input id="token" type="password" placeholder="Access token" autofocus>
    <button type="submit">Sign in</button>
    <div class="err" id="err"></div>
  </form>
</div>
<script>
  function go() {
    const t = document.getElementById('token').value.trim();
    if (!t) return false;
    location.href = '/?token=' + encodeURIComponent(t);
    return false;
  }
  if (new URLSearchParams(location.search).has('token')) {
    document.getElementById('err').textContent = 'Invalid token — try again.';
  }
</script>
</body></html>`;

export function startWebServer(port: number, config: Config) {
  return Bun.serve({
    // LAN-safe default: listen on loopback unless webBind is explicitly set
    // (e.g. 0.0.0.0 to reach the UI from other devices on the LAN).
    port,
    hostname: config.webBind || "127.0.0.1",
    async fetch(req) {
      // Every request is wrapped: a handler crash returns JSON 500 instead of
      // hanging the socket, and the error lands in error.log.
      try {
        return await handleRequest(req, config);
      } catch (e: any) {
        logError("http", `${req.method} ${new URL(req.url).pathname}: ${e?.stack || e}`);
        return Response.json({ ok: false, error: "Internal server error" }, { status: 500 });
      }
    },
  });
}

export async function handleRequest(req: Request, config: Config): Promise<Response> {
  const url = new URL(req.url);

  if (url.pathname === "/") {
    const queryToken = url.searchParams.get("token");
    if (config.webToken && !isAuthorized(req, url, config)) {
      // Missing/wrong token → the sign-in page (401 so browsers don't treat it
      // as the real app). A *valid* query token falls through, gets served the
      // app, and receives an HttpOnly cookie for subsequent requests.
      return new Response(LOGIN_PAGE, {
        status: 401,
        headers: { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" },
      });
    }
    if (existsSync("./web_ui.html")) {
      const headers: Record<string, string> = { "Content-Type": "text/html" };
      if (config.webToken && queryToken) {
        headers["Set-Cookie"] = `yta_token=${encodeURIComponent(queryToken)}; Path=/; HttpOnly; SameSite=Strict; Max-Age=2592000`;
      }
      return new Response(Bun.file("./web_ui.html"), { headers });
    }
    return new Response("web_ui.html not found. Please create it.", { status: 500 });
  }

  // Every API route (status, scan, pause/resume, purge, delete, ...) is gated
  // behind the token too — purge/delete are destructive.
  if (!isAuthorized(req, url, config)) {
    return Response.json({ ok: false, error: "Unauthorized — token required" }, { status: 401 });
  }

  if (url.pathname === "/api/ping") {
    return new Response(null, { status: 200 });
  }

  if (url.pathname === "/api/status") {
    const statsData = db
      .query(
        `SELECT
          SUM(CASE WHEN download_status IN ('pending', 'paused', 'downloading') THEN 1 ELSE 0 END) as queued,
          SUM(CASE WHEN download_status = 'downloading' THEN 1 ELSE 0 END) as downloading,
          SUM(CASE WHEN download_status = 'downloaded' THEN 1 ELSE 0 END) as downloaded,
          SUM(CASE WHEN download_status = 'failed' OR conversion_status = 'failed' OR metadata_status = 'failed' THEN 1 ELSE 0 END) as failed,
          SUM(CASE WHEN metadata_status IN ('pending', 'in_progress') THEN 1 ELSE 0 END) as metadata_pending,
          SUM(CASE WHEN conversion_status IN ('pending', 'in_progress') THEN 1 ELSE 0 END) as converting,
          SUM(CASE WHEN download_status = 'waiting_live' THEN 1 ELSE 0 END) as waiting_live,
          COUNT(*) as total
        FROM jobs`,
      )
      .get() as any;
    const workers: { id: string; type: string; status: string }[] = [];
    for (let i = 1; i <= config.maxDownloadWorkers; i++) {
      workers.push({ id: `DL${i}`, type: "download", status: workerStatuses.get(`DL${i}`) || "Idle" });
    }
    for (let i = 1; i <= config.maxConcurrentConverts; i++) {
      workers.push({ id: `CV${i}`, type: "convert", status: workerStatuses.get(`CV${i}`) || "Idle" });
    }
    for (let i = 1; i <= config.maxMetadataWorkers; i++) {
      workers.push({ id: `MD${i}`, type: "metadata", status: workerStatuses.get(`MD${i}`) || "Idle" });
    }

    const diskStats = await statfs(config.outputRoot).catch(() => ({ bavail: 0, blocks: 1, bsize: 1 }));
    const freeGB = ((diskStats.bavail * diskStats.bsize) / 1024 ** 3).toFixed(1);
    const totalGB = ((diskStats.blocks * diskStats.bsize) / 1024 ** 3).toFixed(1);
    const diskPercent = ((diskStats.bavail / diskStats.blocks) * 100).toFixed(0);

    const memUsage = process.memoryUsage();
    const ramUsedGB = (memUsage.rss / 1024 ** 3).toFixed(2);
    const ramTotalGB = (os.totalmem() / 1024 ** 3).toFixed(2);
    const ramPercent = ((memUsage.rss / os.totalmem()) * 100).toFixed(0);
    const uptime = formatDuration(process.uptime());

    const avgSpeed = autoscaler.getAggregateSpeed();
    const remaining = db
      .query(
        `SELECT SUM(file_size * (1 - COALESCE(progress, 0) / 100)) as remaining FROM jobs WHERE download_status = 'downloading'`,
      )
      .get() as any;
    const secondsRemaining = avgSpeed > 0 && remaining.remaining ? remaining.remaining / avgSpeed : 0;
    const globalETA = secondsRemaining > 0 ? formatDuration(secondsRemaining) : "--";

    return Response.json({
      stats: {
        totalQueued: statsData.queued || 0,
        downloading: statsData.downloading || 0,
        downloaded: statsData.downloaded || 0,
        failed: statsData.failed || 0,
        metadataPending: statsData.metadata_pending || 0,
        converting: statsData.converting || 0,
        waitingLive: statsData.waiting_live || 0,
        total: statsData.total || 0,
      },
      queuePosition: statsData.queued || 0,
      speed: avgSpeed,
      aggregateSpeed: formatBytesPerSec(avgSpeed),
      activeWorkers: activeDlSlots.size,
      targetWorkers: autoscaler.targetWorkers,
      workers,
      isPaused: isPaused(),
      pauseReason: getPauseReason(),
      diskSpace: { free: `${freeGB} GB / ${totalGB} GB`, percent: parseFloat(diskPercent) },
      system: {
        cpu: "--",
        cpuPercent: 0,
        ram: `${ramUsedGB} GB / ${ramTotalGB} GB`,
        ramPercent: parseFloat(ramPercent),
      },
      uptime,
      globalETA,
    });
  }

  if (url.pathname === "/api/jobs") {
    const rows = db
      .query(
        `SELECT id, url, title, folder, output_directory, file_path, target_format,
                download_status, conversion_status, metadata_status, pause_reason, metadata_files,
                retry_count, conversion_retry_count, resume_count, best_progress, last_error,
                file_size, progress, speed, eta, duration, partial_file_path
         FROM jobs ORDER BY created_at DESC LIMIT 500`,
      )
      .all();
    return Response.json({ jobs: rows });
  }

  if (url.pathname === "/api/scan" && req.method === "POST") {
    const body = await req.json().catch(() => ({}));
    const { url: scanUrl, folder } = body || {};
    if (!scanUrl) return Response.json({ ok: false, error: "URL required" }, { status: 400 });
    try {
      const result = await scanAndIngest(scanUrl, getConfig(), folder);
      const message =
        result.found === 0
          ? `No videos found at ${scanUrl} (check the URL, network, or cookies)`
          : `Scanned ${result.found} video(s): ${result.added} added, ${result.skipped} skipped`;
      return Response.json({ ok: true, message, ...result });
    } catch (e: any) {
      logError("scan", `${scanUrl}: ${e?.message || e}`);
      return Response.json({ ok: false, error: e.message || "Scan failed" }, { status: 500 });
    }
  }

  if (url.pathname === "/api/queue/purge" && req.method === "POST") {
    const result = db.run("DELETE FROM jobs WHERE download_status IN ('pending', 'paused', 'waiting_live', 'failed')");
    return Response.json({ ok: true, deleted: result.changes });
  }

  if (url.pathname === "/api/pause" && req.method === "POST") {
    triggerPause("MANUAL_WEB_UI");
    return Response.json({ success: true });
  }
  if (url.pathname === "/api/resume" && req.method === "POST") {
    triggerResume();
    return Response.json({ success: true });
  }

  if (url.pathname.startsWith("/api/retry/") && req.method === "POST") {
    const id = decodeURIComponent(url.pathname.replace("/api/retry/", ""));
    // Re-queue download AND any failed metadata/conversion work; preserve
    // conversion_status='not_needed'. Also clears a user pause and resets the
    // per-stage retry budgets so a manual retry always gets a fresh budget.
    db.run(
      `UPDATE jobs SET
         download_status = 'pending', pause_reason = NULL, progress = 0, retry_count = 0,
         conversion_status = CASE WHEN conversion_status = 'not_needed' THEN 'not_needed' ELSE 'pending' END,
         conversion_retry_count = 0,
         metadata_status = CASE
           WHEN COALESCE(want_subtitles,0) + COALESCE(want_thumbnail,0) + COALESCE(want_description,0) > 0 THEN 'pending'
           ELSE metadata_status END,
         metadata_retry_count = 0,
         last_error = NULL, download_claimed_by = NULL, conversion_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [id],
    );
    return Response.json({ ok: true });
  }

  if (url.pathname.startsWith("/api/failcount/reset/") && req.method === "POST") {
    const id = decodeURIComponent(url.pathname.replace("/api/failcount/reset/", ""));
    if (id) {
      db.run(
        `UPDATE jobs SET retry_count = 0, metadata_retry_count = 0, conversion_retry_count = 0, last_error = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
        [id],
      );
    }
    return Response.json({ ok: true });
  }

  if (url.pathname === "/api/jobs/pause" && req.method === "POST") {
    const body = await req.json().catch(() => ({}));
    const ids = Array.isArray(body?.ids)
      ? body.ids.filter((x: any) => typeof x === "string" && x.length > 0).slice(0, 500)
      : [];
    if (ids.length === 0) return Response.json({ ok: false, error: "No job ids provided" }, { status: 400 });
    const placeholders = ids.map(() => "?").join(",");
    const result = db.run(
      `UPDATE jobs SET download_status = 'paused', pause_reason = 'user',
         download_claimed_by = CASE WHEN download_status = 'downloading' THEN download_claimed_by ELSE NULL END,
         updated_at = CURRENT_TIMESTAMP
       WHERE id IN (${placeholders}) AND download_status IN ('pending', 'downloading', 'paused')`,
      ids,
    );
    return Response.json({ ok: true, paused: result.changes });
  }

  if (url.pathname === "/api/jobs/delete" && req.method === "POST") {
    const body = await req.json().catch(() => ({}));
    const ids = Array.isArray(body?.ids)
      ? body.ids.filter((x: any) => typeof x === "string" && x.length > 0).slice(0, 500)
      : [];
    if (ids.length === 0) return Response.json({ ok: false, error: "No job ids provided" }, { status: 400 });
    const placeholders = ids.map(() => "?").join(",");
    const result = db.run(`DELETE FROM jobs WHERE id IN (${placeholders})`, ids);
    return Response.json({ ok: true, deleted: result.changes });
  }

  if (url.pathname.startsWith("/api/jobs/") && req.method === "DELETE") {
    const id = decodeURIComponent(url.pathname.replace("/api/jobs/", ""));
    db.run(`DELETE FROM jobs WHERE id = ?`, [id]);
    return Response.json({ ok: true });
  }

  if (url.pathname === "/api/failed" && req.method === "GET") {
    const rows = db
      .query(
        `SELECT id, title, folder, output_directory, retry_count, conversion_retry_count, metadata_retry_count,
                download_status, conversion_status, metadata_status, last_error
         FROM jobs
         WHERE download_status = 'failed' OR conversion_status = 'failed' OR metadata_status = 'failed'
         LIMIT 100`,
      )
      .all();
    return Response.json({ ok: true, failed: rows });
  }

  // Re-queue every failed job that is eligible (transient errors, retry budget
  // remaining) immediately, ignoring the cooldown.
  if (url.pathname === "/api/failed/requeue" && req.method === "POST") {
    const result = requeueFailedJobs(config, { ignoreCooldown: true });
    return Response.json({ ok: true, requeued: result });
  }

  // Reliability snapshot for the dashboard: what is paused, how many partial
  // files are being kept for resume, and which knobs are active.
  if (url.pathname === "/api/reliability" && req.method === "GET") {
    const partials = db
      .query(
        `SELECT COUNT(*) as count, COALESCE(SUM(file_size), 0) as bytes FROM jobs WHERE partial_file_path IS NOT NULL`,
      )
      .get() as any;
    // Same eligibility rules as the sweep itself: retry budget remaining and a
    // non-permanent last error. Uses the shared classifier so the dashboard and
    // the sweep can never disagree about what is retryable.
    const cap = Math.min(config.maxRetryAttempts, config.maxFailuresPerVideo);
    const failedDownloads = db
      .query(
        `SELECT retry_count, last_error FROM jobs WHERE download_status = 'failed'`,
      )
      .all() as any[];
    const resumableFailed = failedDownloads.filter(
      (r) => (r.retry_count || 0) < cap && !isPermanentDownloadError(r.last_error),
    ).length;
    const waitingLive = db
      .query(`SELECT COUNT(*) as count FROM jobs WHERE download_status = 'waiting_live'`)
      .get() as any;
    return Response.json({
      ok: true,
      paused: isPaused(),
      pauseReason: getPauseReason(),
      partialFiles: { count: partials?.count || 0, bytes: partials?.bytes || 0 },
      resumableFailed,
      waitingLive: waitingLive?.count || 0,
      policy: {
        maxResumeAttempts: config.maxResumeAttempts,
        retryBackoffBaseSeconds: config.retryBackoffBaseSeconds,
        retryBackoffMaxSeconds: config.retryBackoffMaxSeconds,
        requeueFailedAfterMinutes: config.requeueFailedAfterMinutes,
        verifyExistingFiles: config.verifyExistingFiles,
        downloadTimeoutMinutes: config.downloadTimeoutMinutes,
        maxDownloadMinutes: config.maxDownloadMinutes,
      },
      downloader: {
        engine: resolveDownloaderEngine(config, !!aria2cPath()),
        path: aria2cPath(),
        connectionsPerDownload: config.connectionsPerDownload,
        concurrentFragments: config.concurrentFragments,
        maxBandwidthKBps: config.maxBandwidthKBps,
        autoscaleRampStep: config.autoscaleRampStep,
      },
    });
  }

  if (url.pathname === "/api/history" && req.method === "GET") {
    const limit = parseInt(url.searchParams.get("limit") || "20", 10);
    const rows = db.query("SELECT * FROM run_history ORDER BY ended_at DESC LIMIT ?").all(limit);
    return Response.json({ ok: true, history: rows });
  }

  if (url.pathname === "/api/logs" && req.method === "GET") {
    const logType = url.searchParams.get("type") || "error";
    const limit = parseInt(url.searchParams.get("limit") || "100", 10);
    let logs: string[] = [];
    try {
      if (logType === "report") {
        logs = buildRunReport();
      } else if (existsSync("error.log")) {
        logs = readFileSync("error.log", "utf-8")
          .split("\n")
          .filter((l) => l.trim())
          .slice(-limit);
      } else {
        logs = ["No logs available"];
      }
    } catch (e: any) {
      logs = [`Error: ${e.message}`];
    }
    return Response.json({ ok: true, logs, type: logType });
  }

  return new Response("Not Found", { status: 404 });
}
