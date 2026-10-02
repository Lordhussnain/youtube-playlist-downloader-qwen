// src/web.ts — dashboard web server + JSON API.
//
// Security model: loopback-only by default (webBind), and every request — UI
// and API — is gated behind the optional shared-secret token (cookie,
// Authorization: Bearer, X-Web-Token header, or ?token= query param), with
// timing-safe comparison. The destructive routes (purge, delete) are behind
// the same gate.

import { existsSync, readFileSync } from "node:fs";
import { createHash, timingSafeEqual } from "node:crypto";
import os from "node:os";
import { db, type Job } from "./db";
import { autoscaler, activeDlSlots } from "./autoscale";
import { activeProcs, getConfig, getPauseReason, isPaused, workerStatuses } from "./state";
import { scanAndIngest } from "./scanner";
import { diskUsage, triggerPause, triggerResume } from "./resilience";
import { requeueFailedJobs, STALE_CLAIM_THRESHOLDS, sweepError } from "./reconcile";
import { buildRunReport } from "./report";
import { aria2cPath } from "./tools";
import { applySettings, readSettings } from "./settings";
import { resolveDownloaderEngine } from "./download-args";
import { parseSelectionJson, parseTracksJson, probeAudioTracks } from "./audio-tracks";
import { formatBytesPerSec, formatDuration } from "./util";
import { getStatsSnapshot, invalidateStats } from "./stats";
import { errorLogPath, logError } from "./logger";
import { QUALITY_FORMATS, type Config } from "./config";
import { retentionEnabled } from "./retention";

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
  if (match?.[1]) return safeDecode(match[1])?.trim() ?? null;
  return null;
}

/** decodeURIComponent that returns null on malformed input instead of throwing. */
export function safeDecode(s: string): string | null {
  try {
    return decodeURIComponent(s);
  } catch {
    return null;
  }
}

export function timingSafeEq(a: string, b: string): boolean {
  // Compare fixed-length digests so the comparison takes the same time
  // whatever the presented token's length — a bare length check returned
  // early and leaked the real token's exact length.
  const ab = createHash("sha256").update(a).digest();
  const bb = createHash("sha256").update(b).digest();
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
        // Read the live config, not the one captured at startup: settings
        // changed from the dashboard (POST /api/settings) must be reflected by
        // every endpoint immediately, and setConfig() replaces the object.
        return await handleRequest(req, getConfig());
      } catch (e: any) {
        logError("http", `${req.method} ${new URL(req.url).pathname}: ${e?.stack || e}`);
        return Response.json({ ok: false, error: "Internal server error" }, { status: 500 });
      }
    },
  });
}

// Applied to EVERY response (README "Auth posture"). The UI is a single
// inline-scripted page, so script/style must allow 'unsafe-inline'; everything
// else is locked to the origin, framing is refused, and nothing is cached —
// the page can carry a token in its URL and the API returns live state.
export const SECURITY_HEADERS: Readonly<Record<string, string>> = Object.freeze({
  "Content-Security-Policy":
    "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; " +
    "img-src 'self' data:; connect-src 'self'; font-src 'self'; object-src 'none'; " +
    "base-uri 'none'; form-action 'self'; frame-ancestors 'none'",
  "X-Content-Type-Options": "nosniff",
  "X-Frame-Options": "DENY",
  "Referrer-Policy": "no-referrer",
  "Cache-Control": "no-store",
});

export async function handleRequest(req: Request, config: Config): Promise<Response> {
  const res = await routeRequest(req, config);
  for (const [k, v] of Object.entries(SECURITY_HEADERS)) res.headers.set(k, v);
  return res;
}

async function routeRequest(req: Request, config: Config): Promise<Response> {
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

  // Collapse trailing slashes so /api/jobs/ and /api/jobs are the same route
  // (the root "/" is handled above).
  const pathname = url.pathname.length > 1 ? url.pathname.replace(/\/+$/, "") : url.pathname;
  return handleApi(req, config, url, pathname);
}

// --- API routing --------------------------------------------------------------
// A route table instead of an if-chain: patterns support `:param` segments,
// every response shares the `{ok, ...}` envelope, unknown API paths get a JSON
// 404, and a known path with the wrong method gets a JSON 405 (+ Allow).
//
// Legacy action paths stay alive as aliases of the canonical per-job routes so
// existing bookmarks, scripts, and older dashboards keep working:
//   POST /api/retry/:id             → POST /api/jobs/:id/retry
//   POST /api/failcount/reset/:id   → POST /api/jobs/:id/reset-failures
//   POST /api/jobs/delete {ids}     → DELETE /api/jobs {ids}

type RouteParams = Record<string, string>;
type RouteHandler = (ctx: {
  req: Request;
  config: Config;
  url: URL;
  params: RouteParams;
  /** The `:id` segment — every per-job route binds one; "" when absent. */
  id: string;
}) => Response | Promise<Response>;

interface Route {
  methods: string[];
  pattern: string;
  handler: RouteHandler;
}

/**
 * `?limit=` as a bounded positive integer. `-1` used to mean "no limit" to
 * SQLite and `abc` became a NaN bind; both now fall back to the default.
 */
export function clampLimit(raw: string | null, fallback: number, max: number = 1000): number {
  const n = Number.parseInt(raw ?? "", 10);
  if (!Number.isFinite(n) || n < 1) return fallback;
  return Math.min(max, n);
}

/** Match a `/api/…/:param` pattern against path segments; null = no match. */
function matchRoute(pattern: string, segments: string[]): RouteParams | null {
  const parts = pattern.split("/").filter(Boolean);
  if (parts.length !== segments.length) return null;
  const params: RouteParams = {};
  for (let i = 0; i < parts.length; i++) {
    const p = parts[i] ?? "";
    const seg = segments[i];
    if (p.startsWith(":")) {
      if (!seg) return null; // never bind an empty param
      const decoded = safeDecode(seg);
      if (decoded === null) return null; // `/api/jobs/%` is a 404, not a 500
      params[p.slice(1)] = decoded;
    } else if (p !== seg) {
      return null;
    }
  }
  return params;
}

// The jobs list and the single-job endpoint must return identical shapes.
const JOB_COLUMNS = `id, url, title, folder, output_directory, file_path, target_format,
                download_status, conversion_status, metadata_status, pause_reason, metadata_files,
                retry_count, conversion_retry_count, resume_count, best_progress, last_error,
                file_size, progress, speed, eta, duration, partial_file_path,
                audio_tracks, audio_selection, integrity, quality_override, want_subtitles, duplicate_of`;

/** Audio-track columns are JSON in SQLite; hand the dashboard real values. */
function mapJobRow(r: any) {
  return {
    ...r,
    audio_tracks: parseTracksJson(r.audio_tracks) ?? [],
    audio_selection: parseSelectionJson(r.audio_selection),
  };
}

/**
 * The scan URL is handed to yt-dlp, which will happily fetch file://, the
 * local metadata service, or anything on the LAN. Only public http(s) hosts
 * are accepted; returns an error message or null when the URL is fine.
 */
export function validateScanUrl(raw: unknown): string | null {
  if (typeof raw !== "string" || raw.length === 0) return "URL required";
  if (raw.length > 2048) return "URL too long";
  let u: URL;
  try {
    u = new URL(raw);
  } catch {
    return "Invalid URL";
  }
  if (u.protocol !== "http:" && u.protocol !== "https:") return "Only http(s) URLs can be scanned";
  if (u.username || u.password) return "Credentials in the URL are not allowed";
  const host = u.hostname.replace(/^\[|\]$/g, "").toLowerCase();
  if (!host || host === "localhost" || host.endsWith(".localhost") || host.endsWith(".local")) {
    return "Local addresses cannot be scanned";
  }
  if (isPrivateAddress(host)) return "Private network addresses cannot be scanned";
  return null;
}

/** Loopback, link-local, RFC 1918 / ULA, unspecified and metadata ranges. */
export function isPrivateAddress(host: string): boolean {
  const v4 = host.match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/);
  if (v4) {
    const [a, b] = [Number(v4[1]), Number(v4[2])];
    return (
      a === 0 ||
      a === 10 ||
      a === 127 ||
      (a === 169 && b === 254) ||
      (a === 172 && b >= 16 && b <= 31) ||
      (a === 192 && b === 168) ||
      (a === 100 && b >= 64 && b <= 127) ||
      a >= 224
    );
  }
  if (host.includes(":")) {
    const h = host.toLowerCase();
    if (h === "::" || h === "::1") return true;
    if (h.startsWith("fe80:") || h.startsWith("fc") || h.startsWith("fd")) return true;
    if (h.startsWith("::ffff:")) return isPrivateAddress(h.slice(7));
    return false;
  }
  return false;
}

// --- Live-claim guards ---------------------------------------------------------
// The three worker claims are mutually exclusive on the stage that owns the
// media file (db.ts, gotcha 24). The routes below used to bypass that: a retry
// flipped conversion_status 'in_progress' → 'pending' and nulled the claim, so
// the download guard passed and yt-dlp wrote over the very file ffmpeg was
// reading. Every mutating per-job route now refuses (409) while any stage is
// live, and the purge skips rows holding a claim.

/** SQL predicate: some worker currently owns this job's file. */
const JOB_IN_PROGRESS_SQL = `(download_status = 'downloading'
  OR COALESCE(conversion_status, '') = 'in_progress'
  OR COALESCE(metadata_status, '') = 'in_progress')`;

/** Of `ids`, the ones a worker is actively working on right now. */
export function jobsInProgress(ids: string[]): string[] {
  if (ids.length === 0) return [];
  const placeholders = ids.map(() => "?").join(",");
  const rows = db
    .query(`SELECT id FROM jobs WHERE id IN (${placeholders}) AND ${JOB_IN_PROGRESS_SQL}`)
    .all(...ids) as { id: string }[];
  return rows.map((r) => r.id);
}

function conflictInProgress(ids: string[]): Response {
  return Response.json(
    { ok: false, error: "job is in progress", inProgress: ids },
    { status: 409 },
  );
}

/**
 * Re-queue a job with fresh budgets (download + any failed side stages).
 * Returns 0 when the job does not exist OR is in progress — callers check
 * `jobsInProgress` first so the two cases answer 409 vs 404.
 */
function retryJobById(id: string): number {
  // Re-queue download AND any failed metadata/conversion work; preserve
  // conversion_status='not_needed'. Also clears a user pause and resets the
  // per-stage retry budgets so a manual retry always gets a fresh budget.
  return db.run(
    `UPDATE jobs SET
       download_status = 'pending', pause_reason = NULL, progress = 0, retry_count = 0,
       conversion_status = CASE WHEN conversion_status = 'not_needed' THEN 'not_needed' ELSE 'pending' END,
       conversion_retry_count = 0,
       metadata_status = CASE
         WHEN COALESCE(want_subtitles,0) + COALESCE(want_thumbnail,0) + COALESCE(want_description,0) > 0 THEN 'pending'
         ELSE metadata_status END,
       metadata_retry_count = 0,
       last_error = NULL, download_claimed_by = NULL, conversion_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP
     WHERE id = ? AND NOT ${JOB_IN_PROGRESS_SQL}`,
    [id],
  ).changes;
}

export const OVERRIDE_FORMATS = ["mp4", "mkv", "webm", "m4a", "mp3"] as const;

/**
 * Validate an override request against the job's current row and return the
 * columns to write. Pure: exported for unit tests. Fields left out of `body`
 * keep their current values; `quality: null` clears the override.
 */
export function applyJobOverride(
  row: Pick<Job, "target_format" | "quality_override" | "want_subtitles">,
  body: any,
):
  | { target_format: string; quality_override: string | null; want_subtitles: number; needsConversion: boolean }
  | { error: string } {
  let target_format = (row.target_format || "mp4").toLowerCase();
  let quality_override = row.quality_override;
  let want_subtitles = row.want_subtitles ? 1 : 0;

  if (body?.targetFormat !== undefined) {
    const f = String(body.targetFormat || "").toLowerCase();
    if (!(OVERRIDE_FORMATS as readonly string[]).includes(f)) {
      return { error: `targetFormat must be one of ${OVERRIDE_FORMATS.join(", ")}` };
    }
    target_format = f;
  }
  if (body?.quality !== undefined) {
    if (body.quality === null || body.quality === "") quality_override = null;
    else if (typeof body.quality === "string" && body.quality in QUALITY_FORMATS) quality_override = body.quality;
    else return { error: `quality must be one of ${Object.keys(QUALITY_FORMATS).join(", ")} or null` };
  }
  if (body?.wantSubtitles !== undefined) {
    if (typeof body.wantSubtitles !== "boolean") return { error: "wantSubtitles must be a boolean" };
    want_subtitles = body.wantSubtitles ? 1 : 0;
  }
  // mp4 straight from yt-dlp needs no remux; anything else (or audio-only,
  // which the converter turns into mp3) goes through the conversion stage.
  const needsConversion = target_format !== "mp4" || quality_override === "audio";
  return { target_format, quality_override, want_subtitles, needsConversion };
}

/** Shared by the canonical retry route and its legacy alias. */
function retryRoute(id: string): Response {
  if (jobsInProgress([id]).length > 0) return conflictInProgress([id]);
  const changed = retryJobById(id);
  if (changed === 0) return Response.json({ ok: false, error: "Job not found" }, { status: 404 });
  invalidateStats();
  return Response.json({ ok: true });
}

/** Clear every per-stage failure counter for one job. */
function resetFailCounters(id: string): number {
  if (!id) return 0;
  return db.run(
    `UPDATE jobs SET retry_count = 0, metadata_retry_count = 0, conversion_retry_count = 0, last_error = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
    [id],
  ).changes;
}

/** Delete a set of jobs by id (bulk action from the dashboard). */
async function deleteJobsBulk(req: Request): Promise<Response> {
  const body = await req.json().catch(() => ({}));
  const ids = Array.isArray(body?.ids)
    ? body.ids.filter((x: any) => typeof x === "string" && x.length > 0).slice(0, 500)
    : [];
  if (ids.length === 0) return Response.json({ ok: false, error: "No job ids provided" }, { status: 400 });
  const busy = jobsInProgress(ids);
  if (busy.length > 0) return conflictInProgress(busy);
  const placeholders = ids.map(() => "?").join(",");
  const result = db.run(
    `DELETE FROM jobs WHERE id IN (${placeholders}) AND NOT ${JOB_IN_PROGRESS_SQL}`,
    ids,
  );
  invalidateStats();
  return Response.json({ ok: true, deleted: result.changes });
}

/**
 * Per-job user pause. Three cases, none of which fight a worker for the file:
 *   • pending / paused / failed  → parked as paused+user immediately
 *   • downloading                → pause_reason = 'user' is recorded on the
 *     row (status and claim stay), the owning yt-dlp gets SIGINT, and the
 *     worker's handler parks the job itself (workers/download.ts) — so the
 *     .part is frozen and nothing already fetched is lost
 *   • conversion / metadata in progress → refused with the live ids so the
 *     caller can retry once the stage finishes
 */
export function pauseJobsByUser(ids: string[]): { ok: boolean; paused: number; interrupting: number; inProgress?: string[]; error?: string } {
  const placeholders = ids.map(() => "?").join(",");
  const postBusy = (
    db
      .query(
        `SELECT id FROM jobs WHERE id IN (${placeholders})
           AND (COALESCE(conversion_status, '') = 'in_progress' OR COALESCE(metadata_status, '') = 'in_progress')`,
      )
      .all(...ids) as { id: string }[]
  ).map((r) => r.id);
  const parked = db.run(
    `UPDATE jobs SET download_status = 'paused', pause_reason = 'user', download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP
     WHERE id IN (${placeholders}) AND download_status IN ('pending', 'paused', 'failed', 'waiting_live')
       AND COALESCE(conversion_status, '') != 'in_progress' AND COALESCE(metadata_status, '') != 'in_progress'`,
    ids,
  ).changes;
  const inflight = db
    .query(
      `UPDATE jobs SET pause_reason = 'user', updated_at = CURRENT_TIMESTAMP
       WHERE id IN (${placeholders}) AND download_status = 'downloading'
       RETURNING download_claimed_by`,
    )
    .all(...ids) as { download_claimed_by: string | null }[];
  for (const row of inflight) {
    const m = /^dl-(\d+)$/.exec(row.download_claimed_by || "");
    const proc = m ? activeProcs.get(Number(m[1])) : undefined;
    try {
      proc?.kill("SIGINT");
    } catch {}
  }
  return {
    ok: true,
    paused: parked,
    interrupting: inflight.length,
    ...(postBusy.length > 0 ? { inProgress: postBusy, error: "some jobs have a conversion or metadata pass in progress" } : {}),
  };
}

const ROUTES: Route[] = [
  {
    methods: ["GET", "HEAD"],
    pattern: "/api/ping",
    handler: () => new Response(null, { status: 200 }),
  },
  {
    methods: ["GET"],
    pattern: "/api/version",
    handler: (_ctx) =>
      Response.json({
        ok: true,
        name: "youtube-playlist-downloader",
        runtime: { bun: Bun.version, platform: process.platform, arch: process.arch },
        uptimeSeconds: Math.round(process.uptime()),
      }),
  },
  {
    methods: ["GET"],
    pattern: "/api/status",
    handler: async ({ config }) => {
      // One memoised aggregate shared with /api/reliability and the TUI.
      const snap = getStatsSnapshot(config);
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

      // Goes through the shared probe: on Windows Bun builds without statfs a
      // direct call throws synchronously and would 500 this whole endpoint.
      const disk = await diskUsage(config.outputRoot);
      const known = disk.freeBytes >= 0;
      const freeGB = known ? (disk.freeBytes / 1024 ** 3).toFixed(1) : "--";
      const totalGB = disk.totalBytes > 0 ? (disk.totalBytes / 1024 ** 3).toFixed(1) : "--";
      const diskPercent =
        known && disk.totalBytes > 0 ? ((disk.freeBytes / disk.totalBytes) * 100).toFixed(0) : "0";
      const diskLabel = known ? `${freeGB} GB / ${totalGB} GB` : "unknown";

      const memUsage = process.memoryUsage();
      const ramUsedGB = (memUsage.rss / 1024 ** 3).toFixed(2);
      const ramTotalGB = (os.totalmem() / 1024 ** 3).toFixed(2);
      const ramPercent = ((memUsage.rss / os.totalmem()) * 100).toFixed(0);
      const uptime = formatDuration(process.uptime());

      const avgSpeed = autoscaler.getAggregateSpeed();
      const secondsRemaining = avgSpeed > 0 && snap.remainingBytes > 0 ? snap.remainingBytes / avgSpeed : 0;
      const globalETA = secondsRemaining > 0 ? formatDuration(secondsRemaining) : "--";

      return Response.json({
        ok: true,
        stats: {
          totalQueued: snap.queued,
          downloading: snap.downloading,
          downloaded: snap.downloaded,
          failed: snap.failedAny,
          metadataPending: snap.metadataPending,
          converting: snap.converting,
          waitingLive: snap.waitingLive,
          total: snap.total,
        },
        queuePosition: snap.queued,
        speed: avgSpeed,
        aggregateSpeed: formatBytesPerSec(avgSpeed),
        activeWorkers: activeDlSlots.size,
        targetWorkers: autoscaler.targetWorkers,
        workers,
        isPaused: isPaused(),
        pauseReason: getPauseReason(),
        diskSpace: { free: diskLabel, percent: parseFloat(diskPercent) },
        system: {
          cpu: "--",
          cpuPercent: 0,
          ram: `${ramUsedGB} GB / ${ramTotalGB} GB`,
          ramPercent: parseFloat(ramPercent),
        },
        runtime: { bun: Bun.version, platform: process.platform, arch: process.arch },
        uptime,
        globalETA,
      });
    },
  },
  {
    methods: ["GET"],
    pattern: "/api/jobs",
    handler: () => {
      const rows = db.query(`SELECT ${JOB_COLUMNS} FROM jobs ORDER BY created_at DESC LIMIT 500`).all() as any[];
      return Response.json({ ok: true, jobs: rows.map(mapJobRow) });
    },
  },
  {
    methods: ["GET"],
    pattern: "/api/jobs/:id",
    handler: ({ id }) => {
      const row = db.query(`SELECT ${JOB_COLUMNS} FROM jobs WHERE id = ?`).get(id) as any;
      if (!row) return Response.json({ ok: false, error: "Job not found" }, { status: 404 });
      return Response.json({ ok: true, job: mapJobRow(row) });
    },
  },
  {
    methods: ["POST"],
    pattern: "/api/jobs/:id/retry",
    handler: ({ id }) => retryRoute(id),
  },
  {
    methods: ["POST"],
    pattern: "/api/jobs/:id/reset-failures",
    handler: ({ id }) => {
      const changed = resetFailCounters(id);
      if (changed === 0) return Response.json({ ok: false, error: "Job not found" }, { status: 404 });
      return Response.json({ ok: true });
    },
  },
  {
    methods: ["POST"],
    pattern: "/api/jobs/:id/override",
    handler: async ({ req, id }) => {
      // Per-job format / quality / subtitles override (plan 5.1). One UPDATE
      // plus an optional re-queue: a format-only change on a downloaded job
      // just re-runs the converter from the kept source; a quality change
      // needs `retry: true` so the video is fetched again with the new
      // selector. Refused while the job is mid-flight — the running worker
      // holds its own copy of the row.
      const row = db.query(
        "SELECT id, target_format, quality_override, want_subtitles, download_status FROM jobs WHERE id = ?",
      ).get(id) as Pick<Job, "id" | "target_format" | "quality_override" | "want_subtitles" | "download_status"> | null;
      if (!row) return Response.json({ ok: false, error: "Job not found" }, { status: 404 });
      if (jobsInProgress([id]).length > 0) return conflictInProgress([id]);
      const body = await req.json().catch(() => ({}));
      const next = applyJobOverride(row, body);
      if ("error" in next) return Response.json({ ok: false, error: next.error }, { status: 400 });
      db.run(
        `UPDATE jobs SET target_format = ?, quality_override = ?, want_subtitles = ?,
           conversion_status = CASE WHEN conversion_status = 'not_needed' AND ? = 1 THEN 'pending'
                                    ELSE conversion_status END,
           metadata_status = CASE WHEN ? = 1 AND metadata_status IN ('not_needed', 'completed') THEN 'pending'
                                  ELSE metadata_status END,
           updated_at = CURRENT_TIMESTAMP
         WHERE id = ?`,
        [
          next.target_format,
          next.quality_override,
          next.want_subtitles,
          next.needsConversion ? 1 : 0,
          next.want_subtitles && !row.want_subtitles ? 1 : 0,
          id,
        ],
      );
      if (body?.retry === true) retryJobById(id);
      invalidateStats();
      return Response.json({
        ok: true,
        target_format: next.target_format,
        quality_override: next.quality_override,
        want_subtitles: next.want_subtitles,
        retried: body?.retry === true,
      });
    },
  },
  {
    methods: ["POST"],
    pattern: "/api/jobs/:id/audio-tracks",
    handler: async ({ req, id }) => {
      // Per-job audio-track picker (YouTube multi-language audio): save which
      // languages the next download attempt should keep. `tracks: null` resets
      // the job to the global multi-audio mode. Takes effect on the next
      // attempt — use Retry job to fetch an already-downloaded video again.
      const row = db.query("SELECT id FROM jobs WHERE id = ?").get(id);
      if (!row) return Response.json({ ok: false, error: "Job not found" }, { status: 404 });
      const body = await req.json().catch(() => ({}));
      const raw = body?.tracks;
      if (raw !== null && raw !== undefined && !Array.isArray(raw)) {
        return Response.json(
          { ok: false, error: "tracks must be an array of language codes or null" },
          { status: 400 },
        );
      }
      const cleaned = Array.isArray(raw)
        ? raw
            .filter((t: any) => typeof t === "string" && t.trim())
            .map((t: string) => t.trim().slice(0, 32))
            .slice(0, 40)
        : null;
      db.run(`UPDATE jobs SET audio_selection = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [
        cleaned ? JSON.stringify(cleaned) : null,
        id,
      ]);
      return Response.json({ ok: true, audio_selection: cleaned });
    },
  },
  {
    methods: ["POST"],
    pattern: "/api/jobs/:id/audio-probe",
    handler: async ({ req, config, id }) => {
      // Discover (or refresh) the audio tracks YouTube offers for one job —
      // the dashboard's track picker needs the list before a download has run.
      const job = db.query("SELECT id, url FROM jobs WHERE id = ?").get(id) as
        | { id: string; url: string }
        | null;
      if (!job) return Response.json({ ok: false, error: "Job not found" }, { status: 404 });
      try {
        const tracks = await probeAudioTracks(job.url, config);
        db.run(`UPDATE jobs SET audio_tracks = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [
          JSON.stringify(tracks),
          id,
        ]);
        return Response.json({ ok: true, tracks });
      } catch (e: any) {
        return Response.json(
          { ok: false, error: String(e?.message || e).slice(0, 300) },
          { status: 502 },
        );
      }
    },
  },
  {
    methods: ["DELETE"],
    pattern: "/api/jobs/:id",
    handler: ({ id }) => {
      if (jobsInProgress([id]).length > 0) return conflictInProgress([id]);
      const result = db.run(`DELETE FROM jobs WHERE id = ? AND NOT ${JOB_IN_PROGRESS_SQL}`, [id]);
      if (result.changes === 0) return Response.json({ ok: false, error: "Job not found" }, { status: 404 });
      invalidateStats();
      return Response.json({ ok: true, deleted: result.changes });
    },
  },
  // Static action paths must be listed before the :id routes so GET on them
  // answers 405 (method not allowed) instead of being read as an id.
  {
    methods: ["POST"],
    pattern: "/api/jobs/pause",
    handler: async ({ req }) => {
      const body = await req.json().catch(() => ({}));
      const ids = Array.isArray(body?.ids)
        ? body.ids.filter((x: any) => typeof x === "string" && x.length > 0).slice(0, 500)
        : [];
      if (ids.length === 0) return Response.json({ ok: false, error: "No job ids provided" }, { status: 400 });
      const outcome = pauseJobsByUser(ids);
      invalidateStats();
      return Response.json(outcome);
    },
  },
  {
    methods: ["POST", "DELETE"],
    pattern: "/api/jobs/delete",
    handler: ({ req }) => deleteJobsBulk(req),
  },
  {
    methods: ["DELETE"],
    pattern: "/api/jobs",
    handler: ({ req }) => deleteJobsBulk(req),
  },
  // Legacy aliases (kept for older dashboards/scripts).
  {
    methods: ["POST"],
    pattern: "/api/retry/:id",
    handler: ({ id }) => retryRoute(id),
  },
  {
    methods: ["POST"],
    pattern: "/api/failcount/reset/:id",
    handler: ({ id }) => {
      resetFailCounters(id);
      return Response.json({ ok: true });
    },
  },
  {
    methods: ["POST"],
    pattern: "/api/scan",
    handler: async ({ req, config }) => {
      const body = await req.json().catch(() => ({}));
      const { url: scanUrl, folder } = body || {};
      if (!scanUrl) return Response.json({ ok: false, error: "URL required" }, { status: 400 });
      const bad = validateScanUrl(scanUrl);
      if (bad) return Response.json({ ok: false, error: bad }, { status: 400 });
      if (folder !== undefined && folder !== null && typeof folder !== "string") {
        return Response.json({ ok: false, error: "folder must be a string" }, { status: 400 });
      }
      try {
        const result = await scanAndIngest(scanUrl, getConfig(), folder || undefined);
        invalidateStats();
        const message =
          result.found === 0
            ? `No videos found at ${scanUrl} (check the URL, network, or cookies)`
            : `Scanned ${result.found} video(s): ${result.added} added, ${result.skipped} skipped`;
        return Response.json({ ok: true, message, ...result });
      } catch (e: any) {
        logError("scan", `${scanUrl}: ${e?.message || e}`);
        return Response.json({ ok: false, error: e.message || "Scan failed" }, { status: 500 });
      }
    },
  },
  {
    methods: ["POST"],
    pattern: "/api/queue/purge",
    handler: () => {
      // Never delete a row a worker is holding: a 'paused' row can still carry
      // a live download claim (user pause of an in-flight job), and a failed
      // download may have a conversion/metadata pass in progress.
      const result = db.run(
        `DELETE FROM jobs
          WHERE download_status IN ('pending', 'paused', 'waiting_live', 'failed')
            AND download_claimed_by IS NULL
            AND NOT ${JOB_IN_PROGRESS_SQL}`,
      );
      invalidateStats();
      return Response.json({ ok: true, deleted: result.changes });
    },
  },
  {
    methods: ["POST"],
    pattern: "/api/pause",
    handler: () => {
      triggerPause("MANUAL_WEB_UI");
      return Response.json({ ok: true, success: true, paused: true });
    },
  },
  {
    methods: ["POST"],
    pattern: "/api/resume",
    handler: () => {
      triggerResume();
      invalidateStats();
      return Response.json({ ok: true, success: true, paused: false });
    },
  },
  {
    methods: ["GET"],
    pattern: "/api/failed",
    handler: () => {
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
    },
  },
  {
    methods: ["POST"],
    pattern: "/api/failed/requeue",
    handler: ({ config }) => {
      // Re-queue every failed job that is eligible (transient errors, retry
      // budget remaining) immediately, ignoring the cooldown.
      const result = requeueFailedJobs(config, { ignoreCooldown: true });
      invalidateStats();
      return Response.json({ ok: true, requeued: result });
    },
  },
  {
    methods: ["GET"],
    pattern: "/api/settings",
    handler: ({ config }) => Response.json({ ok: true, ...readSettings(config) }),
  },
  {
    methods: ["POST"],
    pattern: "/api/settings",
    handler: async ({ req, config }) => {
      let patch: unknown;
      try {
        patch = await req.json();
      } catch {
        return Response.json({ ok: false, error: "Expected a JSON body" }, { status: 400 });
      }
      const result = await applySettings(config, patch as Record<string, unknown>);
      if (!result.ok) {
        return Response.json({ ok: false, error: result.error }, { status: 400 });
      }
      // Echo back the fresh snapshot so the panel can re-render from the
      // server's view of the world rather than what it thinks it sent.
      return Response.json({
        ok: true,
        changed: result.changed,
        ...readSettings(result.config ?? config),
      });
    },
  },
  { methods: ["GET"], pattern: "/api/reliability", handler: ({ config }) => reliabilityHandler(config) },
  {
    methods: ["GET"],
    pattern: "/api/history",
    handler: ({ url }) => {
      const limit = clampLimit(url.searchParams.get("limit"), 20);
      const rows = db.query("SELECT * FROM run_history ORDER BY ended_at DESC LIMIT ?").all(limit);
      return Response.json({ ok: true, history: rows });
    },
  },
  {
    methods: ["GET"],
    pattern: "/api/logs",
    handler: ({ url }) => {
      const logType = url.searchParams.get("type") || "error";
      const limit = clampLimit(url.searchParams.get("limit"), 100);
      let logs: string[] = [];
      try {
        if (logType === "report") {
          logs = buildRunReport();
        } else if (existsSync(errorLogPath())) {
          logs = readFileSync(errorLogPath(), "utf-8")
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
    },
  },
];

/** Dispatch an API request through the route table. */
async function handleApi(req: Request, config: Config, url: URL, pathname: string): Promise<Response> {
  const segments = pathname.split("/").filter(Boolean);
  // A static path (/api/jobs/pause) that exists with a different method is a
  // 405 — it must never fall through to a :param route and be read as an id.
  // Several routes may share one pattern (/api/settings GET + POST), so the
  // verdict is per pattern: 405 only when NO route with that exact pattern
  // accepts this method.
  const exactAllow = new Set<string>();
  let exactPath = false;
  let exactAccepted = false;
  for (const route of ROUTES) {
    if (route.pattern.includes(":")) continue;
    if (!matchRoute(route.pattern, segments)) continue;
    exactPath = true;
    for (const m of route.methods) exactAllow.add(m);
    if (route.methods.includes(req.method)) exactAccepted = true;
  }
  if (exactPath && !exactAccepted) {
    return methodNotAllowed(req.method, pathname, [...exactAllow]);
  }

  const allow = new Set<string>();
  for (const route of ROUTES) {
    const params = matchRoute(route.pattern, segments);
    if (!params) continue;
    if (!route.methods.includes(req.method)) {
      for (const m of route.methods) allow.add(m);
      continue;
    }
    return await route.handler({ req, config, url, params, id: params.id ?? "" });
  }
  if (allow.size > 0) {
    return methodNotAllowed(req.method, pathname, [...allow]);
  }
  return Response.json({ ok: false, error: `Unknown API path: ${pathname}` }, { status: 404 });
}

function methodNotAllowed(method: string, pathname: string, allow: string[]): Response {
  return Response.json(
    { ok: false, error: `Method ${method} not allowed for ${pathname}` },
    { status: 405, headers: { Allow: allow.sort().join(", ") } },
  );
}

// --- Larger handlers, kept out of the table for readability -------------------

function reliabilityHandler(config: Config): Response {
  // Every count below comes from the shared memoised snapshot (stats.ts):
  // this handler used to run seven queries of its own, one of them an
  // unbounded `.all()` over every failed row, on every poll.
  const snap = getStatsSnapshot(config);
  const t = STALE_CLAIM_THRESHOLDS(config);
  const resumableFailed = snap.resumableFailed;
  const partials = { count: snap.partialCount, bytes: snap.partialBytes };
  const waitingLive = { count: snap.waitingLive };
  const resumablePartials = { count: snap.resumablePartials };
  const interrupted = { count: snap.interrupted };
  const staleClaims = { count: snap.staleClaims };

  // The four self-healing sweeps, with what each currently has in scope.
  // `pending: null` means "not counted here" — the missing-files sweep has to
  // stat every recorded file, which is far too expensive to run per poll.
  // `error` is the sweep's last swallowed failure (null when its last run was
  // clean) — a sweep that keeps failing is otherwise indistinguishable from
  // one with nothing to do.
  const sweeps = [
    {
      id: "crashed",
      label: "Crashed jobs resume",
      cadence: "startup",
      detail: "Jobs interrupted mid-flight are re-queued and resume from their partial.",
      pending: interrupted?.count || 0,
      error: sweepError("crashed"),
    },
    {
      id: "staleClaims",
      label: "Stale claims reclaimed",
      cadence: "every 60s",
      detail: `Claims with no progress for ${t.downloadMinutes} min (downloads), 3 h (conversions) or 15 min (metadata) are re-queued.`,
      pending: staleClaims?.count || 0,
      error: sweepError("staleClaims"),
    },
    {
      id: "missingFiles",
      label: "Deleted files re-fetched",
      cadence: "startup",
      detail: "Files recorded as downloaded but no longer on disk are queued again.",
      pending: null,
      error: sweepError("missingFiles"),
    },
    {
      id: "requeueFailed",
      label: "Failed jobs retried",
      cadence: "every 60s",
      detail: "Failed jobs retry after a cooldown; permanent failures never do.",
      pending: resumableFailed,
      error: sweepError("requeueFailed"),
    },
    {
      id: "retention",
      label: "Retention policies",
      cadence: retentionEnabled(config) ? "startup + every 6h" : "off",
      detail: retentionEnabled(config)
        ? `run_history > ${config.runHistoryDays || "∞"} d, finished media > ${config.mediaRetentionDays || "∞"} d (job marked pruned), orphan sidecars ${config.pruneOrphanSidecars ? "removed" : "kept"}.`
        : "No retention rule enabled (runHistoryDays, mediaRetentionDays, pruneOrphanSidecars).",
      pending: null,
      error: sweepError("retention"),
    },
    {
      id: "orphanPartials",
      label: "Stale partials cleaned",
      cadence: "startup",
      detail: "Exhausted and orphaned .part files (and stranded aria2c control files) are removed; resumable partials are kept.",
      pending: null,
      error: sweepError("orphanPartials"),
    },
  ];

  return Response.json({
    ok: true,
    paused: isPaused(),
    pauseReason: getPauseReason(),
    partialFiles: { count: partials?.count || 0, bytes: partials?.bytes || 0 },
    resumableFailed,
    waitingLive: waitingLive?.count || 0,
    resume: {
      resumablePartials: resumablePartials?.count || 0,
      interrupted: interrupted?.count || 0,
      staleClaims: staleClaims?.count || 0,
    },
    sweeps,
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
