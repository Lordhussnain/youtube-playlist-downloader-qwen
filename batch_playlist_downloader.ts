// batch_playlist_downloader.ts (V5 - Resilient, Autonomous, Proxy-Free)
// Run with: bun run batch_playlist_downloader.ts
import { mkdir, unlink, readFile, writeFile, statfs, rm, stat, appendFile, readdir, cp, rename } from "node:fs/promises";
import { existsSync, statSync, readFileSync, writeFileSync, unlinkSync, appendFileSync } from "node:fs";
import { join, basename, resolve, dirname } from "node:path";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { Database } from "bun:sqlite";
import { z } from "zod";
import os from "node:os";

// ==========================================
// 1. DATABASE & SCHEMA
// ==========================================
let db: Database;

interface Job {
  id: string; url: string; title: string; output_directory: string; target_format: string;
  want_subtitles: number; want_thumbnail: number; want_description: number;
  download_status: string; conversion_status: string; metadata_status: string;
  pause_reason: string | null; metadata_retry_count: number; metadata_files: string | null;
  download_claimed_by: string | null; download_claimed_at: string | null;
  conversion_claimed_by: string | null; conversion_claimed_at: string | null;
  partial_file_path: string | null; retry_count: number; last_error: string | null;
  folder: string; index: number; file_path: string | null; file_size: number; integrity: string | null;
  progress: number; speed: number; eta: number;
  created_at: string; updated_at: string;
}

// Add a column to an existing table if an older database doesn't have it yet.
function ensureColumn(table: string, column: string, ddl: string) {
  const cols = db.query(`PRAGMA table_info(${table})`).all() as { name: string }[];
  if (!cols.some(c => c.name === column)) db.run(`ALTER TABLE ${table} ADD COLUMN ${ddl}`);
}

function initDatabase() {
  db = new Database("archive.db");
  db.run("PRAGMA journal_mode = WAL;");
  db.run("PRAGMA busy_timeout = 5000;");
  db.run(
    `CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      url TEXT,
      title TEXT,
      output_directory TEXT,
      target_format TEXT,
      want_subtitles INTEGER DEFAULT 0,
      want_thumbnail INTEGER DEFAULT 0,
      want_description INTEGER DEFAULT 0,
      download_status TEXT DEFAULT 'pending',
      conversion_status TEXT DEFAULT 'pending',
      metadata_status TEXT DEFAULT 'not_needed',
      metadata_files TEXT,
      pause_reason TEXT,
      metadata_retry_count INTEGER DEFAULT 0,
      download_claimed_by TEXT,
      download_claimed_at TEXT,
      conversion_claimed_by TEXT,
      conversion_claimed_at TEXT,
      partial_file_path TEXT,
      retry_count INTEGER DEFAULT 0,
      last_error TEXT,
      folder TEXT,
      "index" INTEGER,
      file_path TEXT,
      file_size INTEGER DEFAULT 0,
      integrity TEXT,
      progress REAL DEFAULT 0,
      speed REAL DEFAULT 0,
      eta REAL DEFAULT 0,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )`
  );
  db.run(`CREATE TABLE IF NOT EXISTS playlist_state (folder TEXT PRIMARY KEY, next_index INTEGER NOT NULL DEFAULT 0)`);
  db.run(`CREATE TABLE IF NOT EXISTS run_history (id INTEGER PRIMARY KEY AUTOINCREMENT, started_at TEXT, ended_at TEXT, duration_seconds REAL, downloaded INTEGER, skipped INTEGER, failed INTEGER, total_queued INTEGER)`);

  // Schema migrations for databases created by older versions.
  ensureColumn("jobs", "metadata_status", "metadata_status TEXT DEFAULT 'not_needed'");
  ensureColumn("jobs", "metadata_files", "metadata_files TEXT");
  ensureColumn("jobs", "pause_reason", "pause_reason TEXT");
  ensureColumn("jobs", "metadata_retry_count", "metadata_retry_count INTEGER DEFAULT 0");
  db.run(
    `UPDATE jobs SET metadata_status = CASE
       WHEN COALESCE(want_subtitles,0) + COALESCE(want_thumbnail,0) + COALESCE(want_description,0) > 0 THEN 'pending'
       ELSE 'not_needed' END
     WHERE metadata_status IS NULL OR metadata_status = ''`
  );

  // Claim transactions MUST be created here, after `db` is initialized.
  // Defining them at module top-level would evaluate `db.transaction` while
  // `db` is still undefined and crash the process on startup.
  //
  // Download claim — atomic so two workers can never grab the same video:
  //   'pending'                  → not started yet
  //   'paused' + interrupted     → resumed automatically after a crash/shutdown
  //   'paused' + 'user'          → held until an explicit Resume
  claimDownloadJob = db.transaction((workerId: string) => {
    const row = db.query(
      `UPDATE jobs SET download_status = 'downloading', pause_reason = NULL, download_claimed_by = ?, download_claimed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
      WHERE id = (
        SELECT id FROM jobs
        WHERE download_status = 'pending'
           OR (download_status = 'paused' AND COALESCE(pause_reason, '') <> 'user')
        ORDER BY created_at, rowid LIMIT 1
      )
      RETURNING *`
    ).get(workerId) as Job | null;
    return row;
  });

  // Converter claim — only after download AND metadata work are terminal.
  claimConvertJob = db.transaction((workerId: string) => {
    const row = db.query(
      `UPDATE jobs SET conversion_status = 'in_progress', conversion_claimed_by = ?, conversion_claimed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
      WHERE id = (
        SELECT id FROM jobs
        WHERE download_status = 'downloaded' AND conversion_status = 'pending'
          AND metadata_status IN ('done', 'not_needed', 'failed')
        ORDER BY created_at, rowid LIMIT 1
      )
      RETURNING *`
    ).get(workerId) as Job | null;
    return row;
  });

  // Metadata claim — sidecars (subs/thumbnail/description/info.json) for
  // finished downloads whose flags say metadata is wanted.
  claimMetadataJob = db.transaction((workerId: string) => {
    const row = db.query(
      `UPDATE jobs SET metadata_status = 'in_progress', updated_at = CURRENT_TIMESTAMP
      WHERE id = (
        SELECT id FROM jobs
        WHERE download_status = 'downloaded' AND metadata_status = 'pending'
        ORDER BY created_at, rowid LIMIT 1
      )
      RETURNING *`
    ).get(workerId) as Job | null;
    return row;
  });
}

type ClaimJobFn = (workerId: string) => Job | null;
let claimDownloadJob: ClaimJobFn;
let claimConvertJob: ClaimJobFn;
let claimMetadataJob: ClaimJobFn;

function reconcileCrashedJobs() {
  // Interrupted mid-download jobs become 'paused' + 'interrupted' so they are
  // visible as paused AND automatically re-claimed (resuming where they left
  // off via yt-dlp --continue). User-paused jobs stay held.
  const stmt = db.run(
    `UPDATE jobs SET
      download_status = CASE WHEN download_status = 'downloading' THEN 'paused' ELSE download_status END,
      pause_reason = CASE WHEN download_status = 'downloading' OR (download_status = 'paused' AND pause_reason IS NULL) THEN 'interrupted' ELSE pause_reason END,
      conversion_status = CASE WHEN conversion_status = 'in_progress' THEN 'pending' ELSE conversion_status END,
      metadata_status = CASE WHEN metadata_status = 'in_progress' THEN 'pending' ELSE metadata_status END,
      download_claimed_by = NULL, download_claimed_at = NULL,
      conversion_claimed_by = NULL, conversion_claimed_at = NULL,
      updated_at = CURRENT_TIMESTAMP
    WHERE download_status = 'downloading'
       OR (download_status = 'paused' AND pause_reason IS NULL)
       OR conversion_status = 'in_progress'
       OR metadata_status = 'in_progress'`
  );
  if (stmt.changes > 0) console.log(`🔄 Reconciled ${stmt.changes} interrupted job(s) — paused/interrupted jobs will resume automatically.`);
}

function isVideoInDb(videoId: string): boolean {
  return !!db.query("SELECT id FROM jobs WHERE id = ?").get(videoId);
}

function getNextIndex(folder: string): number {
  const row = db.query("SELECT next_index FROM playlist_state WHERE folder = ?").get(folder) as { next_index: number } | null;
  const next = (row?.next_index || 0) + 1;
  db.run(`INSERT INTO playlist_state (folder, next_index) VALUES (?, ?) ON CONFLICT(folder) DO UPDATE SET next_index = excluded.next_index`, [folder, next]);
  return next;
}

// ==========================================
// 2. CONFIG WITH ZOD VALIDATION
// ==========================================
const ConfigSchema = z.object({
  playlists: z.array(z.string()),
  channels: z.array(z.string()),
  channelPlaylists: z.array(z.string()),
  maxConcurrentDownloads: z.number().min(1).max(20),
  maxConcurrentConverts: z.number().min(1).max(10),
  maxDownloadWorkers: z.number().min(1).max(20),
  minDownloadWorkers: z.number().min(1).max(20),
  maxMetadataWorkers: z.number().min(1).max(10),
  maxBandwidthKBps: z.number().min(0),
  autoscaleEnabled: z.boolean(),
  denoPath: z.string(),
  ytDlpPath: z.string(),
  ffmpegPath: z.string(),
  validateCookiesOnStart: z.boolean(),
  outputRoot: z.string(),
  archiveFile: z.string(),
  cookiesFile: z.string(),
  deleteSourceAfterConvert: z.boolean(),
  videoQuality: z.enum(["highest", "1080p", "720p", "480p", "audio"]),
  downloadSubtitles: z.boolean(),
  embedMetadata: z.boolean(),
  writeInfoJson: z.boolean(),
  writeDescription: z.boolean(),
  writeThumbnail: z.boolean(),
  archiveLiveStreams: z.boolean(),
  verifyIntegrity: z.boolean(),
  skipShorts: z.boolean(),
  downloadShorts: z.boolean(),
  maxRetryAttempts: z.number().min(1),
  maxFailures: z.number().min(1),
  maxFailuresPerVideo: z.number().min(1),
  minFreeSpaceGB: z.number().min(1),
  secondaryStoragePath: z.string(),
  daemonMode: z.boolean(),
  webPort: z.number().min(1).max(65535),
  rssEnabled: z.boolean(),
  rssPollIntervalMinutes: z.number().min(1),
  rescanIntervalHours: z.number().min(0),
});

type Config = z.infer<typeof ConfigSchema>;

const DEFAULT_CONFIG: Config = {
  playlists: [], channels: [], channelPlaylists: [],
  maxConcurrentDownloads: 3, maxConcurrentConverts: 2, maxDownloadWorkers: 5, minDownloadWorkers: 1, maxMetadataWorkers: 2,
  maxBandwidthKBps: 0, autoscaleEnabled: true,
  denoPath: "deno", ytDlpPath: "", ffmpegPath: "", validateCookiesOnStart: true, outputRoot: "./downloads", archiveFile: "downloaded_videos.txt", cookiesFile: "cookies.txt",
  deleteSourceAfterConvert: true, videoQuality: "1080p",
  downloadSubtitles: true, embedMetadata: true, writeInfoJson: true, writeDescription: true, writeThumbnail: true,
  archiveLiveStreams: false, verifyIntegrity: true, skipShorts: true, downloadShorts: false,
  maxRetryAttempts: 3, maxFailures: 10, maxFailuresPerVideo: 4,
  minFreeSpaceGB: 10, secondaryStoragePath: "",
  daemonMode: false, webPort: 3000, rssEnabled: true, rssPollIntervalMinutes: 15, rescanIntervalHours: 24,
};

let globalConfig: Config = { ...DEFAULT_CONFIG };

async function loadConfig(): Promise<Config> {
  let raw: string;
  try {
    raw = await readFile("./config.json", "utf-8");
  } catch (err: any) {
    // Only create a default config when the file genuinely does not exist —
    // never overwrite an existing file because of a parse/validation error.
    if (err?.code === "ENOENT") {
      console.log("⚠️ config.json not found. Creating default...");
      await writeFile("./config.json", JSON.stringify(DEFAULT_CONFIG, null, 2));
      return { ...DEFAULT_CONFIG };
    }
    console.error("❌ Failed to read config.json:", err?.message || err);
    process.exit(1);
  }
  try {
    const parsed = JSON.parse(raw);
    const merged = { ...DEFAULT_CONFIG, ...parsed };
    return ConfigSchema.parse(merged);
  } catch (err: any) {
    if (err?.name === "ZodError") {
      // Zod v4 exposes validation problems via `issues` (not `errors`).
      console.error("❌ Invalid config.json:", JSON.stringify(err.issues ?? [], null, 2));
    } else {
      console.error("❌ config.json contains invalid JSON:", err?.message || err);
    }
    process.exit(1);
  }
}

const QUALITY_FORMATS: Record<string, string> = {
  highest: "bv+ba/b",
  "1080p": "bv[height<=1080]+ba/b[height<=1080]",
  "720p": "bv[height<=720]+ba/b[height<=720]",
  "480p": "bv[height<=480]+ba/b[height<=480]",
  audio: "ba/bestaudio"
};

// ==========================================
// 3. GLOBAL STATE
// ==========================================
let globalIsPaused = false;
let pauseReason: string | null = null;
const workerStatuses = new Map<string, string>();
const stats = { downloaded: 0, skipped: 0, failed: 0, totalQueued: 0, metadata: 0, converted: 0 };
const playlistStates = new Map<string, { downloaded: number; skipped: number; total: number }>();
let webServer: any = null;
const abortController = new AbortController();
let isTTY = process.stdout.isTTY;
const startTime = Date.now();
const activeProcs = new Map<number, Bun.Subprocess>();
const activeMetadataProcs = new Map<number, Bun.Subprocess>();

const autoscaler = {
  enabled: true, targetWorkers: 3, minWorkers: 1, maxWorkers: 5,
  maxBandwidthKBps: 0, workerSpeeds: new Map<number, number>(),
  init(c: Config) {
    this.enabled = c.autoscaleEnabled; this.minWorkers = c.minDownloadWorkers; this.maxWorkers = c.maxDownloadWorkers;
    this.maxBandwidthKBps = c.maxBandwidthKBps; this.targetWorkers = Math.max(this.minWorkers, Math.min(c.maxConcurrentDownloads, this.maxWorkers));
  },
  recordSpeed(id: number, bps: number) { this.workerSpeeds.set(id, bps); },
  clearWorker(id: number) { this.workerSpeeds.delete(id); },
  getAggregateSpeed() { let s = 0; for (const v of this.workerSpeeds.values()) s += v; return s; },
};

const aliveDownloadWorkers = new Set<number>();
let activeConverts = 0;

function formatBytes(bytes: number): string {
  if (!bytes || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0, v = bytes;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(1)} ${units[i]}`;
}

function formatBytesPerSec(bps: number): string {
  if (!bps || bps <= 0) return "0 B/s";
  return formatBytes(bps) + "/s";
}

function parseSpeedToBytesPerSec(speedStr: string): number {
  if (!speedStr || speedStr.trim() === "" || speedStr.toLowerCase() === "na") return 0;
  const match = speedStr.match(/([\d.]+)\s*([KMGT]?i?B)/i);
  if (!match) return 0;
  const val = parseFloat(match[1]);
  const unit = match[2].toLowerCase().replace("ib", "b");
  const multipliers: Record<string, number> = { b: 1, kb: 1024, mb: 1024 ** 2, gb: 1024 ** 3, tb: 1024 ** 4 };
  return val * (multipliers[unit] || 1);
}

// Windows forbids these device names anywhere in a path (CON, NUL, COM1…).
const WINDOWS_RESERVED = /^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])$/i;

// Strip characters Windows rejects, trailing dots/spaces (invisible in
// Explorer but illegal on NTFS), and guard reserved device names.
function hardenName(name: string): string {
  let n = name.replace(/[\x00-\x1f]/g, " ").replace(/\s+/g, " ").trim();
  n = n.replace(/[. ]+$/g, "");
  if (!n) return "";
  const stem = n.split(".")[0];
  if (WINDOWS_RESERVED.test(stem)) n = `_${n}`;
  return n;
}

function sanitizeFolderName(name: string): string {
  return hardenName(name.replace(/[\/:*?"<>|]/g, " ").trim()) || "playlist";
}
function sanitizeFileName(name: string): string {
  return hardenName(name.replace(/[\\/:*?"<>|]/g, " ").trim()) || "video";
}

// Keep generated filenames well under Windows' MAX_PATH (260) once the
// directory and sidecar suffixes (.en.vtt, .info.json …) are added. Long
// titles are truncated and made unique with the video id.
function fitBaseFilename(dir: string, base: string, uniqueId: string): string {
  const SIDE_MARGIN = 20; // ".%(ext)s" + language/extension suffixes + slack
  const budget = 238 - dir.length - SIDE_MARGIN;
  if (base.length <= budget) return base;
  const idPart = ` [${uniqueId}]`;
  const keep = Math.max(8, budget - idPart.length);
  return base.slice(0, keep).trimEnd() + idPart;
}

function logError(scope: string, message: string) {
  try {
    appendFileSync("error.log", `[${new Date().toISOString()}] [${scope}] ${message}\n`);
    if (existsSync("error.log") && statSync("error.log").size > 1_000_000) {
      const lines = readFileSync("error.log", "utf-8").split("\n");
      writeFileSync("error.log", lines.slice(-400).join("\n"));
    }
  } catch {}
}

const MEDIA_EXTENSIONS = new Set([
  "mp4", "mkv", "webm", "mov", "flv", "avi", "ts", "m4v",
  "mp3", "m4a", "opus", "ogg", "flac", "wav", "aac", "ac3", "eac3", "3gp", "amr",
]);

// Fallback used when yt-dlp's `--print after_move:filepath` output was not
// captured: locate the media file we expect from the output template.
async function findDownloadedFile(dir: string, baseFilename: string): Promise<string> {
  try {
    const files = await readdir(dir);
    const matches: { path: string; mtime: number }[] = [];
    for (const f of files) {
      if (!f.startsWith(baseFilename + ".")) continue;
      const ext = f.split(".").pop()?.toLowerCase() || "";
      if (!MEDIA_EXTENSIONS.has(ext)) continue;
      const s = await stat(join(dir, f)).catch(() => null);
      if (s?.isFile()) matches.push({ path: join(dir, f), mtime: s.mtimeMs });
    }
    matches.sort((a, b) => b.mtime - a.mtime);
    return matches[0]?.path || "";
  } catch {
    return "";
  }
}

function formatDuration(seconds: number): string {
  if (!seconds || seconds <= 0) return '--';
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  return `${h}h ${m}m`;
}

// ==========================================
// 4. COOKIE MANAGEMENT (NO PROXIES)
// ==========================================
function cookiesArgs(config: Config): string[] {
  try { if (statSync(config.cookiesFile).size > 0) return ["--cookies", config.cookiesFile]; } catch {}
  return [];
}

async function validateCookies(cookiesFile: string): Promise<boolean> {
  if (!existsSync(cookiesFile)) return false;
  const proc = Bun.spawn([ytDlp(), "--cookies", cookiesFile, "--no-warnings", "--dump-single-json", "https://www.youtube.com/watch?v=dQw4w9WgXcQ"], { stdout: "pipe", stderr: "pipe" });
  const [, stderr, code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited]);
  return code === 0 && !stderr.toLowerCase().includes("login required");
}

// ==========================================
// 4b. DEPENDENCY CHECK (runs first on startup)
// ==========================================
async function probeBinary(bin: string, args: string[]): Promise<{ ok: boolean; version: string }> {
  try {
    const proc = Bun.spawn([bin, ...args], { stdout: "pipe", stderr: "pipe" });
    const [out, err, code] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
      proc.exited,
    ]);
    const firstLine = (out || err || "").split("\n").map(l => l.trim()).find(l => l) || "";
    return { ok: code === 0, version: firstLine.slice(0, 80) };
  } catch {
    return { ok: false, version: "" };
  }
}

// Resolved tool locations — set by checkDependencies(), used everywhere so
// custom Windows installs (exe next to the app, scoop, choco, winget, or an
// explicit config path) all work without touching PATH.
const resolvedTools = { ytDlp: "yt-dlp", ffmpeg: "ffmpeg" };
function ytDlp(): string { return resolvedTools.ytDlp; }
function ffmpeg(): string { return resolvedTools.ffmpeg; }

// Candidate search order: explicit config path → PATH → app folder → folder of
// the compiled exe → common Windows package-manager shims.
function toolCandidates(cfgPath: string, posixNames: string[], winNames: string[]): string[] {
  const cands: string[] = [];
  if (cfgPath && cfgPath.trim()) cands.push(cfgPath.trim());
  const cwd = process.cwd();
  const exeDir = dirname(process.execPath);
  for (const n of posixNames) {
    cands.push(n);               // bare name → PATH lookup
    cands.push(join(cwd, n));    // next to config.json / working dir
    cands.push(join(exeDir, n)); // next to the compiled archive.exe
  }
  if (process.platform === "win32") {
    const home = os.homedir();
    const progData = process.env.ProgramData || "C:\\ProgramData";
    const localAppData = process.env.LOCALAPPDATA || join(home, "AppData", "Local");
    for (const n of winNames) {
      cands.push(
        join(cwd, n),
        join(exeDir, n),
        join(progData, "chocolatey", "bin", n),
        join(home, "scoop", "shims", n),
        join(localAppData, "Microsoft", "WinGet", "Links", n),
      );
    }
  }
  const seen = new Set<string>();
  return cands.filter(c => {
    if (seen.has(c)) return false;
    seen.add(c);
    return true;
  });
}

async function resolveTool(
  cfgPath: string,
  versionArgs: string[],
  posixNames: string[],
  winNames: string[],
): Promise<{ path: string; version: string } | null> {
  for (const cand of toolCandidates(cfgPath, posixNames, winNames)) {
    const isBare = !cand.includes("/") && !cand.includes("\\");
    if (!isBare && !existsSync(cand)) continue;
    const probe = await probeBinary(cand, versionArgs);
    if (probe.ok) return { path: cand, version: probe.version };
  }
  return null;
}

// Verifies every required external tool BEFORE opening the database or
// scanning any links, so misconfigured machines fail fast with clear hints.
async function checkDependencies(): Promise<void> {
  console.log("🔎 Checking dependencies...");
  const missing: string[] = [];
  const [ytdlp, ffm] = await Promise.all([
    resolveTool(globalConfig.ytDlpPath, ["--version"], ["yt-dlp"], ["yt-dlp.exe"]),
    resolveTool(globalConfig.ffmpegPath, ["-version"], ["ffmpeg"], ["ffmpeg.exe"]),
  ]);

  if (ytdlp) {
    resolvedTools.ytDlp = ytdlp.path;
    console.log(`  ✅ yt-dlp: ${ytdlp.version || "ok"}${ytdlp.path.includes("/") || ytdlp.path.includes("\\") ? `  [${ytdlp.path}]` : "  [PATH]"}`);
  } else {
    console.error("  ❌ yt-dlp: not found (PATH, app folder, winget/scoop/chocolatey, ytDlpPath)");
    missing.push(`yt-dlp — Install: winget install yt-dlp  |  scoop install yt-dlp  |  pipx install yt-dlp  |  or set "ytDlpPath" in config.json`);
  }
  if (ffm) {
    resolvedTools.ffmpeg = ffm.path;
    console.log(`  ✅ ffmpeg: ${ffm.version || "ok"}${ffm.path.includes("/") || ffm.path.includes("\\") ? `  [${ffm.path}]` : "  [PATH]"}`);
  } else {
    console.error("  ❌ ffmpeg: not found (PATH, app folder, winget/scoop/chocolatey, ffmpegPath)");
    missing.push(`ffmpeg — Install: winget install ffmpeg  |  scoop install ffmpeg  |  choco install ffmpeg  |  or set "ffmpegPath" in config.json`);
  }

  if (missing.length > 0) {
    const msg = `Missing required dependencies:\n${missing.map(m => `  • ${m}`).join("\n")}`;
    console.error(`\n❌ ${msg}\n`);
    logError("startup", msg.replace(/\n/g, " | "));
    process.exit(1);
  }
  console.log("✅ All dependencies satisfied.");
}

// ==========================================
// 5. RESILIENCE: PAUSE, NETWORK MONITOR, TIMEOUTS
// ==========================================
function triggerPause(reason: string) {
  if (globalIsPaused && pauseReason === reason) return;
  globalIsPaused = true;
  pauseReason = reason;
  console.log(`⏸️ Triggering pause: ${reason}`);
  for (const [id, proc] of activeProcs.entries()) {
    try { proc.kill('SIGINT'); } catch {}
  }
}

function triggerResume() {
  globalIsPaused = false;
  pauseReason = null;
  try {
    // Re-queue ALL paused jobs (global + user-paused) on an explicit Resume All.
    // In-flight jobs still holding a claim finish naturally in their worker.
    const stmt = db.run(
      `UPDATE jobs SET download_status = 'pending', pause_reason = NULL, download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP
       WHERE download_status = 'paused' AND download_claimed_by IS NULL`
    );
    if (stmt.changes > 0) console.log(`▶️ Re-queued ${stmt.changes} paused job(s).`);
  } catch {}
}

async function checkInternet(): Promise<boolean> {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 5000);
    await fetch('https://www.youtube.com/favicon.ico', { signal: controller.signal, method: 'HEAD', redirect: 'follow' });
    clearTimeout(timeout);
    return true;
  } catch {
    return false;
  }
}

async function networkMonitor() {
  let consecutiveFails = 0;
  console.log("🌐 Network monitor started.");
  while (!abortController.signal.aborted) {
    const isUp = await checkInternet();
    if (!isUp) {
      consecutiveFails++;
      if (consecutiveFails >= 2 && !globalIsPaused) {
        triggerPause("NETWORK_DISCONNECTED");
        console.log("🌐 Network down detected. Pausing engine gracefully.");
      }
    } else {
      if (consecutiveFails > 0) {
        console.log("🌐 Network restored!");
        consecutiveFails = 0;
        if (pauseReason === "NETWORK_DISCONNECTED") triggerResume();
      }
    }
    await Bun.sleep(15000);
  }
}

function spawnWithTimeout(cmd: string[], args: string[], timeoutMs: number) {
  const ac = new AbortController();
  const timeout = setTimeout(() => ac.abort(), timeoutMs);
  const proc = Bun.spawn([cmd, ...args], { stdout: "pipe", stderr: "pipe", signal: ac.signal });
  proc.exited.finally(() => clearTimeout(timeout));
  return proc;
}

async function hashFile(filePath: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const hash = createHash("sha256");
    const stream = createReadStream(filePath);
    stream.on("data", (chunk) => hash.update(chunk));
    stream.on("end", () => resolve(hash.digest("hex")));
    stream.on("error", reject);
  });
}

async function cleanOrphanedFiles(rootDir: string) {
  try {
    const failedPaths = db.query("SELECT partial_file_path FROM jobs WHERE download_status = 'failed' AND partial_file_path IS NOT NULL").all() as any[];
    const failedSet = new Set(failedPaths.map(r => r.partial_file_path));
    const files = await readdir(rootDir, { recursive: true });
    for (const file of files) {
      if (file.endsWith('.part') || file.endsWith('.ytdl')) {
        const fullPath = join(rootDir, file);
        const stats = await stat(fullPath).catch(() => null);
        if (!stats) continue;
        const isOld = (Date.now() - stats.mtimeMs) > 7 * 24 * 60 * 60 * 1000;
        if (failedSet.has(fullPath) || isOld) {
          await unlink(fullPath).catch(() => {});
        }
      }
    }
  } catch {}
}

let diskCheckWarned = false;

async function checkDiskSpace(path: string, minGB: number): Promise<{ free: number; ok: boolean }> {
  try {
    const stats = await statfs(path);
    const freeGB = stats.bavail * stats.bsize / (1024 ** 3);
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
            { stdout: "pipe", stderr: "pipe" }
          );
          const [out, code] = await Promise.all([new Response(proc.stdout).text(), proc.exited]);
          const freeGB = parseFloat(out.trim()) / (1024 ** 3);
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

// Periodic safety net: if a worker process/thread dies mid-job the claim can
// be left behind. Downloads have a 15-minute watchdog, so any claim older than
// 20 minutes is definitely dead → mark paused+interrupted for auto-resume.
// Conversion claims older than 3h and metadata older than 15m are re-queued.
function reapStaleClaims() {
  try {
    const dl = db.run(
      `UPDATE jobs SET download_status = 'paused', pause_reason = 'interrupted',
         download_claimed_by = NULL, download_claimed_at = NULL, updated_at = CURRENT_TIMESTAMP
       WHERE download_status = 'downloading'
         AND (download_claimed_at IS NULL OR download_claimed_at < datetime('now', '-20 minutes'))`
    );
    const cv = db.run(
      `UPDATE jobs SET conversion_status = 'pending', conversion_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP
       WHERE conversion_status = 'in_progress'
         AND (conversion_claimed_at IS NULL OR conversion_claimed_at < datetime('now', '-3 hours'))`
    );
    const md = db.run(
      `UPDATE jobs SET metadata_status = 'pending', updated_at = CURRENT_TIMESTAMP
       WHERE metadata_status = 'in_progress' AND updated_at < datetime('now', '-15 minutes')`
    );
    const total = dl.changes + cv.changes + md.changes;
    if (total > 0) {
      console.log(`🧟 Reclaimed ${dl.changes} stale download(s), ${cv.changes} conversion(s), ${md.changes} metadata job(s).`);
      logError("reaper", `reclaimed stale claims: downloads=${dl.changes} conversions=${cv.changes} metadata=${md.changes}`);
    }
  } catch (e: any) {
    logError("reaper", String(e?.message || e));
  }
}

// ==========================================
// 6. SCANNING & INGESTION (BATCH)
// ==========================================
async function getPlaylistItems(url: string, config: Config) {
  const proc = Bun.spawn([ytDlp(), ...cookiesArgs(config), "--flat-playlist", "--print", "%(playlist_title)s|||%(id)s|||%(title)s|||%(duration)s", url], { stdout: "pipe", stderr: "pipe" });
  const [out, , code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited]);
  if (code !== 0) return [];
  return out.split("\n").filter(l => l.trim()).map(line => {
    const [playlist, id, title, duration] = line.split("|||");
    return {
      title: (title || "video").trim(),
      id: (id || "").trim(),
      playlist: (playlist || "playlist").trim(),
      duration: parseFloat(duration),
    };
  }).filter(i => i.id);
}

async function scanAndIngest(url: string, config: Config, overrideFolderName?: string): Promise<{ found: number; added: number; skipped: number }> {
  const items = await getPlaylistItems(url, config);
  if (items.length === 0) return { found: 0, added: 0, skipped: 0 };
  const folder = sanitizeFolderName(overrideFolderName || items[0].playlist || "Single Videos");
  const outputDir = join(config.outputRoot, folder);
  await mkdir(outputDir, { recursive: true });
  const targetFormat = config.videoQuality === 'audio' ? 'mp3' : 'mp4';
  const wantSubs = config.downloadSubtitles ? 1 : 0;
  const wantThumb = config.writeThumbnail ? 1 : 0;
  const wantDesc = config.writeDescription ? 1 : 0;
  const conversionStatus = (config.videoQuality === 'audio' || targetFormat !== 'mp4') ? 'pending' : 'not_needed';
  // Sidecar metadata (subs/thumbnail/description/info.json) is fetched by the
  // metadata worker after the download completes.
  const metadataStatus = (wantSubs || wantThumb || wantDesc || config.writeInfoJson) ? 'pending' : 'not_needed';

  let added = 0;
  let skipped = 0;
  const insertTransaction = db.transaction((items: any[]) => {
    const stmt = db.prepare(`INSERT OR IGNORE INTO jobs (id, url, title, output_directory, target_format, want_subtitles, want_thumbnail, want_description, folder, "index", download_status, conversion_status, metadata_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)`);
    for (const item of items) {
      if (isVideoInDb(item.id)) { skipped++; stats.skipped++; continue; }
      if (config.skipShorts && !config.downloadShorts && Number.isFinite(item.duration) && item.duration > 0 && item.duration < 60) {
        skipped++;
        stats.skipped++;
        continue;
      }
      const index = getNextIndex(folder);
      stmt.run(item.id, `https://www.youtube.com/watch?v=${item.id}`, item.title, outputDir, targetFormat, wantSubs, wantThumb, wantDesc, folder, index, conversionStatus, metadataStatus);
      added++;
      stats.totalQueued++;
    }
  });
  insertTransaction(items);
  return { found: items.length, added, skipped };
}

// ==========================================
// 7. DOWNLOAD WORKER (RESILIENT)
// ==========================================
async function downloadWorker(id: number, config: Config) {
  const workerId = `dl-${id}`;
  aliveDownloadWorkers.add(id);
  while (!abortController.signal.aborted) {
    if (globalIsPaused) { await Bun.sleep(2000); continue; }

    const disk = await checkDiskSpace(config.outputRoot, config.minFreeSpaceGB);
    if (!disk.ok) {
      triggerPause(`LOW_DISK_SPACE (${disk.free.toFixed(1)}GB < ${config.minFreeSpaceGB}GB)`);
      await Bun.sleep(10000);
      continue;
    }

    const job = claimDownloadJob(workerId);
    if (!job) { await Bun.sleep(500); continue; }

    try {
      updateWorkerLine(id, `⬇️ Starting... | ${job.title}`, config);
      const format = QUALITY_FORMATS[config.videoQuality] || QUALITY_FORMATS["1080p"];
      const baseFilename = fitBaseFilename(
        job.output_directory,
        `${String(job.index).padStart(3, "0")} - ${sanitizeFileName(job.title)}`,
        job.id
      );
      const outTemplate = join(job.output_directory, `${baseFilename}.%(ext)s`);

      const args = [
        ytDlp(), job.url, ...cookiesArgs(config), "--format", format,
        "--concurrent-fragments", "16", "-o", outTemplate,
        // --newline/--no-colors keep progress lines parseable from a pipe.
        "--progress", "--newline", "--no-colors",
        "--progress-template", "download:PROGRESS:%(progress.percent).1f|%(progress.speed)f|%(progress.eta)f|%(progress.total_bytes)s|%(progress.downloaded_bytes)s",
        // Print the final path after all post-processing so we can record it.
        // --print implies --simulate, so --no-simulate is required to actually write files.
        "--print", "after_move:%(filepath)s", "--no-simulate",
        "--socket-timeout", "15", "--retries", "10", "--retry-sleep", "5",
        "--fragment-retries", "10", "--extractor-retries", "5",
        "--continue", "--no-overwrites"
      ];

      // Sidecar files (subs/thumbnail/description/info.json) are fetched by the
      // metadata worker once the download completes; the download phase only
      // enriches the container itself (embedded art/metadata/chapters).
      if (config.embedMetadata) args.push("--embed-thumbnail", "--embed-metadata", "--embed-chapters");

      // 15-minute watchdog: abort hung downloads instead of blocking a worker forever.
      let timedOut = false;
      const downloadCtl = new AbortController();
      const downloadTimer = setTimeout(() => { timedOut = true; downloadCtl.abort(); }, 15 * 60 * 1000);
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
              db.run(`UPDATE jobs SET progress = ?, speed = ?, eta = ?, file_size = COALESCE(?, file_size) WHERE id = ?`, [pctNum, bps, parseFloat(parts[2]) || 0, totalBytes, job.id]);
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

      if (globalIsPaused) {
        db.run(`UPDATE jobs SET download_status = 'paused', download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [job.id]);
        updateWorkerLine(id, `⏸️ Paused | ${job.title}`, config);
        continue;
      }

      if (timedOut) throw new Error("Process timed out (15m)");

      if (code === 0) {
        let filePath = finalFilePath || buffer.split("\n").reverse().find(l => l.trim() && existsSync(l.trim()))?.trim() || "";
        if (!filePath) filePath = await findDownloadedFile(job.output_directory, baseFilename);
        if (!filePath) {
          logError("download", `${job.id} exited 0 but the output file could not be located: ${job.title}`);
          throw new Error("Download finished but output file could not be located");
        }
        const fileSize = (await stat(filePath)).size;
        db.run(`UPDATE jobs SET download_status = 'downloaded', file_path = ?, file_size = ?, partial_file_path = ?, progress = 100, download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [filePath, fileSize, filePath, job.id]);
        stats.downloaded++;
        updateWorkerLine(id, `✅ Downloaded | ${job.title}`, config);
      } else {
        const tail = [stderrText, buffer].filter(Boolean).join("\n").split("\n").filter(l => l.trim()).slice(-4).join(" ");
        throw new Error(tail || `yt-dlp exited with code ${code}`);
      }
    } catch (err: any) {
      if (globalIsPaused) {
        db.run(`UPDATE jobs SET download_status = 'paused', download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [job.id]);
        updateWorkerLine(id, `⏸️ Paused | ${job.title}`, config);
        continue;
      }

      const errMsg = String(err).toLowerCase();

      if (errMsg.includes("signature") || errMsg.includes("unable to extract")) {
        console.warn("⚠️ Signature challenge failed. Auto-updating yt-dlp...");
        const updateProc = Bun.spawn([ytDlp(), "-U"], { stdout: "pipe", stderr: "pipe" });
        await updateProc.exited;
        db.run(`UPDATE jobs SET download_status = 'pending', retry_count = 0, download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [job.id]);
        updateWorkerLine(id, `🔄 Auto-updated yt-dlp, retrying... | ${job.title}`, config);
        continue;
      }

      if (errMsg.includes("unable to resume") || errMsg.includes("incomplete") || errMsg.includes("corrupt")) {
        if (job.partial_file_path && existsSync(job.partial_file_path)) {
          await unlink(job.partial_file_path).catch(() => {});
        }
        db.run(`UPDATE jobs SET download_status = 'pending', partial_file_path = NULL, download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [job.id]);
        updateWorkerLine(id, `🗑️ Corrupt fragment deleted, restarting... | ${job.title}`, config);
        continue;
      }

      const isTransient = ["unable to download", "connection reset", "timeout", "network is unreachable", "err_connection", "temporary failure", "could not connect", "sigabrt", "aborted"].some(e => errMsg.includes(e));
      if (isTransient) {
        db.run(`UPDATE jobs SET download_status = 'pending', download_claimed_by = NULL, last_error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [errMsg.slice(0, 500), job.id]);
        updateWorkerLine(id, `🌐 Transient error, backing off... | ${job.title}`, config);
        await Bun.sleep(30000);
        continue;
      }

      const retryCount = job.retry_count + 1;
      const newStatus = retryCount >= config.maxRetryAttempts ? 'failed' : 'pending';
      db.run(`UPDATE jobs SET download_status = ?, retry_count = ?, last_error = ?, download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [newStatus, retryCount, errMsg.slice(0, 500), job.id]);
      if (newStatus === 'failed') {
        stats.failed++;
        logError("download", `${job.id} ${job.title}: ${errMsg.slice(0, 500)}`);
      }
      updateWorkerLine(id, `❌ Failed | ${job.title}`, config);
    } finally {
      activeProcs.delete(id);
      autoscaler.clearWorker(id);
    }
  }
  // Loop exited (shutdown): this worker is no longer alive.
  aliveDownloadWorkers.delete(id);
}

// Sidecar suffixes that belong next to a media file and must travel with it.
const SIDECAR_SUFFIXES = [
  ".vtt", ".srt", ".ass", ".lrc", ".ttml", ".srv1", ".srv2", ".srv3",
  ".description", ".info.json", ".jpg", ".jpeg", ".png", ".webp", ".gif",
];

// Run ffmpeg with a hard timeout so a wedged encode can never pin a worker.
async function runFfmpeg(args: string[], timeoutMs: number): Promise<{ code: number; stderr: string; timedOut: boolean }> {
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

// ==========================================
// 8. CONVERTER WORKER (STREAMING HASH)
// ==========================================
async function converterWorker(id: number, config: Config) {
  const workerId = `cv-${id}`;
  while (!abortController.signal.aborted) {
    if (globalIsPaused) { await Bun.sleep(2000); continue; }
    const job = claimConvertJob(workerId);
    if (!job) { await Bun.sleep(2000); continue; }
    try {
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
          60 * 60 * 1000
        );
        if (res.code !== 0) throw new Error(`FFmpeg mp3 encode ${res.timedOut ? "timed out" : "failed"}: ${res.stderr.split("\n").filter(l => l.trim()).slice(-2).join(" ")}`);
        if (config.deleteSourceAfterConvert) await unlink(sourcePath).catch(() => {});
        finalPath = mp3Path;
      } else if (!wantsMp3 && !sourcePath.endsWith(".mp4")) {
        const mp4Path = sourcePath.replace(/\.[^.]+$/, ".mp4");
        const res = await runFfmpeg(
          ["-y", "-i", sourcePath, "-map", "0:v:0", "-map", "0:a?", "-c:v", "copy", "-c:a", "aac", mp4Path],
          30 * 60 * 1000
        );
        if (res.code !== 0) throw new Error(`FFmpeg remux ${res.timedOut ? "timed out" : "failed"}: ${res.stderr.split("\n").filter(l => l.trim()).slice(-2).join(" ")}`);
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
          if (!SIDECAR_SUFFIXES.some(sfx => f.endsWith(sfx))) continue;
          const sideSrc = join(srcDir, f);
          const sideDest = join(destDir, f);
          await rename(sideSrc, sideDest).catch(async () => {
            await cp(sideSrc, sideDest, { force: true }).catch(() => {});
            await unlink(sideSrc).catch(() => {});
          });
        }
        const destPath = join(destDir, basename(finalPath));
        await rename(finalPath, destPath).catch(async () => { await cp(finalPath, destPath, { force: true }); await unlink(finalPath); });
        finalPath = destPath;
      }
      let integrity = null;
      if (config.verifyIntegrity) {
        integrity = await hashFile(finalPath);
      }
      db.run(`UPDATE jobs SET conversion_status = 'done', file_path = ?, integrity = ?, conversion_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [finalPath, integrity, job.id]);
      stats.converted++;
      updateConvertWorkerLine(id, `✅ Done | ${job.title}`, config);
    } catch (err: any) {
      const errMsg = String(err).slice(0, 500);
      db.run(`UPDATE jobs SET conversion_status = 'failed', last_error = ?, conversion_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [errMsg, job.id]);
      stats.failed++;
      logError("conversion", `${job.id} ${job.title}: ${errMsg}`);
      updateConvertWorkerLine(id, `❌ Failed | ${job.title}`, config);
    }
  }
}

// ==========================================
// 8b. METADATA WORKER (SUBS / THUMBS / DESCRIPTIONS / INFO.JSON)
// ==========================================
async function metadataWorker(id: number, config: Config) {
  const workerId = `md-${id}`;
  while (!abortController.signal.aborted) {
    if (globalIsPaused) { await Bun.sleep(2000); continue; }
    const job = claimMetadataJob(workerId);
    if (!job) { await Bun.sleep(2000); continue; }
    try {
      updateMetadataWorkerLine(id, `📎 Metadata | ${job.title}`, config);

      if (!job.file_path || !existsSync(job.file_path)) {
        throw new Error("Downloaded file missing — cannot fetch metadata");
      }

      // Write sidecars next to the downloaded file using the same basename.
      const mediaDir = dirname(job.file_path);
      const mediaBase = basename(job.file_path).replace(/\.[^.]+$/, "");
      const outTemplate = join(mediaDir, `${mediaBase}.%(ext)s`);

      const args = [
        ytDlp(), job.url, ...cookiesArgs(config),
        "--skip-download", "--no-simulate", "-o", outTemplate,
        "--socket-timeout", "15", "--retries", "5", "--extractor-retries", "3",
        "--newline", "--no-colors",
      ];
      if (job.want_subtitles) args.push("--write-subs", "--write-auto-subs", "--sub-langs", "all.*");
      if (job.want_thumbnail) args.push("--write-thumbnail", "--convert-thumbnails", "jpg");
      if (job.want_description) args.push("--write-description");
      if (config.writeInfoJson) args.push("--write-info-json");

      const ctl = new AbortController();
      const timer = setTimeout(() => ctl.abort(), 10 * 60 * 1000);
      const proc = Bun.spawn(args, { stdout: "pipe", stderr: "pipe", signal: ctl.signal });
      activeMetadataProcs.set(id, proc);
      // Drain both pipes concurrently to avoid deadlock.
      const stdoutPromise = new Response(proc.stdout).text().catch(() => "");
      const stderrPromise = new Response(proc.stderr).text();
      const [stdoutText, stderrText, code] = await Promise.all([stdoutPromise, stderrPromise, proc.exited]);
      clearTimeout(timer);
      activeMetadataProcs.delete(id);

      if (globalIsPaused) {
        db.run(`UPDATE jobs SET metadata_status = 'pending', updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [job.id]);
        continue;
      }
      if (ctl.signal.aborted) throw new Error("Metadata fetch timed out (10m)");
      if (code !== 0) {
        const tail = [stderrText, stdoutText].filter(Boolean).join("\n").split("\n").filter(l => l.trim()).slice(-3).join(" ");
        throw new Error(tail || `yt-dlp exited with code ${code}`);
      }

      // Record which sidecar files now exist next to the media file.
      const entries = await readdir(mediaDir).catch(() => [] as string[]);
      const sidecars = entries.filter(f =>
        f.startsWith(mediaBase + ".") &&
        SIDECAR_SUFFIXES.some(sfx => f.endsWith(sfx)) &&
        f !== basename(job.file_path!)
      );
      db.run(
        `UPDATE jobs SET metadata_status = 'done', metadata_files = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
        [JSON.stringify(sidecars), job.id]
      );
      stats.metadata++;
      updateMetadataWorkerLine(id, `✅ Metadata done (${sidecars.length} file(s)) | ${job.title}`, config);
    } catch (err: any) {
      const errMsg = String(err?.message || err).slice(0, 500);
      if (globalIsPaused) {
        db.run(`UPDATE jobs SET metadata_status = 'pending', updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [job.id]);
        continue;
      }
      const attempts = (job.metadata_retry_count || 0) + 1;
      const newStatus = attempts >= (config.maxRetryAttempts || 3) ? "failed" : "pending";
      db.run(
        `UPDATE jobs SET metadata_status = ?, metadata_retry_count = ?, last_error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
        [newStatus, attempts, errMsg, job.id]
      );
      if (newStatus === "failed") {
        stats.failed++;
        logError("metadata", `${job.id} ${job.title}: ${errMsg}`);
        updateMetadataWorkerLine(id, `❌ Metadata failed | ${job.title}`, config);
      } else {
        await Bun.sleep(15_000);
      }
    }
  }
}

// ==========================================
// 9. TUI DASHBOARD
// ==========================================
function initDashboard(config: Config) {
  if (!isTTY) return;
  const cols = process.stdout.columns || 80;
  const rows = process.stdout.rows || 50;
  const dashboardLines = 2 + config.maxDownloadWorkers + config.maxConcurrentConverts + config.maxMetadataWorkers;
  if (rows <= dashboardLines + 5 || cols < 60) {
    isTTY = false;
    console.log("⚠️ Terminal too small for TUI dashboard. Falling back to standard logs.");
    return;
  }
  process.stdout.write("\x1b[2J\x1b[1;1H");
  process.stdout.write(`\x1b[${dashboardLines + 1};${rows}r\x1b[${dashboardLines + 1};1H`);
  for (let i = 1; i <= dashboardLines; i++) process.stdout.write(`\x1b[${i};1H\x1b[2K`);
  for (let i = 1; i <= config.maxDownloadWorkers; i++) updateWorkerLine(i, "— idle slot —", config);
  for (let i = 1; i <= config.maxConcurrentConverts; i++) updateConvertWorkerLine(i, "💤 Idle", config);
  for (let i = 1; i <= config.maxMetadataWorkers; i++) updateMetadataWorkerLine(i, "💤 Idle", config);
}

function updateAbsoluteLine(row: number, text: string) {
  if (!isTTY) return;
  const cols = process.stdout.columns || 80;
  let safeText = text.length > cols - 1 ? text.slice(0, cols - 4) + '...' : text;
  safeText = safeText.padEnd(cols - 1, ' ');
  process.stdout.write(`\x1b7\x1b[${row};1H\x1b[2K${safeText}\x1b8`);
}

function updateWorkerLine(id: number, text: string, config: Config) {
  workerStatuses.set(`DL${id}`, text);
  updateAbsoluteLine(2 + id, `[DL${id}] ${text}`);
}

function updateConvertWorkerLine(id: number, text: string, config: Config) {
  workerStatuses.set(`CV${id}`, text);
  updateAbsoluteLine(2 + config.maxDownloadWorkers + id, `[CV${id}] ${text}`);
}

function updateMetadataWorkerLine(id: number, text: string, config: Config) {
  workerStatuses.set(`MD${id}`, text);
  updateAbsoluteLine(2 + config.maxDownloadWorkers + config.maxConcurrentConverts + id, `[MD${id}] ${text}`);
}

function renderDashboard() {
  if (!isTTY) return;
  const agg = formatBytesPerSec(autoscaler.getAggregateSpeed());
  const cap = autoscaler.maxBandwidthKBps > 0 ? `/${formatBytesPerSec(autoscaler.maxBandwidthKBps * 1024)}` : "";
  const statsData = db.query(
    `SELECT 
      SUM(CASE WHEN download_status IN ('pending', 'paused', 'downloading') THEN 1 ELSE 0 END) as queued,
      SUM(CASE WHEN download_status = 'downloaded' THEN 1 ELSE 0 END) as downloaded,
      SUM(CASE WHEN download_status = 'failed' THEN 1 ELSE 0 END) as failed,
      COUNT(*) as total
    FROM jobs`
  ).get() as any;
  updateAbsoluteLine(1, `🚀 DL:${aliveDownloadWorkers.size}/${autoscaler.targetWorkers} | ${agg}${cap} | Done:${statsData.downloaded || 0} Fail:${statsData.failed || 0} Tot:${statsData.total || 0}${globalIsPaused ? ' | ⏸️ PAUSED' : ''}`);
}

function resetTerminal() {
  if (!isTTY) return;
  const rows = process.stdout.rows || 50;
  process.stdout.write(`\x1b[1;${rows}r\x1b[${rows};1H`);
}

// ==========================================
// 10. WEB UI SERVER
// ==========================================
function buildRunReport(): string[] {
  try {
    const totals = db.query(
      `SELECT
        COUNT(*) as total,
        SUM(CASE WHEN download_status = 'pending' THEN 1 ELSE 0 END) as pending,
        SUM(CASE WHEN download_status = 'paused' THEN 1 ELSE 0 END) as paused,
        SUM(CASE WHEN download_status = 'downloading' THEN 1 ELSE 0 END) as downloading,
        SUM(CASE WHEN download_status = 'downloaded' THEN 1 ELSE 0 END) as downloaded,
        SUM(CASE WHEN download_status = 'failed' THEN 1 ELSE 0 END) as failed,
        SUM(CASE WHEN conversion_status = 'pending' THEN 1 ELSE 0 END) as conv_pending,
        SUM(CASE WHEN conversion_status = 'in_progress' THEN 1 ELSE 0 END) as conv_active,
        SUM(CASE WHEN conversion_status = 'done' THEN 1 ELSE 0 END) as conv_done,
        SUM(CASE WHEN conversion_status = 'failed' THEN 1 ELSE 0 END) as conv_failed,
        SUM(CASE WHEN metadata_status = 'pending' THEN 1 ELSE 0 END) as meta_pending,
        SUM(CASE WHEN metadata_status = 'in_progress' THEN 1 ELSE 0 END) as meta_active,
        SUM(CASE WHEN metadata_status = 'done' THEN 1 ELSE 0 END) as meta_done,
        SUM(CASE WHEN metadata_status = 'failed' THEN 1 ELSE 0 END) as meta_failed
      FROM jobs`
    ).get() as any;
    const failures = db.query(
      `SELECT id, title, retry_count, metadata_retry_count, last_error FROM jobs
       WHERE download_status = 'failed' OR conversion_status = 'failed' OR metadata_status = 'failed'
       ORDER BY updated_at DESC LIMIT 10`
    ).all() as any[];
    const lines: string[] = [];
    lines.push(`=== Archive Engine Report — ${new Date().toISOString()} ===`);
    lines.push(`Uptime: ${formatDuration(process.uptime())} | Engine: ${globalIsPaused ? `PAUSED (${pauseReason || "unknown"})` : "RUNNING"}`);
    lines.push(`Workers: ${aliveDownloadWorkers.size}/${autoscaler.targetWorkers} download | Speed: ${formatBytesPerSec(autoscaler.getAggregateSpeed())}`);
    lines.push(`Jobs — total: ${totals.total || 0}, pending: ${totals.pending || 0}, paused: ${totals.paused || 0}, downloading: ${totals.downloading || 0}, downloaded: ${totals.downloaded || 0}, failed: ${totals.failed || 0}`);
    lines.push(`Conversion — pending: ${totals.conv_pending || 0}, in progress: ${totals.conv_active || 0}, done: ${totals.conv_done || 0}, failed: ${totals.conv_failed || 0}`);
    lines.push(`Metadata — pending: ${totals.meta_pending || 0}, in progress: ${totals.meta_active || 0}, done: ${totals.meta_done || 0}, failed: ${totals.meta_failed || 0}`);
    lines.push(`This run — queued: ${stats.totalQueued}, downloaded: ${stats.downloaded}, skipped: ${stats.skipped}, failed: ${stats.failed}, metadata: ${stats.metadata}, converted: ${stats.converted}`);
    if (failures.length > 0) {
      lines.push("");
      lines.push("Recent failures:");
      for (const f of failures) {
        lines.push(`  ✗ [${f.id}] ${f.title} (retries: ${f.retry_count || 0}) — ${f.last_error || "no error recorded"}`);
      }
    } else {
      lines.push("");
      lines.push("No failed jobs. All clear. ✅");
    }
    return lines;
  } catch (e: any) {
    return [`Failed to build report: ${e?.message || e}`];
  }
}

function startWebServer(port: number) {
  return Bun.serve({
    port, hostname: "0.0.0.0",
    async fetch(req) {
      // Every request is wrapped: a handler crash returns JSON 500 instead of
      // hanging the socket, and the error lands in error.log.
      try {
        return await handleRequest(req);
      } catch (e: any) {
        logError("http", `${req.method} ${new URL(req.url).pathname}: ${e?.stack || e}`);
        return Response.json({ ok: false, error: "Internal server error" }, { status: 500 });
      }
    }
  });
}

async function handleRequest(req: Request): Promise<Response> {
  const url = new URL(req.url);

  if (url.pathname === "/") {
    if (existsSync("./web_ui.html")) {
      return new Response(Bun.file("./web_ui.html"), { headers: { "Content-Type": "text/html" } });
    }
    return new Response("web_ui.html not found. Please create it.", { status: 500 });
  }

  if (url.pathname === "/api/ping") {
    return new Response(null, { status: 200 });
  }

  if (url.pathname === "/api/status") {
    const statsData = db.query(
      `SELECT
        SUM(CASE WHEN download_status IN ('pending', 'paused', 'downloading') THEN 1 ELSE 0 END) as queued,
        SUM(CASE WHEN download_status = 'downloading' THEN 1 ELSE 0 END) as downloading,
        SUM(CASE WHEN download_status = 'downloaded' THEN 1 ELSE 0 END) as downloaded,
        SUM(CASE WHEN download_status = 'failed' OR conversion_status = 'failed' OR metadata_status = 'failed' THEN 1 ELSE 0 END) as failed,
        SUM(CASE WHEN metadata_status IN ('pending', 'in_progress') THEN 1 ELSE 0 END) as metadata_pending,
        SUM(CASE WHEN conversion_status IN ('pending', 'in_progress') THEN 1 ELSE 0 END) as converting,
        COUNT(*) as total
      FROM jobs`
    ).get() as any;
    const workers = [];
    for (let i = 1; i <= globalConfig.maxDownloadWorkers; i++) {
      workers.push({ id: `DL${i}`, type: "download", status: workerStatuses.get(`DL${i}`) || "Idle" });
    }
    for (let i = 1; i <= globalConfig.maxConcurrentConverts; i++) {
      workers.push({ id: `CV${i}`, type: "convert", status: workerStatuses.get(`CV${i}`) || "Idle" });
    }
    for (let i = 1; i <= globalConfig.maxMetadataWorkers; i++) {
      workers.push({ id: `MD${i}`, type: "metadata", status: workerStatuses.get(`MD${i}`) || "Idle" });
    }

    const diskStats = await statfs(globalConfig.outputRoot).catch(() => ({ bavail: 0, blocks: 1, bsize: 1 }));
    const freeGB = (diskStats.bavail * diskStats.bsize / (1024 ** 3)).toFixed(1);
    const totalGB = (diskStats.blocks * diskStats.bsize / (1024 ** 3)).toFixed(1);
    const diskPercent = ((diskStats.bavail / diskStats.blocks) * 100).toFixed(0);

    const memUsage = process.memoryUsage();
    const ramUsedGB = (memUsage.rss / (1024 ** 3)).toFixed(2);
    const ramTotalGB = (os.totalmem() / (1024 ** 3)).toFixed(2);
    const ramPercent = ((memUsage.rss / os.totalmem()) * 100).toFixed(0);
    const uptime = formatDuration(process.uptime());

    const avgSpeed = autoscaler.getAggregateSpeed();
    const remaining = db.query(`SELECT SUM(file_size * (1 - COALESCE(progress, 0) / 100)) as remaining FROM jobs WHERE download_status = 'downloading'`).get() as any;
    const secondsRemaining = avgSpeed > 0 && remaining.remaining ? (remaining.remaining / avgSpeed) : 0;
    const globalETA = secondsRemaining > 0 ? formatDuration(secondsRemaining) : "--";

    return Response.json({
      stats: {
        totalQueued: statsData.queued || 0,
        downloading: statsData.downloading || 0,
        downloaded: statsData.downloaded || 0,
        failed: statsData.failed || 0,
        metadataPending: statsData.metadata_pending || 0,
        converting: statsData.converting || 0,
        total: statsData.total || 0
      },
      queuePosition: statsData.queued || 0,
      speed: avgSpeed,
      aggregateSpeed: formatBytesPerSec(avgSpeed),
      activeWorkers: aliveDownloadWorkers.size,
      targetWorkers: autoscaler.targetWorkers,
      workers,
      isPaused: globalIsPaused,
      pauseReason,
      diskSpace: { free: `${freeGB} GB / ${totalGB} GB`, percent: parseFloat(diskPercent) },
      system: {
        cpu: "--", cpuPercent: 0,
        ram: `${ramUsedGB} GB / ${ramTotalGB} GB`,
        ramPercent: parseFloat(ramPercent)
      },
      uptime,
      globalETA
    });
  }

  if (url.pathname === "/api/jobs") {
    const rows = db.query(
      `SELECT id, url, title, folder, output_directory, file_path, target_format,
              download_status, conversion_status, metadata_status, pause_reason, metadata_files,
              retry_count, last_error, file_size, progress, speed, eta
       FROM jobs ORDER BY created_at DESC LIMIT 500`
    ).all();
    return Response.json({ jobs: rows });
  }

  if (url.pathname === "/api/scan" && req.method === "POST") {
    const body = await req.json().catch(() => ({}));
    const { url: scanUrl, folder } = body || {};
    if (!scanUrl) return Response.json({ ok: false, error: "URL required" }, { status: 400 });
    try {
      const result = await scanAndIngest(scanUrl, globalConfig, folder);
      const message = result.found === 0
        ? `No videos found at ${scanUrl} (check the URL, network, or cookies)`
        : `Scanned ${result.found} video(s): ${result.added} added, ${result.skipped} skipped`;
      return Response.json({ ok: true, message, ...result });
    } catch (e: any) {
      logError("scan", `${scanUrl}: ${e?.message || e}`);
      return Response.json({ ok: false, error: e.message || "Scan failed" }, { status: 500 });
    }
  }

  if (url.pathname === "/api/queue/purge" && req.method === "POST") {
    const result = db.run("DELETE FROM jobs WHERE download_status IN ('pending', 'paused', 'failed')");
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
    // conversion_status='not_needed'. Also clears a user pause.
    db.run(
      `UPDATE jobs SET
         download_status = 'pending', pause_reason = NULL, progress = 0, retry_count = 0,
         conversion_status = CASE WHEN conversion_status = 'not_needed' THEN 'not_needed' ELSE 'pending' END,
         metadata_status = CASE
           WHEN COALESCE(want_subtitles,0) + COALESCE(want_thumbnail,0) + COALESCE(want_description,0) > 0 THEN 'pending'
           ELSE metadata_status END,
         metadata_retry_count = 0,
         last_error = NULL, download_claimed_by = NULL, conversion_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [id]
    );
    return Response.json({ ok: true });
  }

  if (url.pathname.startsWith("/api/failcount/reset/") && req.method === "POST") {
    const id = decodeURIComponent(url.pathname.replace("/api/failcount/reset/", ""));
    if (id) {
      db.run(`UPDATE jobs SET retry_count = 0, metadata_retry_count = 0, last_error = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [id]);
    }
    return Response.json({ ok: true });
  }

  if (url.pathname === "/api/jobs/pause" && req.method === "POST") {
    const body = await req.json().catch(() => ({}));
    const ids = Array.isArray(body?.ids) ? body.ids.filter((x: any) => typeof x === "string" && x.length > 0).slice(0, 500) : [];
    if (ids.length === 0) return Response.json({ ok: false, error: "No job ids provided" }, { status: 400 });
    const placeholders = ids.map(() => "?").join(",");
    const result = db.run(
      `UPDATE jobs SET download_status = 'paused', pause_reason = 'user',
         download_claimed_by = CASE WHEN download_status = 'downloading' THEN download_claimed_by ELSE NULL END,
         updated_at = CURRENT_TIMESTAMP
       WHERE id IN (${placeholders}) AND download_status IN ('pending', 'downloading', 'paused')`,
      ids
    );
    return Response.json({ ok: true, paused: result.changes });
  }

  if (url.pathname === "/api/jobs/delete" && req.method === "POST") {
    const body = await req.json().catch(() => ({}));
    const ids = Array.isArray(body?.ids) ? body.ids.filter((x: any) => typeof x === "string" && x.length > 0).slice(0, 500) : [];
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
    const rows = db.query(
      `SELECT id, title, folder, output_directory, retry_count, metadata_retry_count,
              download_status, conversion_status, metadata_status, last_error
       FROM jobs
       WHERE download_status = 'failed' OR conversion_status = 'failed' OR metadata_status = 'failed'
       LIMIT 100`
    ).all();
    return Response.json({ ok: true, failed: rows });
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
        logs = readFileSync("error.log", "utf-8").split("\n").filter(l => l.trim()).slice(-limit);
      } else {
        logs = ["No logs available"];
      }
    } catch (e: any) { logs = [`Error: ${e.message}`]; }
    return Response.json({ ok: true, logs, type: logType });
  }

  return new Response("Not Found", { status: 404 });
}

// ==========================================
// 11. AUTONOMOUS POLLING
// ==========================================
async function startAutonomousPolling(config: Config) {
  if (config.rescanIntervalHours > 0 && (config.channels.length > 0 || config.channelPlaylists.length > 0)) {
    const intervalMs = config.rescanIntervalHours * 60 * 60 * 1000;
    setInterval(async () => {
      console.log("🔄 [Daemon] Running full channel rescan...");
      for (const url of [...config.channels, ...config.channelPlaylists]) {
        try {
          await scanAndIngest(url, config);
        } catch (e: any) {
          logError("rescan", `${url}: ${e?.message || e}`);
          console.error(`❌ Rescan failed for ${url}:`, e?.message || e);
        }
      }
    }, intervalMs);
  }
}

// ==========================================
// 12. MAIN EXECUTION
// ==========================================
let isShuttingDown = false;
let runHistoryId: number | null = null;

function runHistorySnapshot() {
  const fmt = (t: number) => new Date(t).toISOString().replace("T", " ").slice(0, 19);
  return [
    fmt(Date.now()),
    (Date.now() - startTime) / 1000,
    stats.downloaded, stats.skipped, stats.failed, stats.totalQueued,
  ] as const;
}

// Insert a run row at startup and refresh it every minute (heartbeat), so a
// hard kill (window close, taskkill, power loss) still leaves usable history.
function startRunHistory() {
  try {
    const fmt = (t: number) => new Date(t).toISOString().replace("T", " ").slice(0, 19);
    const res = db.run(
      `INSERT INTO run_history (started_at, ended_at, duration_seconds, downloaded, skipped, failed, total_queued) VALUES (?, ?, ?, 0, 0, 0, 0)`,
      [fmt(startTime), fmt(startTime), 0]
    );
    runHistoryId = Number(res.lastInsertRowid);
  } catch (e: any) {
    logError("history", String(e?.message || e));
  }
}

function heartbeatRunHistory() {
  if (runHistoryId == null) return;
  try {
    const [endedAt, duration, dl, sk, fl, tq] = runHistorySnapshot();
    db.run(
      `UPDATE run_history SET ended_at = ?, duration_seconds = ?, downloaded = ?, skipped = ?, failed = ?, total_queued = ? WHERE id = ?`,
      [endedAt, duration, dl, sk, fl, tq, runHistoryId]
    );
  } catch {}
}

async function handleShutdown(sig: string) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  console.log(`\n🛑 ${sig} received. Gracefully stopping active downloads...`);
  triggerPause("SHUTDOWN_REQUESTED");
  await Bun.sleep(3000);
  abortController.abort();

  // Kill any still-running child processes.
  for (const [, proc] of activeProcs) { try { proc.kill("SIGINT"); } catch {} }
  for (const [, proc] of activeMetadataProcs) { try { proc.kill("SIGINT"); } catch {} }

  try {
    // Persist an accurate picture of the interrupted pipeline:
    //  - in-flight downloads become 'paused' + 'interrupted' (auto-resumed
    //    and continued from where they left off on the next start)
    //  - in-flight conversions/metadata re-queue to run again on next start
    db.run(
      `UPDATE jobs SET download_status = 'paused', pause_reason = 'interrupted',
         download_claimed_by = NULL, download_claimed_at = NULL, updated_at = CURRENT_TIMESTAMP
       WHERE download_status = 'downloading'`
    );
    db.run(
      `UPDATE jobs SET conversion_status = 'pending', conversion_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP
       WHERE conversion_status = 'in_progress'`
    );
    db.run(
      `UPDATE jobs SET metadata_status = 'pending', updated_at = CURRENT_TIMESTAMP
       WHERE metadata_status = 'in_progress'`
    );
    // Final history flush (row was created at startup + heartbeated since).
    heartbeatRunHistory();
  } catch {}
  webServer?.stop(true);
  resetTerminal();
  process.exit(0);
}

// Restart crashed worker loops with a short backoff instead of dying silently.
function supervise(name: string, fn: () => Promise<void>) {
  fn()
    .catch((err) => {
      logError("worker", `${name} crashed: ${err?.stack || err}`);
      console.error(`❌ Worker ${name} crashed:`, err?.message || err);
    })
    .finally(() => {
      if (!abortController.signal.aborted) {
        console.log(`♻️ Restarting ${name} in 5s...`);
        setTimeout(() => supervise(name, fn), 5000);
      }
    });
}

async function main() {
  process.on("SIGINT", () => handleShutdown("SIGINT"));
  process.on("SIGTERM", () => handleShutdown("SIGTERM"));
  // Windows: Ctrl+Break / console close events surface as SIGBREAK.
  process.on("SIGBREAK", () => handleShutdown("SIGBREAK"));
  process.on("unhandledRejection", (reason) => {
    logError("process", `unhandledRejection: ${reason instanceof Error ? (reason.stack || String(reason)) : String(reason)}`);
  });
  process.on("uncaughtException", (err) => {
    logError("process", `uncaughtException: ${err?.stack || err}`);
    console.error("‼️ Uncaught exception (engine continues):", err);
  });

  // 1) Load configuration first (dependency search may use ytDlpPath/ffmpegPath
  //    from it), then verify external tools before touching the database.
  globalConfig = await loadConfig();
  await checkDependencies();
  // 2) Open/migrate the central database.
  initDatabase();
  reconcileCrashedJobs();
  // Run history: row created now, heartbeated so hard kills still leave data.
  startRunHistory();
  setTimeout(heartbeatRunHistory, 10_000);
  setInterval(heartbeatRunHistory, 60_000);
  // Ensure the output root exists — otherwise statfs fails, the disk check
  // reports 0 GB free, and the engine falsely pauses with LOW_DISK_SPACE.
  await mkdir(globalConfig.outputRoot, { recursive: true });
  await cleanOrphanedFiles(globalConfig.outputRoot);
  autoscaler.init(globalConfig);

  if (globalConfig.validateCookiesOnStart && existsSync(globalConfig.cookiesFile)) {
    const valid = await validateCookies(globalConfig.cookiesFile);
    if (!valid) console.warn("⚠️ Cookies may be invalid or expired.");
    else console.log("✅ Cookies validated.");
  }

  // 3) Load every link from config.json, fetch video details, store in DB.
  const allLinks = [...globalConfig.playlists, ...globalConfig.channels, ...globalConfig.channelPlaylists];
  for (const url of allLinks) {
    try {
      const r = await scanAndIngest(url, globalConfig);
      console.log(`📥 ${url} → found ${r.found}, added ${r.added}, skipped ${r.skipped}`);
    } catch (e: any) {
      logError("scan", `${url}: ${e?.message || e}`);
      console.error(`❌ Failed to scan ${url}:`, e?.message || e);
    }
  }

  initDashboard(globalConfig);
  webServer = startWebServer(globalConfig.webPort);
  console.log(`🌐 Web UI: http://127.0.0.1:${globalConfig.webPort}`);

  networkMonitor();
  setInterval(reapStaleClaims, 60_000);

  // 4) Pipeline workers (each supervised — crashed loops restart automatically):
  //    download → metadata → converter, all driven by job status in the DB.
  for (let i = 1; i <= globalConfig.maxDownloadWorkers; i++) {
    supervise(`download-worker-${i}`, () => downloadWorker(i, globalConfig));
  }
  for (let i = 1; i <= globalConfig.maxMetadataWorkers; i++) {
    supervise(`metadata-worker-${i}`, () => metadataWorker(i, globalConfig));
  }
  for (let i = 1; i <= globalConfig.maxConcurrentConverts; i++) {
    supervise(`converter-worker-${i}`, () => converterWorker(i, globalConfig));
  }

  if (globalConfig.daemonMode) startAutonomousPolling(globalConfig);

  setInterval(renderDashboard, 2000);
  console.log("🚀 Engine V5 started. Resilient, autonomous, proxy-free.");
}

main().catch((err) => {
  logError("startup", `main() failed: ${err?.stack || err}`);
  console.error("❌ Fatal startup error:", err);
  process.exit(1);
});
