// batch_playlist_downloader.ts (V5 - Resilient, Autonomous, Proxy-Free)
// Run with: bun run batch_playlist_downloader.ts
import { mkdir, unlink, readFile, writeFile, statfs, rm, stat, appendFile, readdir, cp, rename } from "node:fs/promises";
import { existsSync, statSync, readFileSync, writeFileSync, unlinkSync } from "node:fs";
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
  download_status: string; conversion_status: string;
  download_claimed_by: string | null; download_claimed_at: string | null;
  conversion_claimed_by: string | null; conversion_claimed_at: string | null;
  partial_file_path: string | null; retry_count: number; last_error: string | null;
  folder: string; index: number; file_path: string | null; file_size: number; integrity: string | null;
  progress: number; speed: number; eta: number;
  created_at: string; updated_at: string;
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
      download_claimed_by TEXT,
      download_claimed_at TEXT,
      conversion_claimed_by TEXT,
      conversion_claimed_at TEXT,
      partial_file_path TEXT,
      retry_count INTEGER DEFAULT 0,
      last_error TEXT,
      folder TEXT,
      index INTEGER,
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
}

function reconcileCrashedJobs() {
  const stmt = db.run(
    `UPDATE jobs SET 
      download_status = CASE WHEN download_status = 'downloading' THEN 'paused' ELSE download_status END,
      conversion_status = CASE WHEN conversion_status = 'in_progress' THEN 'pending' ELSE conversion_status END,
      download_claimed_by = NULL, download_claimed_at = NULL,
      conversion_claimed_by = NULL, conversion_claimed_at = NULL,
      updated_at = CURRENT_TIMESTAMP
    WHERE download_status = 'downloading' OR conversion_status = 'in_progress'`
  );
  if (stmt.changes > 0) console.log(`🔄 Reconciled ${stmt.changes} crashed job(s) back to paused/pending.`);
}

const claimDownloadJob = db.transaction((workerId: string) => {
  const row = db.query(
    `UPDATE jobs SET download_status = 'downloading', download_claimed_by = ?, download_claimed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
    WHERE id = (SELECT id FROM jobs WHERE download_status IN ('pending', 'paused') ORDER BY created_at LIMIT 1)
    RETURNING *`
  ).get(workerId) as Job | null;
  return row;
});

const claimConvertJob = db.transaction((workerId: string) => {
  const row = db.query(
    `UPDATE jobs SET conversion_status = 'in_progress', conversion_claimed_by = ?, conversion_claimed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
    WHERE id = (SELECT id FROM jobs WHERE download_status = 'downloaded' AND conversion_status = 'pending' ORDER BY created_at LIMIT 1)
    RETURNING *`
  ).get(workerId) as Job | null;
  return row;
});

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
  maxBandwidthKBps: z.number().min(0),
  autoscaleEnabled: z.boolean(),
  denoPath: z.string(),
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
  maxConcurrentDownloads: 3, maxConcurrentConverts: 2, maxDownloadWorkers: 5, minDownloadWorkers: 1,
  maxBandwidthKBps: 0, autoscaleEnabled: true,
  denoPath: "deno", validateCookiesOnStart: true, outputRoot: "./downloads", archiveFile: "downloaded_videos.txt", cookiesFile: "cookies.txt",
  deleteSourceAfterConvert: true, videoQuality: "1080p",
  downloadSubtitles: true, embedMetadata: true, writeInfoJson: true, writeDescription: true, writeThumbnail: true,
  archiveLiveStreams: false, verifyIntegrity: true, skipShorts: true, downloadShorts: false,
  maxRetryAttempts: 3, maxFailures: 10, maxFailuresPerVideo: 4,
  minFreeSpaceGB: 10, secondaryStoragePath: "",
  daemonMode: false, webPort: 3000, rssEnabled: true, rssPollIntervalMinutes: 15, rescanIntervalHours: 24,
};

let globalConfig: Config = { ...DEFAULT_CONFIG };

async function loadConfig(): Promise<Config> {
  try {
    const raw = await readFile("./config.json", "utf-8");
    const parsed = JSON.parse(raw);
    const merged = { ...DEFAULT_CONFIG, ...parsed };
    return ConfigSchema.parse(merged);
  } catch (err: any) {
    if (err.name === 'ZodError') {
      console.error("❌ Invalid config.json:", err.errors);
      process.exit(1);
    }
    console.log("⚠️ config.json not found. Creating default...");
    await writeFile("./config.json", JSON.stringify(DEFAULT_CONFIG, null, 2));
    return DEFAULT_CONFIG;
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
const stats = { downloaded: 0, skipped: 0, failed: 0, totalQueued: 0 };
const playlistStates = new Map<string, { downloaded: number; skipped: number; total: number }>();
let webServer: any = null;
const abortController = new AbortController();
let isTTY = process.stdout.isTTY;
const startTime = Date.now();
const activeProcs = new Map<number, Bun.Subprocess>();

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

function sanitizeFolderName(name: string): string { return name.replace(/[/:*?"<>|]/g, " ").trim() || "playlist"; }
function sanitizeFileName(name: string): string { return name.replace(/[\/:*?"<>|]/g, " ").trim() || "video"; }

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
  const proc = Bun.spawn(["yt-dlp", "--cookies", cookiesFile, "--no-warnings", "--dump-single-json", "https://www.youtube.com/watch?v=dQw4w9WgXcQ"], { stdout: "pipe", stderr: "pipe" });
  const [, stderr, code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited]);
  return code === 0 && !stderr.toLowerCase().includes("login required");
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

async function checkDiskSpace(path: string, minGB: number): Promise<{ free: number; ok: boolean }> {
  try {
    const stats = await statfs(path);
    const freeGB = stats.bavail * stats.bsize / (1024 ** 3);
    return { free: freeGB, ok: freeGB > minGB };
  } catch {
    return { free: 0, ok: false };
  }
}

// ==========================================
// 6. SCANNING & INGESTION (BATCH)
// ==========================================
async function getPlaylistItems(url: string, config: Config) {
  const proc = Bun.spawn(["yt-dlp", ...cookiesArgs(config), "--flat-playlist", "--print", "%(playlist_title)s|||%(id)s|||%(title)s", url], { stdout: "pipe", stderr: "pipe" });
  const [out, , code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited]);
  if (code !== 0) return [];
  return out.split("\n").filter(l => l.trim()).map(line => {
    const [playlist, id, title] = line.split("|||");
    return { title: (title || "video").trim(), id: (id || "").trim(), playlist: (playlist || "playlist").trim() };
  }).filter(i => i.id);
}

async function scanAndIngest(url: string, config: Config, overrideFolderName?: string) {
  const items = await getPlaylistItems(url, config);
  if (items.length === 0) return;
  const folder = sanitizeFolderName(overrideFolderName || items[0].playlist || "Single Videos");
  const outputDir = join(config.outputRoot, folder);
  await mkdir(outputDir, { recursive: true });
  const targetFormat = config.videoQuality === 'audio' ? 'mp3' : 'mp4';
  const wantSubs = config.downloadSubtitles ? 1 : 0;
  const wantThumb = config.writeThumbnail ? 1 : 0;
  const wantDesc = config.writeDescription ? 1 : 0;
  const conversionStatus = (config.videoQuality === 'audio' || targetFormat !== 'mp4') ? 'pending' : 'not_needed';

  const insertTransaction = db.transaction((items: any[]) => {
    const stmt = db.prepare(`INSERT OR IGNORE INTO jobs (id, url, title, output_directory, target_format, want_subtitles, want_thumbnail, want_description, folder, index, download_status, conversion_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)`);
    for (const item of items) {
      if (isVideoInDb(item.id)) continue;
      const index = getNextIndex(folder);
      stmt.run(item.id, `https://www.youtube.com/watch?v=${item.id}`, item.title, outputDir, targetFormat, wantSubs, wantThumb, wantDesc, folder, index, conversionStatus);
    }
  });
  insertTransaction(items);
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
      const baseFilename = `${String(job.index).padStart(3, "0")} - ${sanitizeFileName(job.title)}`;
      const outTemplate = join(job.output_directory, `${baseFilename}.%(ext)s`);

      const args = [
        "yt-dlp", job.url, ...cookiesArgs(config), "--format", format,
        "--concurrent-fragments", "16", "-o", outTemplate,
        "--progress", "--progress-template", "download:PROGRESS:%(progress.percent).1f|%(progress.speed)f|%(progress.eta)f|%(progress.total_bytes)s|%(progress.downloaded_bytes)s",
        "--socket-timeout", "15", "--retries", "10", "--retry-sleep", "5", 
        "--fragment-retries", "10", "--extractor-retries", "5",
        "--continue", "--no-overwrites"
      ];

      if (job.want_subtitles) args.push("--write-subs", "--write-auto-subs", "--sub-langs", "all.*", "--embed-subs");
      if (job.want_thumbnail) args.push("--write-thumbnail", "--convert-thumbnails", "jpg");
      if (job.want_description) args.push("--write-description");
      if (config.embedMetadata) args.push("--embed-thumbnail", "--embed-metadata", "--embed-chapters");

      const proc = Bun.spawn(args, { stdout: "pipe", stderr: "pipe" });
      activeProcs.set(id, proc);

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
              db.run(`UPDATE jobs SET progress = ?, speed = ?, eta = ? WHERE id = ?`, [pctNum, bps, parseFloat(parts[2]) || 0, job.id]);
              const speedTxt = bps > 0 ? formatBytesPerSec(bps) : "Calculating...";
              const etaNum = parseFloat(parts[2]);
              const etaTxt = Number.isFinite(etaNum) && etaNum > 0 ? `, ETA ${Math.round(etaNum)}s` : "";
              updateWorkerLine(id, `⬇️ ${pctNum.toFixed(1)}% @ ${speedTxt}${etaTxt} | ${job.title}`, config);
              lastProgressUpdate = Date.now();
            }
          } else if (line.trim() && existsSync(line.trim())) {
            finalFilePath = line.trim();
          }
        }
      }

      const [, code, signal] = await Promise.all([new Response(proc.stderr).text(), proc.exited, new Promise(r => setTimeout(r, 100))]);
      activeProcs.delete(id);

      if (globalIsPaused) {
        db.run(`UPDATE jobs SET download_status = 'paused', download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [job.id]);
        updateWorkerLine(id, `⏸️ Paused | ${job.title}`, config);
        continue;
      }

      if (signal === "SIGABRT") throw new Error("Process timed out (15m)");

      if (code === 0) {
        const filePath = finalFilePath || buffer.split("\n").reverse().find(l => l.trim() && existsSync(l.trim()))?.trim() || "";
        const fileSize = filePath ? (await stat(filePath)).size : 0;
        db.run(`UPDATE jobs SET download_status = 'downloaded', file_path = ?, file_size = ?, partial_file_path = ?, progress = 100, download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [filePath, fileSize, filePath, job.id]);
        updateWorkerLine(id, `✅ Downloaded | ${job.title}`, config);
      } else {
        throw new Error(buffer.split('\n').slice(-3).join(' '));
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
        const updateProc = Bun.spawn(["yt-dlp", "-U"], { stdout: "pipe", stderr: "pipe" });
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
      updateWorkerLine(id, `❌ Failed | ${job.title}`, config);
    } finally {
      activeProcs.delete(id);
      autoscaler.clearWorker(id);
      aliveDownloadWorkers.delete(id);
    }
  }
}

// ==========================================
// 8. CONVERTER WORKER (STREAMING HASH)
// ==========================================
async function converterWorker(id: number, config: Config) {
  const workerId = `cv-${id}`;
  while (!abortController.signal.aborted) {
    const job = claimConvertJob(workerId);
    if (!job) { await Bun.sleep(2000); continue; }
    try {
      updateConvertWorkerLine(id, `🔄 Converting | ${job.title}`, config);
      const sourcePath = job.file_path!;
      if (!sourcePath || !existsSync(sourcePath)) throw new Error("Source file missing");
      let finalPath = sourcePath;
      if (!sourcePath.endsWith(".mp4") && config.videoQuality !== 'audio') {
        const mp4Path = sourcePath.replace(/\.[^.]+$/, ".mp4");
        const proc = Bun.spawn(["ffmpeg", "-y", "-i", sourcePath, "-map", "0:v:0", "-map", "0:a?", "-c:v", "copy", "-c:a", "aac", mp4Path], { stdout: "ignore", stderr: "pipe" });
        const [, code] = await Promise.all([new Response(proc.stderr).text(), proc.exited]);
        if (code !== 0) throw new Error("FFmpeg remux failed");
        if (config.deleteSourceAfterConvert) await unlink(sourcePath).catch(() => {});
        finalPath = mp4Path;
      }
      if (config.secondaryStoragePath) {
        const destDir = join(config.secondaryStoragePath, job.folder);
        await mkdir(destDir, { recursive: true });
        const destPath = join(destDir, basename(finalPath));
        await rename(finalPath, destPath).catch(async () => { await cp(finalPath, destPath, { force: true }); await unlink(finalPath); });
        finalPath = destPath;
      }
      let integrity = null;
      if (config.verifyIntegrity) {
        integrity = await hashFile(finalPath);
      }
      db.run(`UPDATE jobs SET conversion_status = 'done', file_path = ?, integrity = ?, conversion_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [finalPath, integrity, job.id]);
      updateConvertWorkerLine(id, `✅ Done | ${job.title}`, config);
    } catch (err: any) {
      db.run(`UPDATE jobs SET conversion_status = 'failed', last_error = ?, conversion_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [String(err).slice(0, 500), job.id]);
      updateConvertWorkerLine(id, `❌ Failed | ${job.title}`, config);
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
  const dashboardLines = 2 + config.maxDownloadWorkers + config.maxConcurrentConverts;
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
function startWebServer(port: number) {
  return Bun.serve({
    port, hostname: "0.0.0.0",
    async fetch(req) {
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
            SUM(CASE WHEN download_status = 'downloaded' THEN 1 ELSE 0 END) as downloaded,
            SUM(CASE WHEN download_status = 'failed' OR conversion_status = 'failed' THEN 1 ELSE 0 END) as failed,
            COUNT(*) as total
          FROM jobs`
        ).get() as any;
        const workers = [];
        for (let i = 1; i <= globalConfig.maxDownloadWorkers; i++) {
          const status = workerStatuses.get(`DL${i}`) || "Idle";
          workers.push({ id: `DL${i}`, type: 'download', status });
        }
        for (let i = 1; i <= globalConfig.maxConcurrentConverts; i++) {
          const status = workerStatuses.get(`CV${i}`) || "Idle";
          workers.push({ id: `CV${i}`, type: 'convert', status });
        }
        
        // Disk space
        const diskStats = await statfs(globalConfig.outputRoot).catch(() => ({ bavail: 0, blocks: 1, bsize: 1 }));
        const freeGB = (diskStats.bavail * diskStats.bsize / (1024 ** 3)).toFixed(1);
        const totalGB = (diskStats.blocks * diskStats.bsize / (1024 ** 3)).toFixed(1);
        const diskPercent = ((diskStats.bavail / diskStats.blocks) * 100).toFixed(0);
        
        // System resources
        const memUsage = process.memoryUsage();
        const ramUsedGB = (memUsage.rss / (1024 ** 3)).toFixed(2);
        const ramTotalGB = (os.totalmem() / (1024 ** 3)).toFixed(2);
        const ramPercent = ((memUsage.rss / os.totalmem()) * 100).toFixed(0);
        const uptime = formatDuration(process.uptime());
        
        // Global ETA
        const avgSpeed = autoscaler.getAggregateSpeed();
        const remaining = db.query(`SELECT SUM(file_size * (1 - COALESCE(progress, 0) / 100)) as remaining FROM jobs WHERE download_status = 'downloading'`).get() as any;
        const secondsRemaining = avgSpeed > 0 && remaining.remaining ? (remaining.remaining / avgSpeed) : 0;
        const globalETA = secondsRemaining > 0 ? formatDuration(secondsRemaining) : '--';

        return Response.json({
          stats: {
            totalQueued: statsData.queued || 0,
            downloaded: statsData.downloaded || 0,
            failed: statsData.failed || 0,
            total: statsData.total || 0
          },
          aggregateSpeed: formatBytesPerSec(avgSpeed),
          activeWorkers: aliveDownloadWorkers.size,
          targetWorkers: autoscaler.targetWorkers,
          workers,
          isPaused: globalIsPaused,
          pauseReason: pauseReason,
          diskSpace: { free: `${freeGB} GB / ${totalGB} GB`, percent: parseFloat(diskPercent) },
          system: {
            cpu: '--', cpuPercent: 0,
            ram: `${ramUsedGB} GB / ${ramTotalGB} GB`,
            ramPercent: parseFloat(ramPercent)
          },
          uptime,
          globalETA
        });
      }

      if (url.pathname === "/api/jobs") {
        const rows = db.query(
          `SELECT id, url, title, folder, download_status, conversion_status, 
                 retry_count, last_error, file_size, progress, speed, eta
          FROM jobs ORDER BY created_at DESC LIMIT 500`
        ).all();
        return Response.json({ jobs: rows });
      }

      if (url.pathname === "/api/scan" && req.method === "POST") {
        const body = await req.json();
        const { url: scanUrl, folder } = body;
        if (!scanUrl) return Response.json({ ok: false, error: "URL required" }, { status: 400 });
        try {
          await scanAndIngest(scanUrl, globalConfig, folder);
          return Response.json({ ok: true, message: `Scanned ${scanUrl}` });
        } catch (e: any) {
          return Response.json({ ok: false, error: e.message || "Scan failed" }, { status: 500 });
        }
      }

      if (url.pathname === "/api/queue/purge" && req.method === "POST") {
        const stmt = db.prepare("DELETE FROM jobs WHERE download_status IN ('pending', 'paused', 'failed')");
        stmt.run();
        return Response.json({ ok: true, deleted: stmt.changes });
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
        db.run(`UPDATE jobs SET download_status = 'pending', conversion_status = 'pending', retry_count = 0, last_error = NULL, progress = 0 WHERE id = ?`, [id]);
        return Response.json({ ok: true });
      }

      if (url.pathname.startsWith("/api/jobs/") && req.method === "DELETE") {
        const id = decodeURIComponent(url.pathname.replace("/api/jobs/", ""));
        db.run(`DELETE FROM jobs WHERE id = ?`, [id]);
        return Response.json({ ok: true });
      }

      if (url.pathname === "/api/failed" && req.method === "GET") {
        const rows = db.query("SELECT id, title, folder, retry_count, last_error FROM jobs WHERE download_status = 'failed' OR conversion_status = 'failed' LIMIT 100").all();
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
          if (logType === "error" && existsSync("error.log")) {
            logs = readFileSync("error.log", "utf-8").split("\n").filter(l => l.trim()).slice(-limit);
          } else {
            logs = ["No logs available"];
          }
        } catch (e: any) { logs = [`Error: ${e.message}`]; }
        return Response.json({ ok: true, logs, type: logType });
      }

      return new Response("Not Found", { status: 404 });
    }
  });
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
        await scanAndIngest(url, config);
      }
    }, intervalMs);
  }
}

// ==========================================
// 12. MAIN EXECUTION
// ==========================================
async function handleShutdown(sig: string) {
  console.log(`\n🛑 ${sig} received. Gracefully stopping active downloads...`);
  triggerPause("SHUTDOWN_REQUESTED");
  await Bun.sleep(3000);
  abortController.abort();
  webServer?.stop(true);
  resetTerminal();
  process.exit(0);
}

async function main() {
  process.on("SIGINT", () => handleShutdown("SIGINT"));
  process.on("SIGTERM", () => handleShutdown("SIGTERM"));
  
  globalConfig = await loadConfig();
  initDatabase();
  reconcileCrashedJobs();
  await cleanOrphanedFiles(globalConfig.outputRoot);
  autoscaler.init(globalConfig);
  
  if (globalConfig.validateCookiesOnStart && existsSync(globalConfig.cookiesFile)) {
    const valid = await validateCookies(globalConfig.cookiesFile);
    if (!valid) console.warn("⚠️ Cookies may be invalid or expired.");
    else console.log("✅ Cookies validated.");
  }

  for (const url of globalConfig.playlists) await scanAndIngest(url, globalConfig);
  for (const url of globalConfig.channels) await scanAndIngest(url, globalConfig);

  initDashboard(globalConfig);
  webServer = startWebServer(globalConfig.webPort);
  console.log(`🌐 Web UI: http://127.0.0.1:${globalConfig.webPort}`);

  networkMonitor();
  
  for (let i = 1; i <= globalConfig.maxDownloadWorkers; i++) {
    downloadWorker(i, globalConfig).then(() => aliveDownloadWorkers.delete(i));
  }
  for (let i = 1; i <= globalConfig.maxConcurrentConverts; i++) {
    converterWorker(i, globalConfig);
  }

  if (globalConfig.daemonMode) startAutonomousPolling(globalConfig);

  setInterval(renderDashboard, 2000);
  console.log("🚀 Engine V5 started. Resilient, autonomous, proxy-free.");
}

main().catch(console.error);
