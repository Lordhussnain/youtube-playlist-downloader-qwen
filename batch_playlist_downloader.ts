// batch_playlist_downloader.ts (V5 - Autonomous Self-Healing Engine)
// Run with: bun run batch_playlist_downloader.ts
import { mkdir, unlink, readFile, writeFile, statfs, rm, stat, appendFile, readdir, cp, rename } from "node:fs/promises";
import { existsSync, statSync, readFileSync, writeFileSync, unlinkSync, createReadStream } from "node:fs";
import { join, basename, resolve, dirname } from "node:path";
import { networkInterfaces } from "node:os";
import { createHash } from "node:crypto";
import { Database } from "bun:sqlite";
import { z } from "zod";

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
  created_at: string; updated_at: string;
}

function initDatabase() {
  db = new Database("archive.db");
  db.run("PRAGMA journal_mode = WAL;"); // Crucial for concurrent workers
  db.run("PRAGMA busy_timeout = 5000;");

  db.run(`
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      url TEXT,
      title TEXT,
      output_directory TEXT,
      target_format TEXT,
      want_subtitles INTEGER DEFAULT 0,
      want_thumbnail INTEGER DEFAULT 0,
      want_description INTEGER DEFAULT 0,
      
      download_status TEXT DEFAULT 'pending',   -- pending, downloading, paused, downloaded, failed
      conversion_status TEXT DEFAULT 'pending', -- not_needed, pending, in_progress, done, failed
      
      download_claimed_by TEXT, download_claimed_at TEXT,
      conversion_claimed_by TEXT, conversion_claimed_at TEXT,
      
      partial_file_path TEXT,
      retry_count INTEGER DEFAULT 0,
      last_error TEXT,
      
      folder TEXT,
      index INTEGER,
      file_path TEXT,
      file_size INTEGER DEFAULT 0,
      integrity TEXT,
      
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // Keep auxiliary tables for Playlist Indexing and History
  db.run(`CREATE TABLE IF NOT EXISTS playlist_state (folder TEXT PRIMARY KEY, next_index INTEGER NOT NULL DEFAULT 0)`);
  db.run(`CREATE TABLE IF NOT EXISTS run_history (id INTEGER PRIMARY KEY AUTOINCREMENT, started_at TEXT, ended_at TEXT, duration_seconds REAL, downloaded INTEGER, skipped INTEGER, failed INTEGER, total_queued INTEGER)`);
  db.run(`CREATE TABLE IF NOT EXISTS job_queue (id TEXT PRIMARY KEY, url TEXT, folder TEXT, added_at TEXT DEFAULT CURRENT_TIMESTAMP, status TEXT DEFAULT 'pending', fail_count INTEGER DEFAULT 0, last_error TEXT)`);
}

// 🛡️ CRASH RECOVERY: Reset interrupted jobs to paused/pending so they resume
// This handles ungraceful exits where no shutdown handler could run
function reconcileCrashedJobs() {
  const stmt = db.run(`
    UPDATE jobs SET 
      download_status = CASE WHEN download_status = 'downloading' THEN 'paused' ELSE download_status END,
      conversion_status = CASE WHEN conversion_status = 'in_progress' THEN 'pending' ELSE conversion_status END,
      download_claimed_by = NULL, download_claimed_at = NULL,
      conversion_claimed_by = NULL, conversion_claimed_at = NULL,
      updated_at = CURRENT_TIMESTAMP
    WHERE download_status = 'downloading' OR conversion_status = 'in_progress'
  `);
  if (stmt.changes > 0) console.log(`🔄 Reconciled ${stmt.changes} crashed job(s) back to paused/pending.`);
}

// 🌟 ATOMIC CLAIMING: Single UPDATE-with-subquery prevents race conditions
// Two workers cannot grab the same row because the UPDATE itself does the SELECT
const claimDownloadJob = db.transaction((workerId: string) => {
  const row = db.query(`
    UPDATE jobs SET 
      download_status = 'downloading', 
      download_claimed_by = ?, 
      download_claimed_at = CURRENT_TIMESTAMP, 
      updated_at = CURRENT_TIMESTAMP 
    WHERE id = (
      SELECT id FROM jobs 
      WHERE download_status IN ('pending', 'paused') 
      ORDER BY created_at LIMIT 1
    ) RETURNING *
  `).get(workerId) as Job | null;
  return row;
});

const claimConvertJob = db.transaction((workerId: string) => {
  const row = db.query(`
    UPDATE jobs SET 
      conversion_status = 'in_progress', 
      conversion_claimed_by = ?, 
      conversion_claimed_at = CURRENT_TIMESTAMP, 
      updated_at = CURRENT_TIMESTAMP 
    WHERE id = (
      SELECT id FROM jobs 
      WHERE download_status = 'downloaded' AND conversion_status = 'pending' 
      ORDER BY created_at LIMIT 1
    ) RETURNING *
  `).get(workerId) as Job | null;
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
// 2. CONFIG & TYPES (Zod Validated)
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
  // PROXIES REMOVED
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
  denoPath: "deno",
  validateCookiesOnStart: true, outputRoot: "./downloads", archiveFile: "downloaded_videos.txt", cookiesFile: "cookies.txt",
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
    return ConfigSchema.parse(parsed); // Throws clear error if invalid
  } catch (err: any) {
    if (err.name === 'ZodError') {
      console.error("❌ Invalid config.json:", err.errors);
      process.exit(1);
    }
    console.log("⚠️ config.json not found or invalid. Creating default...");
    await writeFile("./config.json", JSON.stringify(DEFAULT_CONFIG, null, 2));
    return DEFAULT_CONFIG;
  }
}

const QUALITY_FORMATS: Record<string, string> = {
  highest: "bv*+ba/b", "1080p": "bv*[height<=1080]+ba/b", "720p": "bv*[height<=720]+ba/b", "480p": "bv*[height<=480]+ba/b", audio: "ba/b"
};

// ==========================================
// 3. GLOBAL STATE & UTILS
// ==========================================
let globalIsPaused = false;
let pauseReason: string | null = null;
const workerStatuses = new Map<string, string>();
let webServer: any = null;
const abortController = new AbortController();
let isTTY = process.stdout.isTTY;
const startTime = Date.now();

// 🛡️ ACTIVE PROCESS TRACKING: Track active subprocesses so we can gracefully interrupt them
const activeProcs = new Map<number, Bun.Subprocess>();

// 🛡️ TRUE PAUSE: Gracefully interrupts active yt-dlp processes
function triggerPause(reason: string) {
  if (globalIsPaused && pauseReason === reason) return;
  globalIsPaused = true;
  pauseReason = reason;
  console.log(`⏸️ Triggering pause: ${reason}`);
  
  // Send SIGINT to all active yt-dlp processes. 
  // yt-dlp handles SIGINT gracefully, finalizing the .part file for future resumption.
  for (const [id, proc] of activeProcs.entries()) {
    try { proc.kill('SIGINT'); } catch {}
  }
}

function triggerResume() {
  globalIsPaused = false;
  pauseReason = null;
}

// Autoscaler state
const autoscaler = {
  enabled: true, targetWorkers: 3, minWorkers: 1, maxWorkers: 5,
  bandwidthMultiplier: 1.0, maxBandwidthKBps: 0,
  workerSpeeds: new Map<number, number>(),
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
  const units = ["B", "KB", "MB", "GB", "TB"]; let i = 0, v = bytes;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(1)}${units[i]}`;
}
function formatBytesPerSec(bps: number): string { return bps <= 0 ? "0 B/s" : `${formatBytes(bps)}/s`; }
function parseSpeedToBytesPerSec(speedStr: string): number {
  if (!speedStr || speedStr.trim() === "" || speedStr === "NA") return 0;
  const s = speedStr.trim().toLowerCase();
  const match = s.match(/([\d.]+)\s*([kmgt]?b)?\/?s/i);
  if (!match) return 0;
  const val = parseFloat(match[1]);
  const unit = (match[2] || "b").toLowerCase();
  const multipliers: Record<string, number> = { b: 1, kb: 1024, mb: 1024**2, gb: 1024**3, tb: 1024**4 };
  return val * (multipliers[unit] || 1);
}
function sanitizeFolderName(name: string): string { return name.replace(/[\/:*?"<>|]/g, "_").trim() || "playlist"; }
function sanitizeFileName(name: string): string { return name.replace(/[\\/:*?"<>|]/g, "_").trim() || "video"; }

// ==========================================
// 🛡️ NETWORK AUTO-PILOT & STREAMING HASH
// ==========================================

// 🌐 Check internet connectivity with a lightweight request
async function checkInternet(): Promise<boolean> {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 5000);
    // Lightweight request to YouTube
    await fetch('https://www.youtube.com/favicon.ico', { 
      signal: controller.signal, method: 'HEAD', redirect: 'follow' 
    });
    clearTimeout(timeout);
    return true;
  } catch {
    return false;
  }
}

// 🌐 Network monitor that auto-pauses on disconnect and resumes when restored
async function networkMonitor() {
  let consecutiveFails = 0;
  console.log("🌐 Network monitor started.");
  
  while (!abortController.signal.aborted) {
    const isUp = await checkInternet();
    
    if (!isUp) {
      consecutiveFails++;
      // If it fails twice in a row (30 seconds), assume network is down
      if (consecutiveFails >= 2 && !globalIsPaused) {
        triggerPause("NETWORK_DISCONNECTED");
        console.log("🌐 Network down detected. Pausing engine gracefully.");
      }
    } else {
      if (consecutiveFails > 0) {
        console.log("🌐 Network restored!");
        consecutiveFails = 0;
        // Auto-resume if it was paused specifically due to network
        if (pauseReason === "NETWORK_DISCONNECTED") {
          triggerResume();
        }
      }
    }
    await Bun.sleep(15000); // Check every 15 seconds
  }
}

// 🛡️ NON-BLOCKING SHA256: Streams file instead of loading into RAM
async function hashFile(filePath: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const hash = createHash("sha256");
    const stream = createReadStream(filePath);
    stream.on("data", (chunk) => hash.update(chunk));
    stream.on("end", () => resolve(hash.digest("hex")));
    stream.on("error", reject);
  });
}

// 🧹 SAFE ORPHAN CLEANUP: Only deletes .part files for FAILED jobs or old orphans
async function cleanOrphanedFiles(rootDir: string) {
  try {
    // Get all failed job paths
    const failedPaths = db.query("SELECT partial_file_path FROM jobs WHERE download_status = 'failed' AND partial_file_path IS NOT NULL").all() as any[];
    const failedSet = new Set(failedPaths.map(r => r.partial_file_path));

    const files = await readdir(rootDir, { recursive: true });
    for (const file of files) {
      if (file.endsWith('.part') || file.endsWith('.ytdl')) {
        const fullPath = join(rootDir, file);
        // Only delete if it belongs to a failed job, or if it's older than 7 days (true orphans)
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

// ==========================================
// 4. COOKIE MANAGEMENT (Proxies Removed)
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
// 5. SCANNING & INGESTION
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

  // 🌟 MERGED METADATA: Download worker handles subs/thumbs/desc during download
  // No separate metadata_status needed - conversion_status determines if post-processing is required
  const conversionStatus = (config.videoQuality === 'audio' || targetFormat !== 'mp4') ? 'pending' : 'not_needed';

  // 🌟 BATCH INSERT: 10x-50x faster than individual inserts
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
// 6. WORKERS
// ==========================================
async function downloadWorker(id: number, config: Config) {
  const workerId = `dl-${id}`;
  while (!abortController.signal.aborted) {
    if (globalIsPaused) { await Bun.sleep(2000); continue; }

    // Pre-flight disk check
    const stats = await statfs(config.outputRoot);
    const freeGB = stats.bavail * stats.bsize / (1024 ** 3);
    if (freeGB < config.minFreeSpaceGB) {
      triggerPause(`LOW_DISK_SPACE (${freeGB.toFixed(1)}GB)`);
      await Bun.sleep(10000);
      continue;
    }

    const job = claimDownloadJob(workerId);
    if (!job) { await Bun.sleep(500); continue; }

    try {
      updateWorkerLine(id, `⬇️ Starting... | ${job.title}`, config);
      const format = QUALITY_FORMATS[config.videoQuality] || QUALITY_FORMATS["1080p"];
      // Deterministic output path for resume capability
      const baseFilename = `${String(job.index).padStart(3, "0")} - ${sanitizeFileName(job.title)}`;
      const outTemplate = join(job.output_directory, `${baseFilename}.%(ext)s`);

      // 🛡️ RESILIENT ARGS: Heavy retries and fragment recovery built into yt-dlp
      const args = [
        "yt-dlp", job.url, ...cookiesArgs(config), "--format", format,
        "--concurrent-fragments", "16", "-o", outTemplate,
        "--progress", "--progress-template", "download:PROGRESS:%(progress.percent).1f|%(progress.speed)f|%(progress.eta)f|%(progress.total_bytes)s|%(progress.downloaded_bytes)s",
        "--socket-timeout", "15", "--retries", "10", "--retry-sleep", "5", 
        "--fragment-retries", "10", "--extractor-retries", "5",
        "--continue", "--no-overwrites" // Crucial for safe resuming
      ];

      // 🌟 MERGED METADATA: Extract subs/thumbs/desc during download
      if (job.want_subtitles) args.push("--write-subs", "--write-auto-subs", "--sub-langs", "all.*", "--embed-subs");
      if (job.want_thumbnail) args.push("--write-thumbnail", "--convert-thumbnails", "jpg");
      if (job.want_description) args.push("--write-description");
      if (config.embedMetadata) args.push("--embed-thumbnail", "--embed-metadata", "--embed-chapters");

      const proc = Bun.spawn(args, { stdout: "pipe", stderr: "pipe" });
      activeProcs.set(id, proc); // 🌟 Track process for active pausing

      let lastProgressUpdate = 0;
      let buffer = "";
      let finalFilePath = "";
      
      // Stream stdout for real-time progress updates
      const reader = proc.stdout.getReader();
      const decoder = new TextDecoder();
      
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";
        
        for (const line of lines) {
          if (line.startsWith("PROGRESS:")) {
            const parts = line.replace("PROGRESS:", "").split("|");
            const percent = parts[0];
            const speed = parts[1];
            const eta = parts[2];
            const size = parts[3];
            const downloaded = parts[4];

            const bps = parseSpeedToBytesPerSec(speed);
            if (bps > 0) autoscaler.recordSpeed(id, bps);
            
            const sizeNum = parseInt(size, 10);
            const dlNum = parseInt(downloaded, 10);
            let pctNum = parseFloat(percent);
            
            // Fragmented downloads (m3u8/dash) report percent=NA — derive it from bytes.
            if (Number.isNaN(pctNum) && dlNum > 0 && sizeNum > 0) pctNum = (dlNum / sizeNum) * 100;
            
            if (!Number.isNaN(pctNum) && pctNum >= 0 && Date.now() - lastProgressUpdate > 500) {
              const sizeTxt = Number.isFinite(sizeNum) && sizeNum > 0 ? formatBytes(sizeNum) : "Calculating...";
              
              // 🌟 FIX: Show "Calculating..." instead of "0 B/s" or "NA" when yt-dlp hasn't computed speed yet
              const speedTxt = bps > 0 ? formatBytesPerSec(bps) : "Calculating..."; 
              
              const etaNum = parseFloat(eta);
              const etaTxt = Number.isFinite(etaNum) && etaNum > 0 ? `, ETA ${Math.round(etaNum)}s` : "";
              
              updateWorkerLine(id, `⬇️ ${pctNum.toFixed(1)}% of ${sizeTxt} @ ${speedTxt}${etaTxt} | ${job.title}`, config);
              lastProgressUpdate = Date.now();
            }
          } else if (line.trim() && existsSync(line.trim())) {
            finalFilePath = line.trim();
          }
        }
      }
      
      const code = await proc.exited;
      activeProcs.delete(id);

      // 🛡️ GRACEFUL PAUSE CATCH: If we killed it via SIGINT, don't mark as failed
      if (globalIsPaused) {
        db.run(`UPDATE jobs SET download_status = 'paused', download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [job.id]);
        updateWorkerLine(id, `⏸️ Paused | ${job.title}`, config);
        continue;
      }

      if (code === 0) {
        const filePath = finalFilePath || buffer.split("\n").reverse().find(l => l.trim() && existsSync(l.trim()))?.trim() || "";
        const fileSize = filePath ? (await stat(filePath)).size : 0;
        db.run(`UPDATE jobs SET download_status = 'downloaded', file_path = ?, file_size = ?, partial_file_path = ?, download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [filePath, fileSize, filePath, job.id]);
        updateWorkerLine(id, `✅ Downloaded | ${job.title}`, config);
      } else {
        throw new Error(buffer.split('\n').slice(-3).join(' '));
      }
    } catch (err: any) {
      // 🛡️ PAUSE CATCH (Fallback if process threw before exiting cleanly)
      if (globalIsPaused) {
        db.run(`UPDATE jobs SET download_status = 'paused', download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [job.id]);
        updateWorkerLine(id, `⏸️ Paused | ${job.title}`, config);
        continue;
      }

      const errMsg = String(err).toLowerCase();

      // 🛡️ CORRUPT PART FILE FALLBACK: Power cuts can corrupt .part files
      if (errMsg.includes("unable to resume") || errMsg.includes("incomplete") || errMsg.includes("corrupt")) {
        if (job.partial_file_path && existsSync(job.partial_file_path)) {
          await unlink(job.partial_file_path).catch(() => {});
        }
        db.run(`UPDATE jobs SET download_status = 'pending', partial_file_path = NULL, download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [job.id]);
        updateWorkerLine(id, `🗑️ Corrupt fragment deleted, restarting... | ${job.title}`, config);
        continue;
      }

      // 🛡️ SMART TRANSIENT ERROR ROUTING
      const isTransient = [
        "unable to download", "connection reset", "timeout", "network is unreachable", 
        "err_connection", "temporary failure", "could not connect", "sigabrt", "aborted"
      ].some(e => errMsg.includes(e));

      if (isTransient) {
        // Network glitch: Don't increment retry_count. Just sleep and let networkMonitor handle it.
        db.run(`UPDATE jobs SET download_status = 'pending', download_claimed_by = NULL, last_error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [errMsg.slice(0, 500), job.id]);
        updateWorkerLine(id, `🌐 Transient error, backing off... | ${job.title}`, config);
        await Bun.sleep(30000); // Wait 30s before trying again
        continue;
      }

      // FATAL ERROR (e.g., Video deleted, 403, Signature fail)
      const retryCount = job.retry_count + 1;
      const newStatus = retryCount >= config.maxRetryAttempts ? 'failed' : 'pending';
      db.run(`UPDATE jobs SET download_status = ?, retry_count = ?, last_error = ?, download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [newStatus, retryCount, errMsg.slice(0, 500), job.id]);
      updateWorkerLine(id, `❌ Failed | ${job.title}`, config);
    } finally {
      activeProcs.delete(id);
      autoscaler.clearWorker(id);
    }
  }
}


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
      if (!sourcePath.endsWith(".mp4")) {
        const mp4Path = sourcePath.replace(/\.[^./\\]+$/, ".mp4");
        // Fast remux
        const proc = Bun.spawn(["ffmpeg", "-y", "-i", sourcePath, "-map", "0:v:0", "-map", "0:a?", "-c:v", "copy", "-c:a", "aac", mp4Path], { stdout: "ignore", stderr: "pipe" });
        const [, code] = await Promise.all([new Response(proc.stderr).text(), proc.exited]);
        if (code !== 0) throw new Error("FFmpeg remux failed");
        if (config.deleteSourceAfterConvert) await unlink(sourcePath).catch(() => {});
        finalPath = mp4Path;
      }

      // Move to NAS if configured
      if (config.secondaryStoragePath) {
        const destDir = join(config.secondaryStoragePath, job.folder);
        await mkdir(destDir, { recursive: true });
        const destPath = join(destDir, basename(finalPath));
        await rename(finalPath, destPath).catch(async () => { await cp(finalPath, destPath, { force: true }); await unlink(finalPath); });
        finalPath = destPath;
      }

      // Integrity check with streaming hash
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
// 7. TUI & WEB UI
// ==========================================
// ---- Terminal UI (TUI) Dashboard --------------------------------------------
function initDashboard(config: Config) {
  if (!isTTY) return;
  const cols = process.stdout.columns || 80;
  const rows = process.stdout.rows || 50;
  const dashboardLines = 2 + config.maxDownloadWorkers + config.maxConcurrentConverts;
  
  // 🛡️ SAFETY NET: If terminal is too narrow or short, disable TUI to prevent overlapping text
  if (rows <= dashboardLines + 5 || cols < 60) { 
    isTTY = false; 
    console.log("⚠️ Terminal too small for TUI dashboard. Falling back to standard logs.");
    return; 
  }
  
  process.stdout.write("\x1b[2J\x1b[1;1H"); // Clear screen
  // Set scrolling region so standard console.log doesn't overwrite the TUI
  process.stdout.write(`\x1b[${dashboardLines + 1};${rows}r\x1b[${dashboardLines + 1};1H`);
  
  for (let i = 1; i <= dashboardLines; i++) process.stdout.write(`\x1b[${i};1H\x1b[2K`);
  for (let i = 1; i <= config.maxDownloadWorkers; i++) updateWorkerLine(i, "— idle slot —", config);
  for (let i = 1; i <= config.maxConcurrentConverts; i++) updateConvertWorkerLine(i, "💤 Idle", config);
}

function updateAbsoluteLine(row: number, text: string) {
  if (!isTTY) return;
  const cols = process.stdout.columns || 80;
  
  // 1. Truncate to prevent line wrapping (which destroys the TUI grid)
  let safeText = text.length > cols - 1 ? text.slice(0, cols - 4) + '...' : text;
  
  // 2. Pad with spaces to overwrite any leftover characters from previous longer strings
  // (This is a failsafe in case \x1b[2K isn't fully supported by the user's terminal)
  safeText = safeText.padEnd(cols - 1, ' ');
  
  // \x1b7 = Save cursor, \x1b8 = Restore cursor (more compatible than \x1b[s / \x1b[u)
  // \x1b[${row};1H = Move to row
  // \x1b[2K = Clear entire line
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
  updateAbsoluteLine(1, `DL:${aliveDownloadWorkers.size}/${autoscaler.targetWorkers} | ${agg}${cap} | Done:${stats.downloaded} Skip:${stats.skipped} Fail:${stats.failed} Tot:${stats.totalQueued}`);
  const plStrs = Array.from(playlistStates.entries()).map(([n, s]) => `${n}: ${s.downloaded + s.skipped}/${s.total}`);
  updateAbsoluteLine(2, `${plStrs.join(' | ')}`);
}

function resetTerminal() {
  if (!isTTY) return;
  const rows = process.stdout.rows || 50;
  process.stdout.write(`\x1b[1;${rows}r\x1b[${rows};1H`);
}

// ==========================================
// 7. WEB UI DASHBOARD
// ==========================================
const WEB_UI_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>🚀 YouTube Archive Engine</title>
  <style>
    :root {
      --bg-primary: #0f172a;
      --bg-secondary: #1e293b;
      --bg-card: #334155;
      --text-primary: #f1f5f9;
      --text-secondary: #94a3b8;
      --accent: #3b82f6;
      --success: #22c55e;
      --warning: #f59e0b;
      --danger: #ef4444;
      --paused: #8b5cf6;
    }
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: 'Segoe UI', system-ui, sans-serif;
      background: var(--bg-primary);
      color: var(--text-primary);
      min-height: 100vh;
      padding: 20px;
    }
    .container { max-width: 1400px; margin: 0 auto; }
    header {
      display: flex; justify-content: space-between; align-items: center;
      margin-bottom: 30px; padding: 20px; background: var(--bg-secondary);
      border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    h1 { font-size: 1.8rem; background: linear-gradient(135deg, var(--accent), var(--paused)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
    .stats-grid {
      display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 20px; margin-bottom: 30px;
    }
    .stat-card {
      background: var(--bg-secondary); padding: 20px; border-radius: 12px;
      text-align: center; box-shadow: 0 4px 6px rgba(0,0,0,0.2);
      transition: transform 0.2s;
    }
    .stat-card:hover { transform: translateY(-2px); }
    .stat-value { font-size: 2.5rem; font-weight: bold; margin-bottom: 5px; }
    .stat-label { color: var(--text-secondary); font-size: 0.9rem; text-transform: uppercase; letter-spacing: 1px; }
    .stat-card.download .stat-value { color: var(--accent); }
    .stat-card.success .stat-value { color: var(--success); }
    .stat-card.failed .stat-value { color: var(--danger); }
    .stat-card.total .stat-value { color: var(--text-primary); }
    .speed-display {
      grid-column: 1 / -1; background: linear-gradient(135deg, var(--bg-secondary), var(--bg-card));
      padding: 25px; border-radius: 12px; text-align: center;
    }
    .speed-value { font-size: 3rem; font-weight: bold; color: var(--success); }
    .speed-label { color: var(--text-secondary); margin-top: 5px; }
    .controls { display: flex; gap: 15px; margin-bottom: 30px; }
    .btn {
      padding: 12px 24px; border: none; border-radius: 8px; font-weight: 600;
      cursor: pointer; transition: all 0.2s; font-size: 0.95rem;
    }
    .btn:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(0,0,0,0.3); }
    .btn-pause { background: var(--warning); color: #000; }
    .btn-resume { background: var(--success); color: #fff; }
    .btn-retry { background: var(--accent); color: #fff; padding: 6px 12px; font-size: 0.85rem; }
    .btn:disabled { opacity: 0.5; cursor: not-allowed; transform: none; }
    .paused-banner {
      background: linear-gradient(135deg, var(--paused), #7c3aed);
      padding: 15px 20px; border-radius: 8px; margin-bottom: 20px;
      display: none; align-items: center; gap: 10px;
    }
    .paused-banner.active { display: flex; }
    .section {
      background: var(--bg-secondary); border-radius: 12px; padding: 20px;
      margin-bottom: 30px; box-shadow: 0 4px 6px rgba(0,0,0,0.2);
    }
    .section-title { font-size: 1.3rem; margin-bottom: 15px; color: var(--text-primary); display: flex; align-items: center; gap: 10px; }
    .workers-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 15px; }
    .worker-card {
      background: var(--bg-card); padding: 15px; border-radius: 8px;
      border-left: 4px solid var(--text-secondary);
    }
    .worker-card.active { border-left-color: var(--success); }
    .worker-card.busy { border-left-color: var(--accent); }
    .worker-id { font-weight: bold; margin-bottom: 8px; color: var(--text-secondary); }
    .worker-status { font-size: 0.9rem; word-break: break-word; }
    .jobs-table { width: 100%; border-collapse: collapse; }
    .jobs-table th, .jobs-table td { padding: 12px; text-align: left; border-bottom: 1px solid var(--bg-card); }
    .jobs-table th { background: var(--bg-card); font-weight: 600; color: var(--text-secondary); text-transform: uppercase; font-size: 0.85rem; }
    .jobs-table tr:hover { background: var(--bg-card); }
    .status-badge {
      padding: 4px 10px; border-radius: 20px; font-size: 0.8rem; font-weight: 600;
      text-transform: uppercase;
    }
    .status-pending { background: #475569; color: #cbd5e1; }
    .status-downloading { background: var(--accent); color: #fff; }
    .status-paused { background: var(--paused); color: #fff; }
    .status-downloaded { background: var(--success); color: #fff; }
    .status-failed { background: var(--danger); color: #fff; }
    .status-in_progress { background: var(--warning); color: #000; }
    .status-not_needed { background: #64748b; color: #cbd5e1; }
    .progress-bar { width: 100%; height: 8px; background: var(--bg-primary); border-radius: 4px; overflow: hidden; }
    .progress-fill { height: 100%; background: linear-gradient(90deg, var(--accent), var(--success)); transition: width 0.3s; }
    .error-text { color: var(--danger); font-size: 0.85rem; max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .tabs { display: flex; gap: 10px; margin-bottom: 20px; }
    .tab { padding: 10px 20px; background: var(--bg-card); border: none; color: var(--text-secondary); cursor: pointer; border-radius: 8px 8px 0 0; font-weight: 600; }
    .tab.active { background: var(--accent); color: #fff; }
    .tab-content { display: none; }
    .tab-content.active { display: block; }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.6; } }
    .live-indicator { width: 10px; height: 10px; background: var(--success); border-radius: 50%; display: inline-block; animation: pulse 2s infinite; margin-right: 8px; }
    .proxy-status { font-size: 0.9rem; color: var(--text-secondary); }
    .filter-controls { display: flex; gap: 10px; margin-bottom: 15px; flex-wrap: wrap; }
    .filter-select { padding: 8px 12px; background: var(--bg-card); border: 1px solid var(--bg-primary); color: var(--text-primary); border-radius: 6px; }
    .search-input { flex: 1; min-width: 200px; padding: 8px 12px; background: var(--bg-card); border: 1px solid var(--bg-primary); color: var(--text-primary); border-radius: 6px; }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <h1>🚀 YouTube Archive Engine</h1>
      <div style="display: flex; align-items: center; gap: 15px;">
        <span class="proxy-status">🌐 <span id="proxyCount">0/0</span> Proxies</span>
        <span><span class="live-indicator"></span>Live</span>
      </div>
    </header>

    <div class="paused-banner" id="pausedBanner">
      <span>⏸️</span>
      <span>System Paused - <strong id="pauseReason">Manual Pause</strong></span>
    </div>

    <div class="stats-grid">
      <div class="stat-card download">
        <div class="stat-value" id="statDownloading">0</div>
        <div class="stat-label">Downloading</div>
      </div>
      <div class="stat-card success">
        <div class="stat-value" id="statDownloaded">0</div>
        <div class="stat-label">Completed</div>
      </div>
      <div class="stat-card failed">
        <div class="stat-value" id="statFailed">0</div>
        <div class="stat-label">Failed</div>
      </div>
      <div class="stat-card total">
        <div class="stat-value" id="statTotal">0</div>
        <div class="stat-label">Total Jobs</div>
      </div>
      <div class="speed-display">
        <div class="speed-value" id="aggregateSpeed">0 B/s</div>
        <div class="speed-label">📊 Aggregate Download Speed</div>
      </div>
    </div>

    <div class="controls">
      <button class="btn btn-pause" id="pauseBtn" onclick="togglePause()">⏸️ Pause All</button>
      <button class="btn btn-resume" id="resumeBtn" onclick="toggleResume()" style="display:none;">▶️ Resume</button>
      <button class="btn" style="background: var(--bg-card); color: var(--text-primary);" onclick="refreshData()">🔄 Refresh</button>
      <button class="btn" style="background: var(--danger); color: #fff;" onclick="purgeQueue()">🗑️ Purge Queue</button>
    </div>

    <div class="section">
      <div class="section-title">👷 Active Workers</div>
      <div class="workers-grid" id="workersGrid"></div>
    </div>

    <div class="section">
      <div class="tabs">
        <button class="tab active" onclick="switchTab('all')">📋 All Jobs</button>
        <button class="tab" onclick="switchTab('downloading')">⬇️ Active</button>
        <button class="tab" onclick="switchTab('pending')">⏳ Pending</button>
        <button class="tab" onclick="switchTab('failed')">❌ Failed</button>
        <button class="tab" onclick="switchTab('completed')">✅ Completed</button>
        <button class="tab" onclick="switchTab('history')">📜 History</button>
        <button class="tab" onclick="switchTab('logs')">📝 Logs</button>
      </div>
      
      <div class="filter-controls">
        <input type="text" class="search-input" id="searchInput" placeholder="🔍 Search by title or URL..." oninput="filterJobs()">
        <select class="filter-select" id="statusFilter" onchange="filterJobs()">
          <option value="">All Statuses</option>
          <option value="pending">Pending</option>
          <option value="downloading">Downloading</option>
          <option value="paused">Paused</option>
          <option value="downloaded">Downloaded</option>
          <option value="failed">Failed</option>
        </select>
      </div>

      <div class="tab-content active" id="jobsTableContainer">
        <table class="jobs-table">
          <thead>
            <tr>
              <th>Title</th>
              <th>Folder</th>
              <th>DL Status</th>
              <th>Metadata</th>
              <th>Convert</th>
              <th>Retry</th>
              <th>Error</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody id="jobsTableBody"></tbody>
        </table>
      </div>
      
      <div class="tab-content" id="historyContainer">
        <table class="jobs-table">
          <thead>
            <tr>
              <th>Started</th>
              <th>Ended</th>
              <th>Duration</th>
              <th>Downloaded</th>
              <th>Skipped</th>
              <th>Failed</th>
              <th>Total</th>
            </tr>
          </thead>
          <tbody id="historyTableBody"></tbody>
        </table>
      </div>
      
      <div class="tab-content" id="logsContainer">
        <div style="display: flex; gap: 10px; margin-bottom: 15px;">
          <select class="filter-select" id="logType" onchange="loadLogs()">
            <option value="error">Error Log</option>
            <option value="report">Report Log</option>
            <option value="stream">Stream Log</option>
          </select>
          <button class="btn btn-retry" onclick="loadLogs()">🔄 Refresh Logs</button>
        </div>
        <pre id="logsContent" style="background: var(--bg-primary); padding: 15px; border-radius: 8px; max-height: 500px; overflow-y: auto; font-size: 0.85rem; white-space: pre-wrap;"></pre>
      </div>
      
      <div class="tab-content" id="failedContainer" style="display: none;">
        <h3 style="margin-bottom: 15px;">❌ Failed Items</h3>
        <table class="jobs-table">
          <thead>
            <tr>
              <th>Title</th>
              <th>Folder</th>
              <th>Retries</th>
              <th>Last Error</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody id="failedTableBody"></tbody>
        </table>
      </div>
    </div>
    
    <div class="section">
      <div class="section-title">➕ Add New Playlist/URL</div>
      <div style="display: flex; gap: 10px; flex-wrap: wrap;">
        <input type="text" id="scanUrl" placeholder="Enter YouTube URL..." style="flex: 1; min-width: 300px; padding: 10px; background: var(--bg-card); border: 1px solid var(--bg-primary); color: var(--text-primary); border-radius: 6px;">
        <input type="text" id="scanFolder" placeholder="Optional folder name" style="width: 200px; padding: 10px; background: var(--bg-card); border: 1px solid var(--bg-primary); color: var(--text-primary); border-radius: 6px;">
        <button class="btn btn-resume" onclick="scanUrl()">📥 Scan & Add</button>
      </div>
    </div>
  </div>

  <script>
    let currentTab = 'all';
    let allJobs = [];
    let isPaused = false;

    async function fetchStatus() {
      try {
        const res = await fetch('/api/status');
        const data = await res.json();
        
        document.getElementById('statDownloading').textContent = data.stats.totalQueued || 0;
        document.getElementById('statDownloaded').textContent = data.stats.downloaded || 0;
        document.getElementById('statFailed').textContent = data.stats.failed || 0;
        document.getElementById('statTotal').textContent = data.stats.total || 0;
        document.getElementById('aggregateSpeed').textContent = data.aggregateSpeed || '0 B/s';
        document.getElementById('proxyCount').textContent = \`\${data.proxyHealth?.active || 0}/\${data.proxyHealth?.total || 0}\`;
        
        isPaused = data.isPaused;
        document.getElementById('pauseBtn').style.display = isPaused ? 'none' : 'inline-block';
        document.getElementById('resumeBtn').style.display = isPaused ? 'inline-block' : 'none';
        document.getElementById('pausedBanner').classList.toggle('active', isPaused);
        document.getElementById('pauseReason').textContent = data.pauseReason || 'Manual Pause';
        
        const workersHtml = (data.workers || []).map(w => \`
          <div class="worker-card \${w.status === 'Idle' ? '' : w.status.includes('⬇️') ? 'busy' : 'active'}">
            <div class="worker-id">\${w.id} [\${w.type}]</div>
            <div class="worker-status">\${w.status || 'Idle'}</div>
          </div>
        \`).join('');
        document.getElementById('workersGrid').innerHTML = workersHtml;
        
      } catch (e) { console.error('Failed to fetch status:', e); }
    }

    async function fetchJobs() {
      try {
        const res = await fetch('/api/jobs');
        const data = await res.json();
        allJobs = data.jobs || [];
        renderJobs();
      } catch (e) { console.error('Failed to fetch jobs:', e); }
    }

    function renderJobs() {
      let filtered = allJobs;
      
      if (currentTab !== 'all') {
        if (currentTab === 'downloading') filtered = allJobs.filter(j => j.download_status === 'downloading');
        else if (currentTab === 'pending') filtered = allJobs.filter(j => j.download_status === 'pending' || j.download_status === 'paused');
        else if (currentTab === 'failed') filtered = allJobs.filter(j => j.download_status === 'failed' || j.conversion_status === 'failed');
        else if (currentTab === 'completed') filtered = allJobs.filter(j => j.download_status === 'downloaded' && (j.metadata_status === 'done' || j.metadata_status === 'not_needed') && (j.conversion_status === 'done' || j.conversion_status === 'not_needed'));
      }
      
      const search = document.getElementById('searchInput').value.toLowerCase();
      if (search) filtered = filtered.filter(j => j.title.toLowerCase().includes(search) || j.url.toLowerCase().includes(search));
      
      const statusFilter = document.getElementById('statusFilter').value;
      if (statusFilter) filtered = filtered.filter(j => j.download_status === statusFilter);
      
      const html = filtered.map(job => \`
        <tr>
          <td style="max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">\${escapeHtml(job.title)}</td>
          <td>\${job.folder || '-'}</td>
          <td><span class="status-badge status-\${job.download_status}">\${job.download_status}</span></td>
          <td><span class="status-badge status-\${job.metadata_status}">\${job.metadata_status}</span></td>
          <td><span class="status-badge status-\${job.conversion_status}">\${job.conversion_status}</span></td>
          <td>\${job.retry_count || 0}</td>
          <td class="error-text" title="\${escapeHtml(job.last_error || '')}">\${escapeHtml(job.last_error || '-')}</td>
          <td>\${job.download_status === 'failed' || job.conversion_status === 'failed' ? \`<button class="btn btn-retry" onclick="retryJob('\${job.id}')">🔄 Retry</button>\` : '-'}</td>
        </tr>
      \`).join('');
      
      document.getElementById('jobsTableBody').innerHTML = html || '<tr><td colspan="8" style="text-align: center; color: var(--text-secondary);">No jobs found</td></tr>';
    }

    function escapeHtml(text) {
      const div = document.createElement('div');
      div.textContent = text || '';
      return div.innerHTML;
    }

    function switchTab(tab) {
      currentTab = tab;
      document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
      event.target.classList.add('active');
      
      // Hide all tab contents
      document.querySelectorAll('.tab-content').forEach(c => c.style.display = 'none');
      
      // Show appropriate content based on tab
      if (tab === 'all' || tab === 'downloading' || tab === 'pending' || tab === 'completed') {
        document.getElementById('jobsTableContainer').style.display = 'block';
        renderJobs();
      } else if (tab === 'failed') {
        document.getElementById('failedContainer').style.display = 'block';
        loadFailedItems();
      } else if (tab === 'history') {
        document.getElementById('historyContainer').style.display = 'block';
        loadHistory();
      } else if (tab === 'logs') {
        document.getElementById('logsContainer').style.display = 'block';
        loadLogs();
      }
    }

    function filterJobs() {
      renderJobs();
    }

    async function togglePause() {
      await fetch('/api/pause', { method: 'POST' });
      refreshData();
    }

    async function toggleResume() {
      await fetch('/api/resume', { method: 'POST' });
      refreshData();
    }

    async function retryJob(id) {
      await fetch(`/api/retry/${encodeURIComponent(id)}`, { method: 'POST' });
      refreshData();
    }
    
    async function resetFailCount(id) {
      await fetch(`/api/failcount/reset/${encodeURIComponent(id)}`, { method: 'POST' });
      refreshData();
    }
    
    async function purgeQueue() {
      if (!confirm('Are you sure you want to purge all pending, paused, and failed jobs?')) return;
      await fetch('/api/queue/purge', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}) });
      refreshData();
    }
    
    async function scanUrl() {
      const url = document.getElementById('scanUrl').value.trim();
      const folder = document.getElementById('scanFolder').value.trim();
      if (!url) { alert('Please enter a URL'); return; }
      
      try {
        const res = await fetch('/api/scan', { 
          method: 'POST', 
          headers: { 'Content-Type': 'application/json' }, 
          body: JSON.stringify({ url, folder }) 
        });
        const data = await res.json();
        if (data.ok) {
          alert(data.message);
          document.getElementById('scanUrl').value = '';
          document.getElementById('scanFolder').value = '';
          refreshData();
        } else {
          alert('Error: ' + data.error);
        }
      } catch (e) {
        alert('Scan failed: ' + e.message);
      }
    }
    
    async function loadFailedItems() {
      try {
        const res = await fetch('/api/failed');
        const data = await res.json();
        const failed = data.failed || [];
        
        const html = failed.map(job => `
          <tr>
            <td style="max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">${escapeHtml(job.title)}</td>
            <td>${job.folder || '-'}</td>
            <td>${job.retry_count || 0}</td>
            <td class="error-text" title="${escapeHtml(job.last_error || '')}">${escapeHtml(job.last_error || '-')}</td>
            <td>
              <button class="btn btn-retry" onclick="retryJob('${job.id}')">🔄 Retry</button>
              <button class="btn btn-retry" style="background: var(--bg-card);" onclick="resetFailCount('${job.id}')">🔢 Reset Count</button>
            </td>
          </tr>
        `).join('');
        
        document.getElementById('failedTableBody').innerHTML = html || '<tr><td colspan="5" style="text-align: center; color: var(--text-secondary);">No failed items</td></tr>';
      } catch (e) { console.error('Failed to load failed items:', e); }
    }
    
    async function loadHistory() {
      try {
        const res = await fetch('/api/history?limit=20');
        const data = await res.json();
        const history = data.history || [];
        
        const html = history.map(run => `
          <tr>
            <td>${run.started_at || '-'}</td>
            <td>${run.ended_at || '-'}</td>
            <td>${run.duration_seconds ? Math.round(run.duration_seconds) + 's' : '-'}</td>
            <td style="color: var(--success);">${run.downloaded || 0}</td>
            <td style="color: var(--text-secondary);">${run.skipped || 0}</td>
            <td style="color: var(--danger);">${run.failed || 0}</td>
            <td>${run.total_queued || 0}</td>
          </tr>
        `).join('');
        
        document.getElementById('historyTableBody').innerHTML = html || '<tr><td colspan="7" style="text-align: center; color: var(--text-secondary);">No run history yet</td></tr>';
      } catch (e) { console.error('Failed to load history:', e); }
    }
    
    async function loadLogs() {
      try {
        const logType = document.getElementById('logType').value;
        const res = await fetch(`/api/logs?type=${logType}&limit=100`);
        const data = await res.json();
        const logs = data.logs || [];
        
        document.getElementById('logsContent').textContent = logs.join('\n') || 'No logs available';
      } catch (e) { 
        document.getElementById('logsContent').textContent = 'Error loading logs: ' + e.message;
      }
    }

    function refreshData() {
      fetchStatus();
      fetchJobs();
    }

    // Initial load and auto-refresh every 2 seconds
    refreshData();
    setInterval(refreshData, 2000);
  </script>
</body>
</html>`;
</html>`;
function startWebServer(port: number) {
  return Bun.serve({
    port, hostname: "0.0.0.0",
    async fetch(req) {
      const url = new URL(req.url);
      if (url.pathname === "/") return new Response(WEB_UI_HTML, { headers: { "Content-Type": "text/html" } });
      
      if (url.pathname === "/api/status") {
        const stats = db.query(`
          SELECT 
            SUM(CASE WHEN download_status IN ('pending', 'paused', 'downloading') THEN 1 ELSE 0 END) as queued,
            SUM(CASE WHEN download_status = 'downloaded' THEN 1 ELSE 0 END) as downloaded,
            SUM(CASE WHEN download_status = 'failed' OR conversion_status = 'failed' THEN 1 ELSE 0 END) as failed,
            COUNT(*) as total
          FROM jobs
        `).get() as any;

        const workers = [];
        for (let i = 1; i <= globalConfig.maxDownloadWorkers; i++) workers.push({ id: `[DL${i}]`, type: "download", status: workerStatuses.get(`DL${i}`) || "Idle" });
        for (let i = 1; i <= globalConfig.maxConcurrentConverts; i++) workers.push({ id: `[CV${i}]`, type: "convert", status: workerStatuses.get(`CV${i}`) || "Idle" });

        return Response.json({
          stats: { totalQueued: stats.queued || 0, downloaded: stats.downloaded || 0, failed: stats.failed || 0 },
          isPaused: globalIsPaused, pauseReason,
          activeWorkers: aliveDownloadWorkers.size, targetWorkers: autoscaler.targetWorkers,
          aggregateSpeed: formatBytesPerSec(autoscaler.getAggregateSpeed()),
          workers
        });
      }
      
      if (url.pathname === "/api/jobs" && req.method === "GET") {
        const rows = db.query("SELECT id, url, title, folder, download_status, metadata_status, conversion_status, retry_count, last_error FROM jobs ORDER BY created_at DESC LIMIT 500").all();
        return Response.json({ ok: true, jobs: rows });
      }
      
      // P3-1: Queue & history management endpoints
      if (url.pathname === "/api/scan" && req.method === "POST") {
        const body = await req.json().catch(() => ({}));
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
        const body = await req.json().catch(() => ({}));
        const { status: purgeStatus } = body;
        let stmt;
        if (purgeStatus) {
          stmt = db.prepare("DELETE FROM jobs WHERE download_status = ?");
          stmt.run(purgeStatus);
        } else {
          stmt = db.prepare("DELETE FROM jobs WHERE download_status IN ('pending', 'paused', 'failed')");
          stmt.run();
        }
        return Response.json({ ok: true, deleted: stmt.changes });
      }
      
      if (url.pathname.startsWith("/api/failcount/reset/") && req.method === "POST") {
        const id = decodeURIComponent(url.pathname.replace("/api/failcount/reset/", ""));
        db.run("UPDATE jobs SET retry_count = 0, last_error = NULL WHERE id = ?", [id]);
        return Response.json({ ok: true });
      }

      if (url.pathname === "/api/pause" && req.method === "POST") { globalIsPaused = true; pauseReason = "MANUAL_WEB_UI"; return Response.json({ success: true }); }
      if (url.pathname === "/api/resume" && req.method === "POST") { globalIsPaused = false; pauseReason = null; return Response.json({ success: true }); }
      
      if (url.pathname === "/api/failed" && req.method === "GET") {
        const rows = db.query("SELECT id, title, folder, retry_count, last_error FROM jobs WHERE download_status = 'failed' OR conversion_status = 'failed' LIMIT 100").all();
        return Response.json({ ok: true, failed: rows.map((r: any) => ({ ...r, fail_count: r.retry_count })) });
      }

      if (url.pathname.startsWith("/api/retry/") && req.method === "POST") {
        const id = decodeURIComponent(url.pathname.replace("/api/retry/", ""));
        db.run("UPDATE jobs SET download_status = 'pending', conversion_status = 'pending', metadata_status = CASE WHEN want_subtitles OR want_thumbnail OR want_description THEN 'pending' ELSE 'not_needed' END, retry_count = 0, last_error = NULL WHERE id = ?", [id]);
        return Response.json({ ok: true });
      }

      // P3-2: Log viewer endpoint
      if (url.pathname === "/api/logs" && req.method === "GET") {
        const logType = url.searchParams.get("type") || "error";
        const limit = parseInt(url.searchParams.get("limit") || "100", 10);
        let logs: string[] = [];
        
        try {
          if (logType === "error" && existsSync("error.log")) {
            const content = readFileSync("error.log", "utf-8");
            logs = content.split("\n").filter(l => l.trim()).slice(-limit);
          } else if (logType === "report" && existsSync("report.json")) {
            const content = readFileSync("report.json", "utf-8");
            logs = content.split("\n").filter(l => l.trim()).slice(-limit);
          } else if (logType === "stream") {
            logs = ["Log streaming not implemented yet"];
          }
        } catch (e: any) {
          logs = [`Error reading logs: ${e.message}`];
        }
        
        return Response.json({ ok: true, logs, type: logType, count: logs.length });
      }

      // P3-3: Run history endpoint
      if (url.pathname === "/api/history" && req.method === "GET") {
        const limit = parseInt(url.searchParams.get("limit") || "20", 10);
        const rows = db.query("SELECT * FROM run_history ORDER BY ended_at DESC LIMIT ?").all(limit);
        return Response.json({ ok: true, history: rows });
      }

      return new Response("Not Found", { status: 404 });
  });
}

// ==========================================
// 8. MAIN EXECUTION
// ==========================================
async function handleShutdown(sig: string) {
  console.log(`\n${sig} received. Gracefully stopping active downloads...`);
  triggerPause("SHUTDOWN_REQUESTED");
  
  // Give yt-dlp 3 seconds to write the .part files before we force kill
  await Bun.sleep(3000);
  
  abortController.abort();
  webServer?.stop(true);
  process.exit(0);
}

async function main() {
  process.on("SIGINT", () => handleShutdown("SIGINT"));
  process.on("SIGTERM", () => handleShutdown("SIGTERM"));

  globalConfig = await loadConfig();
  initDatabase();
  reconcileCrashedJobs(); // Safety net for crashes (handles ungraceful exits)
  await cleanOrphanedFiles(globalConfig.outputRoot); // Clean orphaned .part files from failed jobs
  
  autoscaler.init(globalConfig);

  // Ingest links from config
  for (const url of globalConfig.playlists) await scanAndIngest(url, globalConfig);
  for (const url of globalConfig.channels) await scanAndIngest(url, globalConfig);

  webServer = startWebServer(globalConfig.webPort);
  console.log(`Web UI: http://127.0.0.1:${globalConfig.webPort}`);

  // Start network monitor for auto-pause on disconnect
  networkMonitor();

  // Start Workers - Download and Converter run independently
  for (let i = 1; i <= globalConfig.maxConcurrentDownloads; i++) {
    aliveDownloadWorkers.add(i);
    downloadWorker(i, globalConfig).finally(() => aliveDownloadWorkers.delete(i));
  }
  for (let i = 1; i <= globalConfig.maxConcurrentConverts; i++) {
    converterWorker(i, globalConfig);
  }

  console.log("Engine started. Press Ctrl+C to stop.");
}

main().catch(console.error);
