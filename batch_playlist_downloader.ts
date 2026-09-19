// batch_playlist_downloader.ts (V4 - Central DB Pipeline with Separate Metadata Worker)
// Run with: bun run batch_playlist_downloader.ts
import { mkdir, unlink, readFile, writeFile, statfs, rm, stat, appendFile, readdir, cp, rename } from "node:fs/promises";
import { existsSync, statSync, readFileSync, writeFileSync, unlinkSync } from "node:fs";
import { join, basename, resolve, dirname } from "node:path";
import { networkInterfaces } from "node:os";
import { createHash } from "node:crypto";
import { Database } from "bun:sqlite";

// ==========================================
// 1. DATABASE & SCHEMA
// ==========================================
let db: Database;

interface Job {
  id: string; url: string; title: string; output_directory: string; target_format: string;
  want_subtitles: number; want_thumbnail: number; want_description: number;
  download_status: string; metadata_status: string; conversion_status: string;
  download_claimed_by: string | null; download_claimed_at: string | null;
  metadata_claimed_by: string | null; metadata_claimed_at: string | null;
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
      metadata_status TEXT DEFAULT 'pending',   -- not_needed, pending, in_progress, done, failed
      conversion_status TEXT DEFAULT 'pending', -- not_needed, pending, in_progress, done, failed
      
      download_claimed_by TEXT, download_claimed_at TEXT,
      metadata_claimed_by TEXT, metadata_claimed_at TEXT,
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
}

// 🛡️ CRASH RECOVERY: Reset interrupted jobs to paused/pending so they resume
// This handles ungraceful exits where no shutdown handler could run
function reconcileCrashedJobs() {
  const stmt = db.run(`
    UPDATE jobs SET 
      download_status = CASE WHEN download_status = 'downloading' THEN 'paused' ELSE download_status END,
      metadata_status = CASE WHEN metadata_status = 'in_progress' THEN 'pending' ELSE metadata_status END,
      conversion_status = CASE WHEN conversion_status = 'in_progress' THEN 'pending' ELSE conversion_status END,
      download_claimed_by = NULL, download_claimed_at = NULL,
      metadata_claimed_by = NULL, metadata_claimed_at = NULL,
      conversion_claimed_by = NULL, conversion_claimed_at = NULL,
      updated_at = CURRENT_TIMESTAMP
    WHERE download_status = 'downloading' OR metadata_status = 'in_progress' OR conversion_status = 'in_progress'
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

const claimMetadataJob = db.transaction((workerId: string) => {
  const row = db.query(`
    UPDATE jobs SET 
      metadata_status = 'in_progress', 
      metadata_claimed_by = ?, 
      metadata_claimed_at = CURRENT_TIMESTAMP, 
      updated_at = CURRENT_TIMESTAMP 
    WHERE id = (
      SELECT id FROM jobs 
      WHERE download_status = 'downloaded' AND metadata_status = 'pending' 
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
// 2. CONFIG & TYPES
// ==========================================
interface Config {
  playlists: string[]; channels: string[]; channelPlaylists: string[];
  maxConcurrentDownloads: number; maxConcurrentConverts: number;
  maxDownloadWorkers: number; minDownloadWorkers: number;
  maxBandwidthKBps: number; autoscaleEnabled: boolean;
  useProxies: boolean; webshareApiKey: string; maxProxyCount: number; proxyFile: string;
  denoPath: string; validateCookiesOnStart: boolean; outputRoot: string; archiveFile: string; cookiesFile: string;
  deleteSourceAfterConvert: boolean; videoQuality: "highest" | "1080p" | "720p" | "480p" | "audio";
  downloadSubtitles: boolean; embedMetadata: boolean; writeInfoJson: boolean; writeDescription: boolean; writeThumbnail: boolean;
  archiveLiveStreams: boolean; verifyIntegrity: boolean; skipShorts: boolean; downloadShorts: boolean;
  maxRetryAttempts: number; maxFailures: number; maxFailuresPerVideo: number;
  minFreeSpaceGB: number; secondaryStoragePath: string;
  daemonMode: boolean; webPort: number; rssEnabled: boolean; rssPollIntervalMinutes: number; rescanIntervalHours: number;
}

const DEFAULT_CONFIG: Config = {
  playlists: [], channels: [], channelPlaylists: [],
  maxConcurrentDownloads: 3, maxConcurrentConverts: 2, maxDownloadWorkers: 5, minDownloadWorkers: 1,
  maxBandwidthKBps: 0, autoscaleEnabled: true, useProxies: true, webshareApiKey: "YOUR_NEW_API_KEY_HERE",
  maxProxyCount: 0, proxyFile: "proxies.txt", denoPath: "C:\\Users\\shahh\\.deno\\bin",
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
    return { ...DEFAULT_CONFIG, ...JSON.parse(raw) };
  } catch {
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
// 4. PROXY & COOKIE MANAGEMENT
// ==========================================
// (Simplified for brevity, assumes WebshareProxyManager and cookie logic from V2 is preserved here)
class WebshareProxyManager {
  private proxies: any[] = []; private currentIndex = 0; private apiKey = "";
  async init(apiKey: string) { this.apiKey = apiKey; if (!apiKey || apiKey.includes("YOUR_NEW")) return; await this.refreshProxies(); }
  async refreshProxies() {
    try {
      const res = await fetch("https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&page_size=25", { headers: { "Authorization": this.apiKey } });
      if (!res.ok) return;
      const data = await res.json();
      this.proxies = data.results.filter((p: any) => p.valid).map((p: any) => ({
        id: p.id, url: `http://${p.username}:${p.password}@${p.proxy_address}:${p.port}`, country: p.country_code, city: p.city_name, burnedUntil: 0
      }));
    } catch {}
  }
  getProxy() {
    if (this.proxies.length === 0) return null;
    const p = this.proxies[this.currentIndex];
    this.currentIndex = (this.currentIndex + 1) % this.proxies.length;
    return { url: p.url, geo: `${p.country}-${p.city}` };
  }
  getActiveCount() { return this.proxies.length; }
  getTotalCount() { return this.proxies.length; }
}
const proxyManager = new WebshareProxyManager();

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

  // Determine initial metadata status based on flags
  const metadataStatus = (wantSubs || wantThumb || wantDesc) ? 'pending' : 'not_needed';
  // Determine initial conversion status based on format
  const conversionStatus = (config.videoQuality === 'audio' || targetFormat !== 'mp4') ? 'pending' : 'not_needed';

  const insertStmt = db.prepare(`
    INSERT OR IGNORE INTO jobs 
    (id, url, title, output_directory, target_format, want_subtitles, want_thumbnail, want_description, folder, index, download_status, metadata_status, conversion_status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
  `);

  for (const item of items) {
    if (isVideoInDb(item.id)) continue;
    const index = getNextIndex(folder);
    insertStmt.run(item.id, `https://www.youtube.com/watch?v=${item.id}`, item.title, outputDir, targetFormat, wantSubs, wantThumb, wantDesc, folder, index, metadataStatus, conversionStatus);
  }
}

// ==========================================
// 6. WORKERS
// ==========================================
async function downloadWorker(id: number, config: Config) {
  const workerId = `dl-${id}`;
  while (!abortController.signal.aborted) {
    if (globalIsPaused) { await Bun.sleep(2000); continue; }
    
    const job = claimDownloadJob(workerId);
    if (!job) { await Bun.sleep(2000); continue; }

    try {
      updateWorkerLine(id, `⬇️ Starting... | ${job.title}`, config);
      const format = QUALITY_FORMATS[config.videoQuality] || QUALITY_FORMATS["1080p"];
      // Deterministic output path for resume capability
      const baseFilename = `${String(job.index).padStart(3, "0")} - ${sanitizeFileName(job.title)}`;
      const outTemplate = join(job.output_directory, `${baseFilename}.%(ext)s`);
      
      const args = [
        "yt-dlp", job.url, ...cookiesArgs(config), "--format", format,
        "--concurrent-fragments", "16", "-o", outTemplate,
        "--progress", "--progress-template", "download:PROGRESS:%(progress.percent).1f|%(progress.speed)f|%(progress.eta)f|%(progress.total_bytes)s|%(progress.downloaded_bytes)s",
        "--socket-timeout", "30", "--retries", "5",
        "--continue"  // Resume partial downloads
      ];

      // Note: Metadata (subtitles, thumbnail, description) is handled by separate metadata worker
      // Download worker only fetches the video/audio file

      const proxy = config.useProxies ? proxyManager.getProxy() : null;
      if (proxy) args.push("--proxy", proxy.url);

      const proc = Bun.spawn(args, { stdout: "pipe", stderr: "pipe" });
      let lastProgressUpdate = 0;
      
      // Stream stdout for real-time progress updates
      const reader = proc.stdout.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let finalFilePath = "";
      
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
      
      const [, code] = await Promise.all([new Response(proc.stderr).text(), proc.exited]);

      if (code === 0) {
        const filePath = finalFilePath || buffer.split("\n").reverse().find(l => l.trim() && existsSync(l.trim()))?.trim() || "";
        const fileSize = filePath ? (await stat(filePath)).size : 0;
        
        // Update partial_file_path for potential resume, mark as downloaded
        db.run(`UPDATE jobs SET download_status = 'downloaded', file_path = ?, file_size = ?, partial_file_path = ?, download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [filePath, fileSize, filePath, job.id]);
        updateWorkerLine(id, `✅ Downloaded | ${job.title}`, config);
      } else {
        throw new Error(buffer.split('\n').slice(-3).join(' '));
      }
    } catch (err: any) {
      const retryCount = job.retry_count + 1;
      const newStatus = retryCount >= config.maxRetryAttempts ? 'failed' : 'pending';
      db.run(`UPDATE jobs SET download_status = ?, retry_count = ?, last_error = ?, download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [newStatus, retryCount, String(err).slice(0, 500), job.id]);
      updateWorkerLine(id, `❌ Failed | ${job.title}`, config);
    } finally {
      autoscaler.clearWorker(id);
    }
  }
}

// 📝 METADATA WORKER: Handles subtitles, thumbnails, descriptions independently
async function metadataWorker(id: number, config: Config) {
  const workerId = `md-${id}`;
  while (!abortController.signal.aborted) {
    const job = claimMetadataJob(workerId);
    if (!job) { await Bun.sleep(2000); continue; }

    try {
      updateMetadataWorkerLine(id, `📝 Fetching metadata | ${job.title}`, config);
      const sourcePath = job.file_path;
      if (!sourcePath || !existsSync(sourcePath)) throw new Error("Source file missing for metadata extraction");

      const args = ["yt-dlp", job.url, ...cookiesArgs(config), "--skip-download"];

      // Subtitles
      if (job.want_subtitles) {
        args.push("--write-subs", "--write-auto-subs", "--sub-langs", "all.*", "--embed-subs");
      }
      // Thumbnail
      if (job.want_thumbnail) {
        args.push("--write-thumbnail", "--convert-thumbnails", "jpg");
      }
      // Description
      if (job.want_description) {
        args.push("--write-description");
      }
      // Embed metadata into the video file
      if (config.embedMetadata) {
        args.push("--embed-thumbnail", "--embed-metadata", "--embed-chapters");
      }

      // Output to same directory with same base name
      const baseFilename = `${String(job.index).padStart(3, "0")} - ${sanitizeFileName(job.title)}`;
      args.push("-o", join(job.output_directory, `${baseFilename}.%(ext)s`));

      const proxy = config.useProxies ? proxyManager.getProxy() : null;
      if (proxy) args.push("--proxy", proxy.url);

      const proc = Bun.spawn(args, { stdout: "pipe", stderr: "pipe" });
      const [, stderr, code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited]);

      if (code === 0) {
        db.run(`UPDATE jobs SET metadata_status = 'done', metadata_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [job.id]);
        updateMetadataWorkerLine(id, `✅ Metadata done | ${job.title}`, config);
      } else {
        throw new Error(stderr.split('\n').slice(-3).join(' '));
      }
    } catch (err: any) {
      db.run(`UPDATE jobs SET metadata_status = 'failed', last_error = ?, metadata_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [String(err).slice(0, 500), job.id]);
      updateMetadataWorkerLine(id, `❌ Metadata failed | ${job.title}`, config);
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

      // Integrity check
      let integrity = null;
      if (config.verifyIntegrity) {
        const hash = createHash("sha256").update(await readFile(finalPath)).digest("hex");
        integrity = hash;
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

function updateMetadataWorkerLine(id: number, text: string, config: Config) {
  workerStatuses.set(`MD${id}`, text);
  updateAbsoluteLine(2 + config.maxDownloadWorkers + id, `[MD${id}] ${text}`);
}

function updateConvertWorkerLine(id: number, text: string, config: Config) {
  workerStatuses.set(`CV${id}`, text);
  updateAbsoluteLine(2 + config.maxDownloadWorkers + id, `[CV${id}] ${text}`);
}

function renderDashboard() {
  if (!isTTY) return;
  const agg = formatBytesPerSec(autoscaler.getAggregateSpeed());
  const cap = autoscaler.maxBandwidthKBps > 0 ? `/${formatBytesPerSec(autoscaler.maxBandwidthKBps * 1024)}` : "";
  const proxyTotal = proxyManager.getTotalCount();
  const proxyStr = proxyTotal > 0 ? `| 🌐 ${proxyManager.getActiveCount()}/${proxyTotal} IPs` : "";
  updateAbsoluteLine(1, `🚀 DL:${aliveDownloadWorkers.size}/${autoscaler.targetWorkers} | ${agg}${cap} | Done:${stats.downloaded} Skip:${stats.skipped} Fail:${stats.failed} Tot:${stats.totalQueued}${proxyStr}`);
  const plStrs = Array.from(playlistStates.entries()).map(([n, s]) => `${n}: ${s.downloaded + s.skipped}/${s.total}`);
  updateAbsoluteLine(2, `📂 ${plStrs.join(' | ')}`);
}

function resetTerminal() {
  if (!isTTY) return;
  const rows = process.stdout.rows || 50;
  process.stdout.write(`\x1b[1;${rows}r\x1b[${rows};1H`);
}

// (Web UI HTML omitted for brevity, use the exact HTML string from your V2 file)
const WEB_UI_HTML = `<!DOCTYPE html><html><body><h1>Archival Engine V3</h1><p>Check /api/status</p></body></html>`;

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
          workers, proxyHealth: { active: proxyManager.getActiveCount(), total: proxyManager.getTotalCount() }
        });
      }
      
      if (url.pathname === "/api/pause" && req.method === "POST") { globalIsPaused = true; pauseReason = "MANUAL_WEB_UI"; return Response.json({ success: true }); }
      if (url.pathname === "/api/resume" && req.method === "POST") { globalIsPaused = false; pauseReason = null; return Response.json({ success: true }); }
      
      if (url.pathname === "/api/failed" && req.method === "GET") {
        const rows = db.query("SELECT id, title, folder, retry_count, last_error FROM jobs WHERE download_status = 'failed' OR conversion_status = 'failed' LIMIT 100").all();
        return Response.json({ ok: true, failed: rows.map((r: any) => ({ ...r, fail_count: r.retry_count })) });
      }

      if (url.pathname.startsWith("/api/retry/") && req.method === "POST") {
        const id = decodeURIComponent(url.pathname.replace("/api/retry/", ""));
        db.run("UPDATE jobs SET download_status = 'pending', conversion_status = 'pending', retry_count = 0, last_error = NULL WHERE id = ?", [id]);
        return Response.json({ ok: true });
      }

      return new Response("Not Found", { status: 404 });
    }
  });
}

// ==========================================
// 8. MAIN EXECUTION
// ==========================================
async function handleShutdown(sig: string) {
  console.log(`\n🛑 ${sig} received. Pausing active jobs...`);
  // Graceful shutdown: mark claimed jobs as paused so they can resume
  db.run(`UPDATE jobs SET download_status = 'paused', download_claimed_by = NULL, download_claimed_at = NULL WHERE download_status = 'downloading'`);
  db.run(`UPDATE jobs SET metadata_status = 'pending', metadata_claimed_by = NULL, metadata_claimed_at = NULL WHERE metadata_status = 'in_progress'`);
  db.run(`UPDATE jobs SET conversion_status = 'pending', conversion_claimed_by = NULL, conversion_claimed_at = NULL WHERE conversion_status = 'in_progress'`);
  abortController.abort();
  webServer?.stop(true);
  process.exit(0);
}

async function main() {
  process.on("SIGINT", () => handleShutdown("SIGINT"));
  process.on("SIGTERM", () => handleShutdown("SIGTERM"));

  globalConfig = await loadConfig();
  initDatabase();
  reconcileCrashedJobs(); // 🌟 Safety net for crashes (handles ungraceful exits)
  
  await proxyManager.init(globalConfig.webshareApiKey);
  autoscaler.init(globalConfig);

  // Ingest links from config
  for (const url of globalConfig.playlists) await scanAndIngest(url, globalConfig);
  for (const url of globalConfig.channels) await scanAndIngest(url, globalConfig);

  webServer = startWebServer(globalConfig.webPort);
  console.log(`🌐 Web UI: http://127.0.0.1:${globalConfig.webPort}`);

  // Start Workers - Download, Metadata, and Converter run independently
  for (let i = 1; i <= globalConfig.maxConcurrentDownloads; i++) {
    aliveDownloadWorkers.add(i);
    downloadWorker(i, globalConfig).finally(() => aliveDownloadWorkers.delete(i));
  }
  // Start metadata workers (same count as download workers for parallel processing)
  for (let i = 1; i <= globalConfig.maxConcurrentDownloads; i++) {
    metadataWorker(i, globalConfig);
  }
  for (let i = 1; i <= globalConfig.maxConcurrentConverts; i++) {
    converterWorker(i, globalConfig);
  }

  console.log("🚀 Engine started. Press Ctrl+C to stop.");
}

main().catch(console.error);
