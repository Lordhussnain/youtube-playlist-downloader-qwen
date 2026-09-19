// batch_playlist_downloader.ts (V3 - Central DB Pipeline)
// Run with: bun run batch_playlist_downloader.ts
import { mkdir, unlink, readFile, writeFile, statfs, rm, stat, appendFile, readdir, cp, rename } from "node:fs/promises";
import { existsSync, statSync, readFileSync, writeFileSync, unlinkSync } from "node:fs";
import { join, basename, resolve } from "node:path";
import { networkInterfaces } from "node:os";
import { createHash } from "node:crypto";
import { Database } from "bun:sqlite";

// ==========================================
// 1. DATABASE & SCHEMA
// ==========================================
let db: Database;

interface Job {
  id: string; url: string; title: string; output_directory: string; target_format: string;
  want_subtitles: number; want_thumbnail: number; want_description: number; want_info_json: number;
  download_status: string; metadata_status: string; conversion_status: string;
  download_claimed_by: string | null; conversion_claimed_by: string | null;
  partial_file_path: string | null; retry_count: number; last_error: string | null;
  folder: string; index: number; file_path: string | null; file_size: number; integrity: string | null;
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
      want_info_json INTEGER DEFAULT 1,
      
      download_status TEXT DEFAULT 'pending',   -- pending, downloading, paused, downloaded, failed
      metadata_status TEXT DEFAULT 'done',      -- done, not_needed (handled by download worker)
      conversion_status TEXT DEFAULT 'pending', -- pending, in_progress, done, failed, not_needed
      
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
      playlist_count INTEGER DEFAULT 1,
      
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // Keep auxiliary tables for RSS, Playlist Indexing, and History
  db.run(`CREATE TABLE IF NOT EXISTS playlist_state (folder TEXT PRIMARY KEY, next_index INTEGER NOT NULL DEFAULT 0)`);
  db.run(`CREATE TABLE IF NOT EXISTS rss_state (source TEXT PRIMARY KEY, channel_id TEXT, channel TEXT, last_poll TEXT, last_error TEXT)`);
  db.run(`CREATE TABLE IF NOT EXISTS discovered_playlists (channel_id TEXT NOT NULL, playlist_id TEXT NOT NULL, title TEXT, updated_at TEXT, PRIMARY KEY (channel_id, playlist_id))`);
  db.run(`CREATE TABLE IF NOT EXISTS run_history (id INTEGER PRIMARY KEY AUTOINCREMENT, started_at TEXT, ended_at TEXT, duration_seconds REAL, downloaded INTEGER, skipped INTEGER, failed INTEGER, total_queued INTEGER)`);
}

// 🛡️ CRASH RECOVERY: Reset interrupted jobs to paused/pending so they resume
function reconcileCrashedJobs() {
  const stmt = db.run(`
    UPDATE jobs SET 
      download_status = CASE WHEN download_status = 'downloading' THEN 'paused' ELSE download_status END,
      conversion_status = CASE WHEN conversion_status = 'in_progress' THEN 'pending' ELSE conversion_status END,
      download_claimed_by = NULL, conversion_claimed_by = NULL,
      updated_at = CURRENT_TIMESTAMP
    WHERE download_status = 'downloading' OR conversion_status = 'in_progress'
  `);
  if (stmt.changes > 0) console.log(`🔄 Reconciled ${stmt.changes} crashed job(s) back to paused/pending.`);
}

// 🌟 ATOMIC CLAIMING: Prevents two workers from grabbing the same job
const claimDownloadJob = db.transaction((workerId: string) => {
  const row = db.query(`SELECT id FROM jobs WHERE download_status IN ('pending', 'paused') ORDER BY created_at LIMIT 1`).get() as { id: string } | null;
  if (!row) return null;
  db.run(`UPDATE jobs SET download_status = 'downloading', download_claimed_by = ?, download_claimed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [workerId, row.id]);
  return db.query(`SELECT * FROM jobs WHERE id = ?`).get(row.id) as Job;
});

const claimConvertJob = db.transaction((workerId: string) => {
  const row = db.query(`SELECT id FROM jobs WHERE download_status = 'downloaded' AND conversion_status = 'pending' ORDER BY created_at LIMIT 1`).get() as { id: string } | null;
  if (!row) return null;
  db.run(`UPDATE jobs SET conversion_status = 'in_progress', conversion_claimed_by = ?, conversion_claimed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [workerId, row.id]);
  return db.query(`SELECT * FROM jobs WHERE id = ?`).get(row.id) as Job;
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
function sanitizeFolderName(name: string): string { return name.replace(/[\/:*?"<>|]/g, "_").trim() || "playlist"; }

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
  const wantInfo = config.writeInfoJson ? 1 : 0;

  const insertStmt = db.prepare(`
    INSERT OR IGNORE INTO jobs 
    (id, url, title, output_directory, target_format, want_subtitles, want_thumbnail, want_description, want_info_json, folder, index, download_status, conversion_status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 'pending')
  `);

  for (const item of items) {
    if (isVideoInDb(item.id)) continue;
    const index = getNextIndex(folder);
    insertStmt.run(item.id, `https://www.youtube.com/watch?v=${item.id}`, item.title, outputDir, targetFormat, wantSubs, wantThumb, wantDesc, wantInfo, folder, index);
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
      const outTemplate = join(job.output_directory, `${String(job.index).padStart(3, "0")} - %(title)s.%(ext)s`);
      
      const args = [
        "yt-dlp", job.url, ...cookiesArgs(config), "--format", format,
        "--concurrent-fragments", "16", "-o", outTemplate,
        "--progress", "--progress-template", "download:PROGRESS:%(progress.percent).1f|%(progress.speed)f|%(progress.eta)f|%(progress.total_bytes)s|%(progress.downloaded_bytes)s",
        "--socket-timeout", "30", "--retries", "5"
      ];

      // 🌟 FETCH METADATA NATIVELY (Saves a whole network pass!)
      if (job.want_subtitles) args.push("--write-subs", "--write-auto-subs", "--sub-langs", "all", "--embed-subs");
      if (job.want_thumbnail) args.push("--write-thumbnail", "--convert-thumbnails", "jpg");
      if (job.want_description) args.push("--write-description");
      if (job.want_info_json) args.push("--write-info-json");
      if (config.embedMetadata) args.push("--embed-thumbnail", "--embed-metadata", "--embed-chapters");

      const proxy = config.useProxies ? proxyManager.getProxy() : null;
      if (proxy) args.push("--proxy", proxy.url);

      const proc = Bun.spawn(args, { stdout: "pipe", stderr: "pipe" });
      const [stdout, stderr, code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited]);

      if (code === 0) {
        const filePath = stdout.split("\n").reverse().find(l => l && existsSync(l)) || "";
        const fileSize = filePath ? (await stat(filePath)).size : 0;
        
        db.run(`UPDATE jobs SET download_status = 'downloaded', metadata_status = 'done', file_path = ?, file_size = ?, download_claimed_by = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [filePath, fileSize, job.id]);
        updateWorkerLine(id, `✅ Downloaded | ${job.title}`, config);
      } else {
        throw new Error(stderr.split('\n').slice(-3).join(' '));
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
function updateWorkerLine(id: number, text: string, config: Config) {
  workerStatuses.set(`DL${id}`, text);
  if (isTTY) process.stdout.write(`\x1b[s\x1b[${2 + id};1H\x1b[2K[DL${id}] ${text}\x1b[u`);
}
function updateConvertWorkerLine(id: number, text: string, config: Config) {
  workerStatuses.set(`CV${id}`, text);
  if (isTTY) process.stdout.write(`\x1b[s\x1b[${2 + config.maxDownloadWorkers + id};1H\x1b[2K[CV${id}] ${text}\x1b[u`);
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
  db.run(`UPDATE jobs SET download_status = 'paused', download_claimed_by = NULL WHERE download_status = 'downloading'`);
  db.run(`UPDATE jobs SET conversion_status = 'pending', conversion_claimed_by = NULL WHERE conversion_status = 'in_progress'`);
  abortController.abort();
  webServer?.stop(true);
  process.exit(0);
}

async function main() {
  process.on("SIGINT", () => handleShutdown("SIGINT"));
  process.on("SIGTERM", () => handleShutdown("SIGTERM"));

  globalConfig = await loadConfig();
  initDatabase();
  reconcileCrashedJobs(); // 🌟 Safety net for crashes
  
  await proxyManager.init(globalConfig.webshareApiKey);
  autoscaler.init(globalConfig);

  // Ingest links from config
  for (const url of globalConfig.playlists) await scanAndIngest(url, globalConfig);
  for (const url of globalConfig.channels) await scanAndIngest(url, globalConfig);

  webServer = startWebServer(globalConfig.webPort);
  console.log(`🌐 Web UI: http://127.0.0.1:${globalConfig.webPort}`);

  // Start Workers
  for (let i = 1; i <= globalConfig.maxConcurrentDownloads; i++) {
    aliveDownloadWorkers.add(i);
    downloadWorker(i, globalConfig).finally(() => aliveDownloadWorkers.delete(i));
  }
  for (let i = 1; i <= globalConfig.maxConcurrentConverts; i++) {
    converterWorker(i, globalConfig);
  }

  console.log("🚀 Engine started. Press Ctrl+C to stop.");
}

main().catch(console.error);
