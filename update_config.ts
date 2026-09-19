// update_config.ts (V3)
// Interactive configuration manager for the YouTube Archival Engine
// Run with: bun run update_config.ts

import { readFile, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { createInterface } from "node:readline/promises";

const CONFIG_PATH = "./config.json";
const rl = createInterface({ input: process.stdin, output: process.stdout });

// ---- Types & Defaults (MUST match batch_playlist_downloader.ts) ------------
interface Config {
  playlists: string[];
  channels: string[];
  channelPlaylists: string[];
  maxConcurrentDownloads: number;
  maxConcurrentConverts: number;
  maxDownloadWorkers: number;
  minDownloadWorkers: number;
  maxBandwidthKBps: number;
  autoscaleEnabled: boolean;
  useProxies: boolean;
  webshareApiKey: string;
  maxProxyCount: number;
  proxyFile: string;
  denoPath: string;
  validateCookiesOnStart: boolean;
  outputRoot: string;
  archiveFile: string;
  cookiesFile: string;
  deleteSourceAfterConvert: boolean;
  videoQuality: "highest" | "1080p" | "720p" | "480p" | "audio";
  downloadSubtitles: boolean;
  embedMetadata: boolean;
  writeInfoJson: boolean;
  writeDescription: boolean;
  writeThumbnail: boolean;
  archiveLiveStreams: boolean;
  verifyIntegrity: boolean;
  skipShorts: boolean;
  downloadShorts: boolean;
  maxRetryAttempts: number;
  maxFailures: number;
  maxFailuresPerVideo: number;
  minFreeSpaceGB: number;
  secondaryStoragePath: string;
  daemonMode: boolean;
  webPort: number;
  rssEnabled: boolean;
  rssPollIntervalMinutes: number;
  rescanIntervalHours: number;
}

const DEFAULT_CONFIG: Config = {
  playlists: [],
  channels: [],
  channelPlaylists: [],
  maxConcurrentDownloads: 3,
  maxConcurrentConverts: 2,
  maxDownloadWorkers: 5,
  minDownloadWorkers: 1,
  maxBandwidthKBps: 0,
  autoscaleEnabled: true,
  useProxies: true,
  webshareApiKey: "YOUR_NEW_API_KEY_HERE",
  maxProxyCount: 0,
  proxyFile: "proxies.txt",
  denoPath: "C:\\Users\\shahh\\.deno\\bin",
  validateCookiesOnStart: true,
  outputRoot: "./downloads",
  archiveFile: "downloaded_videos.txt",
  cookiesFile: "cookies.txt",
  deleteSourceAfterConvert: true,
  videoQuality: "1080p",
  downloadSubtitles: true,
  embedMetadata: true,
  writeInfoJson: true,
  writeDescription: true,
  writeThumbnail: true,
  archiveLiveStreams: false,
  verifyIntegrity: true,
  skipShorts: true,
  downloadShorts: false,
  maxRetryAttempts: 3,
  maxFailures: 10,
  maxFailuresPerVideo: 4,
  minFreeSpaceGB: 10,
  secondaryStoragePath: "",
  daemonMode: false,
  webPort: 3000,
  rssEnabled: true,
  rssPollIntervalMinutes: 15,
  rescanIntervalHours: 24,
};

// ---- Load / Save -----------------------------------------------------------
async function loadConfig(): Promise<Config> {
  if (!existsSync(CONFIG_PATH)) return { ...DEFAULT_CONFIG };
  try {
    return { ...DEFAULT_CONFIG, ...JSON.parse(await readFile(CONFIG_PATH, "utf-8")) };
  } catch {
    return { ...DEFAULT_CONFIG };
  }
}

async function saveConfig(config: Config) {
  await writeFile(CONFIG_PATH, JSON.stringify(config, null, 2));
}

// ---- Prompt Helpers --------------------------------------------------------
async function ask(q: string): Promise<string> {
  return rl.question(q);
}

async function askYesNo(q: string, def: boolean): Promise<boolean> {
  const ans = await ask(`${q} ${def ? "[Y/n]" : "[y/N]"}: `);
  const t = ans.trim().toLowerCase();
  if (t === "") return def;
  return t === "y" || t === "yes";
}

async function askNumber(q: string, current: number, min?: number, max?: number): Promise<number> {
  while (true) {
    const ans = await ask(`${q} [current: ${current}]: `);
    if (ans.trim() === "") return current;
    const n = parseInt(ans, 10);
    if (isNaN(n)) { console.log("   ⚠️ Please enter a valid number."); continue; }
    if (min !== undefined && n < min) { console.log(`   ⚠️ Must be >= ${min}.`); continue; }
    if (max !== undefined && n > max) { console.log(`   ⚠️ Must be <= ${max}.`); continue; }
    return n;
  }
}

async function askChoice(q: string, options: string[], current: string): Promise<string> {
  while (true) {
    console.log(`\n${q} [current: ${current}]`);
    options.forEach((o, i) => console.log(`  ${i + 1}. ${o}`));
    const ans = await ask("Select number (or Enter to keep): ");
    if (ans.trim() === "") return current;
    const idx = parseInt(ans, 10) - 1;
    if (idx >= 0 && idx < options.length) return options[idx];
    console.log("   ⚠️ Invalid selection.");
  }
}

async function askMultiLine(prompt: string): Promise<string[]> {
  console.log(`\n${prompt}`);
  console.log("(Paste one or more lines; press Enter on an empty line to finish)\n");
  const links: string[] = [];
  while (true) {
    const line = await ask(" > ");
    if (line.trim() === "") break;
    links.push(...line.split(/[,\n]+/).map(s => s.trim()).filter(s => s.length > 0));
  }
  return links;
}

function isValidPlaylistUrl(url: string): boolean {
  try {
    const u = new URL(url);
    return u.hostname.includes("youtube.com") && u.searchParams.has("list");
  } catch {
    return false;
  }
}

// ---- URL List Manager (Playlists / Channels / Channel Playlists) -----------
async function manageUrlList(
  config: Config,
  key: "playlists" | "channels" | "channelPlaylists",
  title: string,
): Promise<Config> {
  while (true) {
    console.clear();
    console.log(`📂 Manage ${title}\n`);
    const list = config[key] || [];
    if (list.length === 0) console.log("  (none configured)\n");
    else list.forEach((url, i) => console.log(`  ${i + 1}. ${url}`));
    
    console.log("\n  A. Add new");
    console.log("  R. Remove one");
    console.log("  C. Clear all");
    console.log("  B. Back\n");
    
    const choice = (await ask("Select: ")).trim().toLowerCase();
    
    if (choice === "a") {
      const newLinks = await askMultiLine(`Enter ${title} URLs/handles:`);
      let added = 0;
      for (const link of newLinks) {
        if (key === "playlists" && !isValidPlaylistUrl(link)) {
          console.log(`   ⚠️ Skipping invalid playlist URL: ${link}`);
          continue;
        }
        if (!list.includes(link)) { list.push(link); added++; }
      }
      console.log(`\n✅ Added ${added} item(s).`);
      await ask("Press Enter to continue...");
    } else if (choice === "r") {
      if (list.length === 0) { await ask("Nothing to remove. Press Enter..."); continue; }
      const ans = await ask("Enter number to remove (or 'all'): ");
      if (ans.trim().toLowerCase() === "all") {
        if (await askYesNo("Remove ALL?", false)) config[key] = [];
      } else {
        const i = parseInt(ans, 10) - 1;
        if (i >= 0 && i < list.length) list.splice(i, 1);
        else console.log("   ⚠️ Invalid number.");
      }
      await ask("Press Enter to continue...");
    } else if (choice === "c") {
      if (await askYesNo("Clear ALL?", false)) config[key] = [];
      await ask("Press Enter to continue...");
    } else if (choice === "b") {
      return config;
    }
  }
}

// ---- Download Settings -----------------------------------------------------
async function changeDownloadSettings(config: Config): Promise<Config> {
  console.clear();
  console.log("⚙️  Download Settings\n");
  
  // Quality
  config.videoQuality = (await askChoice(
    "Video Quality:",
    ["highest", "1080p", "720p", "480p", "audio"],
    config.videoQuality,
  )) as Config["videoQuality"];
  
  // Concurrency
  console.log("\n— Concurrency —");
  config.maxConcurrentDownloads = await askNumber("Starting download workers", config.maxConcurrentDownloads, 1, 20);
  config.maxConcurrentConverts = await askNumber("Concurrent conversion workers", config.maxConcurrentConverts, 1, 10);
  
  // Autoscaling
  console.log("\n— Dynamic Autoscaling —");
  config.autoscaleEnabled = await askYesNo("Enable dynamic worker autoscaling?", config.autoscaleEnabled);
  if (config.autoscaleEnabled) {
    config.maxDownloadWorkers = await askNumber("Max download workers (ceiling)", config.maxDownloadWorkers, 1, 20);
    config.minDownloadWorkers = await askNumber("Min download workers (floor)", config.minDownloadWorkers, 1, config.maxDownloadWorkers);
  }
  
  // Bandwidth
  console.log("\n— Bandwidth Throttle —");
  config.maxBandwidthKBps = await askNumber("Max bandwidth in KB/s (0 = unlimited)", config.maxBandwidthKBps, 0);
  if (config.maxBandwidthKBps > 0) {
    console.log(`   ≈ ${(config.maxBandwidthKBps / 1024).toFixed(2)} MB/s global cap`);
  }
  
  // Proxies
  console.log("\n— Proxies (Webshare API) —");
  config.useProxies = await askYesNo("Use Webshare API-driven proxies?", config.useProxies);
  if (config.useProxies) {
    console.log("   ℹ️  Get an API key at https://www.webshare.io (Proxy > API).");
    const apiKeyAns = await ask(`   Webshare API key [current: ${config.webshareApiKey}]: `);
    if (apiKeyAns.trim()) config.webshareApiKey = apiKeyAns.trim();
  }
  
  // Storage Management
  console.log("\n— Storage Management —");
  config.minFreeSpaceGB = await askNumber("Min free space (GB) before pausing", config.minFreeSpaceGB, 1);
  const nasPath = await ask(`   Secondary NAS/Storage path [current: ${config.secondaryStoragePath || 'none'}]: `);
  if (nasPath.trim() !== "") config.secondaryStoragePath = nasPath.trim();
  
  // Web UI & Daemon
  console.log("\n— Web UI & Headless Daemon —");
  config.daemonMode = await askYesNo("Headless daemon mode (run forever, add links later from the Web UI)?", config.daemonMode);
  config.webPort = await askNumber("Web UI port", config.webPort, 1, 65535);
  if (config.daemonMode) {
    config.rssEnabled = await askYesNo("Watch RSS feeds (subscribe to new uploads, cheap, ~15 min latency)?", config.rssEnabled);
    config.rssPollIntervalMinutes = await askNumber("RSS poll interval (minutes)", config.rssPollIntervalMinutes, 1);
    config.rescanIntervalHours = await askNumber("Full rescan interval (hours, 0 = never)", config.rescanIntervalHours, 0, 24 * 30);
  }
  
  // JS Runtime (yt-dlp signature extraction)
  console.log("\n— JS Runtime —");
  const denoAns = await ask(`   Deno path (directory or .exe) [current: ${config.denoPath}]: `);
  if (denoAns.trim()) config.denoPath = denoAns.trim();
  
  await ask("\n✅ Settings updated. Press Enter to return...");
  return config;
}

// ---- Feature Toggles -------------------------------------------------------
async function changeFeatureToggles(config: Config): Promise<Config> {
  console.clear();
  console.log("🎛️  Feature Toggles\n");
  
  config.downloadSubtitles = await askYesNo("Download & embed subtitles?", config.downloadSubtitles);
  config.embedMetadata = await askYesNo("Embed metadata, chapters & thumbnail?", config.embedMetadata);
  config.writeInfoJson = await askYesNo("Write .info.json sidecar files?", config.writeInfoJson);
  config.writeDescription = await askYesNo("Write .description sidecar files?", config.writeDescription);
  config.writeThumbnail = await askYesNo("Write .jpg thumbnail sidecar files?", config.writeThumbnail);
  config.verifyIntegrity = await askYesNo("Verify file integrity (SHA256) after download?", config.verifyIntegrity);
  config.skipShorts = await askYesNo("Skip YouTube Shorts (< 60s)?", config.skipShorts);
  config.archiveLiveStreams = await askYesNo("Archive Live Streams (wait for VOD)?", config.archiveLiveStreams);
  config.deleteSourceAfterConvert = await askYesNo("Delete source file after conversion?", config.deleteSourceAfterConvert);
  
  await ask("\n✅ Toggles updated. Press Enter to return...");
  return config;
}

// ---- View Config -----------------------------------------------------------
async function viewConfig(config: Config) {
  console.clear();
  console.log("📄 Current Configuration\n");
  console.log(JSON.stringify(config, null, 2));
  await ask("\nPress Enter to return to menu...");
}

// ---- Main Menu -------------------------------------------------------------
async function mainMenu() {
  let config = await loadConfig();
  
  while (true) {
    console.clear();
    console.log("=======================================");
    console.log("  🎬 Archiver V3 Config Manager ");
    console.log("=======================================\n");
    console.log("  1. Manage Standard Playlists");
    console.log("  2. Manage Channels (all uploads)");
    console.log("  3. Manage Channel Playlists");
    console.log("  4. Change Download Settings (Quality, Concurrency, Autoscale, Bandwidth, Proxies, Storage, Web UI/daemon)");
    console.log("  5. Change Feature Toggles (Subs, Metadata, Shorts, Integrity)");
    console.log("  6. View Current Configuration");
    console.log("  7. Save and Exit");
    console.log("  8. Exit without Saving\n");
    
    const choice = (await ask("Select (1-8): ")).trim();
    
    switch (choice) {
      case "1": config = await manageUrlList(config, "playlists", "Playlists"); break;
      case "2": config = await manageUrlList(config, "channels", "Channels"); break;
      case "3": config = await manageUrlList(config, "channelPlaylists", "Channel Playlists"); break;
      case "4": config = await changeDownloadSettings(config); break;
      case "5": config = await changeFeatureToggles(config); break;
      case "6": await viewConfig(config); break;
      case "7":
        await saveConfig(config);
        console.log("\n✅ Configuration saved to " + CONFIG_PATH);
        rl.close();
        return;
      case "8":
        console.log("\n👋 Exited without saving.");
        rl.close();
        return;
      default:
        console.log("   ⚠️ Invalid option.");
        await ask("Press Enter to try again...");
    }
  }
}

mainMenu();
