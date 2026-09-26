// update_config.ts — interactive configuration manager for the engine.
//
// The Zod schema and defaults are imported from src/config.ts so this tool and
// the engine can never disagree about what a valid config looks like.
//
// Run with: bun run update_config.ts   (or: bun run config)

import { createInterface } from "node:readline/promises";
import { CONFIG_PATH, loadConfigSafe, saveConfig, type Config } from "./src/config";

const rl = createInterface({ input: process.stdin, output: process.stdout });

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
    if (isNaN(n)) {
      console.log("   ⚠️ Please enter a valid number.");
      continue;
    }
    if (min !== undefined && n < min) {
      console.log(`   ⚠️ Must be >= ${min}.`);
      continue;
    }
    if (max !== undefined && n > max) {
      console.log(`   ⚠️ Must be <= ${max}.`);
      continue;
    }
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
    links.push(...line.split(/[,\n]+/).map((s) => s.trim()).filter((s) => s.length > 0));
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
        if (!list.includes(link)) {
          list.push(link);
          added++;
        }
      }
      console.log(`\n✅ Added ${added} item(s).`);
      await ask("Press Enter to continue...");
    } else if (choice === "r") {
      if (list.length === 0) {
        await ask("Nothing to remove. Press Enter...");
        continue;
      }
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
  config.maxMetadataWorkers = await askNumber("Metadata workers (subs/thumbs/descriptions)", config.maxMetadataWorkers, 1, 10);

  // Autoscaling
  console.log("\n— Dynamic Autoscaling —");
  config.autoscaleEnabled = await askYesNo("Enable dynamic worker autoscaling?", config.autoscaleEnabled);
  if (config.autoscaleEnabled) {
    config.maxDownloadWorkers = await askNumber("Max download workers (ceiling)", config.maxDownloadWorkers, 1, 20);
    config.minDownloadWorkers = await askNumber(
      "Min download workers (floor)",
      config.minDownloadWorkers,
      1,
      config.maxDownloadWorkers,
    );
  }

  // Bandwidth
  console.log("\n— Bandwidth Throttle —");
  config.maxBandwidthKBps = await askNumber(
    "Max bandwidth in KB/s (0 = unlimited, enforced via yt-dlp --limit-rate)",
    config.maxBandwidthKBps,
    0,
  );
  if (config.maxBandwidthKBps > 0) {
    console.log(`   ≈ ${(config.maxBandwidthKBps / 1024).toFixed(2)} MB/s global cap`);
  }

  // Failure handling (circuit breaker + backoff)
  console.log("\n— Failure Handling —");
  config.maxRetryAttempts = await askNumber("Retries per video before it fails", config.maxRetryAttempts, 1);
  config.maxFailuresPerVideo = await askNumber(
    "Max failures per video (effective cap = min with retries)",
    config.maxFailuresPerVideo,
    1,
  );
  config.maxFailures = await askNumber(
    "Consecutive failures before the engine auto-pauses (circuit breaker)",
    config.maxFailures,
    1,
  );
  config.retryBackoffBaseSeconds = await askNumber(
    "Retry backoff base delay (seconds, doubles each retry)",
    config.retryBackoffBaseSeconds,
    1,
  );
  config.retryBackoffMaxSeconds = await askNumber(
    "Retry backoff maximum delay (seconds)",
    config.retryBackoffMaxSeconds,
    config.retryBackoffBaseSeconds,
  );

  // Download archive (yt-dlp idempotence)
  const archAns = await ask(`   yt-dlp download-archive file [current: ${config.archiveFile}]: `);
  if (archAns.trim() !== "") config.archiveFile = archAns.trim();

  // Storage Management
  console.log("\n— Storage Management —");
  config.minFreeSpaceGB = await askNumber("Min free space (GB) before pausing", config.minFreeSpaceGB, 1);
  const nasPath = await ask(`   Secondary NAS/Storage path [current: ${config.secondaryStoragePath || "none"}]: `);
  if (nasPath.trim() !== "") config.secondaryStoragePath = nasPath.trim();

  // Web UI & Daemon
  console.log("\n— Web UI & Headless Daemon —");
  config.daemonMode = await askYesNo("Headless daemon mode (run forever, add links later from Web UI)?", config.daemonMode);
  config.webPort = await askNumber("Web UI port", config.webPort, 1, 65535);
  const bindAns = await ask(
    `   Web UI bind address (127.0.0.1 = this PC only, 0.0.0.0 = reachable from LAN) [current: ${config.webBind}]: `,
  );
  if (bindAns.trim() !== "") config.webBind = bindAns.trim();
  const tokenAns = await ask(
    `   Web UI access token (blank keeps current, '-' clears it) [current: ${config.webToken ? "(set)" : "none"}]: `,
  );
  if (tokenAns.trim() === "-") config.webToken = "";
  else if (tokenAns.trim() !== "") config.webToken = tokenAns.trim();
  if (config.webToken && config.webBind === "0.0.0.0") {
    console.log("   ℹ️ UI reachable from the LAN with a token set — good.");
  } else if (config.webBind === "0.0.0.0") {
    console.log("   ⚠️ 0.0.0.0 without a token: anyone on your network can pause/purge jobs.");
  }

  // Channel watching (RSS + rescans)
  console.log("\n— Channel Watching —");
  config.rssEnabled = await askYesNo("Watch channel RSS feeds for new uploads (cheap, ~15 min latency)?", config.rssEnabled);
  if (config.rssEnabled) {
    config.rssPollIntervalMinutes = await askNumber("RSS poll interval (minutes)", config.rssPollIntervalMinutes, 1);
  }
  config.rescanIntervalHours = await askNumber("Full rescan interval (hours, 0 = never)", config.rescanIntervalHours, 0, 24 * 30);

  console.log("\n— External Tools (Windows-friendly auto-detect) —");
  const ytdlpAns = await ask(
    `   yt-dlp path (blank = auto-detect: PATH / app folder / winget / scoop / choco) [current: ${config.ytDlpPath || "auto"}]: `,
  );
  if (ytdlpAns.trim()) config.ytDlpPath = ytdlpAns.trim();
  const ffAns = await ask(`   ffmpeg path (blank = auto-detect) [current: ${config.ffmpegPath || "auto"}]: `);
  if (ffAns.trim()) config.ffmpegPath = ffAns.trim();

  await ask("\n✅ Settings updated. Press Enter to return...");
  return config;
}

// ---- Reliability & Resume Settings ------------------------------------------
async function changeReliabilitySettings(config: Config): Promise<Config> {
  console.clear();
  console.log("🛡️  Reliability & Resume Settings\n");

  console.log("These control what happens when a download is interrupted,\n");
  console.log("fails, or a file disappears. Defaults are sensible for most setups.\n");

  console.log("— Resume —");
  config.maxResumeAttempts = await askNumber(
    "Max resume attempts per video before restarting it from scratch",
    config.maxResumeAttempts,
    0,
  );
  console.log(
    `   Interrupted downloads keep their .part file and continue with yt-dlp --continue;\n   after ${config.maxResumeAttempts} failed resumes the partial is discarded and the video restarts.`,
  );

  console.log("\n— Watchdogs —");
  config.downloadTimeoutMinutes = await askNumber(
    "Minimum download timeout (minutes) — scales up with video length automatically",
    config.downloadTimeoutMinutes,
    1,
  );
  config.maxDownloadMinutes = await askNumber(
    "Maximum download timeout (minutes) for very long videos",
    config.maxDownloadMinutes,
    config.downloadTimeoutMinutes,
  );

  console.log("\n— Failed-job sweep —");
  config.requeueFailedAfterMinutes = await askNumber(
    "Re-queue transiently failed jobs after (minutes; 0 = never)",
    config.requeueFailedAfterMinutes,
    0,
  );
  console.log("   Permanent failures (private / removed / age-gated videos) are never re-queued.");

  console.log("\n— Startup checks —");
  config.verifyExistingFiles = await askYesNo(
    "On startup, verify downloaded files still exist and re-queue missing ones?",
    config.verifyExistingFiles,
  );

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
  let config = await loadConfigSafe(CONFIG_PATH);

  while (true) {
    console.clear();
    console.log("=======================================");
    console.log("  🎬 Archiver Config Manager          ");
    console.log("=======================================\n");
    console.log("  1. Manage Standard Playlists");
    console.log("  2. Manage Channels (all uploads)");
    console.log("  3. Manage Channel Playlists");
    console.log("  4. Change Download Settings (Quality, Concurrency, Autoscale, Storage, Web UI)");
    console.log("  5. Change Feature Toggles (Subs, Metadata, Shorts, Integrity)");
    console.log("  6. Change Reliability & Resume Settings");
    console.log("  7. View Current Configuration");
    console.log("  8. Save and Exit");
    console.log("  9. Exit without Saving\n");

    const choice = (await ask("Select (1-9): ")).trim();

    switch (choice) {
      case "1":
        config = await manageUrlList(config, "playlists", "Playlists");
        break;
      case "2":
        config = await manageUrlList(config, "channels", "Channels");
        break;
      case "3":
        config = await manageUrlList(config, "channelPlaylists", "Channel Playlists");
        break;
      case "4":
        config = await changeDownloadSettings(config);
        break;
      case "5":
        config = await changeFeatureToggles(config);
        break;
      case "6":
        config = await changeReliabilitySettings(config);
        break;
      case "7":
        await viewConfig(config);
        break;
      case "8":
        try {
          await saveConfig(config, CONFIG_PATH);
          console.log("\n✅ Configuration saved to " + CONFIG_PATH);
        } catch (e: any) {
          console.error("\n❌ Failed to save config:", e.message);
        }
        rl.close();
        return;
      case "9":
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
