// src/config.ts — single source of truth for configuration.
//
// The Zod schema, defaults, and load/save helpers live here so the engine
// (batch_playlist_downloader.ts) and the interactive config manager
// (update_config.ts) can never drift apart again.

import { readFile, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { z } from "zod";

export const CONFIG_PATH = "./config.json";

export const ConfigSchema = z
  .object({
    // --- Sources -------------------------------------------------------------
    playlists: z.array(z.string()),
    channels: z.array(z.string()),
    channelPlaylists: z.array(z.string()),
    // --- Concurrency ---------------------------------------------------------
    maxConcurrentDownloads: z.number().min(1).max(20),
    maxConcurrentConverts: z.number().min(1).max(10),
    maxDownloadWorkers: z.number().min(1).max(20),
    minDownloadWorkers: z.number().min(1).max(20),
    maxMetadataWorkers: z.number().min(1).max(10),
    maxBandwidthKBps: z.number().min(0),
    autoscaleEnabled: z.boolean(),
    // --- External tools ------------------------------------------------------
    ytDlpPath: z.string(),
    ffmpegPath: z.string(),
    validateCookiesOnStart: z.boolean(),
    // --- Output --------------------------------------------------------------
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
    // --- Failure handling ----------------------------------------------------
    maxRetryAttempts: z.number().min(1),
    maxFailures: z.number().min(1),
    maxFailuresPerVideo: z.number().min(1),
    // --- Reliability & resume -------------------------------------------------
    // Retry backoff (exponential, with jitter) applied to transient failures.
    retryBackoffBaseSeconds: z.number().min(1).max(3600),
    retryBackoffMaxSeconds: z.number().min(1).max(86_400),
    // How many times a single video may resume from its .part file before the
    // engine throws the partial away and restarts that download from scratch.
    maxResumeAttempts: z.number().min(0).max(100),
    // Auto-requeue of failed jobs after a cooldown (0 disables the sweep).
    // Permanent errors (private/removed/age-gated videos) are never requeued.
    requeueFailedAfterMinutes: z.number().min(0).max(20_160),
    // On startup, verify that files recorded as downloaded still exist; missing
    // ones are scrubbed from the yt-dlp archive and queued again.
    verifyExistingFiles: z.boolean(),
    // Download watchdog: minimum per-video timeout, and the ceiling used for
    // very long videos (timeout scales with the real duration in between).
    downloadTimeoutMinutes: z.number().min(1).max(240),
    maxDownloadMinutes: z.number().min(1).max(2880),
    // --- Storage -------------------------------------------------------------
    minFreeSpaceGB: z.number().min(1),
    secondaryStoragePath: z.string(),
    // --- Web UI / daemon -----------------------------------------------------
    daemonMode: z.boolean(),
    webPort: z.number().min(1).max(65535),
    webBind: z.string(),
    webToken: z.string(),
    // --- Channel watching ----------------------------------------------------
    rssEnabled: z.boolean(),
    rssPollIntervalMinutes: z.number().min(1),
    rescanIntervalHours: z.number().min(0),
  })
  // Cross-field sanity: the backoff ceiling must be reachable from the base.
  .refine((c) => c.retryBackoffMaxSeconds >= c.retryBackoffBaseSeconds, {
    message: "retryBackoffMaxSeconds must be >= retryBackoffBaseSeconds",
    path: ["retryBackoffMaxSeconds"],
  })
  .refine((c) => c.maxDownloadMinutes >= c.downloadTimeoutMinutes, {
    message: "maxDownloadMinutes must be >= downloadTimeoutMinutes",
    path: ["maxDownloadMinutes"],
  })
  .refine((c) => c.minDownloadWorkers <= c.maxDownloadWorkers, {
    message: "minDownloadWorkers must be <= maxDownloadWorkers",
    path: ["minDownloadWorkers"],
  });

export type Config = z.infer<typeof ConfigSchema>;

export const DEFAULT_CONFIG: Config = {
  playlists: [],
  channels: [],
  channelPlaylists: [],
  maxConcurrentDownloads: 3,
  maxConcurrentConverts: 2,
  maxDownloadWorkers: 5,
  minDownloadWorkers: 1,
  maxMetadataWorkers: 2,
  maxBandwidthKBps: 0,
  autoscaleEnabled: true,
  ytDlpPath: "",
  ffmpegPath: "",
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
  // Reliability & resume
  retryBackoffBaseSeconds: 30,
  retryBackoffMaxSeconds: 900,
  maxResumeAttempts: 5,
  requeueFailedAfterMinutes: 30,
  verifyExistingFiles: true,
  downloadTimeoutMinutes: 15,
  maxDownloadMinutes: 180,
  // Storage
  minFreeSpaceGB: 10,
  secondaryStoragePath: "",
  // Web UI / daemon
  daemonMode: false,
  webPort: 3000,
  webBind: "127.0.0.1",
  webToken: "",
  // Channel watching
  rssEnabled: true,
  rssPollIntervalMinutes: 15,
  rescanIntervalHours: 24,
};

// yt-dlp format selectors per quality preset.
export const QUALITY_FORMATS: Record<string, string> = {
  highest: "bv+ba/b",
  "1080p": "bv[height<=1080]+ba/b[height<=1080]",
  "720p": "bv[height<=720]+ba/b[height<=720]",
  "480p": "bv[height<=480]+ba/b[height<=480]",
  audio: "ba/bestaudio",
};

/** Merge raw JSON over the defaults and validate the result. */
export function parseConfig(raw: unknown): Config {
  const parsed = (raw ?? {}) as Record<string, unknown>;
  const merged = { ...DEFAULT_CONFIG, ...parsed };
  return ConfigSchema.parse(merged);
}

/**
 * Engine loader: a missing config.json is created from defaults; a broken or
 * invalid one is fatal (exit) so misconfiguration is never silently ignored.
 */
export async function loadConfig(configPath: string = CONFIG_PATH): Promise<Config> {
  let raw: string;
  try {
    raw = await readFile(configPath, "utf-8");
  } catch (err: any) {
    if (err?.code === "ENOENT") {
      console.log("⚠️ config.json not found. Creating default...");
      await writeFile(configPath, JSON.stringify(DEFAULT_CONFIG, null, 2));
      return { ...DEFAULT_CONFIG };
    }
    console.error("❌ Failed to read config.json:", err?.message || err);
    process.exit(1);
  }
  try {
    return parseConfig(JSON.parse(raw));
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

/**
 * Config-manager loader: never exits, never throws — falls back to defaults so
 * the interactive tool stays usable with a corrupted file.
 */
export async function loadConfigSafe(configPath: string = CONFIG_PATH): Promise<Config> {
  if (!existsSync(configPath)) return { ...DEFAULT_CONFIG };
  try {
    return parseConfig(JSON.parse(await readFile(configPath, "utf-8")));
  } catch (err: any) {
    if (err?.name === "ZodError") {
      console.error("❌ config.json is invalid or corrupted:", JSON.stringify(err.issues ?? [], null, 2));
    } else {
      console.error("❌ Failed to parse config.json:", err?.message || err);
    }
    console.log("Falling back to default configuration.");
    return { ...DEFAULT_CONFIG };
  }
}

/** Validate and persist a config object. */
export async function saveConfig(config: Config, configPath: string = CONFIG_PATH): Promise<void> {
  const validated = ConfigSchema.parse(config);
  await writeFile(configPath, JSON.stringify(validated, null, 2));
}
