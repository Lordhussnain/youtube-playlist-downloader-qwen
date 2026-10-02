// src/polling.ts — autonomous full rescans (daemon mode).
//
// The slow safety net behind the cheap RSS watcher: every rescanIntervalHours
// the engine re-lists every configured channel/playlist so videos that never
// appeared in an RSS feed (or were skipped as shorts and later re-evaluated)
// still get picked up. Ingest dedup makes rescans cheap to run often.

import { scanAndIngest } from "./scanner";
import { logError } from "./logger";
import { everyInterval, getConfig } from "./state";
import type { Config } from "./config";

/** One rescan pass over every configured channel/playlist. Never throws. */
export async function rescanAll(config: Config): Promise<{ scanned: number; failed: number }> {
  let scanned = 0;
  let failed = 0;
  for (const url of [...config.channels, ...config.channelPlaylists]) {
    try {
      await scanAndIngest(url, config);
      scanned++;
    } catch (e: any) {
      failed++;
      logError("rescan", `${url}: ${e?.message || e}`);
      console.error(`❌ Rescan failed for ${url}:`, e?.message || e);
    }
  }
  return { scanned, failed };
}

/**
 * Build the daemon tick. Exported for tests: the latch and the live-config
 * read are the behaviours worth pinning, and neither needs a real interval.
 *
 *   • in-flight latch — a rescan of many channels can outlast the interval
 *     (dead network, stuck yt-dlp); without the latch ticks pile up and run
 *     the same scans concurrently (rss.ts already guards this way)
 *   • live config — `rescanIntervalHours` / the channel lists are re-read on
 *     every tick (gotcha 16) so dashboard changes apply without a restart
 */
export function createRescanTick(
  readConfig: () => Config = getConfig,
  rescan: (config: Config) => Promise<unknown> = rescanAll,
): () => Promise<boolean> {
  let inFlight = false;
  return async () => {
    if (inFlight) return false;
    inFlight = true;
    try {
      const config = readConfig();
      console.log("🔄 [Daemon] Running full channel rescan...");
      await rescan(config);
      return true;
    } finally {
      inFlight = false;
    }
  };
}

export function startAutonomousPolling(config: Config): ReturnType<typeof setInterval> | null {
  if (!(config.rescanIntervalHours > 0 && (config.channels.length > 0 || config.channelPlaylists.length > 0))) {
    return null;
  }
  const intervalMs = config.rescanIntervalHours * 60 * 60 * 1000;
  console.log(`🤖 Daemon mode: full rescan every ${config.rescanIntervalHours}h.`);
  const tick = createRescanTick();
  return everyInterval(() => {
    // A change to rescanIntervalHours takes effect on the next tick: the
    // interval itself is fixed at startup, but a tick whose interval shrank
    // to 0 simply becomes a no-op rather than a surprise scan.
    if (getConfig().rescanIntervalHours <= 0) return;
    void tick();
  }, intervalMs);
}
