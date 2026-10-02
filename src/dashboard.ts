// src/dashboard.ts — terminal UI (TUI) dashboard.
//
// A fixed-height status block at the top of the terminal: one line of global
// stats plus one line per worker slot. Falls back to plain logs when stdout is
// not a TTY or the terminal is too small, so piping output still works.

import { autoscaler } from "./autoscale";
import { formatBytesPerSec } from "./util";
import { getStatsSnapshot } from "./stats";
import { getConfig, getPauseReason, isPaused, isTty, setTty, workerStatuses } from "./state";
import type { Config } from "./config";

// Terminal rows reserved for the dashboard: 2 header lines + one per slot.
export function dashboardLineCount(config: Config): number {
  return 2 + config.maxDownloadWorkers + config.maxConcurrentConverts + config.maxMetadataWorkers;
}

export function initDashboard(config: Config): void {
  if (!isTty()) return;
  const cols = process.stdout.columns || 80;
  const rows = process.stdout.rows || 50;
  const dashboardLines = dashboardLineCount(config);
  if (rows <= dashboardLines + 5 || cols < 60) {
    setTty(false);
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

export function updateAbsoluteLine(row: number, text: string): void {
  if (!isTty()) return;
  const cols = process.stdout.columns || 80;
  let safeText = text.length > cols - 1 ? text.slice(0, cols - 4) + "..." : text;
  safeText = safeText.padEnd(cols - 1, " ");
  // Save/restore the cursor so log scrolling above the block is untouched.
  process.stdout.write(`\x1b7\x1b[${row};1H\x1b[2K${safeText}\x1b8`);
}

export function updateWorkerLine(id: number, text: string, _config: Config): void {
  workerStatuses.set(`DL${id}`, text);
  updateAbsoluteLine(2 + id, `[DL${id}] ${text}`);
}

export function updateConvertWorkerLine(id: number, text: string, config: Config): void {
  workerStatuses.set(`CV${id}`, text);
  updateAbsoluteLine(2 + config.maxDownloadWorkers + id, `[CV${id}] ${text}`);
}

export function updateMetadataWorkerLine(id: number, text: string, config: Config): void {
  workerStatuses.set(`MD${id}`, text);
  updateAbsoluteLine(2 + config.maxDownloadWorkers + config.maxConcurrentConverts + id, `[MD${id}] ${text}`);
}

export function renderDashboard(): void {
  if (!isTty()) return;
  const agg = formatBytesPerSec(autoscaler.getAggregateSpeed());
  const cap = autoscaler.maxBandwidthKBps > 0 ? `/${formatBytesPerSec(autoscaler.maxBandwidthKBps * 1024)}` : "";
  // Shared memoised aggregate (stats.ts) — no private full-table scan here.
  const snap = getStatsSnapshot(getConfig());
  updateAbsoluteLine(
    1,
    formatHeaderLine(
      {
        downloading: snap.downloading,
        downloaded: snap.downloaded,
        failed: snap.failedDownloads,
        total: snap.total,
        resumable: snap.resumablePartials,
      },
      agg,
      cap,
    ),
  );
}

/** The aggregate row: global counters, bandwidth, and pause state. */
export function formatHeaderLine(
  stats: {
    downloading?: number;
    downloaded?: number;
    failed?: number;
    total?: number;
    resumable?: number;
  },
  speed: string,
  capSuffix: string,
): string {
  const reason = isPaused() ? ` | ⏸️ PAUSED${getPauseReason() ? ` (${getPauseReason()})` : ""}` : "";
  // Jobs still holding a .part they will resume from. Shown only when non-zero:
  // it is the terminal-side twin of the dashboard's "will resume" tile, and a
  // quiet engine has nothing to report here. Kept terse (`Res:n`, not
  // "n resumable") because the header already overflows an 80-column terminal
  // and the pause reason has to stay readable at the end of the line.
  const resumable = stats.resumable || 0;
  const resumeNote = resumable > 0 ? ` Res:${resumable}` : "";
  return (
    `🚀 DL:${stats.downloading || 0}/${autoscaler.targetWorkers} | ${speed}${capSuffix}` +
    ` | Done:${stats.downloaded || 0} Fail:${stats.failed || 0} Tot:${stats.total || 0}` +
    `${resumeNote}${reason}`
  );
}

export function resetTerminal(): void {
  if (!isTty()) return;
  const rows = process.stdout.rows || 50;
  process.stdout.write(`\x1b[1;${rows}r\x1b[${rows};1H`);
}
