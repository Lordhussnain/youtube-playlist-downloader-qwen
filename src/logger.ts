// src/logger.ts — append-only error log with size-based rotation.

import { appendFileSync, existsSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const MAX_BYTES = 1_000_000;

/**
 * The file `logError` appends to.
 *
 * Production: `error.log` next to the engine (cwd-relative, like archive.db
 * and config.json). Under `bun test` — which sets NODE_ENV=test — the path
 * moves into the OS temp dir instead. Unit tests drive logError directly via
 * the reconcile sweeps and retry-classification fixtures, and those recovery
 * messages ("reclaimed stale claims…", "gone999: file gone…") used to land in
 * the operator's real error.log when the suite ran from the repo root, reading
 * as engine faults. The dashboard's /api/logs endpoint reads through this
 * function too, so viewer and writer always agree.
 */
export function errorLogPath(): string {
  return process.env.NODE_ENV === "test" ? join(tmpdir(), "yta-test-error.log") : "error.log";
}

/**
 * One log record is exactly one line, and nothing in it came from a video
 * title or a stack trace verbatim:
 *   • newlines / CR / other control characters collapse to a space — a
 *     multi-line `err.stack` used to count as many "lines" for rotation's
 *     400-line budget, and a crafted title containing "\n[reaper] …" could
 *     forge a scoped record
 *   • `--cookies <path>` (yt-dlp argv echoed into messages), bearer tokens and
 *     `token=` query values are redacted
 *   • the record is capped so a runaway message cannot blow the file up
 */
export function sanitizeLogMessage(message: string): string {
  let m = String(message ?? "");
  m = m.replace(/[\u0000-\u001f\u007f\u2028\u2029]+/g, " ");
  m = m.replace(/(--cookies(?:-from-browser)?)(?:\s+|=)(?:"[^"]*"|'[^']*'|\S+)/gi, "$1 [redacted]");
  m = m.replace(/(bearer\s+)[a-z0-9._~+/=-]+/gi, "$1[redacted]");
  m = m.replace(/([?&](?:token|webToken|yta_token)=)[^&\s"']+/gi, "$1[redacted]");
  m = m.replace(/(yta_token=)[^;\s"']+/gi, "$1[redacted]");
  if (m.length > MAX_MESSAGE_CHARS) m = m.slice(0, MAX_MESSAGE_CHARS) + "…[truncated]";
  return m;
}

const MAX_MESSAGE_CHARS = 4000;

export function logError(scope: string, message: string): void {
  try {
    const path = errorLogPath();
    const safeScope = String(scope).replace(/[^\w.:-]+/g, "_").slice(0, 40) || "engine";
    appendFileSync(path, `[${new Date().toISOString()}] [${safeScope}] ${sanitizeLogMessage(message)}\n`);
    // Keep the log bounded: when it grows past ~1MB, keep only the last 400 lines.
    if (existsSync(path) && statSync(path).size > MAX_BYTES) {
      const lines = readFileSync(path, "utf-8").split("\n");
      writeFileSync(path, lines.slice(-400).join("\n"));
    }
  } catch {
    // Logging must never take the engine down.
  }
}
