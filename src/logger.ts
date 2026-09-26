// src/logger.ts — append-only error log with size-based rotation.

import { appendFileSync, existsSync, readFileSync, statSync, writeFileSync } from "node:fs";

const LOG_FILE = "error.log";
const MAX_BYTES = 1_000_000;

export function logError(scope: string, message: string): void {
  try {
    appendFileSync(LOG_FILE, `[${new Date().toISOString()}] [${scope}] ${message}\n`);
    // Keep the log bounded: when it grows past ~1MB, keep only the last 400 lines.
    if (existsSync(LOG_FILE) && statSync(LOG_FILE).size > MAX_BYTES) {
      const lines = readFileSync(LOG_FILE, "utf-8").split("\n");
      writeFileSync(LOG_FILE, lines.slice(-400).join("\n"));
    }
  } catch {
    // Logging must never take the engine down.
  }
}
