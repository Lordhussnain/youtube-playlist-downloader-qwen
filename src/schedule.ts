// src/schedule.ts — download scheduling windows (plan 5.6).
//
// `downloadWindows` is a list of local-time ranges ("HH:MM-HH:MM"). An empty
// list means "always". Outside every window the engine pauses itself with a
// SCHEDULE_WINDOW reason (same mechanism as low disk / no network: active
// downloads get SIGINT and keep their .part), and resumes on its own when a
// window opens — but only if *it* was the one that paused. A pause with any
// other reason (cookies, disk, a human on the dashboard) is never touched.
//
// The pure parts (`parseWindow`, `isWithinWindows`, `nextWindowOpen`) take a
// Date so tests can pin the clock; `scheduleTick` is the only impure entry.

import type { Config } from "./config";
import { getPauseReason, isPaused } from "./state";
import { triggerPause, triggerResume } from "./resilience";

export const SCHEDULE_PAUSE_PREFIX = "SCHEDULE_WINDOW";

export interface Window {
  /** Minutes since local midnight, inclusive. */
  start: number;
  /** Minutes since local midnight, exclusive. end <= start means it wraps past midnight. */
  end: number;
}

const WINDOW_RE = /^([01]\d|2[0-3]):([0-5]\d)-([01]\d|2[0-3]):([0-5]\d)$/;

/** "22:00-07:00" → {start: 1320, end: 420}; null when malformed. */
export function parseWindow(spec: string): Window | null {
  const m = WINDOW_RE.exec(spec.trim());
  if (!m) return null;
  return { start: Number(m[1]) * 60 + Number(m[2]), end: Number(m[3]) * 60 + Number(m[4]) };
}

export function parseWindows(specs: readonly string[]): Window[] {
  return specs.map(parseWindow).filter((w): w is Window => w !== null);
}

function minuteOfDay(d: Date): number {
  return d.getHours() * 60 + d.getMinutes();
}

function inWindow(w: Window, minute: number): boolean {
  if (w.start === w.end) return true; // "09:00-09:00" = the whole day
  return w.start < w.end ? minute >= w.start && minute < w.end : minute >= w.start || minute < w.end;
}

/** True when no windows are configured, or `now` falls inside at least one. */
export function isWithinWindows(windows: readonly Window[], now: Date = new Date()): boolean {
  if (windows.length === 0) return true;
  const minute = minuteOfDay(now);
  return windows.some((w) => inWindow(w, minute));
}

/** "HH:MM" of the soonest window start after `now` (for the pause reason). */
export function nextWindowOpen(windows: readonly Window[], now: Date = new Date()): string | null {
  if (windows.length === 0) return null;
  const minute = minuteOfDay(now);
  let best = Infinity;
  for (const w of windows) {
    const delta = (w.start - minute + 1440) % 1440 || 1440;
    if (delta < best) best = delta;
  }
  const at = (minute + best) % 1440;
  return `${String(Math.floor(at / 60)).padStart(2, "0")}:${String(at % 60).padStart(2, "0")}`;
}

export type ScheduleAction = "pause" | "resume" | "none";

/**
 * Decide what this tick should do. Pure: given the config windows, the clock
 * and the current pause state. Exported so the policy is table-testable.
 */
export function decideSchedule(
  windows: readonly Window[],
  now: Date,
  state: { paused: boolean; reason: string | null },
): ScheduleAction {
  const open = isWithinWindows(windows, now);
  const ours = state.paused && (state.reason || "").startsWith(SCHEDULE_PAUSE_PREFIX);
  if (!open && !state.paused) return "pause";
  if (open && ours) return "resume";
  return "none";
}

/** Called from the engine every 30 s. Returns the action taken (for logs/tests). */
export function scheduleTick(config: Pick<Config, "downloadWindows">, now: Date = new Date()): ScheduleAction {
  const windows = parseWindows(config.downloadWindows || []);
  const action = decideSchedule(windows, now, { paused: isPaused(), reason: getPauseReason() });
  if (action === "pause") {
    const next = nextWindowOpen(windows, now);
    triggerPause(`${SCHEDULE_PAUSE_PREFIX} (outside ${config.downloadWindows.join(", ")}; resumes at ${next})`);
  } else if (action === "resume") {
    console.log("🕒 Download window open — resuming.");
    triggerResume();
  }
  return action;
}
