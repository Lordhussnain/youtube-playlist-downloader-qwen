// tests/schedule.test.ts — download scheduling windows (plan 5.6): the pure
// window parser/predicate, the tick policy table, and the impure tick's
// interaction with the shared pause state.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { ConfigSchema, DEFAULT_CONFIG } from "../src/config";
import { initDatabase } from "../src/db";
import { setNotifyTransport } from "../src/notify";
import { triggerPause, triggerResume } from "../src/resilience";
import {
  decideSchedule,
  isWithinWindows,
  nextWindowOpen,
  parseWindow,
  parseWindows,
  SCHEDULE_PAUSE_PREFIX,
  scheduleTick,
} from "../src/schedule";
import { getPauseReason, isPaused, setConfig } from "../src/state";

/** A local-time Date at HH:MM today. */
const at = (hhmm: string): Date => {
  const [h, m] = hhmm.split(":").map(Number) as [number, number];
  const d = new Date();
  d.setHours(h, m, 0, 0);
  return d;
};

describe("parseWindow", () => {
  test("accepts HH:MM-HH:MM, rejects everything else", () => {
    expect(parseWindow("22:00-07:00")).toEqual({ start: 1320, end: 420 });
    expect(parseWindow(" 09:30-17:45 ")).toEqual({ start: 570, end: 1065 });
    for (const bad of ["9:00-17:00", "24:00-01:00", "09:60-10:00", "0900-1700", "09:00", ""]) {
      expect(parseWindow(bad)).toBeNull();
    }
    expect(parseWindows(["22:00-07:00", "nope", "12:00-13:00"]).length).toBe(2);
  });

  test("the Zod schema enforces the same shape", () => {
    expect(() => ConfigSchema.parse({ ...DEFAULT_CONFIG, downloadWindows: ["22:00-07:00"] })).not.toThrow();
    expect(() => ConfigSchema.parse({ ...DEFAULT_CONFIG, downloadWindows: ["10pm-7am"] })).toThrow();
    expect(DEFAULT_CONFIG.downloadWindows).toEqual([]);
  });
});

describe("isWithinWindows", () => {
  const night = parseWindows(["22:00-07:00"]);
  const office = parseWindows(["09:00-17:00"]);

  test("no windows = always open", () => {
    expect(isWithinWindows([], at("03:00"))).toBe(true);
  });

  test("a same-day window is start-inclusive, end-exclusive", () => {
    expect(isWithinWindows(office, at("08:59"))).toBe(false);
    expect(isWithinWindows(office, at("09:00"))).toBe(true);
    expect(isWithinWindows(office, at("16:59"))).toBe(true);
    expect(isWithinWindows(office, at("17:00"))).toBe(false);
  });

  test("a window that wraps midnight covers both sides", () => {
    expect(isWithinWindows(night, at("23:30"))).toBe(true);
    expect(isWithinWindows(night, at("00:00"))).toBe(true);
    expect(isWithinWindows(night, at("06:59"))).toBe(true);
    expect(isWithinWindows(night, at("07:00"))).toBe(false);
    expect(isWithinWindows(night, at("12:00"))).toBe(false);
  });

  test("several windows OR together; identical start/end = whole day", () => {
    const two = parseWindows(["09:00-10:00", "20:00-21:00"]);
    expect(isWithinWindows(two, at("09:30"))).toBe(true);
    expect(isWithinWindows(two, at("20:30"))).toBe(true);
    expect(isWithinWindows(two, at("15:00"))).toBe(false);
    expect(isWithinWindows(parseWindows(["09:00-09:00"]), at("03:00"))).toBe(true);
  });

  test("nextWindowOpen names the soonest start, wrapping past midnight", () => {
    expect(nextWindowOpen(night, at("12:00"))).toBe("22:00");
    expect(nextWindowOpen(parseWindows(["09:00-10:00", "20:00-21:00"]), at("12:00"))).toBe("20:00");
    expect(nextWindowOpen(parseWindows(["09:00-10:00"]), at("12:00"))).toBe("09:00");
    expect(nextWindowOpen([], at("12:00"))).toBeNull();
  });
});

describe("decideSchedule (policy table)", () => {
  const night = parseWindows(["22:00-07:00"]);
  const cases: [string, Date, { paused: boolean; reason: string | null }, string][] = [
    ["outside + running → pause", at("12:00"), { paused: false, reason: null }, "pause"],
    ["outside + already ours → none", at("12:00"), { paused: true, reason: `${SCHEDULE_PAUSE_PREFIX} (…)` }, "none"],
    ["outside + paused for another reason → none (never override)", at("12:00"), { paused: true, reason: "LOW_DISK_SPACE" }, "none"],
    ["inside + ours → resume", at("23:00"), { paused: true, reason: `${SCHEDULE_PAUSE_PREFIX} (…)` }, "resume"],
    ["inside + someone else's pause → none", at("23:00"), { paused: true, reason: "COOKIES_EXPIRED" }, "none"],
    ["inside + running → none", at("23:00"), { paused: false, reason: null }, "none"],
  ];
  for (const [name, now, state, want] of cases) {
    test(name, () => expect(decideSchedule(night, now, state)).toBe(want as any));
  }
  test("no windows configured never does anything", () => {
    expect(decideSchedule([], at("12:00"), { paused: false, reason: null })).toBe("none");
  });
});

describe("scheduleTick", () => {
  beforeEach(() => {
    initDatabase(":memory:");
    setConfig({ ...DEFAULT_CONFIG });
    setNotifyTransport(async () => {});
    triggerResume();
  });
  afterEach(() => {
    triggerResume();
    setNotifyTransport(null);
  });

  test("pauses with a SCHEDULE_WINDOW reason outside the window and resumes inside it", () => {
    const cfg = { downloadWindows: ["22:00-07:00"] };
    expect(scheduleTick(cfg, at("12:00"))).toBe("pause");
    expect(isPaused()).toBe(true);
    expect(getPauseReason()).toStartWith(SCHEDULE_PAUSE_PREFIX);
    expect(getPauseReason()).toContain("resumes at 22:00");
    expect(scheduleTick(cfg, at("12:30"))).toBe("none"); // idempotent
    expect(scheduleTick(cfg, at("22:00"))).toBe("resume");
    expect(isPaused()).toBe(false);
  });

  test("leaves a pause it did not create alone, in both directions", () => {
    const cfg = { downloadWindows: ["22:00-07:00"] };
    triggerPause("LOW_DISK_SPACE (1GB < 10GB)");
    expect(scheduleTick(cfg, at("12:00"))).toBe("none");
    expect(scheduleTick(cfg, at("23:00"))).toBe("none");
    expect(getPauseReason()).toStartWith("LOW_DISK_SPACE");
  });

  test("the window list can be emptied live: a schedule pause is then lifted", () => {
    expect(scheduleTick({ downloadWindows: ["22:00-07:00"] }, at("12:00"))).toBe("pause");
    expect(scheduleTick({ downloadWindows: [] }, at("12:00"))).toBe("resume");
  });
});
