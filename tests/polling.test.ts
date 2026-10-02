// tests/polling.test.ts — daemon-mode rescan tick: the in-flight latch and the
// live config read (gotcha 16).

import { describe, expect, test } from "bun:test";
import { DEFAULT_CONFIG, type Config } from "../src/config";
import { createRescanTick, startAutonomousPolling } from "../src/polling";

const cfg = (o: Partial<Config> = {}): Config => ({ ...DEFAULT_CONFIG, channels: ["https://www.youtube.com/@a"], ...o });

describe("createRescanTick", () => {
  test("overlapping ticks are dropped while a rescan is in flight", async () => {
    let release: () => void = () => {};
    const gate = new Promise<void>((r) => (release = r));
    let runs = 0;
    const tick = createRescanTick(
      () => cfg(),
      async () => {
        runs++;
        await gate;
      },
    );
    const first = tick();
    const second = await tick(); // while the first is still running
    expect(second).toBe(false);
    expect(runs).toBe(1);
    release();
    expect(await first).toBe(true);
    // Latch released: the next tick runs again.
    expect(await tick()).toBe(true);
    expect(runs).toBe(2);
  });

  test("every tick re-reads the config instead of the startup snapshot", async () => {
    const seen: string[][] = [];
    let current = cfg({ channels: ["https://www.youtube.com/@one"] });
    const tick = createRescanTick(
      () => current,
      async (c) => {
        seen.push(c.channels);
      },
    );
    await tick();
    current = cfg({ channels: ["https://www.youtube.com/@one", "https://www.youtube.com/@two"] });
    await tick();
    expect(seen[0]).toEqual(["https://www.youtube.com/@one"]);
    expect(seen[1]).toEqual(["https://www.youtube.com/@one", "https://www.youtube.com/@two"]);
  });

  test("the latch is released when the rescan throws", async () => {
    let n = 0;
    const tick = createRescanTick(
      () => cfg(),
      async () => {
        n++;
        if (n === 1) throw new Error("boom");
      },
    );
    await expect(tick()).rejects.toThrow("boom");
    expect(await tick()).toBe(true);
  });
});

describe("startAutonomousPolling", () => {
  test("does nothing without sources or with the interval disabled", () => {
    expect(startAutonomousPolling(cfg({ channels: [], channelPlaylists: [] }))).toBeNull();
    expect(startAutonomousPolling(cfg({ rescanIntervalHours: 0 }))).toBeNull();
  });

  test("returns a timer that can be cleared when enabled", () => {
    const timer = startAutonomousPolling(cfg({ rescanIntervalHours: 24 }));
    expect(timer).not.toBeNull();
    clearInterval(timer!);
  });
});
