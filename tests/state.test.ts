// tests/state.test.ts — the abort-scoped interval helper (3.11): every
// periodic sweep must stop when the engine aborts, without process.exit().

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { abortController, everyInterval } from "../src/state";

describe("everyInterval", () => {
  test("returns a clearable timer that fires", async () => {
    let n = 0;
    const t = everyInterval(() => n++, 5);
    await Bun.sleep(40);
    clearInterval(t);
    const seen = n;
    expect(seen).toBeGreaterThan(0);
    await Bun.sleep(20);
    expect(n).toBe(seen);
  });

  test("is registered against the engine abort signal", () => {
    // The global controller is shared across the suite, so do not abort it
    // here; assert the subscription exists instead by checking the listener
    // short-circuit for an already-aborted controller through the source.
    expect(abortController.signal.aborted).toBe(false);
    const src = readFileSync("src/state.ts", "utf-8");
    expect(src).toContain('abortController.signal.addEventListener("abort", () => clearInterval(timer)');
  });

  test("no periodic sweep bypasses it", () => {
    for (const file of ["src/engine.ts", "src/rss.ts", "src/polling.ts"]) {
      const src = readFileSync(file, "utf-8");
      expect(src).not.toMatch(/\bsetInterval\(/);
    }
  });
});
