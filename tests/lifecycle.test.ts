// tests/lifecycle.test.ts — supervise(): crashed (or early-returning) worker
// loops restart after a backoff, and stop restarting once the engine aborts.

import { describe, expect, test } from "bun:test";
import { initDatabase } from "../src/db";
import { supervise } from "../src/lifecycle";

initDatabase(":memory:");

describe("supervise", () => {
  test("restarts a crashing loop until the signal aborts", async () => {
    const ctl = new AbortController();
    let runs = 0;
    const restarts: string[] = [];
    supervise(
      "crashy",
      async () => {
        runs++;
        if (runs >= 3) ctl.abort(); // third run "shuts down"
        throw new Error("boom");
      },
      { restartDelayMs: 5, signal: ctl.signal, onRestart: (n) => restarts.push(n) },
    );
    await Bun.sleep(100);
    expect(runs).toBe(3);
    expect(restarts).toEqual(["crashy", "crashy"]);
    // no further runs after abort
    await Bun.sleep(30);
    expect(runs).toBe(3);
  });

  test("an early clean return is also restarted", async () => {
    const ctl = new AbortController();
    let runs = 0;
    supervise(
      "quitter",
      async () => {
        runs++;
        if (runs === 2) ctl.abort();
      },
      { restartDelayMs: 5, signal: ctl.signal },
    );
    await Bun.sleep(60);
    expect(runs).toBe(2);
  });

  test("an abort during the backoff cancels the pending restart", async () => {
    const ctl = new AbortController();
    let runs = 0;
    supervise(
      "late-abort",
      async () => {
        runs++;
        throw new Error("x");
      },
      { restartDelayMs: 30, signal: ctl.signal },
    );
    await Bun.sleep(5);
    ctl.abort();
    await Bun.sleep(60);
    expect(runs).toBe(1);
  });
});
