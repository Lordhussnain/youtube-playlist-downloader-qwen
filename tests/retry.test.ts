// tests/retry.test.ts — retry policy: backoff, watchdogs, error classification.

import { describe, expect, test } from "bun:test";
import {
  computeBackoffMs,
  computeDownloadTimeoutMs,
  isDownloaderArgsError,
  isPermanentDownloadError,
  isTransientDownloadError,
} from "../src/retry";

describe("computeBackoffMs", () => {
  test("grows exponentially from the base", () => {
    const noJitter = () => 0;
    expect(computeBackoffMs(0, 30, 900, noJitter)).toBe(30_000);
    expect(computeBackoffMs(1, 30, 900, noJitter)).toBe(60_000);
    expect(computeBackoffMs(2, 30, 900, noJitter)).toBe(120_000);
    expect(computeBackoffMs(3, 30, 900, noJitter)).toBe(240_000);
  });

  test("is capped at maxSeconds", () => {
    const noJitter = () => 0;
    expect(computeBackoffMs(10, 30, 900, noJitter)).toBe(900_000);
    expect(computeBackoffMs(20, 30, 300, noJitter)).toBe(300_000);
  });

  test("jitter stays within +0..30% of the capped value", () => {
    for (const attempt of [0, 1, 2, 5]) {
      const base = computeBackoffMs(attempt, 10, 1000, () => 0);
      const maxJittered = computeBackoffMs(attempt, 10, 1000, () => 1);
      expect(maxJittered).toBeGreaterThanOrEqual(base);
      expect(maxJittered).toBeLessThanOrEqual(base * 1.3);
    }
  });

  test("is deterministic for an injected RNG", () => {
    expect(computeBackoffMs(2, 30, 900, () => 0.5)).toBe(computeBackoffMs(2, 30, 900, () => 0.5));
  });

  test("treats negative/NaN attempts as the first attempt", () => {
    const noJitter = () => 0;
    expect(computeBackoffMs(-5, 30, 900, noJitter)).toBe(30_000);
    expect(computeBackoffMs(NaN, 30, 900, noJitter)).toBe(30_000);
  });

  test("never returns a non-positive delay", () => {
    for (let i = 0; i < 50; i++) {
      expect(computeBackoffMs(i, 1, 5)).toBeGreaterThan(0);
    }
  });
});

describe("computeDownloadTimeoutMs", () => {
  const opts = { minMinutes: 15, maxMinutes: 180 };

  test("unknown duration → the configured minimum", () => {
    expect(computeDownloadTimeoutMs(null, opts)).toBe(15 * 60_000);
    expect(computeDownloadTimeoutMs(undefined, opts)).toBe(15 * 60_000);
    expect(computeDownloadTimeoutMs(0, opts)).toBe(15 * 60_000);
    expect(computeDownloadTimeoutMs(NaN, opts)).toBe(15 * 60_000);
  });

  test("short videos get the minimum, not less", () => {
    // 3×60s + 300s = 480s < 15 min → clamped up to the minimum
    expect(computeDownloadTimeoutMs(60, opts)).toBe(15 * 60_000);
  });

  test("long videos scale with duration", () => {
    // 3×3600 + 300 = 11100s = 185 min → clamped to the 180 min ceiling
    expect(computeDownloadTimeoutMs(3600, opts)).toBe(180 * 60_000);
    // 3×1800 + 300 = 5700s = 95 min → inside the range
    expect(computeDownloadTimeoutMs(1800, opts)).toBe(95 * 60_000);
  });

  test("a 2-hour video on a slow link is not killed at 15 minutes", () => {
    const twoHour = computeDownloadTimeoutMs(7200, { minMinutes: 15, maxMinutes: 240 });
    expect(twoHour).toBeGreaterThan(15 * 60_000);
  });
});

describe("isPermanentDownloadError", () => {
  test("flags unrecoverable errors", () => {
    expect(isPermanentDownloadError("ERROR: [youtube] abc: Video unavailable")).toBe(true);
    expect(isPermanentDownloadError("Private video. Sign in if you've been granted access")).toBe(true);
    expect(isPermanentDownloadError("ERROR: members-only content")).toBe(true);
    expect(isPermanentDownloadError("Sign in to confirm your age")).toBe(true);
    expect(isPermanentDownloadError("HTTP Error 404: Not Found")).toBe(true);
    expect(isPermanentDownloadError("This video has been removed by the uploader")).toBe(true);
    expect(isPermanentDownloadError("The uploader has not made this video available in your country")).toBe(true);
  });

  test("does not flag transient errors", () => {
    expect(isPermanentDownloadError("Unable to download webpage: Connection reset by peer")).toBe(false);
    expect(isPermanentDownloadError("HTTP Error 429: Too Many Requests")).toBe(false);
    expect(isPermanentDownloadError("The read operation timed out")).toBe(false);
    expect(isPermanentDownloadError(null)).toBe(false);
    expect(isPermanentDownloadError(undefined)).toBe(false);
    expect(isPermanentDownloadError("")).toBe(false);
  });
});

describe("isTransientDownloadError", () => {
  test("flags network-ish failures", () => {
    expect(isTransientDownloadError("Unable to download webpage: Connection reset by peer")).toBe(true);
    expect(isTransientDownloadError("The read operation timed out")).toBe(true);
    expect(isTransientDownloadError("HTTP Error 429: Too Many Requests")).toBe(true);
    expect(isTransientDownloadError("HTTP Error 503: Service Unavailable")).toBe(true);
    expect(isTransientDownloadError("network is unreachable")).toBe(true);
  });

  test("does not flag permanent failures", () => {
    expect(isTransientDownloadError("Video unavailable")).toBe(false);
    expect(isTransientDownloadError("Private video")).toBe(false);
  });
});

describe("isDownloaderArgsError", () => {
  test("flags aria2c exit 28 and its option help block", () => {
    // The exact shape of the production failure: aria2c prints the offending
    // option's help, then yt-dlp reports the exit code.
    expect(
      isDownloaderArgsError(
        "Possible Values: 1-16\nDefault: 1\nTags: #basic, #http, #ftp ERROR: aria2c exited with code 28",
      ),
    ).toBe(true);
    expect(isDownloaderArgsError("ERROR: aria2c exited with code 28")).toBe(true);
    expect(isDownloaderArgsError("aria2c: unrecognized option '--splitt'")).toBe(true);
  });

  test("does not flag network, corrupt, or permanent failures", () => {
    expect(isDownloaderArgsError("Unable to download webpage: Connection reset by peer")).toBe(false);
    expect(isDownloaderArgsError("unable to resume download, incomplete or corrupt data")).toBe(false);
    expect(isDownloaderArgsError("This video is private")).toBe(false);
    expect(isDownloaderArgsError(null)).toBe(false);
  });
});

// --- decideFailureOutcome — the whole failure policy as one table -------------
import { decideFailureOutcome, type FailureContext, type FailureOutcome } from "../src/retry";

const baseCtx = (o: Partial<FailureContext> = {}): FailureContext => ({
  error: "something odd happened",
  isPaused: false,
  userPaused: false,
  retryCount: 0,
  retryCap: 3,
  resumeCount: 0,
  maxResume: 3,
  progress: 0,
  bestProgress: 0,
  hasPartial: false,
  ...o,
});

describe("decideFailureOutcome (plan 4.1)", () => {
  const table: Array<[string, Partial<FailureContext>, Partial<FailureOutcome> & { kind: FailureOutcome["kind"] }]> = [
    // precedence: pause first, whatever the error says
    ["global pause parks (auto-resumable) even on a permanent error", { isPaused: true, error: "Video unavailable" }, { kind: "park", pauseReason: null }],
    ["per-job user pause parks with the user hold", { userPaused: true, error: "timeout" }, { kind: "park", pauseReason: "user" }],
    ["global pause outranks the user hold", { isPaused: true, userPaused: true }, { kind: "park", pauseReason: null }],
    // signature
    ["signature → self-update", { error: "ERROR: Unable to extract nsig function" }, { kind: "self-update" }],
    ["'signature' keyword → self-update", { error: "signature solving failed" }, { kind: "self-update" }],
    // downloader args
    ["aria2c exit 28 → bad-args", { error: "aria2c exited with code 28 Possible Values: 1-16" }, { kind: "bad-args" }],
    ["signature outranks bad-args", { error: "unable to extract; exited with code 28" }, { kind: "self-update" }],
    // corrupt partial / resume budget
    ["corrupt with budget and a partial → resume #1", { error: "unable to resume", hasPartial: true }, { kind: "resume", resumeCount: 1, backoffAttempt: 1 }],
    ["corrupt, second time → resume #2", { error: "corrupt", hasPartial: true, resumeCount: 1 }, { kind: "resume", resumeCount: 2 }],
    ["corrupt, budget spent → restart-fresh", { error: "incomplete", hasPartial: true, resumeCount: 2, maxResume: 3 }, { kind: "restart-fresh", retryDelta: 1, discardPartial: true }],
    ["corrupt with no partial on disk → restart-fresh immediately", { error: "unable to resume", hasPartial: false }, { kind: "restart-fresh" }],
    ["maxResume 0 is treated as 1", { error: "corrupt", hasPartial: true, maxResume: 0 }, { kind: "restart-fresh" }],
    // archive mismatch
    ["archive says done but the file is gone → scrub", { error: "output file could not be located" }, { kind: "archive-scrub", scrubArchive: true, discardPartial: true, retryDelta: 1 }],
    // live
    ["live now → wait-live", { error: "video does not pass filter (!is_live)" }, { kind: "wait-live" }],
    ["'is live' → wait-live", { error: "This video is live" }, { kind: "wait-live" }],
    // transient
    ["transient with no progress spends the budget", { error: "HTTP Error 503", retryCount: 1, progress: 10, bestProgress: 10 }, { kind: "transient", retryCount: 2, bestProgress: 10, backoffAttempt: 2 }],
    ["transient that advanced is forgiven", { error: "connection reset", retryCount: 1, progress: 40, bestProgress: 10 }, { kind: "transient", retryCount: 1, bestProgress: 40, backoffAttempt: 1 }],
    ["transient never flips to failed by itself", { error: "timed out", retryCount: 99, retryCap: 3 }, { kind: "transient", retryCount: 100 }],
    // permanent / unknown
    ["permanent with budget left → pending, no breaker", { error: "Video unavailable", retryCount: 0, retryCap: 3 }, { kind: "permanent", status: "pending", retryCount: 1, tripBreaker: false }],
    ["permanent that exhausts the cap → failed + breaker", { error: "Private video", retryCount: 2, retryCap: 3 }, { kind: "permanent", status: "failed", retryCount: 3, tripBreaker: true }],
    ["unknown error is treated like permanent", { error: "???", retryCount: 0, retryCap: 1 }, { kind: "permanent", status: "failed", retryCount: 1, tripBreaker: true }],
    ["retryCap 0 still fails on the first strike", { error: "???", retryCap: 0 }, { kind: "permanent", status: "failed" }],
  ];

  for (const [name, ctx, expected] of table) {
    test(name, () => {
      const out = decideFailureOutcome(baseCtx(ctx)) as any;
      for (const [k, v] of Object.entries(expected)) expect(out[k]).toEqual(v);
    });
  }

  test("is pure: the same context always yields the same outcome and does not mutate it", () => {
    const ctx = baseCtx({ error: "timeout", retryCount: 1, progress: 5, bestProgress: 5 });
    const snapshot = JSON.stringify(ctx);
    const a = decideFailureOutcome(ctx);
    const b = decideFailureOutcome(ctx);
    expect(a).toEqual(b);
    expect(JSON.stringify(ctx)).toBe(snapshot);
  });
});
