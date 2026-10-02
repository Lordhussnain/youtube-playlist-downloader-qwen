// tests/logger.test.ts — the error log must stay out of the repo under tests.
//
// Regression guard: logError used to append to cwd-relative `error.log`
// unconditionally, so a plain `bun test` at the repo root dropped fixture
// recovery lines ("reclaimed stale claims…", "gone999: file gone…") into the
// same file the production engine uses — they read as engine faults in the
// operator's log.

import { describe, expect, test } from "bun:test";
import { existsSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { errorLogPath, logError, sanitizeLogMessage } from "../src/logger";

describe("errorLogPath", () => {
  test("routes test-run logs to the temp dir, never the repo's error.log", () => {
    // bun test sets NODE_ENV=test; the operator's error.log is cwd-relative.
    const path = errorLogPath();
    expect(path).not.toBe("error.log");
    expect(path.startsWith(join(tmpdir(), ""))).toBe(true);
    expect(path.endsWith("error.log")).toBe(true);
  });

  test("logError appends timestamped, scoped lines to the routed file", () => {
    logError("logger-test", "routing probe");
    const path = errorLogPath();
    expect(existsSync(path)).toBe(true);
    const line = readFileSync(path, "utf-8")
      .split("\n")
      .find((l) => l.includes("[logger-test] routing probe"));
    expect(line).toBeTruthy();
    // ISO timestamp prefix, same shape every scope writes.
    expect(line).toMatch(/^\[\d{4}-\d{2}-\d{2}T/);
  });
});

describe("sanitizeLogMessage (3.7)", () => {
  test("one record is one line — control characters collapse", () => {
    const stack = "Error: boom\n    at a (x.ts:1:1)\n    at b (y.ts:2:2)";
    const out = sanitizeLogMessage(stack);
    expect(out).not.toContain("\n");
    expect(out).toContain("at a (x.ts:1:1)");
    expect(sanitizeLogMessage("a\r\nb\tc\u0007d")).toBe("a b c d");
  });

  test("a crafted title cannot forge a scoped record", () => {
    logError("scan", "title: innocent\n[reaper] forged line");
    const lines = readFileSync(errorLogPath(), "utf-8").split("\n");
    expect(lines.some((l) => l.startsWith("[reaper] forged line"))).toBe(false);
    expect(lines.some((l) => l.includes("[scan] title: innocent [reaper] forged line"))).toBe(true);
  });

  test("secrets are redacted", () => {
    expect(sanitizeLogMessage("yt-dlp --cookies /home/me/cookies.txt --flat-playlist")).toBe(
      "yt-dlp --cookies [redacted] --flat-playlist",
    );
    expect(sanitizeLogMessage('--cookies "C:\\Users\\me\\cookies.txt" x')).toBe("--cookies [redacted] x");
    expect(sanitizeLogMessage("Authorization: Bearer abc.DEF-123")).toBe("Authorization: Bearer [redacted]");
    expect(sanitizeLogMessage("GET /?token=s3cret&x=1")).toBe("GET /?token=[redacted]&x=1");
    expect(sanitizeLogMessage("cookie yta_token=s3cret; other=1")).toBe("cookie yta_token=[redacted]; other=1");
  });

  test("a runaway message is capped", () => {
    const out = sanitizeLogMessage("x".repeat(10_000));
    expect(out.length).toBeLessThan(4100);
    expect(out.endsWith("…[truncated]")).toBe(true);
  });

  test("the scope itself is constrained to a token", () => {
    logError("we ird]\n[x", "scope probe");
    const line = readFileSync(errorLogPath(), "utf-8")
      .split("\n")
      .find((l) => l.includes("scope probe"));
    expect(line).toMatch(/\] \[we_ird_x\] scope probe$/);
  });
});
