// tests/web-hardening.test.ts — Phase 3 hardening of the HTTP surface:
// security headers on every response, limit clamps, malformed percent-encoding,
// scan-URL validation, and the length-oblivious token comparison.

import { beforeEach, describe, expect, test } from "bun:test";
import { DEFAULT_CONFIG, type Config } from "../src/config";
import { setConfig } from "../src/state";
import { db, initDatabase } from "../src/db";
import {
  clampLimit,
  handleRequest,
  isPrivateAddress,
  safeDecode,
  SECURITY_HEADERS,
  timingSafeEq,
  validateScanUrl,
} from "../src/web";

const cfg = (o: Partial<Config> = {}): Config => ({ ...DEFAULT_CONFIG, ...o });
const api = (path: string, init?: RequestInit, c: Config = cfg()) =>
  handleRequest(new Request(`http://localhost${path}`, init), c);

beforeEach(() => {
  initDatabase(":memory:");
  setConfig(cfg());
});

describe("3.9 security headers", () => {
  test("every response carries the full header set", async () => {
    const responses = await Promise.all([
      api("/api/status"),
      api("/api/nope"),
      api("/api/jobs/missing", { method: "DELETE" }),
      api("/", undefined, cfg({ webToken: "secret" })), // 401 login page
      api("/api/status", undefined, cfg({ webToken: "secret" })), // 401 JSON
    ]);
    for (const res of responses) {
      for (const [k, v] of Object.entries(SECURITY_HEADERS)) expect(res.headers.get(k)).toBe(v);
    }
    expect(SECURITY_HEADERS["X-Frame-Options"]).toBe("DENY");
    expect(SECURITY_HEADERS["X-Content-Type-Options"]).toBe("nosniff");
    expect(SECURITY_HEADERS["Cache-Control"]).toBe("no-store");
    expect(SECURITY_HEADERS["Content-Security-Policy"]).toContain("frame-ancestors 'none'");
    expect(SECURITY_HEADERS["Content-Security-Policy"]).toContain("default-src 'self'");
  });
});

describe("3.1 limit clamps", () => {
  test("clampLimit bounds and falls back", () => {
    expect(clampLimit(null, 20)).toBe(20);
    expect(clampLimit("abc", 20)).toBe(20);
    expect(clampLimit("-1", 20)).toBe(20);
    expect(clampLimit("0", 20)).toBe(20);
    expect(clampLimit("5", 20)).toBe(5);
    expect(clampLimit("999999", 20)).toBe(1000);
    expect(clampLimit("50", 20, 30)).toBe(30);
  });

  test("/api/history?limit=-1 no longer means unlimited", async () => {
    for (let i = 0; i < 30; i++) db.run("INSERT INTO run_history (started_at, ended_at) VALUES (?, ?)", [`s${i}`, `e${i}`]);
    const unlimited = await (await api("/api/history?limit=-1")).json();
    expect(unlimited.history.length).toBe(20);
    const nan = await (await api("/api/history?limit=abc")).json();
    expect(nan.history.length).toBe(20);
    const five = await (await api("/api/history?limit=5")).json();
    expect(five.history.length).toBe(5);
  });
});

describe("3.3 malformed percent-encoding", () => {
  test("safeDecode returns null instead of throwing", () => {
    expect(safeDecode("a%20b")).toBe("a b");
    expect(safeDecode("%")).toBeNull();
    expect(safeDecode("%E0%A4%A")).toBeNull();
  });

  test("/api/jobs/% is a 404, not a 500", async () => {
    const res = await api("/api/jobs/%");
    expect(res.status).toBe(404);
    expect((await res.json()).ok).toBe(false);
  });

  test("a malformed cookie is unauthorized, not a crash", async () => {
    const res = await api("/api/status", { headers: { cookie: "yta_token=%" } }, cfg({ webToken: "secret" }));
    expect(res.status).toBe(401);
    const ok = await api("/api/status", { headers: { cookie: "yta_token=secret" } }, cfg({ webToken: "secret" }));
    expect(ok.status).toBe(200);
  });
});

describe("3.8 timingSafeEq", () => {
  test("is length-oblivious and still exact", () => {
    expect(timingSafeEq("abc", "abc")).toBe(true);
    expect(timingSafeEq("abc", "abd")).toBe(false);
    expect(timingSafeEq("abc", "abcd")).toBe(false);
    expect(timingSafeEq("", "")).toBe(true);
    expect(timingSafeEq("", "x")).toBe(false);
  });
});

describe("3.2 scan URL validation", () => {
  test("validateScanUrl accepts public http(s) only", () => {
    expect(validateScanUrl("https://www.youtube.com/playlist?list=PL123")).toBeNull();
    expect(validateScanUrl("http://youtube.com/@chan")).toBeNull();
    expect(validateScanUrl("file:///etc/passwd")).toMatch(/http/);
    expect(validateScanUrl("ftp://example.com/x")).toMatch(/http/);
    expect(validateScanUrl("http://localhost:8080/")).toMatch(/Local/);
    expect(validateScanUrl("http://foo.localhost/")).toMatch(/Local/);
    expect(validateScanUrl("http://127.0.0.1/")).toMatch(/Private/);
    expect(validateScanUrl("http://10.1.2.3/")).toMatch(/Private/);
    expect(validateScanUrl("http://192.168.1.1/")).toMatch(/Private/);
    expect(validateScanUrl("http://172.16.0.1/")).toMatch(/Private/);
    expect(validateScanUrl("http://169.254.169.254/latest/meta-data")).toMatch(/Private/);
    expect(validateScanUrl("http://[::1]/")).toMatch(/Private/);
    expect(validateScanUrl("http://[fe80::1]/")).toMatch(/Private/);
    expect(validateScanUrl("http://user:pw@youtube.com/")).toMatch(/Credentials/);
    expect(validateScanUrl("not a url")).toBe("Invalid URL");
    expect(validateScanUrl(42)).toBe("URL required");
    expect(validateScanUrl("https://x.com/" + "a".repeat(3000))).toBe("URL too long");
  });

  test("isPrivateAddress covers the usual ranges and nothing public", () => {
    expect(isPrivateAddress("8.8.8.8")).toBe(false);
    expect(isPrivateAddress("172.32.0.1")).toBe(false);
    expect(isPrivateAddress("100.64.0.1")).toBe(true);
    expect(isPrivateAddress("0.0.0.0")).toBe(true);
    expect(isPrivateAddress("224.0.0.1")).toBe(true);
    expect(isPrivateAddress("::ffff:127.0.0.1")).toBe(true);
    expect(isPrivateAddress("2001:db8::1")).toBe(false);
  });

  test("POST /api/scan rejects bad URLs and a non-string folder with 400", async () => {
    const post = (body: unknown) =>
      api("/api/scan", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
    expect((await post({ url: "file:///etc/passwd" })).status).toBe(400);
    expect((await post({ url: "http://127.0.0.1:9/" })).status).toBe(400);
    const res = await post({ url: "https://www.youtube.com/@x", folder: { nested: true } });
    expect(res.status).toBe(400);
    expect((await res.json()).error).toMatch(/folder/);
  });
});
