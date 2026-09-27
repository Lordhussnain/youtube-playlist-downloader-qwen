// tests/settings.test.ts — the dashboard settings API.
//
// The dashboard may tune the downloader, but it must never be able to rewrite
// playlists, credentials, or the network binding. These tests pin the
// allow-list, the validation path (including cross-field refinements), the
// "reject rather than silently ignore" rule for unknown keys, and the
// all-or-nothing behaviour when a patch is invalid.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DEFAULT_CONFIG, type Config } from "../src/config";
import { getConfig, setConfig } from "../src/state";
import { initDatabase } from "../src/db";
import { EDITABLE_SETTINGS, applySettings, isEditableSetting, readSettings } from "../src/settings";
import { handleRequest } from "../src/web";

const tmpDirs: string[] = [];

afterEach(async () => {
  while (tmpDirs.length) {
    const d = tmpDirs.pop();
    if (d) await rm(d, { recursive: true, force: true });
  }
});

async function makeConfigDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "yta-settings-"));
  tmpDirs.push(dir);
  return dir;
}

function baseConfig(overrides: Partial<Config> = {}): Config {
  return { ...DEFAULT_CONFIG, ...overrides };
}

beforeEach(() => {
  initDatabase(":memory:"); // /api/reliability queries the jobs table
  setConfig(baseConfig());
});

describe("the editable allow-list", () => {
  test("includes the downloader tuning knobs", () => {
    for (const k of [
      "useAria2c",
      "connectionsPerDownload",
      "minSplitSize",
      "concurrentFragments",
      "fragmentRetries",
      "httpChunkSize",
      "bufferSize",
      "maxBandwidthKBps",
      "autoscaleRampStep",
    ]) {
      expect(isEditableSetting(k)).toBe(true);
    }
  });

  test("excludes credentials, paths, and the network binding", () => {
    for (const k of ["webToken", "webBind", "webPort", "playlists", "channels", "cookiesFile", "archiveFile", "outputRoot", "ytDlpPath"]) {
      expect(isEditableSetting(k)).toBe(false);
    }
  });

  test("every field has a label, a group, and help text", () => {
    for (const f of EDITABLE_SETTINGS) {
      expect(f.label.length).toBeGreaterThan(0);
      expect(f.help.length).toBeGreaterThan(0);
      expect(["downloader", "concurrency", "reliability"]).toContain(f.group);
    }
  });

  test("number fields carry a lower bound, and an upper one where the schema has it", () => {
    for (const f of EDITABLE_SETTINGS.filter((x) => x.type === "number")) {
      expect(f.min).toBeDefined();
      // Some knobs are genuinely unbounded above (e.g. the bandwidth cap), so
      // max is optional — but when present it must not contradict min.
      if (f.max !== undefined) expect(f.max).toBeGreaterThanOrEqual(f.min!);
    }
    // Sanity: the bounded ones really are bounded in the UI.
    const conns = EDITABLE_SETTINGS.find((f) => f.key === "connectionsPerDownload")!;
    expect(conns.min).toBe(1);
    expect(conns.max).toBe(64);
  });
});

describe("readSettings", () => {
  test("returns current values and flags non-defaults", () => {
    const snapshot = readSettings(baseConfig({ connectionsPerDownload: 4 }));
    expect(snapshot.values.connectionsPerDownload).toBe(4);
    expect(snapshot.nonDefault).toContain("connectionsPerDownload");
    expect(snapshot.nonDefault).not.toContain("concurrentFragments");
  });
});

describe("applySettings", () => {
  test("applies a patch, persists it, and makes it live", async () => {
    const dir = await makeConfigDir();
    const cfgPath = join(dir, "config.json");
    const current = baseConfig();
    const result = await applySettings(current, { connectionsPerDownload: 32 }, cfgPath);

    expect(result.ok).toBe(true);
    expect(result.changed).toEqual(["connectionsPerDownload"]);
    expect(getConfig().connectionsPerDownload).toBe(32); // live, no restart needed

    const onDisk = JSON.parse(await readFile(cfgPath, "utf8"));
    expect(onDisk.connectionsPerDownload).toBe(32);
  });

  test("coerces text input into the declared type", async () => {
    const dir = await makeConfigDir();
    const result = await applySettings(baseConfig(), { connectionsPerDownload: "8" }, join(dir, "config.json"));
    expect(result.ok).toBe(true);
    expect(getConfig().connectionsPerDownload).toBe(8);
  });

  test("coerces boolean-ish strings", async () => {
    const dir = await makeConfigDir();
    const result = await applySettings(baseConfig(), { useAria2c: "false" }, join(dir, "config.json"));
    expect(result.ok).toBe(true);
    expect(getConfig().useAria2c).toBe(false);
  });

  test("reports no change when the value is already current", async () => {
    const dir = await makeConfigDir();
    const result = await applySettings(baseConfig(), { concurrentFragments: 16 }, join(dir, "config.json"));
    expect(result.ok).toBe(true);
    expect(result.changed).toEqual([]);
  });

  test("rejects keys outside the allow-list instead of ignoring them", async () => {
    const dir = await makeConfigDir();
    const result = await applySettings(
      baseConfig(),
      { connectionsPerDownload: 8, webToken: "hunter2" },
      join(dir, "config.json"),
    );
    expect(result.ok).toBe(false);
    expect(result.error).toContain("webToken");
    expect(result.changed).toEqual([]);
    // Nothing was written and nothing went live.
    expect(getConfig().connectionsPerDownload).toBe(DEFAULT_CONFIG.connectionsPerDownload);
    await expect(readFile(join(dir, "config.json"), "utf8")).rejects.toThrow();
  });

  test("rejects out-of-range numbers", async () => {
    const dir = await makeConfigDir();
    const result = await applySettings(baseConfig(), { connectionsPerDownload: 999 }, join(dir, "config.json"));
    expect(result.ok).toBe(false);
    expect(result.error).toBeTruthy();
    expect(getConfig().connectionsPerDownload).toBe(DEFAULT_CONFIG.connectionsPerDownload);
  });

  test("rejects a non-numeric value for a number field", async () => {
    const dir = await makeConfigDir();
    const result = await applySettings(baseConfig(), { fragmentRetries: "lots" }, join(dir, "config.json"));
    expect(result.ok).toBe(false);
    expect(result.error).toContain("Fragment retries");
  });

  test("enforces the cross-field refinement (backoff max >= base)", async () => {
    const dir = await makeConfigDir();
    const result = await applySettings(
      baseConfig({ retryBackoffBaseSeconds: 30, retryBackoffMaxSeconds: 900 }),
      { retryBackoffMaxSeconds: 5 },
      join(dir, "config.json"),
    );
    expect(result.ok).toBe(false);
    expect(getConfig().retryBackoffMaxSeconds).toBe(900); // unchanged
  });

  test("enforces the cross-field refinement (maxDownloadMinutes >= downloadTimeoutMinutes)", async () => {
    const dir = await makeConfigDir();
    const result = await applySettings(
      baseConfig({ downloadTimeoutMinutes: 15, maxDownloadMinutes: 180 }),
      { maxDownloadMinutes: 2 },
      join(dir, "config.json"),
    );
    expect(result.ok).toBe(false);
    expect(getConfig().maxDownloadMinutes).toBe(180);
  });

  test("enforces the cross-field refinement (minDownloadWorkers <= maxDownloadWorkers)", async () => {
    const dir = await makeConfigDir();
    const result = await applySettings(
      baseConfig({ minDownloadWorkers: 1, maxDownloadWorkers: 5 }),
      { minDownloadWorkers: 9 },
      join(dir, "config.json"),
    );
    expect(result.ok).toBe(false);
    expect(getConfig().minDownloadWorkers).toBe(1);
  });

  test("rejects a non-object body", async () => {
    const dir = await makeConfigDir();
    expect((await applySettings(baseConfig(), [] as any, join(dir, "config.json"))).ok).toBe(false);
    expect((await applySettings(baseConfig(), null as any, join(dir, "config.json"))).ok).toBe(false);
    expect((await applySettings(baseConfig(), "nope" as any, join(dir, "config.json"))).ok).toBe(false);
  });

  test("an empty patch is a harmless no-op", async () => {
    const dir = await makeConfigDir();
    const result = await applySettings(baseConfig(), {}, join(dir, "config.json"));
    expect(result.ok).toBe(true);
    expect(result.changed).toEqual([]);
  });
});

describe("GET /api/settings", () => {
  test("returns the fields and current values", async () => {
    setConfig(baseConfig({ connectionsPerDownload: 12 }));
    const res = await handleRequest(new Request("http://x/api/settings"), getConfig());
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.ok).toBe(true);
    expect(body.fields.length).toBe(EDITABLE_SETTINGS.length);
    expect(body.values.connectionsPerDownload).toBe(12);
  });
});

describe("POST /api/settings", () => {
  test("applies a valid patch and echoes the new snapshot", async () => {
    const dir = await makeConfigDir();
    const cwd = process.cwd();
    process.chdir(dir); // CONFIG_PATH is CWD-relative
    try {
      setConfig(baseConfig());
      const res = await handleRequest(
        new Request("http://x/api/settings", {
          method: "POST",
          body: JSON.stringify({ connectionsPerDownload: 24, maxBandwidthKBps: 5000 }),
        }),
        getConfig(),
      );
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.ok).toBe(true);
      expect(body.changed.sort()).toEqual(["connectionsPerDownload", "maxBandwidthKBps"]);
      expect(body.values.connectionsPerDownload).toBe(24);
      expect(body.values.maxBandwidthKBps).toBe(5000);
      expect(getConfig().connectionsPerDownload).toBe(24);
    } finally {
      process.chdir(cwd);
    }
  });

  test("rejects a non-editable key with 400", async () => {
    const dir = await makeConfigDir();
    const cwd = process.cwd();
    process.chdir(dir);
    try {
      setConfig(baseConfig());
      const res = await handleRequest(
        new Request("http://x/api/settings", {
          method: "POST",
          body: JSON.stringify({ webToken: "nope" }),
        }),
        getConfig(),
      );
      expect(res.status).toBe(400);
      const body = await res.json();
      expect(body.ok).toBe(false);
      expect(body.error).toContain("webToken");
      expect(getConfig().webToken).toBe("");
    } finally {
      process.chdir(cwd);
    }
  });

  test("rejects an invalid value with 400 and leaves the config alone", async () => {
    const dir = await makeConfigDir();
    const cwd = process.cwd();
    process.chdir(dir);
    try {
      setConfig(baseConfig());
      const res = await handleRequest(
        new Request("http://x/api/settings", {
          method: "POST",
          body: JSON.stringify({ connectionsPerDownload: 5000 }),
        }),
        getConfig(),
      );
      expect(res.status).toBe(400);
      expect(getConfig().connectionsPerDownload).toBe(DEFAULT_CONFIG.connectionsPerDownload);
    } finally {
      process.chdir(cwd);
    }
  });

  test("rejects a malformed body with 400", async () => {
    setConfig(baseConfig());
    const res = await handleRequest(
      new Request("http://x/api/settings", { method: "POST", body: "not json" }),
      getConfig(),
    );
    expect(res.status).toBe(400);
  });

  test("the reliability endpoint reflects the new values, not the startup snapshot", async () => {
    // Regression guard: the web server must read the LIVE config. setConfig()
    // replaces the object, so a server holding the startup reference would keep
    // reporting stale policy knobs after a settings change.
    const dir = await makeConfigDir();
    const cwd = process.cwd();
    process.chdir(dir);
    try {
      const startup = baseConfig({ connectionsPerDownload: 16, maxResumeAttempts: 5 });
      setConfig(startup);
      await applySettings(startup, { connectionsPerDownload: 40, maxResumeAttempts: 9 }, join(dir, "config.json"));

      const res = await handleRequest(new Request("http://x/api/reliability"), getConfig());
      const body = await res.json();
      expect(body.downloader.connectionsPerDownload).toBe(40);
      expect(body.policy.maxResumeAttempts).toBe(9);
    } finally {
      process.chdir(cwd);
    }
  });

  test("requires the token when one is configured", async () => {
    setConfig(baseConfig({ webToken: "s3cret" }));
    const anon = await handleRequest(new Request("http://x/api/settings"), getConfig());
    expect(anon.status).toBe(401);
    const authed = await handleRequest(
      new Request("http://x/api/settings", { headers: { "X-Web-Token": "s3cret" } }),
      getConfig(),
    );
    expect(authed.status).toBe(200);
  });
});
