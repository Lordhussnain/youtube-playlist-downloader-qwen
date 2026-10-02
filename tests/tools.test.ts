// tests/tools.test.ts — the binary discovery order (plan 4.6). The search is
// table-driven and platform-dependent, which made it easy to regress silently.

import { describe, expect, test } from "bun:test";
import { dirname, join } from "node:path";
import { toolCandidates } from "../src/tools";

const base = { cwd: "/work", execPath: "/opt/app/archive", home: "/home/me", vars: {} as Record<string, string | undefined> };

describe("toolCandidates", () => {
  test("posix: config path first, then PATH name, cwd, exe folder — no Windows shims", () => {
    const c = toolCandidates("/usr/local/bin/yt-dlp", ["yt-dlp"], ["yt-dlp.exe"], { ...base, platform: "linux" });
    expect(c).toEqual(["/usr/local/bin/yt-dlp", "yt-dlp", join("/work", "yt-dlp"), join("/opt/app", "yt-dlp")]);
  });

  test("an empty/blank config path is skipped", () => {
    expect(toolCandidates("   ", ["ffmpeg"], ["ffmpeg.exe"], { ...base, platform: "linux" })[0]).toBe("ffmpeg");
  });

  test("windows: adds winget, scoop and chocolatey shims after the generic spots", () => {
    const c = toolCandidates("", ["yt-dlp"], ["yt-dlp.exe"], {
      ...base,
      platform: "win32",
      cwd: "C:\\app",
      execPath: "C:\\app\\dist\\archive.exe",
      home: "C:\\Users\\me",
      vars: { ProgramData: "C:\\ProgramData", LOCALAPPDATA: "C:\\Users\\me\\AppData\\Local" },
    });
    expect(c[0]).toBe("yt-dlp"); // PATH lookup still first
    expect(c).toContain(join("C:\\app", "yt-dlp.exe"));
    // (dirname of a Windows path is platform-dependent on the host running
    // the test, so derive the expectation the same way the code does)
    expect(c).toContain(join(dirname("C:\\app\\dist\\archive.exe"), "yt-dlp.exe"));
    expect(c).toContain(join("C:\\ProgramData", "chocolatey", "bin", "yt-dlp.exe"));
    expect(c).toContain(join("C:\\Users\\me", "scoop", "shims", "yt-dlp.exe"));
    expect(c).toContain(join("C:\\Users\\me\\AppData\\Local", "Microsoft", "WinGet", "Links", "yt-dlp.exe"));
    // generic spots come before the package-manager shims
    expect(c.indexOf(join("C:\\app", "yt-dlp.exe"))).toBeLessThan(c.indexOf(join("C:\\ProgramData", "chocolatey", "bin", "yt-dlp.exe")));
  });

  test("windows: falls back to the default ProgramData / LocalAppData when the env vars are unset", () => {
    const c = toolCandidates("", ["ffmpeg"], ["ffmpeg.exe"], { ...base, platform: "win32", home: "C:\\Users\\me", vars: {} });
    expect(c).toContain(join("C:\\ProgramData", "chocolatey", "bin", "ffmpeg.exe"));
    expect(c).toContain(join("C:\\Users\\me", "AppData", "Local", "Microsoft", "WinGet", "Links", "ffmpeg.exe"));
  });

  test("duplicates collapse while keeping first-seen order", () => {
    const c = toolCandidates("yt-dlp", ["yt-dlp", "yt-dlp"], [], { ...base, platform: "linux" });
    expect(c.filter((x) => x === "yt-dlp").length).toBe(1);
    expect(new Set(c).size).toBe(c.length);
  });

  test("multiple posix names are all tried (e.g. ffmpeg vs avconv)", () => {
    const c = toolCandidates("", ["ffmpeg", "avconv"], [], { ...base, platform: "darwin" });
    expect(c).toContain("ffmpeg");
    expect(c).toContain("avconv");
    expect(c).toContain(join("/work", "avconv"));
  });
});
