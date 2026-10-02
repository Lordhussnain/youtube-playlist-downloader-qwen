// tests/convert.test.ts — converter crash-window recovery.
//
// findConvertedOutput is the adoption probe: when a converter finds the source
// gone but a previous attempt already wrote the converted media (crash between
// the encode and the database update), the job is finished from that output
// instead of failing — or worse, re-downloading the whole video.

import { describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { findConvertedOutput, planConversion } from "../src/workers/convert";

const dirs: string[] = [];
async function makeDir(): Promise<string> {
  const d = await mkdtemp(join(tmpdir(), "yta-convert-"));
  dirs.push(d);
  return d;
}

describe("findConvertedOutput", () => {
  test("adopts the mp3 a crashed mp3 encode left behind", async () => {
    const dir = await makeDir();
    const mp3 = join(dir, "001 - Video.mp3");
    await writeFile(mp3, "audio");
    expect(findConvertedOutput(join(dir, "001 - Video.webm"), true)).toBe(mp3);
  });

  test("prefers the mp4 remux; a kept mkv source is its own final container", async () => {
    const dir = await makeDir();
    const mkv = join(dir, "002 - Video.mkv");
    await writeFile(mkv, "mkv");
    // A multi-audio mkv source is already the final container — the probe
    // must not "adopt" it as a conversion of itself.
    expect(findConvertedOutput(mkv, false)).toBe("");
    // A webm source with a finished mp4 remux next to it: the mp4 is adopted.
    await writeFile(join(dir, "002 - Video.mp4"), "mp4");
    expect(findConvertedOutput(join(dir, "002 - Video.webm"), false)).toBe(join(dir, "002 - Video.mp4"));
  });

  test("returns empty when only unrelated files exist", async () => {
    const dir = await makeDir();
    await writeFile(join(dir, "003 - Video.info.json"), "{}");
    await writeFile(join(dir, "003 - Video.en.vtt"), "WEBVTT");
    expect(findConvertedOutput(join(dir, "003 - Video.webm"), true)).toBe("");
    expect(findConvertedOutput(join(dir, "003 - Video.webm"), false)).toBe("");
  });

  test("never adopts the source itself as its own output", async () => {
    const dir = await makeDir();
    await writeFile(join(dir, "004 - Video.mp3"), "already mp3");
    // An .mp3 source with wantsMp3 has nothing to convert; the probe must not
    // "adopt" the source path as its own output.
    expect(findConvertedOutput(join(dir, "004 - Video.mp3"), true)).toBe("");
  });

  test("handles an empty source path", () => {
    expect(findConvertedOutput("", true)).toBe("");
  });
});

describe("findConvertedOutput — the newer containers (4.3)", () => {
  test("adopts a finished .webm / .m4a / .mkv for the matching target", async () => {
    const dir = await makeDir();
    const webm = join(dir, "010 - V.webm");
    await writeFile(webm, "x");
    expect(findConvertedOutput(join(dir, "010 - V.mp4"), false, "webm")).toBe(webm);
    const m4a = join(dir, "011 - V.m4a");
    await writeFile(m4a, "x");
    expect(findConvertedOutput(join(dir, "011 - V.webm"), false, "m4a")).toBe(m4a);
    const mkv = join(dir, "012 - V.mkv");
    await writeFile(mkv, "x");
    expect(findConvertedOutput(join(dir, "012 - V.webm"), false, "mkv")).toBe(mkv);
  });

  test("prefers the requested target when several outputs exist", async () => {
    const dir = await makeDir();
    await writeFile(join(dir, "013 - V.mp4"), "x");
    await writeFile(join(dir, "013 - V.mkv"), "x");
    expect(findConvertedOutput(join(dir, "013 - V.webm"), false, "mkv")).toBe(join(dir, "013 - V.mkv"));
    expect(findConvertedOutput(join(dir, "013 - V.webm"), false, "mp4")).toBe(join(dir, "013 - V.mp4"));
  });
});

describe("planConversion (4.3)", () => {
  const src = join("/arc", "P", "001 - V.webm");

  test("mp3 target: audio-only libmp3lame encode of the first track", () => {
    const p = planConversion(src, "mp4", true)!;
    expect(p.kind).toBe("encode-mp3");
    expect(p.outputPath).toBe(join("/arc", "P", "001 - V.mp3"));
    expect(p.args).toContain("-vn");
    expect(p.args[p.args.indexOf("-c:a") + 1]).toBe("libmp3lame");
    expect(p.args[p.args.indexOf("-map") + 1]).toBe("0:a:0");
    expect(p.args[p.args.length - 1]).toBe(p.outputPath);
    // already mp3 → nothing to do
    expect(planConversion(join("/arc", "P", "a.mp3"), "mp4", true)).toBeNull();
  });

  test("mp4 target: copy video, AAC audio, faststart", () => {
    const p = planConversion(src, "mp4", false)!;
    expect(p.kind).toBe("remux");
    expect(p.label).toBe("remux to .mp4");
    expect(p.outputPath).toBe(join("/arc", "P", "001 - V.mp4"));
    expect(p.args[p.args.indexOf("-c:v") + 1]).toBe("copy");
    expect(p.args[p.args.indexOf("-c:a") + 1]).toBe("aac");
    expect(p.args).toContain("+faststart");
    expect(p.args).toContain("0:a?"); // every audio stream, not just the first
  });

  test("mkv / webm / m4a targets: pure stream copy, every audio track kept", () => {
    for (const fmt of ["mkv", "webm", "m4a"]) {
      const p = planConversion(join("/arc", "P", "001 - V.mp4"), fmt, false)!;
      expect(p.kind).toBe("remux");
      expect(p.outputPath.endsWith(`.${fmt}`)).toBe(true);
      expect(p.args[p.args.indexOf("-c:a") + 1]).toBe("copy");
      expect(p.args).not.toContain("+faststart");
      expect(p.args).toContain("0:a?");
    }
  });

  test("already in the target container → null (case-insensitive target)", () => {
    expect(planConversion(join("/arc", "P", "001 - V.mkv"), "MKV", false)).toBeNull();
    expect(planConversion(join("/arc", "P", "001 - V.mp4"), "", false)).toBeNull(); // default mp4
  });

  test("multi-audio + mkv: the plan is a remux that keeps all tracks (the worker only adds a status line)", () => {
    const p = planConversion(join("/arc", "P", "multi.webm"), "mkv", false)!;
    expect(p.args.filter((a) => a === "-map").length).toBe(2);
    expect(p.args[p.args.indexOf("-c:a") + 1]).toBe("copy");
  });
});
