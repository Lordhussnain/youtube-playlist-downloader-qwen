// tests/metadata-args.test.ts — the sidecar pass argv (plan 4.3): subtitles
// with --convert-subs in the configured format, thumbnail → jpg, description,
// info.json, all written next to the media file under its basename.

import { describe, expect, test } from "bun:test";
import { join } from "node:path";
import { buildMetadataArgs } from "../src/workers/metadata";

const job = (o: Record<string, unknown> = {}) => ({
  url: "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
  file_path: join("/arc", "Playlist", "001 - Title [dQw4w9WgXcQ].mkv"),
  want_subtitles: 0,
  want_thumbnail: 0,
  want_description: 0,
  ...o,
});
const cfg = (o: Record<string, unknown> = {}) => ({ cookiesFile: "", subtitleFormat: "srt", writeInfoJson: false, ...o }) as any;

describe("buildMetadataArgs", () => {
  test("writes next to the media file with the same basename and never downloads", () => {
    const args = buildMetadataArgs(job(), cfg());
    expect(args[0]).toBe("https://www.youtube.com/watch?v=dQw4w9WgXcQ");
    expect(args).toContain("--skip-download");
    expect(args).toContain("--no-simulate");
    const o = args.indexOf("-o");
    expect(args[o + 1]).toBe(join("/arc", "Playlist", "001 - Title [dQw4w9WgXcQ].%(ext)s"));
    // nothing requested → no sidecar flags at all
    for (const f of ["--write-subs", "--write-thumbnail", "--write-description", "--write-info-json"]) {
      expect(args).not.toContain(f);
    }
  });

  test("subtitles: all languages, auto-subs, converted to the configured format", () => {
    const srt = buildMetadataArgs(job({ want_subtitles: 1 }), cfg({ subtitleFormat: "srt" }));
    expect(srt).toContain("--write-subs");
    expect(srt).toContain("--write-auto-subs");
    expect(srt[srt.indexOf("--sub-langs") + 1]).toBe("all.*");
    expect(srt[srt.indexOf("--convert-subs") + 1]).toBe("srt");

    const vtt = buildMetadataArgs(job({ want_subtitles: 1 }), cfg({ subtitleFormat: "vtt" }));
    expect(vtt[vtt.indexOf("--convert-subs") + 1]).toBe("vtt");

    // An empty/missing subtitleFormat falls back to srt rather than emitting
    // a dangling flag.
    const fallback = buildMetadataArgs(job({ want_subtitles: 1 }), cfg({ subtitleFormat: "" }));
    expect(fallback[fallback.indexOf("--convert-subs") + 1]).toBe("srt");
  });

  test("thumbnail is converted to jpg; description and info.json are opt-in", () => {
    const args = buildMetadataArgs(job({ want_thumbnail: 1, want_description: 1 }), cfg({ writeInfoJson: true }));
    expect(args[args.indexOf("--convert-thumbnails") + 1]).toBe("jpg");
    expect(args).toContain("--write-thumbnail");
    expect(args).toContain("--write-description");
    expect(args).toContain("--write-info-json");
  });

  test("cookies ride along when configured", () => {
    const none = buildMetadataArgs(job(), cfg({ cookiesFile: "" }));
    expect(none).not.toContain("--cookies");
  });
});
