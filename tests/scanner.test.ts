// tests/scanner.test.ts — listing parse (3.4), folder-name cap (3.5) and the
// bounded probe helper every short-lived spawn now goes through (3.6).

import { beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp } from "node:fs/promises";
import { existsSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { DEFAULT_CONFIG, type Config } from "../src/config";
import { db, initDatabase } from "../src/db";
import { ingestItems, MAX_FOLDER_CHARS, parseListing } from "../src/scanner";
import { spawnBounded, tailLines } from "../src/spawn";

const cfg = (o: Partial<Config> = {}): Config => ({ ...DEFAULT_CONFIG, ...o });
const posix = process.platform === "win32" ? test.skip : test;

beforeEach(() => {
  initDatabase(":memory:");
});

describe("parseListing (3.4)", () => {
  test("the happy path", () => {
    const items = parseListing("My List|||dQw4w9WgXcQ|||Never Gonna|||212\nMy List|||abcdefghijk|||Second|||NA\n\n");
    expect(items).toEqual([
      { playlist: "My List", id: "dQw4w9WgXcQ", title: "Never Gonna", duration: 212 },
      { playlist: "My List", id: "abcdefghijk", title: "Second", duration: NaN },
    ]);
  });

  test("a separator inside the title no longer corrupts the id", () => {
    const items = parseListing("List|||dQw4w9WgXcQ|||Part A ||| Part B|||100");
    expect(items.length).toBe(1);
    expect(items[0].id).toBe("dQw4w9WgXcQ");
    expect(items[0].title).toBe("Part A ||| Part B");
    expect(items[0].duration).toBe(100);
  });

  test("a separator inside the playlist title is tolerated too", () => {
    const items = parseListing("A ||| B|||dQw4w9WgXcQ|||Title|||5");
    expect(items[0].playlist).toBe("A ||| B");
    expect(items[0].id).toBe("dQw4w9WgXcQ");
    expect(items[0].title).toBe("Title");
  });

  test("lines whose id does not validate are skipped, not inserted", () => {
    const items = parseListing(["List|||bad id|||T|||1", "List|||ab|||T|||1", "garbage", "List|||dQw4w9WgXcQ|||ok|||1"].join("\n"));
    expect(items.map((i) => i.id)).toEqual(["dQw4w9WgXcQ"]);
  });

  test("empty fields fall back to placeholders", () => {
    const [item] = parseListing("|||dQw4w9WgXcQ||||||NA");
    expect(item.playlist).toBe("playlist");
    expect(item.title).toBe("video");
  });
});

describe("ingestItems hardening (3.4 / 3.5)", () => {
  test("a very long playlist title does not abort the ingest", async () => {
    const root = await mkdtemp(join(tmpdir(), "yta-scan-"));
    const longName = "p".repeat(1000);
    const r = await ingestItems([{ id: "dQw4w9WgXcQ", title: "T", playlist: longName, duration: 100 }], cfg({ outputRoot: root }));
    expect(r.added).toBe(1);
    const dirs = readdirSync(root);
    expect(dirs.length).toBe(1);
    expect(dirs[0].length).toBeLessThanOrEqual(MAX_FOLDER_CHARS);
    const row = db.query("SELECT folder, output_directory FROM jobs WHERE id = 'dQw4w9WgXcQ'").get() as any;
    expect(existsSync(row.output_directory)).toBe(true);
  });

  test("an item with an unsafe id is skipped and logged, the rest ingest", async () => {
    const root = await mkdtemp(join(tmpdir(), "yta-scan-"));
    const r = await ingestItems(
      [
        { id: "has space", title: "T", playlist: "P", duration: 100 },
        { id: "", title: "T", playlist: "P", duration: 100 },
        { id: "dQw4w9WgXcQ", title: "T", playlist: "P", duration: 100 },
      ],
      cfg({ outputRoot: root }),
    );
    expect(r).toEqual({ found: 3, added: 1, skipped: 2 });
  });
});

describe("spawnBounded (3.6)", () => {
  const mock = resolve(import.meta.dir, "mocks", "yt-dlp");

  posix("collects output and exit code", async () => {
    const r = await spawnBounded(["sh", "-c", "echo out; echo err 1>&2; exit 3"], { timeoutMs: 5000 });
    expect(r.code).toBe(3);
    expect(r.stdout.trim()).toBe("out");
    expect(r.stderr.trim()).toBe("err");
    expect(r.timedOut).toBe(false);
  });

  posix("kills a hung child at the deadline and reports it", async () => {
    const started = Date.now();
    const r = await spawnBounded(["sh", "-c", "echo partial; sleep 30"], { timeoutMs: 300 });
    expect(r.timedOut).toBe(true);
    expect(r.code).not.toBe(0);
    expect(r.stdout.trim()).toBe("partial");
    expect(Date.now() - started).toBeLessThan(5000);
  });

  posix("honours an outer abort signal", async () => {
    const ctl = new AbortController();
    setTimeout(() => ctl.abort(), 100);
    const started = Date.now();
    const r = await spawnBounded(["sh", "-c", "sleep 30"], { timeoutMs: 60_000, signal: ctl.signal });
    expect(r.code).not.toBe(0);
    expect(r.timedOut).toBe(false);
    expect(Date.now() - started).toBeLessThan(5000);
  });

  posix("the hanging mock yt-dlp is bounded the same way", async () => {
    const dir = await mkdtemp(join(tmpdir(), "yta-scan-"));
    const r = await spawnBounded(
      ["env", "FAKE_HANG=1", mock, "-o", join(dir, "v.%(ext)s"), "https://www.youtube.com/watch?v=dQw4w9WgXcQ"],
      { timeoutMs: 400 },
    );
    expect(r.timedOut).toBe(true);
  });

  test("rejects only when the binary does not exist", async () => {
    await expect(spawnBounded(["/definitely/not/a/binary"], { timeoutMs: 1000 })).rejects.toThrow();
  });

  test("tailLines keeps the last non-empty lines", () => {
    expect(tailLines("a\n\nb\n c \n", 2)).toBe("b c");
    expect(tailLines("", 3)).toBe("");
  });
});
