// tests/dedupe.test.ts — content-hash dedupe (plan 5.2): candidate lookup,
// the atomic hard-link swap, and the post-hash hook's off/on behaviour.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { db, initDatabase } from "../src/db";
import { claimDuplicate, dedupeAfterHash, findDuplicateByHash, linkDuplicate } from "../src/dedupe";
import { hashFile } from "../src/util";

let dir: string;
beforeEach(async () => {
  initDatabase(":memory:");
  dir = await mkdtemp(join(tmpdir(), "yta-dedupe-"));
});
afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

function insertJob(id: string, file: string | null, integrity: string | null, createdAt = "2026-01-01 00:00:00") {
  db.run(
    `INSERT INTO jobs (id, url, title, output_directory, target_format, download_status, file_path, integrity, created_at)
     VALUES (?, ?, ?, ?, 'mp4', 'downloaded', ?, ?, ?)`,
    [id, `https://www.youtube.com/watch?v=${id}`, id, dir, file, integrity, createdAt],
  );
}

const sameInode = async (a: string, b: string) => {
  const [x, y] = await Promise.all([stat(a), stat(b)]);
  return x.ino === y.ino && x.dev === y.dev;
};

describe("findDuplicateByHash", () => {
  test("returns the oldest other job whose file still exists", async () => {
    const f1 = join(dir, "one.mp4");
    const f2 = join(dir, "two.mp4");
    await writeFile(f1, "same");
    await writeFile(f2, "same");
    const h = await hashFile(f1);
    insertJob("gone0000001", join(dir, "missing.mp4"), h, "2025-01-01 00:00:00"); // oldest but gone
    insertJob("orig0000001", f1, h, "2025-06-01 00:00:00");
    insertJob("dupe0000001", f2, h, "2026-01-01 00:00:00");
    expect(findDuplicateByHash(h, "dupe0000001")?.id).toBe("orig0000001");
    expect(findDuplicateByHash(h, "orig0000001")?.id).toBe("dupe0000001");
    // Rows that are themselves duplicates are never offered as an original.
    db.run("UPDATE jobs SET duplicate_of = 'orig0000001' WHERE id = 'dupe0000001'");
    expect(findDuplicateByHash(h, "gone0000001")?.id).toBe("orig0000001");
    expect(findDuplicateByHash("deadbeef", "dupe0000001")).toBeNull();
    expect(findDuplicateByHash("", "dupe0000001")).toBeNull();
  });
});

describe("linkDuplicate", () => {
  test("swaps the duplicate for a hard link to the original, atomically", async () => {
    const orig = join(dir, "orig.mp4");
    const dupe = join(dir, "dupe.mp4");
    await writeFile(orig, "payload");
    await writeFile(dupe, "payload");
    expect(await sameInode(orig, dupe)).toBe(false);
    expect(await linkDuplicate(dupe, orig)).toBe("hardlinked");
    expect(await sameInode(orig, dupe)).toBe(true);
    expect(await readFile(dupe, "utf8")).toBe("payload");
    expect(await linkDuplicate(dupe, orig)).toBe("already-linked");
    expect(await linkDuplicate(orig, orig)).toBe("already-linked");
  });

  test("refuses when either side is missing or sizes disagree", async () => {
    const orig = join(dir, "orig.mp4");
    const dupe = join(dir, "dupe.mp4");
    await writeFile(orig, "payload");
    expect(await linkDuplicate(dupe, orig)).toBe("skipped");
    await writeFile(dupe, "payload-but-longer");
    expect(await linkDuplicate(dupe, orig)).toBe("skipped");
    expect(await readFile(dupe, "utf8")).toBe("payload-but-longer");
  });
});

describe("claimDuplicate (atomic)", () => {
  test("two identical files finishing together: exactly one becomes the duplicate", async () => {
    const a = join(dir, "a.mp4");
    const b = join(dir, "b.mp4");
    await writeFile(a, "same");
    await writeFile(b, "same");
    const h = await hashFile(a);
    insertJob("aaaa0000001", a, h);
    insertJob("bbbb0000001", b, h);
    const first = claimDuplicate(h, "bbbb0000001");
    const second = claimDuplicate(h, "aaaa0000001");
    expect(first?.id).toBe("aaaa0000001");
    expect(second).toBeNull();
    const rows = db.query("SELECT id, duplicate_of FROM jobs ORDER BY id").all() as any[];
    expect(rows).toEqual([
      { id: "aaaa0000001", duplicate_of: null },
      { id: "bbbb0000001", duplicate_of: "aaaa0000001" },
    ]);
  });

  test("a failed link releases the claim", async () => {
    const a = join(dir, "a.mp4");
    const b = join(dir, "b.mp4");
    await writeFile(a, "same");
    await writeFile(b, "same-but-different-size"); // hash claims equal, size says no → link skipped
    const h = "feedfacefeedface";
    insertJob("aaaa0000001", a, h);
    insertJob("bbbb0000001", b, h);
    expect(await dedupeAfterHash({ id: "bbbb0000001", title: "B" }, b, h, true)).toBeNull();
    expect((db.query("SELECT duplicate_of FROM jobs WHERE id = 'bbbb0000001'").get() as any).duplicate_of).toBeNull();
  });
});

describe("dedupeAfterHash", () => {
  test("off by default: nothing happens even with a match", async () => {
    const orig = join(dir, "a.mp4");
    const dupe = join(dir, "b.mp4");
    await writeFile(orig, "x");
    await writeFile(dupe, "x");
    const h = await hashFile(orig);
    insertJob("orig0000001", orig, h);
    insertJob("dupe0000001", dupe, h);
    expect(await dedupeAfterHash({ id: "dupe0000001", title: "B" }, dupe, h, false)).toBeNull();
    expect(await sameInode(orig, dupe)).toBe(false);
  });

  test("on: links the file and records duplicate_of; the original is untouched", async () => {
    const orig = join(dir, "a.mp4");
    const dupe = join(dir, "b.mp4");
    await writeFile(orig, "x");
    await writeFile(dupe, "x");
    const h = await hashFile(orig);
    insertJob("orig0000001", orig, h, "2025-01-01 00:00:00");
    insertJob("dupe0000001", dupe, h, "2026-01-01 00:00:00");
    expect(await dedupeAfterHash({ id: "dupe0000001", title: "B" }, dupe, h, true)).toBe("orig0000001");
    // Now the original asks (the simultaneous-finish race): someone already
    // points at it, so it stays the original.
    expect(await dedupeAfterHash({ id: "orig0000001", title: "A" }, orig, h, true)).toBeNull();
    expect(await sameInode(orig, dupe)).toBe(true);
    const row = db.query("SELECT duplicate_of, file_path FROM jobs WHERE id = 'dupe0000001'").get() as any;
    expect(row.duplicate_of).toBe("orig0000001");
    expect(row.file_path).toBe(dupe); // the job still owns a valid path
    expect((db.query("SELECT duplicate_of FROM jobs WHERE id = 'orig0000001'").get() as any).duplicate_of).toBeNull();
  });

  test("no hash, or no other copy, is a no-op", async () => {
    const only = join(dir, "only.mp4");
    await writeFile(only, "y");
    const h = await hashFile(only);
    insertJob("only0000001", only, h);
    expect(await dedupeAfterHash({ id: "only0000001", title: "O" }, only, h, true)).toBeNull();
    expect(await dedupeAfterHash({ id: "only0000001", title: "O" }, only, null, true)).toBeNull();
  });
});
