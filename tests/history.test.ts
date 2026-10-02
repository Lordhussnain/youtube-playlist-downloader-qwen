// tests/history.test.ts — run_history heartbeat (plan 4.2): a row at start,
// refreshed with the live counters, pruned at startup.

import { beforeEach, describe, expect, test } from "bun:test";
import { db, initDatabase, pruneRunHistory } from "../src/db";
import { heartbeatRunHistory, startRunHistory } from "../src/history";
import { stats } from "../src/state";

beforeEach(() => {
  initDatabase(":memory:");
});

describe("run history", () => {
  test("startRunHistory inserts one row; heartbeat refreshes it in place", () => {
    startRunHistory();
    const rows = () => db.query("SELECT * FROM run_history ORDER BY id").all() as any[];
    expect(rows().length).toBe(1);
    expect(rows()[0].downloaded).toBe(0);

    stats.downloaded += 3;
    stats.failed += 1;
    heartbeatRunHistory();
    heartbeatRunHistory();
    const after = rows();
    expect(after.length).toBe(1); // updated, not re-inserted
    expect(after[0].downloaded).toBe(stats.downloaded);
    expect(after[0].failed).toBe(stats.failed);
    expect(typeof after[0].duration_seconds).toBe("number");
    expect(after[0].ended_at >= after[0].started_at).toBe(true);
  });

  test("heartbeat without a started row is a no-op, and a DB error never throws", () => {
    // Fresh DB, startRunHistory from the previous test pointed at a row id
    // that no longer exists here: the UPDATE matches nothing and must not throw.
    expect(() => heartbeatRunHistory()).not.toThrow();
    db.run("DROP TABLE run_history");
    expect(() => heartbeatRunHistory()).not.toThrow();
    expect(() => startRunHistory()).not.toThrow();
  });

  test("history is pruned to the newest rows", () => {
    for (let i = 0; i < 20; i++) db.run("INSERT INTO run_history (started_at, ended_at) VALUES (?, ?)", [`s${i}`, `e${i}`]);
    expect(pruneRunHistory(5)).toBe(15);
    const left = db.query("SELECT started_at FROM run_history ORDER BY id").all() as any[];
    expect(left.map((r) => r.started_at)).toEqual(["s15", "s16", "s17", "s18", "s19"]);
  });
});
