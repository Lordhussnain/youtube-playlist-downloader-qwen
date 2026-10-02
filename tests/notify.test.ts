// tests/notify.test.ts — webhook notifications (plan 5.3): event filtering,
// Discord vs generic bodies, failure batching, the queue-drained edge, and
// the pause/resume hooks — all through an injected transport.

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { DEFAULT_CONFIG, type Config } from "../src/config";
import { initDatabase } from "../src/db";
import {
  buildWebhookBody,
  FAILURE_BATCH_MAX,
  flushFailureNotifications,
  isDiscordWebhook,
  notify,
  observeQueueState,
  queueFailureNotification,
  resetFailureBatch,
  resetQueueObserver,
  setNotifyConfigReader,
  setNotifyTransport,
} from "../src/notify";
import { setConfig } from "../src/state";
import { triggerPause, triggerResume } from "../src/resilience";

const cfg = (o: Partial<Config> = {}): Config => ({ ...DEFAULT_CONFIG, webhookUrl: "https://hooks.example.com/x", ...o });

let sent: { url: string; body: any }[] = [];
let failNext = false;

beforeEach(() => {
  initDatabase(":memory:");
  sent = [];
  failNext = false;
  setNotifyTransport(async (url, body) => {
    if (failNext) throw new Error("HTTP 500");
    sent.push({ url, body });
  });
  resetFailureBatch();
  resetQueueObserver();
  setNotifyConfigReader(() => cfg({ notifyOn: ["failure", "pause", "resume", "complete"] }));
});

afterEach(() => {
  setNotifyTransport(null);
  resetFailureBatch();
  triggerResume();
});

describe("notify", () => {
  test("sends only configured events, and nothing without a URL", async () => {
    expect(await notify(cfg({ notifyOn: ["pause"] }), "pause", "p")).toBe(true);
    expect(await notify(cfg({ notifyOn: ["pause"] }), "failure", "f")).toBe(false);
    expect(await notify(cfg({ webhookUrl: "", notifyOn: ["pause"] }), "pause", "p")).toBe(false);
    expect(sent.length).toBe(1);
    expect(sent[0]!.body.event).toBe("pause");
    expect(sent[0]!.body.message).toBe("p");
    expect(typeof sent[0]!.body.at).toBe("string");
  });

  test("a transport failure is swallowed (logged), never thrown", async () => {
    failNext = true;
    expect(await notify(cfg({ notifyOn: ["pause"] }), "pause", "p")).toBe(false);
  });

  test("Discord webhooks get a {content} body; others the JSON envelope", () => {
    const payload = { event: "pause" as const, message: "hello", at: "now" };
    expect(isDiscordWebhook("https://discord.com/api/webhooks/1/abc")).toBe(true);
    expect(isDiscordWebhook("https://discordapp.com/api/webhooks/1/abc")).toBe(true);
    expect(isDiscordWebhook("https://hooks.slack.com/x")).toBe(false);
    expect(isDiscordWebhook("not a url")).toBe(false);
    const d = buildWebhookBody("https://discord.com/api/webhooks/1/abc", payload) as any;
    expect(d.content).toBe("hello");
    expect(d.event).toBeUndefined();
    const g = buildWebhookBody("https://hooks.example.com/x", payload) as any;
    expect(g.event).toBe("pause");
    // Discord's 2000-char cap is respected
    const long = buildWebhookBody("https://discord.com/api/webhooks/1/abc", { ...payload, message: "x".repeat(5000) }) as any;
    expect(long.content.length).toBeLessThanOrEqual(1900);
  });
});

describe("failure batching", () => {
  test("several failures within the window become one message", async () => {
    queueFailureNotification({ id: "a", title: "A", stage: "download", error: "Video unavailable" }, { batchMs: 20 });
    queueFailureNotification({ id: "b", title: "B", stage: "convert", error: "ffmpeg died" }, { batchMs: 20 });
    expect(sent.length).toBe(0); // not yet
    await Bun.sleep(60);
    expect(sent.length).toBe(1);
    const body = sent[0]!.body;
    expect(body.event).toBe("failure");
    expect(body.message).toContain("2 job(s) failed permanently");
    expect(body.message).toContain("[download] A (a): Video unavailable");
    expect(body.message).toContain("[convert] B (b)");
    expect(body.details.count).toBe(2);
  });

  test("the batch flushes early at FAILURE_BATCH_MAX and truncates the listing", async () => {
    for (let i = 0; i < FAILURE_BATCH_MAX; i++) {
      queueFailureNotification({ id: `v${i}`, title: `T${i}`, stage: "download", error: "x" }, { batchMs: 60_000 });
    }
    await Bun.sleep(10);
    expect(sent.length).toBe(1);
    expect(sent[0]!.body.message).toContain(`… and ${FAILURE_BATCH_MAX - 15} more`);
  });

  test("nothing is queued when failure notifications are off; flush is idempotent", async () => {
    setNotifyConfigReader(() => cfg({ notifyOn: ["pause"] }));
    queueFailureNotification({ id: "a", title: "A", stage: "download", error: "x" }, { batchMs: 5 });
    expect(await flushFailureNotifications()).toBe(false);
    await Bun.sleep(20);
    expect(sent.length).toBe(0);
  });

  test("flush at shutdown sends a pending batch immediately", async () => {
    queueFailureNotification({ id: "a", title: "A", stage: "download", error: "x" }, { batchMs: 60_000 });
    expect(await flushFailureNotifications()).toBe(true);
    expect(sent.length).toBe(1);
  });
});

describe("queue drained", () => {
  const snap = (queued: number, extra: Partial<Parameters<typeof observeQueueState>[1]> = {}) => ({
    queued,
    converting: 0,
    metadataPending: 0,
    downloaded: 5,
    failedAny: 1,
    total: 6,
    ...extra,
  });

  test("fires once on the busy → idle edge, not while idle", async () => {
    const c = cfg({ notifyOn: ["complete"] });
    expect(observeQueueState(c, snap(0))).toBe(false); // idle from the start: nothing
    expect(observeQueueState(c, snap(3))).toBe(false); // busy
    expect(observeQueueState(c, snap(0, { converting: 1 }))).toBe(false); // still busy (converting)
    expect(observeQueueState(c, snap(0))).toBe(true); // drained
    expect(observeQueueState(c, snap(0))).toBe(false); // stays quiet
    await Bun.sleep(5);
    expect(sent.length).toBe(1);
    expect(sent[0]!.body.message).toContain("5 downloaded, 1 failed, 6 total");
  });
});

describe("pause / resume hooks", () => {
  test("triggerPause and triggerResume notify through the live config", async () => {
    setConfig(cfg({ notifyOn: ["pause", "resume"] }));
    triggerPause("LOW_DISK_SPACE (0.5GB < 10GB)");
    triggerPause("LOW_DISK_SPACE (0.5GB < 10GB)"); // same reason again: no duplicate
    await Bun.sleep(5);
    expect(sent.length).toBe(1);
    expect(sent[0]!.body.event).toBe("pause");
    expect(sent[0]!.body.message).toContain("LOW_DISK_SPACE");
    triggerResume();
    triggerResume(); // not paused: no second resume event
    await Bun.sleep(5);
    expect(sent.length).toBe(2);
    expect(sent[1]!.body.event).toBe("resume");
  });
});
