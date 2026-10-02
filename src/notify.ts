// src/notify.ts — webhook notifications (Discord or any JSON endpoint).
//
// Operators running the engine headless want to hear about three things
// without watching a dashboard: the engine paused itself (cookies expired,
// disk full, circuit breaker), a batch of videos failed for good, and the
// queue drained. This module turns those moments into one HTTP POST each.
//
// Design rules:
//   • fire-and-forget — a webhook can never slow or fail the pipeline; errors
//     are logged once per failure, never thrown
//   • failures are BATCHED: permanent failures arrive in bursts (a private
//     playlist, an outage), so they are collected for FAILURE_BATCH_MS (or
//     until FAILURE_BATCH_MAX) and sent as one message
//   • Discord webhooks get a `{content}` body (their required shape);
//     everything else gets a plain JSON event envelope
//   • config is read per call (gotcha 16): toggling notifyOn or the URL from
//     the dashboard takes effect immediately

import { logError } from "./logger";
import type { Config } from "./config";

export type NotifyEvent = "failure" | "pause" | "resume" | "complete";

export interface NotifyPayload {
  event: NotifyEvent;
  message: string;
  at: string;
  /** Event-specific details (job list, pause reason, counters). */
  details?: Record<string, unknown>;
}

export const FAILURE_BATCH_MS = 30_000;
export const FAILURE_BATCH_MAX = 25;
const REQUEST_TIMEOUT_MS = 10_000;

/** Injectable transport so tests never hit the network. */
export type NotifyTransport = (url: string, body: unknown) => Promise<void>;

let transport: NotifyTransport = defaultTransport;
export function setNotifyTransport(t: NotifyTransport | null): void {
  transport = t ?? defaultTransport;
}

async function defaultTransport(url: string, body: unknown): Promise<void> {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), REQUEST_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json", "user-agent": "youtube-archive-engine" },
      body: JSON.stringify(body),
      signal: ctl.signal,
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
  } finally {
    clearTimeout(timer);
  }
}

export function isDiscordWebhook(url: string): boolean {
  try {
    const h = new URL(url).hostname.toLowerCase();
    return h === "discord.com" || h === "discordapp.com" || h.endsWith(".discord.com");
  } catch {
    return false;
  }
}

/** Shape the body for the destination. Exported for tests. */
export function buildWebhookBody(url: string, payload: NotifyPayload): unknown {
  if (isDiscordWebhook(url)) {
    // Discord rejects bodies without content/embeds and caps content at 2000.
    return { content: payload.message.slice(0, 1900), username: "YouTube Archive" };
  }
  return payload;
}

function wanted(config: Pick<Config, "webhookUrl" | "notifyOn">, event: NotifyEvent): boolean {
  return !!config.webhookUrl && Array.isArray(config.notifyOn) && config.notifyOn.includes(event);
}

/** Send one event now (if configured). Never throws. */
export async function notify(
  config: Pick<Config, "webhookUrl" | "notifyOn">,
  event: NotifyEvent,
  message: string,
  details?: Record<string, unknown>,
): Promise<boolean> {
  if (!wanted(config, event)) return false;
  const payload: NotifyPayload = { event, message, at: new Date().toISOString(), details };
  try {
    await transport(config.webhookUrl, buildWebhookBody(config.webhookUrl, payload));
    return true;
  } catch (e: any) {
    logError("notify", `${event} webhook failed: ${e?.message || e}`);
    return false;
  }
}

// --- Failure batching -------------------------------------------------------

interface FailedItem {
  id: string;
  title: string;
  stage: string;
  error: string;
}
let pendingFailures: FailedItem[] = [];
let flushTimer: ReturnType<typeof setTimeout> | null = null;
let readConfig: () => Pick<Config, "webhookUrl" | "notifyOn"> = () => ({ webhookUrl: "", notifyOn: [] });

/** Where the batcher reads the live config from (engine wires getConfig). */
export function setNotifyConfigReader(reader: () => Pick<Config, "webhookUrl" | "notifyOn">): void {
  readConfig = reader;
}

/**
 * Record a permanent failure. Batched: the message goes out after
 * FAILURE_BATCH_MS or once FAILURE_BATCH_MAX items are waiting.
 */
export function queueFailureNotification(item: FailedItem, opts: { batchMs?: number } = {}): void {
  if (!wanted(readConfig(), "failure")) return;
  pendingFailures.push(item);
  if (pendingFailures.length >= FAILURE_BATCH_MAX) {
    void flushFailureNotifications();
    return;
  }
  if (!flushTimer) {
    flushTimer = setTimeout(() => void flushFailureNotifications(), opts.batchMs ?? FAILURE_BATCH_MS);
    // never keep the process alive just for a pending webhook
    (flushTimer as any).unref?.();
  }
}

/** Send whatever is waiting (also called at shutdown). */
export async function flushFailureNotifications(): Promise<boolean> {
  if (flushTimer) {
    clearTimeout(flushTimer);
    flushTimer = null;
  }
  const batch = pendingFailures;
  pendingFailures = [];
  if (batch.length === 0) return false;
  const lines = batch.slice(0, 15).map((f) => `• [${f.stage}] ${f.title} (${f.id}): ${f.error.slice(0, 140)}`);
  if (batch.length > 15) lines.push(`… and ${batch.length - 15} more`);
  const message = `❌ ${batch.length} job(s) failed permanently\n${lines.join("\n")}`;
  return notify(readConfig(), "failure", message, { count: batch.length, jobs: batch });
}

/** Test hook: drop any pending batch. */
export function resetFailureBatch(): void {
  if (flushTimer) clearTimeout(flushTimer);
  flushTimer = null;
  pendingFailures = [];
}

// --- Queue drained ------------------------------------------------------------
// "complete" fires on the busy → idle edge of the pipeline, not on every
// finished video. The caller feeds it the stats snapshot each tick.

let wasBusy = false;
export function observeQueueState(
  config: Pick<Config, "webhookUrl" | "notifyOn">,
  snap: { queued: number; converting: number; metadataPending: number; downloaded: number; failedAny: number; total: number },
): boolean {
  const busy = snap.queued + snap.converting + snap.metadataPending > 0;
  const drained = wasBusy && !busy;
  wasBusy = busy;
  if (!drained) return false;
  void notify(
    config,
    "complete",
    `✅ Queue drained: ${snap.downloaded} downloaded, ${snap.failedAny} failed, ${snap.total} total`,
    { downloaded: snap.downloaded, failed: snap.failedAny, total: snap.total },
  );
  return true;
}

/** Test hook. */
export function resetQueueObserver(): void {
  wasBusy = false;
}
