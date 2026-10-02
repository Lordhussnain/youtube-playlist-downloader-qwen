// src/retry.ts — pure retry/resume policy (no I/O, fully unit-tested).

/**
 * Exponential backoff with jitter, in milliseconds.
 *
 * attempt 0 → base, 1 → 2×base, 2 → 4×base … capped at maxSeconds, plus up to
 * 30% random jitter so many workers that failed together don't retry in
 * lockstep. The RNG is injectable to keep tests deterministic.
 */
export function computeBackoffMs(
  attempt: number,
  baseSeconds: number,
  maxSeconds: number,
  rnd: () => number = Math.random,
): number {
  const a = Math.max(0, Math.floor(Number.isFinite(attempt) ? attempt : 0));
  const base = Math.max(1, baseSeconds);
  const cap = Math.max(base, maxSeconds);
  const exponential = Math.min(cap, base * 2 ** a);
  const jittered = exponential * (1 + 0.3 * Math.min(1, Math.max(0, rnd())));
  return Math.round(jittered * 1000);
}

/**
 * Per-video download watchdog.
 *
 * A flat 15-minute timeout kills legitimate long downloads (a 2-hour video on
 * a slow connection needs far longer), so the timeout scales with the video's
 * real duration — 3× realtime plus 5 minutes of slack — clamped between the
 * configured minimum and ceiling. Unknown duration → the minimum.
 */
export function computeDownloadTimeoutMs(
  durationSeconds: number | null | undefined,
  opts: { minMinutes: number; maxMinutes: number },
): number {
  const minMs = Math.max(1, opts.minMinutes) * 60_000;
  const maxMs = Math.max(minMs / 60_000, opts.maxMinutes) * 60_000;
  if (!durationSeconds || !Number.isFinite(durationSeconds) || durationSeconds <= 0) return minMs;
  const scaled = (durationSeconds * 3 + 300) * 1000;
  return Math.max(minMs, Math.min(maxMs, scaled));
}

// Errors that will never succeed on retry — the video is gone, gated, or the
// URL is wrong. Re-queueing these just burns time (and bandwidth), so the
// failed-job sweep and the transient-retry path both skip them.
const PERMANENT_ERROR_PATTERNS: RegExp[] = [
  /video unavailable/i,
  /private video/i,
  /members[- ]only/i,
  /sign in to confirm your age/i,
  /age[- ]restricted/i,
  /inappropriate for some users/i,
  /removed by the uploader/i,
  /has been terminated/i,
  /account associated with this video has been terminated/i,
  /this video does not exist/i,
  /no video formats/i,
  /requested format is not available/i,
  /unsupported url/i,
  /is not a valid url/i,
  /http error 404/i,
  /http error 410/i,
  /copyright/i,
  /blocked it in your country/i,
  /not available in your country/i,
  /(?:not )?available in your country/i,
  /geo restriction/i,
  /video is unavailable in your country/i,
];

/**
 * True when a download error is permanent (the video can never be fetched) and
 * retrying is pointless.
 */
export function isPermanentDownloadError(message: string | null | undefined): boolean {
  if (!message) return false;
  return PERMANENT_ERROR_PATTERNS.some((re) => re.test(message));
}

/**
 * True when an error looks transient (network hiccups, throttling, timeouts)
 * and an immediate-ish retry is worthwhile.
 */
export function isTransientDownloadError(message: string): boolean {
  const m = message.toLowerCase();
  return [
    "unable to download",
    "connection reset",
    "timeout",
    "timed out",
    "network is unreachable",
    "err_connection",
    "temporary failure",
    "could not connect",
    "sigabrt",
    "aborted",
    "http error 429",
    "http error 5",
    "ssl",
    "eof",
    "broken pipe",
    "giving up after",
    "read error",
    "write error",
  ].some((e) => m.includes(e));
}

/**
 * True when aria2c rejected the command line itself instead of downloading:
 * exit 28 is "bad/unrecognized option was given or unexpected option argument
 * was given", and aria2c prints the offending option's help block (e.g.
 * "Possible Values: 1-16" for `-x`) right before dying.
 *
 * That is a global misconfiguration (a `-x` above aria2c's cap, a malformed
 * `--min-split-size`, …), not a video problem: every download in the batch
 * fails identically in about a second, so retrying videos only burns retry
 * budgets until the circuit breaker trips. The engine pauses itself with an
 * actionable reason instead (see workers/download.ts).
 */
export function isDownloaderArgsError(message: string | null | undefined): boolean {
  if (!message) return false;
  const m = message.toLowerCase();
  return (
    m.includes("exited with code 28") ||
    m.includes("unrecognized option") ||
    m.includes("possible values:")
  );
}

// --- Failure outcome decision ---------------------------------------------------
// The whole "what happens to a failed download" policy, as one pure function.
// workers/download.ts handleDownloadFailure() gathers the context (DB state,
// whether a partial exists on disk), calls this, and applies the outcome —
// it holds no policy of its own. Precedence, top to bottom:
//
//   pause (global, then per-job user hold) → signature → downloader-args →
//   corrupt (resume budget) → archive-scrub → live → transient → permanent/budget

export interface FailureContext {
  /** The error message (String(err.message || err)). */
  error: string;
  /** Global engine pause is in effect. */
  isPaused: boolean;
  /** The dashboard asked for THIS job to pause while it was in flight. */
  userPaused: boolean;
  /** jobs.retry_count as it is now. */
  retryCount: number;
  /** perVideoCap(config): min(maxRetryAttempts, maxFailuresPerVideo). */
  retryCap: number;
  /** jobs.resume_count as it is now. */
  resumeCount: number;
  /** config.maxResumeAttempts. */
  maxResume: number;
  /** jobs.progress / jobs.best_progress (percent). */
  progress: number;
  bestProgress: number;
  /** A resumable partial exists on disk for this job. */
  hasPartial: boolean;
}

export type FailureOutcome =
  /** Park as 'paused' (reason null → auto-resumable, "user" → held). */
  | { kind: "park"; pauseReason: "user" | null }
  /** Run `yt-dlp -U`, then re-queue with a clean budget. */
  | { kind: "self-update" }
  /** Park the job and pause the engine (BAD_DOWNLOADER_ARGS). */
  | { kind: "bad-args" }
  /** Corrupt partial, budget left: keep it, bump resume_count, back off. */
  | { kind: "resume"; resumeCount: number; backoffAttempt: number }
  /** Corrupt partial, budget spent (or none on disk): discard and restart. */
  | { kind: "restart-fresh"; retryDelta: 1; discardPartial: true }
  /** Archive says downloaded but the file is gone: scrub + restart. */
  | { kind: "archive-scrub"; retryDelta: 1; discardPartial: true; scrubArchive: true }
  /** Stream is live now: park as waiting_live until a rescan or manual retry. */
  | { kind: "wait-live" }
  /** Transient: re-queue, forgiving the budget when progress advanced. */
  | { kind: "transient"; retryCount: number; bestProgress: number; backoffAttempt: number }
  /** Permanent/unknown: spend budget; 'failed' trips the breaker. */
  | { kind: "permanent"; status: "failed" | "pending"; retryCount: number; tripBreaker: boolean };

/** Message classes that mean the partial itself is unusable. */
export function isCorruptPartialError(message: string): boolean {
  const m = message.toLowerCase();
  return m.includes("unable to resume") || m.includes("incomplete") || m.includes("corrupt");
}

/** yt-dlp's extractor broke (signature / "unable to extract"). */
export function isSignatureError(message: string): boolean {
  const m = message.toLowerCase();
  return m.includes("signature") || m.includes("unable to extract");
}

/** --download-archive has the id but our copy is gone. */
export function isArchiveMismatchError(message: string): boolean {
  return message.toLowerCase().includes("output file could not be located");
}

/** archiveLiveStreams: the video is a live stream right now. */
export function isLiveNowError(message: string): boolean {
  const m = message.toLowerCase();
  return m.includes("does not pass filter") || m.includes("is live") || m.includes("live event");
}

export function decideFailureOutcome(ctx: FailureContext): FailureOutcome {
  if (ctx.isPaused) return { kind: "park", pauseReason: null };
  // A per-job user pause outranks every failure class: the operator asked for
  // this video to stop, so it parks (partial kept) instead of being written
  // back to 'pending' by the transient branch.
  if (ctx.userPaused) return { kind: "park", pauseReason: "user" };

  const msg = ctx.error;
  if (isSignatureError(msg)) return { kind: "self-update" };
  if (isDownloaderArgsError(msg)) return { kind: "bad-args" };

  if (isCorruptPartialError(msg)) {
    const resumeCount = (ctx.resumeCount || 0) + 1;
    if (resumeCount >= Math.max(1, ctx.maxResume) || !ctx.hasPartial) {
      return { kind: "restart-fresh", retryDelta: 1, discardPartial: true };
    }
    return { kind: "resume", resumeCount, backoffAttempt: resumeCount };
  }

  if (isArchiveMismatchError(msg)) {
    return { kind: "archive-scrub", retryDelta: 1, discardPartial: true, scrubArchive: true };
  }

  if (isLiveNowError(msg)) return { kind: "wait-live" };

  if (isTransientDownloadError(msg)) {
    // The retry budget only shrinks when the video makes no forward progress:
    // a flaky connection that keeps advancing is forgiven, a video stuck at
    // the same percentage eventually exhausts its budget.
    const forgiven = ctx.progress > ctx.bestProgress;
    const retryCount = forgiven ? ctx.retryCount : ctx.retryCount + 1;
    return {
      kind: "transient",
      retryCount,
      bestProgress: Math.max(ctx.bestProgress, ctx.progress),
      backoffAttempt: retryCount,
    };
  }

  const retryCount = ctx.retryCount + 1;
  const status = retryCount >= Math.max(1, ctx.retryCap) ? "failed" : "pending";
  return { kind: "permanent", status, retryCount, tripBreaker: status === "failed" };
}
