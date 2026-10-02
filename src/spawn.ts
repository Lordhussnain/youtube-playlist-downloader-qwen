// src/spawn.ts — one bounded "run a tool, collect its output" helper.
//
// Leaf module (no engine imports). Every short-lived probe in the codebase —
// playlist listing, channel-id resolution, audio-track probing, cookie
// validation, binary discovery, ffmpeg stream counting — used to be an
// unbounded `Bun.spawn` + `await proc.exited`. A yt-dlp stuck on a dead
// socket, or a binary waiting on a tty it will never get, hung the caller
// forever: a request handler, the RSS tick, or the download worker about to
// start a job. Every one of those now goes through here and gets a deadline.
//
// The long-running children (yt-dlp download, ffmpeg conversion, metadata
// pass, `yt-dlp -U`) keep their own lifecycle code in the workers because they
// stream progress and are registered for pause/shutdown; this helper is for
// run-to-completion probes only.

export interface BoundedResult {
  code: number;
  stdout: string;
  stderr: string;
  /** The deadline hit and the child was SIGKILLed. `code` is then non-zero. */
  timedOut: boolean;
}

export interface BoundedOptions {
  /** Hard deadline in ms. */
  timeoutMs: number;
  /** Optional outer signal (e.g. the engine's abortController) to honour too. */
  signal?: AbortSignal;
  /** Cap on captured bytes per stream (default 8 MiB); the rest is dropped. */
  maxOutputBytes?: number;
}

/**
 * Spawn `cmd`, drain both pipes, and resolve when it exits or the deadline
 * passes (the child is SIGKILLed on timeout). Never rejects for a non-zero
 * exit; rejects only when the binary cannot be spawned at all (ENOENT), which
 * callers already treat as "tool unavailable".
 */
export async function spawnBounded(cmd: string[], opts: BoundedOptions): Promise<BoundedResult> {
  const ctl = new AbortController();
  const onOuterAbort = () => ctl.abort();
  opts.signal?.addEventListener("abort", onOuterAbort, { once: true });
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    ctl.abort();
  }, opts.timeoutMs);
  let proc: Bun.Subprocess<"ignore", "pipe", "pipe"> | null = null;
  try {
    proc = Bun.spawn(cmd, { stdin: "ignore", stdout: "pipe", stderr: "pipe", signal: ctl.signal, env: process.env });
    const cap = opts.maxOutputBytes ?? 8 * 1024 * 1024;
    const out = { text: "" };
    const err = { text: "" };
    const drains = Promise.all([drain(proc.stdout, cap, out), drain(proc.stderr, cap, err)]);
    const code = await proc.exited;
    // The pipes normally close with the child. They do NOT when the child
    // left a grandchild holding them (`sh -c "… | …"`, a stuck helper) — so
    // after exit the drains get a short grace period, then we return what
    // has been captured rather than hang on an orphan's stdout.
    await Promise.race([drains, new Promise((r) => setTimeout(r, PIPE_GRACE_MS))]);
    return { code, stdout: out.text, stderr: err.text, timedOut };
  } finally {
    clearTimeout(timer);
    opts.signal?.removeEventListener("abort", onOuterAbort);
    if (proc && proc.exitCode === null && !proc.killed) {
      try {
        proc.kill("SIGKILL");
      } catch {
        // already gone
      }
    }
  }
}

/** How long after exit we keep reading pipes an orphaned grandchild holds. */
const PIPE_GRACE_MS = 500;

/**
 * Read a stream into `into.text` incrementally (so a partial capture survives
 * a timeout), keeping at most `cap` bytes. Never throws.
 */
async function drain(stream: ReadableStream<Uint8Array>, cap: number, into: { text: string }): Promise<void> {
  const decoder = new TextDecoder();
  let bytes = 0;
  try {
    for await (const chunk of stream) {
      bytes += chunk.byteLength;
      if (bytes <= cap) into.text += decoder.decode(chunk, { stream: true });
    }
    into.text += decoder.decode();
  } catch {
    // the child was killed mid-write
  }
}

/** Last `n` non-empty lines of a stream, joined — for error messages. */
export function tailLines(text: string, n: number = 3): string {
  return text
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean)
    .slice(-n)
    .join(" ");
}
