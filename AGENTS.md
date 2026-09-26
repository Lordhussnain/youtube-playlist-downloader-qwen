# AGENTS.md

Operator manual for coding agents working on **YT Playlist Downloader** — a
batch YouTube playlist/channel archival engine built on Bun + TypeScript.

Everything an agent needs to navigate, modify, and safely test this codebase is
below. Read sections 1–4 before making changes; section 10 (Gotchas) before
touching the database, workers, or config.

---

## 1. What this software is

A long-running **archival engine**: you give it YouTube playlist/channel URLs,
it downloads every video, fetches sidecar metadata (subtitles, thumbnails,
descriptions, info.json), converts to a target container, and keeps going
forever — watching RSS feeds for new uploads and rescanning on a schedule.

It is designed to survive **crashes, hard kills, network drops, full disks, and
files deleted behind its back**. Every state transition is persisted in SQLite,
so a restart continues where the last run stopped rather than starting over.
That principle — *never throw away work that has already been done* — is the
single most important thing to understand before changing failure-handling code.

| Layer | Technology |
| --- | --- |
| Runtime | Bun ≥ 1.0 (uses `bun:sqlite`, `Bun.spawn`, `Bun.serve`) |
| Language | TypeScript, ESM, strict mode |
| Job store | SQLite (`archive.db`, WAL mode) |
| Media tools | `yt-dlp` (extraction), `ffmpeg` (transcode/remux) |
| Frontend | Single-file vanilla-JS dashboard (`web_ui.html`) |
| Config | Zod-validated `config.json` |

---

## 2. Quick start

```bash
bun install            # zod (+ dev: typescript, @types/bun)
bun run start          # run the engine (reads ./config.json, creates it if absent)
bun run config         # interactive config manager (TTY)
bun run typecheck      # tsc --noEmit
bun test               # full suite: unit + end-to-end (~45s, no network needed)
bun run check          # typecheck + tests
bun run build:win      # cross-compile dist/youtube-archive.exe (Windows)
```

Runtime requirements: `yt-dlp` and `ffmpeg` on `PATH` (or `ytDlpPath` /
`ffmpegPath` in config). `checkDependencies()` probes them **before** the
database opens and exits with install hints if missing.

**Sandbox note:** YouTube is unreachable from this environment. The test suite
therefore runs the real engine against mock binaries (`tests/mocks/`) — see
section 9. Do not "fix" failing integration tests by adding network calls.

---

## 3. Architecture

```
batch_playlist_downloader.ts   entry point → src/engine.ts main()
update_config.ts               config manager → imports src/config.ts (shared schema)
web_ui.html                    dashboard frontend, served by src/web.ts
config.json                    user config (created from defaults on first run)
archive.db                     SQLite job store (+ -wal/-shm while running)
error.log                      rotating error log
src/
  config.ts      Zod schema, DEFAULT_CONFIG, load/loadSafe/save, QUALITY_FORMATS
  db.ts          SQLite schema, migrations, atomic claim transactions, helpers
  state.ts       shared mutable runtime state (leaf module — imports nothing)
  tools.ts       yt-dlp/ffmpeg discovery, cookiesArgs, validateCookies
  retry.ts       PURE retry policy: backoff, watchdog, error classification
  resilience.ts  pause/resume, circuit breaker, network + disk guards
  reconcile.ts   self-healing sweeps (crashes, stale claims, missing files, failed jobs)
  scanner.ts     playlist/channel listing + deduplicated ingestion
  autoscale.ts   dynamic download-slot management
  workers/
    download.ts  yt-dlp download loop + all failure classification
    metadata.ts  sidecar fetching loop
    convert.ts   ffmpeg transcode/remux + secondary-storage move loop
  rss.ts         cheap per-channel RSS watcher (parseRssFeed is pure)
  polling.ts     daemon-mode full rescans
  dashboard.ts   TUI rendering
  report.ts      human-readable run report
  web.ts         Bun.serve dashboard + JSON API + token auth
  history.ts     heartbeated run_history rows
  lifecycle.ts   worker supervision + graceful shutdown
  engine.ts      orchestration (main): wires everything together
tests/           bun test suite (see section 9)
```

### Import graph (acyclic — keep it that way)

```
state.ts ──────────────────────────────────────┐ (imports only config types)
config.ts, util.ts, logger.ts, retry.ts,
archive.ts, tools.ts                            │ (leaf modules, zero deps)
db.ts → config                                  │
resilience.ts → config, db, logger, state       │
reconcile.ts → archive, config, db, logger, retry│
scanner.ts → config, db, state, tools, util     │
autoscale.ts → db, state                        │
dashboard.ts → autoscale, config, db, state, util│
report.ts → autoscale, db, state, util          │
web.ts → autoscale, config, db, logger, reconcile, report, resilience, retry, scanner, state, util
rss.ts → config, logger, scanner, tools         │
polling.ts → config, logger, scanner            │
history.ts → db, logger, state                  │
lifecycle.ts → dashboard, db, history, logger, resilience, state
workers/* → config, dashboard, db, logger, resilience, retry, state, tools, util (+ autoscale/archive/reconcile)
engine.ts → everything (composition root)
```

**Rule:** if you need a new shared behavior, put it in a leaf module and inject
it. Workers must never import `engine.ts`, and `state.ts` must stay
dependency-free — it is the module that breaks every import cycle.

### Startup order (engine.ts `main()`)

1. `loadConfig()` → `setConfig()` (must be first: dependency search uses paths from it)
2. `checkDependencies()` — fails fast with install hints
3. `initDatabase("archive.db")` — schema + migrations + claim transactions
4. `reconcileCrashedJobs()` → `reconcileMissingFiles()`
5. `startRunHistory()` + heartbeat interval
6. `mkdir(outputRoot)` → `cleanOrphanedFiles()` → `autoscaler.init()`
7. cookie validation (if enabled)
8. scan every configured playlist/channel into the jobs table
9. `initDashboard()`, `startWebServer()`
10. `networkMonitor()`, `reapStaleClaims` (60s), `autoscaleTick` (15s),
    `requeueFailedJobs` (60s), `startRssPolling()`
11. supervised worker pools (download × N, metadata × N, convert × N)
12. `startAutonomousPolling()` if daemon mode

---

## 4. Data model

### The `jobs` table (one row per video, keyed by YouTube video id)

| Field | Meaning |
| --- | --- |
| `id` | YouTube video id (primary key → natural dedupe) |
| `url` | canonical `https://www.youtube.com/watch?v=<id>` |
| `title`, `folder`, `index` | display name, output folder, `001`-style ordering |
| `output_directory` | absolute-ish path where files land |
| `target_format` | `mp4` or `mp3` |
| `want_subtitles` / `want_thumbnail` / `want_description` | 0/1 sidecar flags |
| `duration` | seconds from the listing (drives the watchdog) |
| `download_status` | `pending` \| `downloading` \| `downloaded` \| `paused` \| `failed` \| `waiting_live` |
| `conversion_status` | `pending` \| `in_progress` \| `done` \| `failed` \| `not_needed` |
| `metadata_status` | `pending` \| `in_progress` \| `done` \| `failed` \| `not_needed` |
| `pause_reason` | `user` \| `interrupted` \| `waiting_live` \| NULL |
| `*_claimed_by` / `*_claimed_at` | worker id + timestamp of the atomic claim |
| `retry_count` | download attempts spent |
| `conversion_retry_count`, `metadata_retry_count` | per-stage attempt budgets |
| `resume_count` | `--continue` resumes spent for this job |
| `best_progress` | high-water mark of progress % (drives budget forgiveness) |
| `partial_file_path` | the `.part` file to resume from (NULL once complete) |
| `file_path`, `file_size`, `integrity` | final location + SHA-256 |
| `progress`, `speed`, `eta` | live values for the dashboard |
| `last_error` | last failure message (classified by `retry.ts`) |

Other tables: `playlist_state(folder, next_index)`,
`run_history(id, started_at, ended_at, duration_seconds, downloaded, skipped, failed, total_queued)`.

### State machine

```
ingest ──► pending ──claim──► downloading ──success──► downloaded
              ▲                    │
              │                    ├─ transient ──► pending (keep .part, backoff)
              │                    ├─ corrupt ────► pending (resume_count++, .part kept)
              │                    ├─ permanent ──► pending … budget spent ──► failed
              │                    ├─ live ───────► waiting_live (requeued by next scan)
              │                    └─ shutdown ───► paused + interrupted (auto-resume)
              └── sweep (cooldown) ── failed (transient only, budget permitting)

downloaded ──► metadata worker ──► metadata: pending → in_progress → done | failed
downloaded ──► convert worker ──► conversion: pending → in_progress → done | failed
                                      (skipped when conversion_status = 'not_needed')
```

**Claims are the concurrency primitive.** Each stage claims work with a single
atomic `UPDATE … WHERE id = (SELECT … LIMIT 1) RETURNING *` inside a
`db.transaction`. Two workers can never grab the same job. Never add a
read-then-write claim sequence.

Conversion claims additionally require `metadata_status IN ('done','not_needed','failed')`
— the converter waits for metadata to be *terminal*, not necessarily successful.

---

## 5. The worker loops

All three workers follow the same shape:

```ts
while (!abortController.signal.aborted) {
  if (isPaused()) { await Bun.sleep(2000); continue; }
  const job = claimXJob(workerId);
  if (!job) { await Bun.sleep(pollMs); continue; }
  try { await doWork(job, config); }
  catch (err) { await handleFailure(job, config, err); }
  finally { /* cleanup: unregister proc, autoscaler speed */ }
}
```

- **downloadWorker** — gates on `activeDlSlots.has(id)` (autoscaling) and
  `checkDiskSpace()` before claiming. Spawns yt-dlp with `--continue`,
  `--no-overwrites`, `--download-archive`, parses `PROGRESS:` lines from the
  progress template into the DB, and captures the final path from
  `--print after_move:%(filepath)s`.
- **metadataWorker** — second yt-dlp pass with `--skip-download`, writes
  sidecars next to the media file using the same basename, records the file
  list in `metadata_files`.
- **converterWorker** — ffmpeg mp3 transcode or mp4 remux (`-c:v copy`), then
  optionally moves the media file **and its sidecars** to
  `secondaryStoragePath/<folder>/`, then hashes it.

Workers are supervised (`lifecycle.ts supervise()`): a crashed loop is logged
and restarted after 5s. Never let a worker loop exit on error.

---

## 6. Reliability policies — where each lives

| Policy | Location | Notes |
| --- | --- | --- |
| Exponential backoff + jitter | `retry.ts computeBackoffMs()` | base × 2^attempt, capped, +0–30% jitter; RNG injectable for tests |
| Download watchdog | `retry.ts computeDownloadTimeoutMs()` | 3× duration + 5 min, clamped to config min/max; unknown duration → min |
| Transient vs permanent errors | `retry.ts isTransientDownloadError / isPermanentDownloadError` | permanent = private/removed/age-gated/geo-blocked/404/410/copyright |
| Retry-budget forgiveness | `workers/download.ts handleDownloadFailure()` | `retry_count` only increments when `progress <= best_progress` |
| Resume budget | `workers/download.ts` corrupt branch | `.part` kept until `resume_count >= maxResumeAttempts`, then discarded |
| Partial-file bookkeeping | `workers/download.ts` + `reconcile.ts findPartialFile()` | recorded on failure, cleared on success |
| Circuit breaker | `resilience.ts notePipelineFailure()` | N consecutive failures per stage pauses the engine (`TOO_MANY_FAILURES`) |
| Pause / resume | `resilience.ts triggerPause / triggerResume` | SIGINTs child yt-dlp; resume re-queues paused jobs |
| Network monitor | `resilience.ts networkMonitor()` | probes 3 hosts, pauses after 2 consecutive failures |
| Disk guard | `resilience.ts checkDiskSpace()` | statfs → PowerShell fallback → degraded mode (never bricks the engine) |
| Crash recovery | `reconcile.ts reconcileCrashedJobs()` | `downloading` → `paused + interrupted` (auto-claimable) |
| Stale-claim reaper | `reconcile.ts reapStaleClaims()` | downloads >20 min, conversions >3 h, metadata >15 min |
| Missing-file reconciliation | `reconcile.ts reconcileMissingFiles()` | scrubs the yt-dlp archive + re-queues |
| Failed-job sweep | `reconcile.ts requeueFailedJobs()` | cooldown + per-video cap + permanent-error skip; `ignoreCooldown` for the UI button |
| Partial-file cleanup | `reconcile.ts cleanOrphanedFiles()` | keeps resume-able partials, deletes exhausted (>cap) and day-old orphans |
| Archive scrubbing | `archive.ts removeFromArchive()` | needed whenever a file disappears, else yt-dlp skips it forever |
| Signature self-heal | `workers/download.ts` | auto-runs `yt-dlp -U` and retries with a clean budget |
| WAL checkpoint | `lifecycle.ts handleShutdown()` | keeps `archive.db` self-contained after exit |

**Adding a new failure class:** extend the classifiers in `retry.ts` (pure,
unit-tested), then handle it in `workers/download.ts handleDownloadFailure()`
in the right precedence order: signature → corrupt → archive-scrub → live →
transient → permanent/budget.

---

## 7. Configuration

- **Single source of truth:** `src/config.ts` (`ConfigSchema` + `DEFAULT_CONFIG`).
  The engine and `update_config.ts` both import it — never re-declare the schema.
- `loadConfig()` (engine) exits the process on invalid config;
  `loadConfigSafe()` (manager) falls back to defaults.
- Partial configs are merged over `DEFAULT_CONFIG`, so new keys are always
  backwards compatible with existing `config.json` files.
- Cross-field rules are enforced with `.refine()`: backoff max ≥ base,
  maxDownloadMinutes ≥ downloadTimeoutMinutes, minDownloadWorkers ≤ maxDownloadWorkers.
- **Adding a key:** add to `ConfigSchema`, add to `DEFAULT_CONFIG`, add a prompt
  to `update_config.ts` (menu 4 = download settings, menu 6 = reliability), and
  add a test in `tests/config.test.ts`.

Reliability keys: `maxResumeAttempts`, `retryBackoffBaseSeconds`,
`retryBackoffMaxSeconds`, `requeueFailedAfterMinutes`, `verifyExistingFiles`,
`downloadTimeoutMinutes`, `maxDownloadMinutes`.

---

## 8. Web API (`src/web.ts`)

Auth: every request (UI + API) is gated when `webToken` is set — via cookie
(`yta_token`, HttpOnly after sign-in), `Authorization: Bearer`, `X-Web-Token`,
or `?token=`. Comparison is timing-safe. Default bind is `127.0.0.1`.

| Method | Route | Purpose |
| --- | --- | --- |
| GET | `/` | dashboard (login page when a token is required) |
| HEAD | `/api/ping` | liveness probe used by the UI |
| GET | `/api/status` | stats, speed, ETA, disk, workers, pause state |
| GET | `/api/jobs` | up to 500 jobs with status/retry/progress fields |
| POST | `/api/scan` | `{url, folder?}` → scan & ingest |
| POST | `/api/pause` · `/api/resume` | global pause / resume-all |
| POST | `/api/retry/<id>` | re-queue one job (all stages, budgets reset) |
| POST | `/api/failcount/reset/<id>` | zero the retry counters |
| POST | `/api/jobs/pause` · `/api/jobs/delete` | bulk by `{ids: []}` |
| DELETE | `/api/jobs/<id>` | delete one job |
| GET | `/api/failed` | failed jobs |
| POST | `/api/failed/requeue` | force-requeue eligible failed jobs (cooldown ignored, permanent errors still skipped) |
| GET | `/api/reliability` | pause state, kept partials, retryable count, active policy |
| GET | `/api/history?limit=20` | run history rows |
| GET | `/api/logs?type=error\|report&limit=100` | error.log or the run report |

Frontend is plain JS in `web_ui.html` — no build step. After editing it,
re-extract the inline `<script>` and syntax-check it (see section 9.4).

---

## 9. Testing

### 9.1 Running

```bash
bun test                       # everything (~45s)
bun test tests/retry.test.ts   # one file
bunx tsc --noEmit              # typecheck (tsconfig covers *.ts, src/**, tests/**)
```

100 tests across 8 files. Tests share one process, so any file that touches the
database calls `initDatabase(":memory:")` in `beforeEach` — **the module-level
`db` binding is replaced, which is exactly why it is a live ESM binding**.

### 9.2 What is covered where

| File | Focus |
| --- | --- |
| `tests/retry.test.ts` | backoff math, watchdog scaling, error classification |
| `tests/util.test.ts` | formatters, Windows filename hardening, `fitBaseFilename`, hashing |
| `tests/config.test.ts` | defaults, validation, cross-field refinements, load/save |
| `tests/db.test.ts` | schema + legacy migration, atomic claims, all reconcile/requeue sweeps, ingestion dedupe |
| `tests/rss.test.ts` | `parseRssFeed` against a realistic feed (CDATA, missing duration) |
| `tests/webauth.test.ts` | token extraction, timing-safe compare, authorization |
| `tests/report.test.ts` | run report contents |
| `tests/integration.test.ts` | **end-to-end engine runs** (see 9.3) |

### 9.3 End-to-end tests with mock tools

`tests/mocks/yt-dlp` and `tests/mocks/ffmpeg` are Bun scripts that implement
just enough of each CLI for the engine to run its full pipeline. The integration
test prepends `tests/mocks` to `PATH`, writes a `config.json` into a temp dir,
and spawns the real `batch_playlist_downloader.ts`, then drives it over HTTP
(`/api/status`, `/api/jobs`, `/api/reliability`) and asserts final job states.

Scenarios: happy path (scan → download → metadata → convert), transient-failure
retries with backoff, corrupt-partial resume, permanent failures (never
requeued), and restart reconciliation after deleting files.

Mock controls (environment variables):

| Variable | Effect |
| --- | --- |
| `FAKE_FAIL_TIMES=N` | fail the first N download attempts, leaving a `.part` file behind |
| `FAKE_FAIL_MODE` | `transient` \| `permanent` \| `corrupt` (which error message to emit) |
| `FAKE_DELAY_MS` | artificial per-attempt delay |
| `FAKE_HANG=1` | never exit (watchdog testing) |

**When adding an engine behavior, add an integration scenario rather than
mocking internals** — the mocks are the contract boundary.

### 9.4 Editing `web_ui.html`

```bash
python3 - <<'PY'
import re
html = open('web_ui.html').read()
open('/tmp/inline.js','w').write('\n;\n'.join(re.findall(r'<script>(.*?)</script>', html, re.S)))
PY
node --check /tmp/inline.js   # syntax gate before committing UI changes
```

---

## 10. Gotchas (read before changing things)

1. **ESM live bindings.** `db` is `export let db` reassigned by `initDatabase()`.
   Importers see the new value because bindings are live — but you can never
   assign to an imported binding. Route mutations through setter functions
   (`state.ts` does this: `setPaused`, `setConfig`, `setTty`).
2. **Claim transactions must be created inside `initDatabase()`.** Defining
   `db.transaction(...)` at module top level evaluates `db` while it is still
   `undefined` and crashes startup. This is load-bearing; don't "clean it up".
3. **Everything is CWD-relative.** `archive.db`, `config.json`, `error.log`,
   `downloaded_videos.txt`, and `web_ui.html` are resolved from `process.cwd()`.
   Tests therefore run the engine in a temp dir; the compiled exe expects
   `web_ui.html` next to it.
4. **Windows path rules are enforced everywhere.** `hardenName()` strips
   control chars, trailing dots/spaces, reserved device names (`CON`, `NUL`,
   `COM1`…); `fitBaseFilename()` keeps paths under MAX_PATH (260) by truncating
   and appending `[videoId]`. Any new filename construction must go through
   these helpers.
5. **`partial_file_path` must always point at a real `.part` file or NULL.**
   Storing a completed path there makes the corrupt-handler delete good files
   (that was a real bug). Keep it in sync: set on failure, NULL on success.
6. **Never re-queue permanent errors.** `isPermanentDownloadError()` gates the
   sweep; bypassing it causes infinite retry loops against dead videos.
7. **`bun:sqlite` specifics.** `MAX(a,b)` is the scalar two-arg form; use
   `COALESCE` before it. `datetime('now', '-N minutes')` modifiers must be
   built from validated integers, never user text. Open read-only handles
   (`new Database(path, { readonly: true })`) when inspecting a live DB.
8. **Progress updates are throttled to 500ms** and go straight to SQLite —
   keep DB writes inside the progress loop cheap.
9. **Don't add blocking work to the worker claim path.** Long operations belong
   inside the try-block after a claim, or the whole pool stalls.
10. **TUI vs piped output.** `dashboard.ts` no-ops when `isTty()` is false;
    log lines must remain parseable when stdout is a pipe (integration tests
    rely on this).
11. **Backwards compatibility.** Existing users have `archive.db` files from
    older versions. New columns go through `ensureColumn()`; never assume a
    fresh schema. The legacy-migration test in `tests/db.test.ts` is the guard.

---

## 11. Common tasks

| Task | Where |
| --- | --- |
| Add a config key | `src/config.ts` (schema + defaults) → `update_config.ts` prompt → test |
| Add a failure class | `src/retry.ts` classifier → `src/workers/download.ts` handler branch → unit test |
| Add an API endpoint | `src/web.ts handleRequest()` (after the auth gate) → `web_ui.html` caller |
| Add a dashboard field | `src/web.ts` response → `web_ui.html` render function |
| Change claim semantics | `src/db.ts` claim transactions + `tests/db.test.ts` atomicity tests |
| Add a sweep | `src/reconcile.ts` (pure-ish, take `Config`) → register interval in `src/engine.ts` |
| Add a worker | `src/workers/<name>.ts` → claim fn in `db.ts` → `supervise()` in `engine.ts` → TUI line in `dashboard.ts` |
| Support a new site/URL shape | `src/scanner.ts normalizeVideoUrl()` (canonicalization + dedupe) |

## 12. Definition of done

- `bun run check` passes (strict typecheck + 100 tests).
- New pure logic has unit tests; new engine behavior has an integration scenario.
- No new import cycles; `state.ts` stays dependency-free.
- Config changes are backwards compatible (defaults merge + `ensureColumn`).
- `README.md` updated if user-visible behavior or settings changed.
