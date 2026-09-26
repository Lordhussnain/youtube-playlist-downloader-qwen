# YT Playlist Downloader

Batch YouTube playlist downloader and converter, built with Bun + TypeScript.
Feed it a list of playlist or video links and it handles fetching, format
selection, subtitle/thumbnail/description extraction, and conversion — with
a terminal UI and a web dashboard to watch it all happen.

## Features

- **Batch downloads** from a list of YouTube playlist or video URLs defined in `config.json`
- **Concurrent worker pools** for downloading, metadata fetching, and format conversion, all driven by job state in a central SQLite database
- **Resilient by design** — interrupted downloads keep their `.part` file and resume exactly where they stopped; the retry budget only shrinks while a video makes no forward progress
- **Automatic retries** with exponential backoff + jitter on transient failures (network drops, throttling, timeouts)
- **Permanent-failure detection** — private / removed / age-gated / geo-blocked videos fail fast and are never auto-requeued
- **Self-healing sweeps** — crashed jobs resume, stale claims are reclaimed, deleted downloads are re-fetched, and failed jobs are retried after a cooldown
- **Duration-aware watchdog** — long videos are not killed by a flat 15-minute timeout
- **Disk space precheck** before starting a batch
- **Graceful shutdown** — safely stops in-flight downloads on exit
- **Terminal UI (TUI)** with live progress across all workers
- Correct format selection across VP9/AV1 containers (fixes yt-dlp/ffmpeg mismatches)
- Compatible with authenticated downloads (`--cookies`) alongside the Android player-client extractor args
- **Web dashboard** with live job status, bulk actions, failed-job recovery, and a reliability panel

## Tech Stack

- [Bun](https://bun.sh) + TypeScript — runtime and application logic
- [yt-dlp](https://github.com/yt-dlp/yt-dlp) — video and metadata extraction
- [ffmpeg](https://ffmpeg.org) — format conversion

## Requirements

- Bun ≥ 1.0
- `yt-dlp` and `ffmpeg` available on `PATH` (or configured explicitly)
- Tested on Windows 11

Dependencies are checked automatically on startup; the app exits with a clear
error if anything required is missing.

## Development

```bash
bun install
bun run typecheck   # tsc --noEmit
bun test            # unit + end-to-end suite (mocked yt-dlp/ffmpeg, no network needed)
bun run check       # both
```

The end-to-end tests (`tests/integration.test.ts`) run the real engine against
the mock binaries in `tests/mocks/`, covering the happy path, transient-failure
retries, corrupt-partial resume, permanent failures, and restart reconciliation.

## Installation

```bash
git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>
bun install
```

## Configuration

Define your batch in `config.json`:

```json
{
  "output_directory": "D:/Downloads/YT",
  "target_format": "mp4",
  "download": {
    "subtitles": true,
    "thumbnail": true,
    "description": false
  },
  "links": [
    "https://www.youtube.com/playlist?list=...",
    "https://www.youtube.com/watch?v=..."
  ]
}
```

Per-link overrides (format, output folder, subtitle/thumbnail flags) are
supported alongside these global defaults.

### Reliability settings

```json
{
  "maxResumeAttempts": 5,
  "retryBackoffBaseSeconds": 30,
  "retryBackoffMaxSeconds": 900,
  "requeueFailedAfterMinutes": 30,
  "verifyExistingFiles": true,
  "downloadTimeoutMinutes": 15,
  "maxDownloadMinutes": 180
}
```

| Key | Meaning |
| --- | --- |
| `maxResumeAttempts` | How many times one video may resume from its `.part` file before the partial is discarded and the download restarts from scratch |
| `retryBackoffBaseSeconds` / `retryBackoffMaxSeconds` | Exponential backoff window (with jitter) for transient failures — base doubles per retry, capped at the max |
| `requeueFailedAfterMinutes` | Cooldown before failed jobs are retried automatically (`0` disables the sweep). Permanent failures are never re-queued |
| `verifyExistingFiles` | On startup, verify that files recorded as downloaded still exist; missing ones are scrubbed from the yt-dlp archive and queued again |
| `downloadTimeoutMinutes` | Minimum per-video download timeout |
| `maxDownloadMinutes` | Ceiling for the timeout. The effective timeout scales with the video's real duration (3× realtime + 5 min) between the two |

Edit these interactively with `bun run config` → **Change Reliability & Resume
Settings**, or from the web dashboard's reliability panel.

## Usage

```bash
bun run start
```

With CLI overrides:

```bash
bun run start --config ./my-config.json --format mkv
```

The TUI shows live status for every video across all active workers. The web
dashboard (`http://127.0.0.1:3000` by default) adds bulk actions, the failed-job
tab, run history, and a live reliability panel.

## Architecture

```
batch_playlist_downloader.ts     entry point (bun run start / build:win)
update_config.ts                 interactive config manager (shares src/config.ts)
web_ui.html                      dashboard frontend (served by src/web.ts)
src/
  config.ts        Zod schema + defaults + load/save (single source of truth)
  db.ts            SQLite schema, migrations, atomic job claims
  state.ts         shared mutable runtime state (pause, stats, workers)
  tools.ts         yt-dlp/ffmpeg discovery + cookies helpers
  retry.ts         pure retry policy: backoff, watchdogs, error classification
  resilience.ts    pause/resume, circuit breaker, network + disk guards
  reconcile.ts     self-healing sweeps (crashes, stale claims, missing files, failed jobs)
  scanner.ts       playlist/channel scanning + deduplicated ingestion
  workers/         download, metadata, and conversion worker loops
  autoscale.ts     dynamic download-slot autoscaling
  rss.ts           cheap per-channel RSS new-upload watcher
  polling.ts       daemon-mode full rescans
  dashboard.ts     TUI
  report.ts        human-readable run report
  web.ts           dashboard server + JSON API + token auth
  history.ts       heartbeated run history
  lifecycle.ts     worker supervision + graceful shutdown
  engine.ts        orchestration (main)
tests/             bun test suite (unit + end-to-end with mocked tools)
```

## How It Works

1. **Startup** — load config, verify dependencies, open/migrate the database,
   then self-heal: reconcile crashed jobs, re-queue jobs whose files vanished,
   clean up unusable partials
2. **Scan** — every configured playlist/channel is listed (yt-dlp flat scan or
   cheap RSS polling) and deduplicated into the jobs table by video id
3. **Download workers** — pull videos into the configured output directory.
   Failures keep the `.part` file and retry with exponential backoff; the retry
   budget only shrinks while the video makes no forward progress
4. **Metadata workers** — fetch subtitles, thumbnails, and descriptions per video, based on config flags
5. **Converter workers** — convert completed downloads into the target format,
   optionally moving them (with sidecars) to a secondary storage path
6. **Sweeps** — every minute: reclaim stale claims, re-queue cooled-down
   transient failures, heartbeat the run history

## Security & operations

- **Loopback-only Web UI by default** — `webBind` defaults to `127.0.0.1`, so the
  dashboard (pause/purge/delete!) is not reachable from your LAN. Set
  `"webBind": "0.0.0.0"` to expose it deliberately.
- **Optional shared-secret token** — set `webToken` and every request (UI and
  API) needs it, via cookie, `Authorization: Bearer`, `X-Web-Token`, or
  `?token=`. The login page sets an `HttpOnly` cookie after the first
  sign-in; comparisons are timing-safe.
- **Real bandwidth cap** — `maxBandwidthKBps` maps to yt-dlp `--limit-rate`,
  split across the active download slots.
- **Worker autoscaling** — with `autoscaleEnabled` the engine grows download
  slots toward `maxDownloadWorkers` while a backlog exists and bandwidth
  headroom remains, sheds slots when the cap saturates, and returns to
  `minDownloadWorkers` when idle.
- **Cheap new-upload watching** — `rssEnabled` polls each channel's RSS feed
  every `rssPollIntervalMinutes` (one HTTP GET per channel, ~15 min latency)
  instead of waiting for a full rescan.
- **Circuit breaker** — after `maxFailures` consecutive pipeline failures
  (dead cookies overnight, a YouTube outage) the engine pauses itself with
  `TOO_MANY_FAILURES` instead of burning through the queue. Resume from the
  UI when you're ready. Per-video, the effective retry cap is
  `min(maxRetryAttempts, maxFailuresPerVideo)`.
- **yt-dlp download archive** — `archiveFile` is passed to
  `--download-archive` as a second idempotence layer; if a downloaded file
  disappears (moved/deleted by hand) the archive entry is scrubbed and the
  video is fetched again on the next attempt.
- **Startup file reconciliation** — with `verifyExistingFiles` (default on) the
  engine checks that every file it recorded as downloaded still exists on
  disk; anything missing is scrubbed from the archive and re-queued instead of
  being silently skipped forever.
- **Wait for VOD** — with `archiveLiveStreams` enabled, currently-live
  streams are never grabbed mid-broadcast: the job parks as
  `waiting for VOD` and is re-queued by the next scan/RSS pass once the
  stream has ended.

## Roadmap

- [x] Central SQLite job database — persist per-video status (`pending` →
      `downloading` → `downloaded` → `converted`) so downloads survive
      crashes and restarts, and workers claim jobs atomically instead of
      relying on in-memory state
- [x] Resume interrupted downloads from exactly where they left off
      (`.part` files kept, `--continue`, bounded resume budget)
- [x] Fully independent, parallel metadata and conversion pipelines
- [x] Modular architecture with a unit + end-to-end test suite
- [ ] Deduplicate identical videos across playlists by content hash
- [ ] Per-link quality/format overrides in the web dashboard
- [ ] Desktop notifications (Discord/webhook) on completion and failures

## Windows 11

The engine is fully supported on Windows 11. Recommended setup:

**Quick start (no runtime install):**

```powershell
# 1. Build the standalone exe (requires Bun once, on any machine)
bun run build:win          # → dist\youtube-archive.exe

# 2. Copy dist\youtube-archive.exe into this folder, then double-click:
start-archive.bat
```

`start-archive.bat` sets UTF-8 codepage, puts the app folder first on `PATH`
(so a local `yt-dlp.exe` / `ffmpeg.exe` sitting next to the app is picked up
automatically), and prefers the compiled exe over a source checkout.

**Dependency auto-detection** — at startup the engine searches, in order:

1. `ytDlpPath` / `ffmpegPath` in `config.json` (set them via `bun run config`)
2. `PATH`
3. The app folder (next to `archive.exe` / `config.json`)
4. Winget, Scoop, and Chocolatey shims

**Start automatically at logon:**

```powershell
powershell -ExecutionPolicy Bypass -File .\install-task.ps1
# to remove later:
powershell -ExecutionPolicy Bypass -File .\install-task.ps1 -Uninstall
```

**Windows-specific safeguards built in:**

- Filenames are stripped of reserved device names (`CON`, `NUL`, `COM1`…),
  trailing dots/spaces, and illegal characters `/\:*?"<>|`
- Long titles are truncated so paths stay under `MAX_PATH` (260) — no registry
  tweak required
- Run history is heartbeated every minute, so even `taskkill /F` or a window
  close still leaves a usable history row
- Interrupted downloads are marked `paused (resume)` and continue from the
  `.part` file on next start
- If `statfs` is unavailable, free space falls back to PowerShell
  (`Get-PSDrive`); if that also fails the engine runs in degraded mode
  instead of pausing forever

## License

MIT — replace with your preferred license.
