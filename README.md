# YT Playlist Downloader

Batch YouTube playlist downloader and converter, built with Bun + TypeScript.
Feed it a list of playlist or video links and it handles fetching, format
selection, subtitle/thumbnail/description extraction, and conversion — with
a terminal UI to watch it all happen.

## Features

- **Batch downloads** from a list of YouTube playlist or video URLs defined in `config.json`
- **Concurrent worker pools** for downloading, metadata fetching, and format conversion
- **Automatic retries** with exponential backoff on failed downloads
- **Disk space precheck** before starting a batch
- **Graceful shutdown** — safely stops in-flight downloads on exit
- **CLI argument support** for one-off overrides without editing the config file
- **Terminal UI (TUI)** with live progress across all workers
- Correct format selection across VP9/AV1 containers (fixes yt-dlp/ffmpeg mismatches)
- **YouTube signature challenge solving** via an embedded Deno JS runtime
- Compatible with authenticated downloads (`--cookies`) alongside the Android player-client extractor args

## Tech Stack

- [Bun](https://bun.sh) + TypeScript — runtime and application logic
- [yt-dlp](https://github.com/yt-dlp/yt-dlp) — video and metadata extraction
- [aria2c](https://aria2.github.io) — multi-connection downloading
- [ffmpeg](https://ffmpeg.org) — format conversion

## Requirements

- Bun ≥ 1.0
- `yt-dlp`, `aria2c`, and `ffmpeg` available on `PATH`
- Tested on Windows 11

Dependencies are checked automatically on startup; the app exits with a clear
error if anything required is missing.

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

## Usage

```bash
bun run start
```

With CLI overrides:

```bash
bun run start --config ./my-config.json --format mkv
```

The TUI shows live status for every video across all active workers.

## How It Works

1. **Startup** — checks dependencies, then loads and parses `config.json`
2. **Download workers** — pull videos into the configured output directory, retrying on failure with exponential backoff
3. **Metadata workers** — fetch subtitles, thumbnails, and descriptions per video, based on config flags
4. **Converter workers** — convert completed downloads into the target format

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
- **Wait for VOD** — with `archiveLiveStreams` enabled, currently-live
  streams are never grabbed mid-broadcast: the job parks as
  `waiting for VOD` and is re-queued by the next scan/RSS pass once the
  stream has ended.

## Roadmap

- [ ] Central SQLite job database — persist per-video status (`pending` →
      `downloading` → `downloaded` → `converted`) so downloads survive
      crashes and restarts, and workers claim jobs atomically instead of
      relying on in-memory state
- [ ] Resume interrupted downloads from exactly where they left off
- [ ] Fully independent, parallel metadata and conversion pipelines

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
