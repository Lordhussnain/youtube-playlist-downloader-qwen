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
- [Deno](https://deno.land) — sandboxed JS runtime for YouTube signature decoding

## Requirements

- Bun ≥ 1.0
- `yt-dlp`, `aria2c`, and `ffmpeg` available on `PATH`
- Deno (used for signature-challenge solving)
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
What was added
1. 📦 Standalone .exe (no Bun needed on Windows)
PowerShell

bun run build:win        # → dist\youtube-archive.exe  (86 MB, verified cross-compile)
bun run build:win:all    # also builds archive-config.exe
2. 🚀 start-archive.bat (double-click launcher, CRLF-encoded)
chcp 65001 → emoji/UTF-8 render correctly even in old conhost
Puts the app folder first on PATH → a yt-dlp.exe/ffmpeg.exe sitting next to the app "just works"
Prefers archive.exe if present, falls back to bun run, pauses on error so messages don't vanish
3. ⚙️ Auto-start via Task Scheduler — install-task.ps1
PowerShell

powershell -ExecutionPolicy Bypass -File .\install-task.ps1           # install (runs at logon)
powershell -ExecutionPolicy Bypass -File .\install-task.ps1 -Uninstall # remove
Battery-safe, restarts on failure (3×), no execution time limit, correct working directory.

4. 🔎 Windows-aware tool auto-detection
Search order: ytDlpPath/ffmpegPath in config.json → PATH → app folder → next to the compiled exe → winget/Scoop/Chocolatey shims. Startup prints exactly where each tool was found:

text

✅ yt-dlp: 2026.01.01  [C:\tools\yt-dlp.exe]
✅ ffmpeg: 6.0         [PATH]
New config keys editable via bun run config (blank = auto).

5. 🛡️ Windows filename hardening (all verified live)
Reserved device names: CON → 010 - _CON.mp4
Trailing dots/spaces stripped: "Trailing dots... " → 012 - Trailing dots.mp4
MAX_PATH protection: a 300-char title was truncated to 011 - LLL…L [vidLONG].mp4 — stays under 260 with sidecars, no registry long-path tweak needed
6. 💾 Run-history heartbeat
Row inserted at startup, flushed at 10s then every 60s. Tested with kill -9: history row survived with correct duration. Closing the window / taskkill /F no longer loses history. Added SIGBREAK handling for Ctrl+Break.

7. 💿 Disk-check fallback
statfs → if it fails on Windows → PowerShell Get-PSDrive → if that also fails, one warning + degraded mode (never permanently pauses the engine over a failed probe).

Your Windows 11 quick start
PowerShell

git clone <repo> && cd <repo>
bun run build:win                    # once
copy dist\youtube-archive.exe .\
# put yt-dlp.exe & ffmpeg.exe next to it (or winget install yt-dlp ffmpeg)
bun run config                       # set links + paths
start-archive.bat                    # run — or install-task.ps1 for auto-start
## License

MIT — replace with your preferred license.
