@echo off
setlocal
chcp 65001 >nul 2>nul
title YouTube Archive Engine V5
cd /d "%~dp0"

REM Put the app folder first on PATH so local yt-dlp.exe / ffmpeg.exe are found.
set "PATH=%~dp0;%PATH%"

if exist "%~dp0archive.exe" (
  "%~dp0archive.exe"
  if errorlevel 1 pause
  exit /b %errorlevel%
)

where bun >nul 2>nul
if errorlevel 1 (
  echo.
  echo   Neither archive.exe nor Bun was found.
  echo.
  echo   Option A: Build the standalone exe:   bun run build:win
  echo   Option B: Install Bun:                https://bun.sh
  echo.
  pause
  exit /b 1
)

bun run batch_playlist_downloader.ts
if errorlevel 1 (
  echo.
  echo   The engine exited with an error. See messages above / error.log.
  pause
)
endlocal
