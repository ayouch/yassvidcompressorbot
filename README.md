# Video Compressor Telegram Bot

Send a video to the bot; it re-encodes it with FFmpeg and sends the smaller file back.

**Compression command used:**

```
ffmpeg -i input -c:v libx264 -crf 23 -preset slow -c:a aac -b:a 128k output.mp4
```

H.264 / CRF 23 / `slow` preset is visually lossless for most content while
meaningfully reducing size. (Truly lossless encoding usually makes files
*larger*, so this is the practical choice.)

## Why the bundled Bot API server

The public Telegram Bot API limits bots to **downloading 20 MB** and
**uploading 50 MB** files — too small for real videos. This project ships a
self-hosted `telegram-bot-api` server (via Docker Compose) that raises both
limits to **2 GB**. The bot talks to that server instead of the public API.

## Prerequisites

- A VPS with Docker + Docker Compose.
- A bot token from [@BotFather](https://t.me/BotFather).
- `api_id` and `api_hash` from <https://my.telegram.org> → *API development tools*
  (required by the self-hosted API server).

## Setup

```bash
cp .env.example .env
# edit .env: BOT_TOKEN, TELEGRAM_API_ID, TELEGRAM_API_HASH
# (optional) set ALLOWED_USER_IDS to your numeric Telegram user id

docker compose up -d --build
docker compose logs -f bot
```

Then open Telegram, message your bot, and send it a video.

> **Tip:** For large videos, send them as a **File/Document** (paperclip →
> File), not as a regular video. Telegram pre-compresses videos sent the normal
> way before they ever reach the bot.

## Configuration (`.env`)

| Variable | Purpose |
| --- | --- |
| `BOT_TOKEN` | Bot token from BotFather (required) |
| `TELEGRAM_API_ID` / `TELEGRAM_API_HASH` | Credentials for the local API server (required) |
| `ALLOWED_USER_IDS` | Comma-separated user ids allowed to use the bot. Empty = everyone |
| `MAX_CONCURRENT_JOBS` | Simultaneous FFmpeg jobs (default `1`) |

To find your numeric user id, message [@userinfobot](https://t.me/userinfobot).

## Running without the local API server (small clips only)

If you don't want the extra server and only send clips under ~20 MB, you can run
the bot directly against the public API:

```bash
pip install -r requirements.txt
BOT_TOKEN=xxxx LOCAL_MODE=false TELEGRAM_API_BASE=https://api.telegram.org python bot.py
```

## How it works

1. Bot receives a video/document and downloads it (locally, in local mode).
2. Runs the FFmpeg command above into a temp dir, one job at a time by default.
3. Sends the result back as a document (so Telegram doesn't re-process it),
   with a before/after size summary, then deletes the temp files.
