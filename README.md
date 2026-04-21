# YouTube Feed

Local Python CLI that polls YouTube channel RSS feeds, retrieves transcripts, summarizes new videos with Gemini, and notifies Telegram only for high-value items.

## Features

- Poll subscribed channels without the YouTube Data API
- Persist local state in SQLite
- Retrieve transcripts with `youtube-transcript-api`
- Summarize in the transcript language with Gemini structured JSON output
- Send compact Telegram notifications with length control

## Commands

```bash
uv run youtube-feed init-db
uv run youtube-feed doctor
uv run youtube-feed poll
uv run youtube-feed find-channel-id "Channel Name"
uv run youtube-feed requeue-video --id 56 --stage summary
```

## Environment

Copy `.env.example` to `.env` and fill in the required secrets.

```bash
cp .env.example .env
```

### Required variables

These variables are required for `poll` and `doctor`.

| Variable | Description |
| --- | --- |
| `YOUTUBE_CHANNELS_IDS` | Comma-separated YouTube channel IDs to poll. |
| `GEMINI_API_KEY` | Gemini API key used for transcript summarization. |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token used to send notifications. |
| `TELEGRAM_CHAT_ID` | Telegram target chat or channel ID. |

`TELEGRAM_CHANNEL_ID` is supported as a compatibility alias when `TELEGRAM_CHAT_ID` is empty.

### Variables with defaults

These variables are optional. The CLI prints the resolved effective configuration on startup, masking secret values.

| Variable | Default | Description |
| --- | --- | --- |
| `GEMINI_MODEL` | `gemini-2.0-flash` | Gemini model used for summarization. |
| `DATABASE_PATH` | `data/youtube_feed.db` | SQLite database path. |
| `SUMMARY_LANGUAGE_MODE` | `transcript` | Summary language policy. Only `transcript` is currently supported. |
| `MIN_NOTIFICATION_SCORE` | `75` | Minimum score required for Telegram delivery. |
| `FIRST_RUN_MODE` | `mark_seen` | First-run behavior. Use `mark_seen` or `process_all`. |
| `HTTP_TIMEOUT_SECONDS` | `30` | General HTTP timeout fallback. |
| `RSS_HTTP_TIMEOUT_SECONDS` | `20` | YouTube RSS and channel fallback timeout. |
| `GEMINI_HTTP_TIMEOUT_SECONDS` | `60` | Gemini request timeout. |
| `TELEGRAM_HTTP_TIMEOUT_SECONDS` | `15` | Telegram API request timeout. |
| `CHANNEL_LOOKUP_HTTP_TIMEOUT_SECONDS` | `20` | Timeout for channel ID lookup. |
| `TELEGRAM_MESSAGE_LIMIT` | `3800` | Safe Telegram message length cap. |
| `GEMINI_MIN_REQUEST_INTERVAL_SECONDS` | `3` | Minimum delay between Gemini requests. |
| `GEMINI_COOLDOWN_AFTER_RATE_LIMIT_SECONDS` | `60` | Pause after Gemini rate-limit responses. |
| `GEMINI_COOLDOWN_AFTER_TIMEOUT_SECONDS` | `20` | Pause after Gemini transport timeouts. |
| `GEMINI_COOLDOWN_AFTER_SERVICE_UNAVAILABLE_SECONDS` | `15` | Pause after Gemini service-unavailable responses. |
| `GEMINI_TRANSPORT_INLINE_RETRY_DELAYS_SECONDS` | `5,15` | Inline retry delays for Gemini transport failures. |
| `GEMINI_MAX_INPUT_TOKENS_PER_MINUTE` | `225000` | Input token budget used to pace Gemini requests. |
| `MAX_TRANSCRIPT_VIDEO_AGE_DAYS` | `7` | Maximum age for transcript processing candidates. |
| `MAX_TRANSCRIPT_CANDIDATES_PER_CHANNEL_PER_POLL` | `1` | Per-channel transcript attempts per poll. |
| `MIN_VIDEO_DURATION_SECONDS` | `180` | Minimum duration after transcript retrieval; helps filter Shorts and short clips. |
| `TELEGRAM_MESSAGE_INTERVAL_SECONDS` | `0.5` | Delay between Telegram messages. |
| `TRANSCRIPT_RETRY_DELAYS_MINUTES` | `15,60,180` | Retry schedule for transcript failures. |
| `SUMMARY_RETRY_DELAYS_MINUTES` | `5,15,45` | Retry schedule for Gemini summary failures. |
| `TRANSCRIPT_GLOBAL_BLOCK_COOLDOWN_MINUTES` | `120` | Global pause after YouTube transcript request blocking. |

## Operational Recovery

Requeue a video without editing SQLite manually:

```bash
uv run youtube-feed requeue-video --id 56 --stage summary
uv run youtube-feed requeue-video --youtube-video-id dQw --stage transcript
```

## Scheduling

Recommended approach: run `uv run youtube-feed poll` every 12 hours via cron or a similar scheduler. Adjust frequency based on your needs and Gemini rate limits.

Example cron entry:

```cron
15 */12 * * * cd /path/to/youtube-feed && uv run youtube-feed poll
```

## Development

```bash
uv sync --dev
uv run ruff check .
uv run ruff format .
uv run pytest
```

## YouTube Transcript Blocking

YouTube can temporarily block transcript requests from an IP address, especially after many transcript lookups or when running from cloud provider IP ranges. The poller limits transcript attempts and applies a global transcript cooldown after recognized blocking, but repeated polling can still extend the block. If blocking persists, reduce polling frequency, keep `MAX_TRANSCRIPT_CANDIDATES_PER_CHANNEL_PER_POLL` low, or run from a less restricted network.
