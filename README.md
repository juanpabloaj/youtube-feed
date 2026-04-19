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

Supported variables:

- `YOUTUBE_CHANNELS_IDS`
- `GEMINI_API_KEY`
- `GEMINI_MODEL`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHANNEL_ID`
- `DATABASE_PATH`
- `SUMMARY_LANGUAGE_MODE`
- `MIN_NOTIFICATION_SCORE`
- `FIRST_RUN_MODE`
- `HTTP_TIMEOUT_SECONDS`
- `RSS_HTTP_TIMEOUT_SECONDS`
- `GEMINI_HTTP_TIMEOUT_SECONDS`
- `TELEGRAM_HTTP_TIMEOUT_SECONDS`
- `CHANNEL_LOOKUP_HTTP_TIMEOUT_SECONDS`
- `TELEGRAM_MESSAGE_LIMIT`
- `GEMINI_MIN_REQUEST_INTERVAL_SECONDS`
- `GEMINI_COOLDOWN_AFTER_RATE_LIMIT_SECONDS`
- `GEMINI_COOLDOWN_AFTER_TIMEOUT_SECONDS`
- `GEMINI_COOLDOWN_AFTER_SERVICE_UNAVAILABLE_SECONDS`
- `GEMINI_TRANSPORT_INLINE_RETRY_DELAYS_SECONDS`
- `GEMINI_MAX_INPUT_TOKENS_PER_MINUTE`
- `MAX_TRANSCRIPT_VIDEO_AGE_DAYS`
- `MAX_TRANSCRIPT_CANDIDATES_PER_CHANNEL_PER_POLL`
- `MIN_VIDEO_DURATION_SECONDS`
- `TELEGRAM_MESSAGE_INTERVAL_SECONDS`
- `TRANSCRIPT_RETRY_DELAYS_MINUTES`
- `SUMMARY_RETRY_DELAYS_MINUTES`
- `TRANSCRIPT_GLOBAL_BLOCK_COOLDOWN_MINUTES`

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
