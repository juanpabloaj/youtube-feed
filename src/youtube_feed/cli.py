from __future__ import annotations

import logging
from contextlib import ExitStack
from uuid import uuid4

import httpx
import typer

from youtube_feed.channel_lookup import YouTubeChannelLookupClient, build_search_url
from youtube_feed.config import AppConfig
from youtube_feed.db import Database
from youtube_feed.exceptions import ConfigurationError, GeminiError, TelegramError
from youtube_feed.logging_config import configure_logging
from youtube_feed.rss import YouTubeRssClient
from youtube_feed.summarizer import GeminiSummarizer
from youtube_feed.telegram import TelegramNotifier
from youtube_feed.transcripts import TranscriptService
from youtube_feed.workflow import PollWorkflow

app = typer.Typer(add_completion=False, no_args_is_help=True)
LOGGER = logging.getLogger(__name__)


def _resolve_video(database: Database, video_id: int | None, youtube_video_id: str | None):
    if (video_id is None) == (youtube_video_id is None):
        raise typer.BadParameter("Provide exactly one of --id or --youtube-video-id.")
    if video_id is not None:
        video = database.get_video(video_id)
    else:
        video = database.get_video_by_youtube_id(youtube_video_id or "")
    if video is None:
        raise typer.BadParameter("Video not found.")
    return video


@app.command("init-db")
def init_db() -> None:
    config = _load_config()
    config.validate_for_init_db()
    database = Database(config.database_path)
    try:
        database.init_schema()
    finally:
        database.close()
    typer.echo("Database initialized.")


@app.command()
def poll() -> None:
    poll_run_id = uuid4().hex[:8]
    logger = _PrefixedLoggerAdapter(LOGGER, {"prefix": f"[poll_run_id={poll_run_id}] "})
    config = _load_config(use_logging=True, logger=logger)
    config.validate_for_poll()
    logger.info("Starting poll.")
    with ExitStack() as stack:
        rss_http_client = stack.enter_context(httpx.Client(timeout=config.rss_http_timeout_seconds))
        gemini_http_client = stack.enter_context(
            httpx.Client(timeout=config.gemini_http_timeout_seconds)
        )
        telegram_http_client = stack.enter_context(
            httpx.Client(timeout=config.telegram_http_timeout_seconds)
        )
        database = Database(config.database_path)
        try:
            workflow = PollWorkflow(
                config=config,
                database=database,
                rss_client=YouTubeRssClient(rss_http_client),
                transcript_service=TranscriptService(),
                summarizer=GeminiSummarizer(
                    config.gemini_api_key,
                    config.gemini_model,
                    gemini_http_client,
                ),
                notifier=TelegramNotifier(
                    config.telegram_bot_token,
                    config.telegram_chat_id,
                    telegram_http_client,
                ),
                logger=logger,
            )
            stats = workflow.run()
        finally:
            database.close()
    logger.info(
        "Poll finished: "
        f"inserted={stats.inserted_videos} "
        f"bootstrap_marked_seen={stats.bootstrap_marked_seen} "
        f"transcripts_ready={stats.transcripts_ready} "
        f"transcript_unavailable={stats.transcript_unavailable} "
        f"summarized={stats.summarized} "
        f"notification_sent={stats.notification_sent} "
        f"notification_skipped={stats.notification_skipped}"
    )


@app.command()
def doctor() -> None:
    config = _load_config()
    config.validate_for_doctor()
    database = Database(config.database_path)
    try:
        database.init_schema()
        typer.echo("Database: OK")
    finally:
        database.close()

    with ExitStack() as stack:
        rss_http_client = stack.enter_context(httpx.Client(timeout=config.rss_http_timeout_seconds))
        gemini_http_client = stack.enter_context(
            httpx.Client(timeout=config.gemini_http_timeout_seconds)
        )
        telegram_http_client = stack.enter_context(
            httpx.Client(timeout=config.telegram_http_timeout_seconds)
        )
        rss_client = YouTubeRssClient(rss_http_client)
        feed = rss_client.fetch_channel(config.youtube_channel_ids[0])
        typer.echo(f"RSS: OK ({len(feed)} entries from {config.youtube_channel_ids[0]})")

        summarizer = GeminiSummarizer(
            config.gemini_api_key,
            config.gemini_model,
            gemini_http_client,
        )
        gemini_response = summarizer.healthcheck()
        typer.echo(f"Gemini: OK ({gemini_response})")

        notifier = TelegramNotifier(
            config.telegram_bot_token,
            config.telegram_chat_id,
            telegram_http_client,
        )
        telegram_data = notifier.healthcheck()
        chat_label = telegram_data["chat_title"] or config.telegram_chat_id
        typer.echo(f"Telegram: OK (bot=@{telegram_data['bot_username']}, chat={chat_label})")


@app.command("find-channel-id")
def find_channel_id(
    query: str = typer.Argument(..., help="YouTube channel name to search for."),
    limit: int = typer.Option(
        5,
        "--limit",
        min=1,
        max=10,
        help="Maximum number of candidates to show.",
    ),
) -> None:
    config = _load_config()
    with httpx.Client(
        timeout=config.channel_lookup_http_timeout_seconds,
        follow_redirects=True,
    ) as http_client:
        lookup_client = YouTubeChannelLookupClient(http_client)
        results = lookup_client.search(query, limit=limit)

    typer.echo(f"Search URL: {build_search_url(query)}")
    if not results:
        typer.echo("No channel candidates were found.")
        raise typer.Exit(code=1)

    typer.echo(f"Found {len(results)} candidate(s):")
    for index, result in enumerate(results, start=1):
        typer.echo(f"{index}. title={result.title}")
        typer.echo(f"   channel_id={result.channel_id}")
        typer.echo(f"   handle={result.handle or 'n/a'}")
        typer.echo(f"   url={result.url}")
        if result.description:
            typer.echo(f"   description={result.description}")


@app.command("requeue-video")
def requeue_video(
    id: int | None = typer.Option(None, "--id", help="Internal SQLite video id."),
    youtube_video_id: str | None = typer.Option(
        None,
        "--youtube-video-id",
        help="YouTube video id such as dQw4w9WgXcQ.",
    ),
    stage: str = typer.Option(
        "auto",
        "--stage",
        help="Which stage to requeue: auto, transcript, summary, or notification.",
    ),
) -> None:
    config = _load_config()
    config.validate_for_init_db()
    database = Database(config.database_path)
    try:
        database.init_schema()
        video = _resolve_video(database, id, youtube_video_id)
        analysis = database.get_analysis_for_video(video.id)

        normalized_stage = stage.strip().lower()
        if normalized_stage == "auto":
            if analysis is not None:
                target_status = "summarized"
            elif video.transcript_text and video.transcript_language_code:
                target_status = "ready_for_summary"
            else:
                target_status = "pending_transcript"
        elif normalized_stage == "transcript":
            target_status = "pending_transcript"
        elif normalized_stage == "summary":
            if not video.transcript_text or not video.transcript_language_code:
                raise typer.BadParameter("Cannot requeue summary: transcript is missing.")
            target_status = "ready_for_summary"
        elif normalized_stage == "notification":
            if analysis is None:
                raise typer.BadParameter("Cannot requeue notification: analysis is missing.")
            target_status = "summarized"
        else:
            raise typer.BadParameter(
                "Stage must be one of: auto, transcript, summary, notification."
            )

        database.requeue_video(video.id, status=target_status)
        typer.echo(
            f"Video {video.youtube_video_id} requeued. "
            f"Old status={video.status} new status={target_status}"
        )
    finally:
        database.close()


def main() -> None:
    configure_logging()
    try:
        app()
    except ConfigurationError as exc:
        typer.echo(f"Configuration error: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    except (GeminiError, TelegramError, httpx.HTTPError) as exc:
        LOGGER.error("%s", exc)
        raise typer.Exit(code=1) from exc


class _PrefixedLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f"{self.extra['prefix']}{msg}", kwargs


def _load_config(
    *,
    use_logging: bool = False,
    logger: logging.Logger | logging.LoggerAdapter | None = None,
) -> AppConfig:
    config = AppConfig.from_env()
    config.ensure_data_directory()
    if use_logging:
        active_logger = logger or LOGGER
        for line in config.render_summary().splitlines():
            active_logger.info("%s", line)
    else:
        typer.echo(config.render_summary())
    return config
