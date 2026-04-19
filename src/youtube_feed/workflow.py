from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx

from youtube_feed.config import AppConfig
from youtube_feed.db import Database
from youtube_feed.exceptions import (
    GeminiHTTPError,
    GeminiResponseError,
    GeminiTransportError,
    TelegramHTTPError,
    TranscriptBlockedError,
    TranscriptTemporaryError,
    TranscriptUnavailableError,
)
from youtube_feed.gemini_rate_limiter import GeminiRateLimiter
from youtube_feed.models import TranscriptData, TranscriptSnippet
from youtube_feed.retry_policy import RetryPolicy
from youtube_feed.scoring import should_notify
from youtube_feed.summarizer import PROMPT_VERSION, estimate_input_tokens
from youtube_feed.telegram import render_message

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class PollStats:
    inserted_videos: int = 0
    bootstrap_marked_seen: int = 0
    transcripts_ready: int = 0
    transcript_unavailable: int = 0
    summarized: int = 0
    notification_sent: int = 0
    notification_skipped: int = 0


class PollWorkflow:
    def __init__(
        self,
        *,
        config: AppConfig,
        database: Database,
        rss_client,
        transcript_service,
        summarizer,
        notifier,
        logger: logging.Logger | None = None,
        monotonic_func=None,
        sleep_func=None,
    ) -> None:
        self._config = config
        self._database = database
        self._rss_client = rss_client
        self._transcript_service = transcript_service
        self._summarizer = summarizer
        self._notifier = notifier
        self._logger = logger or LOGGER
        self._sleep = sleep_func or time.sleep
        self._telegram_messages_sent_in_run = 0
        self._retry_policy = RetryPolicy(
            database=database,
            transcript_retry_delays_minutes=config.transcript_retry_delays_minutes,
            summary_retry_delays_minutes=config.summary_retry_delays_minutes,
            now_func=_utc_now,
        )
        self._gemini_rate_limiter = GeminiRateLimiter(
            min_request_interval_seconds=config.gemini_min_request_interval_seconds,
            max_input_tokens_per_minute=config.gemini_max_input_tokens_per_minute,
            logger=self._logger,
            monotonic_func=monotonic_func,
            sleep_func=sleep_func,
        )

    def run(self) -> PollStats:
        self._database.init_schema()
        self._telegram_messages_sent_in_run = 0
        stats = PollStats()
        is_bootstrap = self._database.count_videos() == 0
        initial_status = "pending_transcript"
        if is_bootstrap and self._config.first_run_mode == "mark_seen":
            initial_status = "seen"

        inserted_count = 0
        for channel_id in self._config.youtube_channel_ids:
            try:
                feed_videos = self._rss_client.fetch_channel(channel_id)
            except httpx.HTTPError as exc:
                self._logger.warning("RSS fetch failed for channel %s: %s", channel_id, exc)
                continue
            inserted_count += self._database.upsert_feed_videos(
                feed_videos,
                initial_status=initial_status,
            )

        stats = PollStats(inserted_videos=inserted_count)
        if is_bootstrap and self._config.first_run_mode == "mark_seen":
            return PollStats(
                inserted_videos=inserted_count,
                bootstrap_marked_seen=inserted_count,
            )

        transcripts_ready, transcript_unavailable = self._process_transcripts()
        summarized, notification_skipped, immediate_notification_sent = self._process_summaries()
        retry_notification_sent = self._process_notifications()
        notification_sent = immediate_notification_sent + retry_notification_sent
        return PollStats(
            inserted_videos=stats.inserted_videos,
            bootstrap_marked_seen=stats.bootstrap_marked_seen,
            transcripts_ready=transcripts_ready,
            transcript_unavailable=transcript_unavailable,
            summarized=summarized,
            notification_sent=notification_sent,
            notification_skipped=notification_skipped,
        )

    def _process_transcripts(self) -> tuple[int, int]:
        ready_count = 0
        unavailable_count = 0
        now = _utc_now()
        blocked_until = self._database.get_transcript_blocked_until()
        if blocked_until is not None and now < blocked_until:
            block_kind = self._database.get_transcript_block_kind() or "request_blocked"
            self._logger.info(
                "Transcript fetching remains globally paused until %s due to %s.",
                blocked_until.isoformat(),
                _describe_transcript_block_kind(block_kind),
            )
            return ready_count, unavailable_count
        if blocked_until is not None and now >= blocked_until:
            self._database.clear_transcript_block()
            self._logger.info(
                "Transcript global block cooldown expired; resuming transcript fetches."
            )
        deferred_count = self._database.count_deferred_videos(("pending_transcript",), now=now)
        if deferred_count:
            self._logger.info(
                "Skipping %s transcript candidate(s) until their retry window opens.",
                deferred_count,
            )
        due_videos = self._database.list_due_videos(("pending_transcript",), now=now)
        selected_videos = self._select_transcript_candidates(due_videos, now=now)
        selected_video_ids = {video.id for video in selected_videos}
        channels_with_accepted_candidate: set[str] = set()

        for video in selected_videos:
            try:
                transcript = self._transcript_service.fetch(video.youtube_video_id)
            except TranscriptUnavailableError as exc:
                next_retry_at, finalized = self._retry_policy.schedule_or_finalize(
                    video_id=video.id,
                    retry_count=video.retry_count,
                    error=str(exc),
                    stage="transcript",
                )
                if finalized:
                    unavailable_count += 1
                if next_retry_at is not None:
                    self._logger.warning(
                        ("Transcript unavailable for video %s (%s): %s Retry scheduled for %s."),
                        video.youtube_video_id,
                        _short_title(video.title),
                        exc,
                        next_retry_at.isoformat(),
                    )
                else:
                    self._logger.warning(
                        (
                            "Transcript unavailable for video %s (%s): %s "
                            "Retry budget exhausted; marked as transcript_unavailable."
                        ),
                        video.youtube_video_id,
                        _short_title(video.title),
                        exc,
                    )
                continue
            except TranscriptBlockedError as exc:
                global_block_until = _utc_now() + timedelta(
                    minutes=_transcript_block_cooldown_minutes(self._config, exc.kind)
                )
                self._database.defer_transcript_until(
                    video_id=video.id,
                    retry_count=video.retry_count,
                    next_retry_at=global_block_until,
                    last_error=str(exc),
                )
                self._database.set_transcript_block(
                    blocked_until=global_block_until,
                    reason=str(exc),
                    kind=exc.kind,
                )
                self._logger.warning(
                    (
                        "YouTube blocked transcript fetching for video %s (%s) due to %s. "
                        "Pausing all transcript requests until %s."
                    ),
                    video.youtube_video_id,
                    _short_title(video.title),
                    _describe_transcript_block_kind(exc.kind),
                    global_block_until.isoformat(),
                )
                break
            except TranscriptTemporaryError as exc:
                next_retry_at, _ = self._retry_policy.schedule_retry(
                    video_id=video.id,
                    retry_count=video.retry_count,
                    error=str(exc),
                    stage="transcript",
                )
                self._logger.warning(
                    "Transcript retrieval should be retried for video %s (%s): %s. %s.",
                    video.youtube_video_id,
                    _short_title(video.title),
                    exc,
                    (
                        f"Retry scheduled for {next_retry_at.isoformat()}"
                        if next_retry_at is not None
                        else "Retry budget exhausted; marked as transcript_unavailable"
                    ),
                )
                continue

            if transcript.duration_seconds < self._config.min_video_duration_seconds:
                self._database.mark_video_too_short(
                    video.id,
                    transcript=transcript,
                    min_duration_seconds=self._config.min_video_duration_seconds,
                )
                self._logger.info(
                    (
                        "Transcript skipped for video %s because duration_seconds=%s is below "
                        "minimum=%s."
                    ),
                    video.youtube_video_id,
                    transcript.duration_seconds,
                    self._config.min_video_duration_seconds,
                )
                continue

            self._database.save_transcript(video.id, transcript)
            self._logger.info(
                "Transcript captured for video %s (language=%s, duration_seconds=%s).",
                video.youtube_video_id,
                transcript.language_code,
                transcript.duration_seconds,
            )
            channels_with_accepted_candidate.add(video.channel_id)
            ready_count += 1

        for video in due_videos:
            if (
                video.id in selected_video_ids
                or video.channel_id not in channels_with_accepted_candidate
            ):
                continue
            self._database.mark_transcript_skipped(
                video.id,
                "Skipped transcript fetch due to channel recency selection policy.",
            )
            self._logger.debug(
                "Transcript skipped for video %s due to channel recency selection policy.",
                video.youtube_video_id,
            )
        return ready_count, unavailable_count

    def _select_transcript_candidates(
        self,
        videos: list,
        *,
        now: datetime,
    ) -> list:
        max_age = timedelta(days=self._config.max_transcript_video_age_days)
        grouped: dict[str, list] = {}
        for video in videos:
            if _is_youtube_short_url(video.url):
                self._database.mark_transcript_skipped(
                    video.id,
                    "Skipped transcript fetch because the RSS link identifies the item as a Short.",
                )
                self._logger.info(
                    "Transcript skipped for video %s because it is a YouTube Short.",
                    video.youtube_video_id,
                )
                continue
            if now - video.published_at > max_age:
                self._database.mark_transcript_skipped(
                    video.id,
                    "Skipped transcript fetch because the video is older than the configured age.",
                )
                self._logger.debug(
                    ("Transcript skipped for video %s because it is older than %s day(s)."),
                    video.youtube_video_id,
                    self._config.max_transcript_video_age_days,
                )
                continue
            grouped.setdefault(video.channel_id, []).append(video)

        selected: list = []
        limit = self._config.max_transcript_candidates_per_channel_per_poll
        for channel_videos in grouped.values():
            channel_videos.sort(key=lambda item: item.published_at, reverse=True)
            selected.extend(channel_videos[:limit])
        selected.sort(key=lambda item: item.published_at)
        return selected

    def _process_summaries(self) -> tuple[int, int, int]:
        summarized_count = 0
        skipped_count = 0
        sent_count = 0
        now = _utc_now()
        deferred_count = self._database.count_deferred_videos(("ready_for_summary",), now=now)
        if deferred_count:
            self._logger.info(
                "Skipping %s summary candidate(s) until their retry window opens.",
                deferred_count,
            )
        due_videos = self._database.list_due_videos(("ready_for_summary",), now=now)
        for index, video in enumerate(due_videos):
            transcript = self._build_transcript_from_video(video)
            if transcript is None:
                self._database.mark_summary_error(
                    video.id,
                    "Transcript content was missing before summary.",
                )
                continue
            estimated_tokens = estimate_input_tokens(
                video_title=video.title,
                channel_title=video.channel_title,
                transcript=transcript,
            )
            try:
                summary = self._summarize_with_inline_transport_retries(
                    video,
                    transcript,
                    estimated_tokens=estimated_tokens,
                )
                self._gemini_rate_limiter.mark_request_completed()
            except GeminiHTTPError as exc:
                if exc.retryable:
                    next_retry_at, _ = self._retry_policy.schedule_retry(
                        video_id=video.id,
                        retry_count=video.retry_count,
                        error=str(exc),
                        stage="summary",
                    )
                    if next_retry_at is not None:
                        self._logger.warning(
                            (
                                "Gemini returned HTTP %s for video %s (%s): %s "
                                "Retry scheduled for %s."
                            ),
                            exc.status_code,
                            video.youtube_video_id,
                            _short_title(video.title),
                            exc,
                            next_retry_at.isoformat(),
                        )
                    if exc.status_code == 429:
                        remaining = len(due_videos) - index - 1
                        self._logger.warning(
                            (
                                "Stopping Gemini summary processing for this poll after HTTP 429. "
                                "%s video(s) remain queued for a future poll."
                            ),
                            remaining,
                        )
                        cooldown_seconds = (
                            exc.retry_after_seconds
                            if exc.retry_after_seconds is not None
                            else self._config.gemini_cooldown_after_rate_limit_seconds
                        )
                        self._gemini_rate_limiter.cooldown(
                            seconds=cooldown_seconds,
                            reason=(
                                "HTTP 429 with Retry-After"
                                if exc.retry_after_seconds is not None
                                else "HTTP 429"
                            ),
                        )
                        break
                    if exc.status_code == 503:
                        self._gemini_rate_limiter.cooldown(
                            seconds=self._config.gemini_cooldown_after_service_unavailable_seconds,
                            reason="HTTP 503",
                        )
                else:
                    self._database.mark_summary_error(video.id, str(exc))
                    if exc.status_code in {401, 403, 404}:
                        remaining = len(due_videos) - index - 1
                        self._logger.error(
                            (
                                "Gemini returned HTTP %s for video %s (%s): %s "
                                "Stopping Gemini summary processing for this poll. "
                                "%s video(s) remain queued for a future poll."
                            ),
                            exc.status_code,
                            video.youtube_video_id,
                            _short_title(video.title),
                            exc,
                            remaining,
                        )
                        break
                    self._logger.warning(
                        (
                            "Gemini returned HTTP %s for video %s (%s): %s "
                            "Summary marked as error; no retry scheduled."
                        ),
                        exc.status_code,
                        video.youtube_video_id,
                        _short_title(video.title),
                        exc,
                    )
                continue
            except GeminiTransportError as exc:
                next_retry_at, _ = self._retry_policy.schedule_retry(
                    video_id=video.id,
                    retry_count=video.retry_count,
                    error=str(exc),
                    stage="summary",
                )
                if next_retry_at is not None:
                    self._logger.warning(
                        ("Gemini transport error for video %s (%s): %s Retry scheduled for %s."),
                        video.youtube_video_id,
                        _short_title(video.title),
                        exc,
                        next_retry_at.isoformat(),
                    )
                else:
                    self._logger.warning(
                        (
                            "Gemini transport error for video %s (%s): %s "
                            "Retry budget exhausted; summary marked as error."
                        ),
                        video.youtube_video_id,
                        _short_title(video.title),
                        exc,
                    )
                self._gemini_rate_limiter.cooldown(
                    seconds=self._config.gemini_cooldown_after_timeout_seconds,
                    reason="transport error",
                )
                continue
            except GeminiResponseError as exc:
                next_retry_at, _ = self._retry_policy.schedule_or_finalize(
                    video_id=video.id,
                    retry_count=video.retry_count,
                    error=str(exc),
                    stage="summary",
                )
                if next_retry_at is not None:
                    self._logger.warning(
                        (
                            "Gemini returned an invalid structured response for video %s (%s): %s "
                            "Retry scheduled for %s."
                        ),
                        video.youtube_video_id,
                        _short_title(video.title),
                        exc,
                        next_retry_at.isoformat(),
                    )
                else:
                    self._logger.warning(
                        (
                            "Gemini returned an invalid structured response for video %s (%s): %s "
                            "Summary marked as error; no retry scheduled."
                        ),
                        video.youtube_video_id,
                        _short_title(video.title),
                        exc,
                    )
                continue

            transcript_hash = hashlib.sha256(transcript.text.encode("utf-8")).hexdigest()
            self._database.store_analysis(
                video_id=video.id,
                model=self._summarizer.model,
                prompt_version=PROMPT_VERSION,
                transcript=transcript,
                summary=summary,
                transcript_hash=transcript_hash,
            )
            summarized_count += 1

            if not should_notify(summary, self._config.min_notification_score):
                self._database.mark_notification_skipped(video.id)
                self._logger.info(
                    (
                        "Summary skipped for Telegram for video %s "
                        "(priority=%s, score=%s, threshold=%s)."
                    ),
                    video.youtube_video_id,
                    summary.priority,
                    summary.score,
                    self._config.min_notification_score,
                )
                skipped_count += 1
                continue

            self._logger.info(
                "Summary accepted for Telegram for video %s (priority=%s, score=%s).",
                video.youtube_video_id,
                summary.priority,
                summary.score,
            )
            if self._deliver_notification(video, summary):
                sent_count += 1

        return summarized_count, skipped_count, sent_count

    def _process_notifications(self) -> int:
        sent_count = 0
        pending_notifications = self._database.list_pending_notifications(
            min_score=self._config.min_notification_score,
        )
        if pending_notifications:
            self._logger.info(
                "Found %s pending Telegram notifications.",
                len(pending_notifications),
            )
        else:
            self._logger.debug("No pending Telegram notifications were found.")

        for video, analysis in pending_notifications:
            if self._deliver_notification(video, analysis):
                sent_count += 1
        return sent_count

    def _deliver_notification(self, video, analysis) -> bool:
        if (
            self._telegram_messages_sent_in_run > 0
            and self._config.telegram_message_interval_seconds > 0
        ):
            self._logger.debug(
                "Sleeping %.2fs before the next Telegram message.",
                self._config.telegram_message_interval_seconds,
            )
            self._sleep(self._config.telegram_message_interval_seconds)
        message_text = render_message(
            video,
            analysis,
            limit=self._config.telegram_message_limit,
        )
        self._logger.info(
            "Sending Telegram notification for video %s.",
            video.youtube_video_id,
        )
        try:
            message_id = self._notifier.send_message(message_text)
        except TelegramHTTPError as exc:
            self._logger.warning(
                "Telegram delivery failed for video %s: %s",
                video.youtube_video_id,
                exc,
            )
            self._database.record_notification(
                video_id=video.id,
                target_chat_id=self._config.telegram_chat_id,
                message_text=message_text,
                delivery_status="failed",
                telegram_message_id=None,
                error=str(exc),
            )
            return False

        self._database.record_notification(
            video_id=video.id,
            target_chat_id=self._config.telegram_chat_id,
            message_text=message_text,
            delivery_status="delivered",
            telegram_message_id=message_id,
            error=None,
        )
        self._database.mark_notification_sent(video.id)
        self._logger.info(
            "Telegram notification delivered for video %s with message_id=%s.",
            video.youtube_video_id,
            message_id,
        )
        self._telegram_messages_sent_in_run += 1
        return True

    def _summarize_with_inline_transport_retries(self, video, transcript, *, estimated_tokens: int):
        delays = (0, *self._config.gemini_transport_inline_retry_delays_seconds)
        last_exc: GeminiTransportError | None = None
        for attempt_index, delay_seconds in enumerate(delays, start=1):
            if delay_seconds > 0:
                self._logger.warning(
                    (
                        "Retrying Gemini summary for video %s after transport error. "
                        "attempt=%s/%s sleep_seconds=%s."
                    ),
                    video.youtube_video_id,
                    attempt_index,
                    len(delays),
                    delay_seconds,
                )
                self._sleep(delay_seconds)
            try:
                self._gemini_rate_limiter.reserve_slot(estimated_tokens)
                return self._summarizer.summarize(
                    video_title=video.title,
                    channel_title=video.channel_title,
                    transcript=transcript,
                )
            except GeminiTransportError as exc:
                self._gemini_rate_limiter.mark_request_completed()
                last_exc = exc
                if attempt_index == len(delays):
                    raise
                self._logger.warning(
                    (
                        "Gemini transport error for video %s (%s): %s "
                        "Will retry inline before scheduling persistent retry."
                    ),
                    video.youtube_video_id,
                    _short_title(video.title),
                    exc,
                )
        if last_exc is None:
            raise RuntimeError("Gemini inline retry loop finished without a result.")
        raise last_exc

    def _build_transcript_from_video(self, video) -> TranscriptData | None:
        if (
            not video.transcript_text
            or not video.transcript_language
            or not video.transcript_language_code
        ):
            return None
        duration = float(video.duration_seconds or 0)
        return TranscriptData(
            video_id=video.youtube_video_id,
            language=video.transcript_language,
            language_code=video.transcript_language_code,
            is_generated=False,
            snippets=(
                TranscriptSnippet(
                    text=video.transcript_text,
                    start=0.0,
                    duration=duration,
                ),
            ),
        )


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _short_title(title: str, limit: int = 80) -> str:
    if len(title) <= limit:
        return title
    return f"{title[: limit - 3].rstrip()}..."


def _describe_transcript_block_kind(kind: str) -> str:
    if kind == "ip_blocked":
        return "YouTube IP blocking"
    if kind == "request_blocked":
        return "YouTube request blocking"
    return "YouTube transcript blocking"


def _transcript_block_cooldown_minutes(config: AppConfig, block_kind: str) -> int:
    if block_kind == "request_blocked":
        return max(15, config.transcript_global_block_cooldown_minutes // 4)
    return config.transcript_global_block_cooldown_minutes


def _is_youtube_short_url(url: str) -> bool:
    return "/shorts/" in url
