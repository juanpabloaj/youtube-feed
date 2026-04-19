from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from xml.etree.ElementTree import ParseError

import httpx
import pytest
from requests import Timeout

from youtube_feed.config import AppConfig
from youtube_feed.db import Database
from youtube_feed.exceptions import (
    GeminiHTTPError,
    GeminiResponseError,
    GeminiTransportError,
    TranscriptBlockedError,
    TranscriptTemporaryError,
    TranscriptUnavailableError,
)
from youtube_feed.models import FeedVideo, SummaryResult, TranscriptData, TranscriptSnippet
from youtube_feed.summarizer import GeminiSummarizer
from youtube_feed.transcripts import TranscriptService
from youtube_feed.workflow import PollWorkflow


class FakeRssClient:
    def __init__(self, entries: list[FeedVideo]) -> None:
        self._entries = entries

    def fetch_channel(self, channel_id: str) -> list[FeedVideo]:
        return [entry for entry in self._entries if entry.channel_id == channel_id]


class FakeTranscriptService:
    def __init__(self, results: dict[str, TranscriptData | Exception]) -> None:
        self._results = results
        self.calls: list[str] = []

    def fetch(self, video_id: str) -> TranscriptData:
        self.calls.append(video_id)
        result = self._results[video_id]
        if isinstance(result, Exception):
            raise result
        return result


class FakeSummarizer:
    def __init__(self, result: SummaryResult | Exception) -> None:
        self.model = "gemini-2.0-flash"
        self._result = result
        self.calls: list[tuple[str, str, TranscriptData]] = []

    def summarize(
        self,
        *,
        video_title: str,
        channel_title: str,
        transcript: TranscriptData,
    ) -> SummaryResult:
        self.calls.append((video_title, channel_title, transcript))
        if isinstance(self._result, Exception):
            raise self._result
        return self._result


class SequenceSummarizer:
    def __init__(self, results: list[SummaryResult | Exception]) -> None:
        self.model = "gemini-2.0-flash"
        self._results = results
        self.calls: list[tuple[str, str, TranscriptData]] = []

    def summarize(
        self,
        *,
        video_title: str,
        channel_title: str,
        transcript: TranscriptData,
    ) -> SummaryResult:
        self.calls.append((video_title, channel_title, transcript))
        result = self._results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class FakeNotifier:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def send_message(self, message_text: str) -> int:
        self.messages.append(message_text)
        return len(self.messages)


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def make_config(
    tmp_path: Path,
    *,
    first_run_mode: str = "mark_seen",
    min_score: int = 75,
    min_video_duration_seconds: int = 0,
) -> AppConfig:
    return AppConfig(
        youtube_channel_ids=("channel-1",),
        gemini_api_key="gemini-secret",
        gemini_model="gemini-2.0-flash",
        telegram_bot_token="telegram-secret",
        telegram_chat_id="@feed",
        database_path=tmp_path / "youtube_feed.db",
        summary_language_mode="transcript",
        min_notification_score=min_score,
        first_run_mode=first_run_mode,
        http_timeout_seconds=30.0,
        telegram_message_limit=3800,
        gemini_min_request_interval_seconds=0.0,
        gemini_cooldown_after_rate_limit_seconds=0.0,
        gemini_cooldown_after_timeout_seconds=0.0,
        gemini_cooldown_after_service_unavailable_seconds=0.0,
        gemini_transport_inline_retry_delays_seconds=(),
        gemini_max_input_tokens_per_minute=225000,
        max_transcript_video_age_days=7,
        max_transcript_candidates_per_channel_per_poll=1,
        min_video_duration_seconds=min_video_duration_seconds,
        telegram_message_interval_seconds=0.0,
    )


def make_feed_video(video_id: str = "video-1", *, channel_id: str = "channel-1") -> FeedVideo:
    return FeedVideo(
        youtube_video_id=video_id,
        channel_id=channel_id,
        channel_title=f"Channel {channel_id}",
        title="A useful video",
        published_at=datetime(2026, 4, 17, 12, 0, tzinfo=UTC),
        url=f"https://www.youtube.com/watch?v={video_id}",
    )


def make_transcript(language: str = "Spanish", code: str = "es") -> TranscriptData:
    return TranscriptData(
        video_id="video-1",
        language=language,
        language_code=code,
        is_generated=False,
        snippets=(
            TranscriptSnippet(text="hola mundo", start=0.0, duration=120.0),
            TranscriptSnippet(text="esto explica el tema", start=120.0, duration=120.0),
        ),
    )


def test_first_run_mark_seen_is_bootstrap_only(tmp_path) -> None:
    config = make_config(tmp_path, first_run_mode="mark_seen")
    database = Database(config.database_path)
    rss_client = FakeRssClient([make_feed_video()])
    workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=rss_client,
        transcript_service=FakeTranscriptService({"video-1": make_transcript()}),
        summarizer=FakeSummarizer(
            SummaryResult(
                score=90,
                priority="high",
                confidence=90,
                why_it_matters="Useful",
                summary_bullets=("a", "b", "c"),
            )
        ),
        notifier=FakeNotifier(),
    )

    stats = workflow.run()
    stored = database.get_video_by_youtube_id("video-1")
    database.close()

    assert stats.bootstrap_marked_seen == 1
    assert stats.notification_sent == 0
    assert stored is not None
    assert stored.status == "seen"


def test_poll_is_idempotent_and_sends_single_notification(tmp_path) -> None:
    config = make_config(tmp_path, first_run_mode="process_all")
    database = Database(config.database_path)
    transcript_service = FakeTranscriptService(
        {"video-1": make_transcript(language="English", code="en")}
    )
    summarizer = FakeSummarizer(
        SummaryResult(
            score=84,
            priority="high",
            confidence=91,
            why_it_matters="It has practical takeaways.",
            summary_bullets=("one", "two", "three"),
        )
    )
    notifier = FakeNotifier()
    workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=FakeRssClient([make_feed_video()]),
        transcript_service=transcript_service,
        summarizer=summarizer,
        notifier=notifier,
    )

    first = workflow.run()
    second = workflow.run()
    stored = database.get_video_by_youtube_id("video-1")
    analysis = database.get_analysis_for_video(stored.id if stored else -1)
    database.close()

    assert first.notification_sent == 1
    assert second.notification_sent == 0
    assert len(notifier.messages) == 1
    assert stored is not None
    assert stored.status == "notification_sent"
    assert analysis is not None


def test_summary_uses_transcript_language_and_skips_low_priority(tmp_path) -> None:
    config = make_config(tmp_path, first_run_mode="process_all", min_score=75)
    database = Database(config.database_path)
    transcript = make_transcript(language="Portuguese", code="pt")
    summarizer = FakeSummarizer(
        SummaryResult(
            score=60,
            priority="medium",
            confidence=88,
            why_it_matters="Not enough signal.",
            summary_bullets=("um", "dois", "tres"),
        )
    )
    workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=FakeRssClient([make_feed_video()]),
        transcript_service=FakeTranscriptService({"video-1": transcript}),
        summarizer=summarizer,
        notifier=FakeNotifier(),
    )

    stats = workflow.run()
    stored = database.get_video_by_youtube_id("video-1")
    analysis = database.get_analysis_for_video(stored.id if stored else -1)
    database.close()

    assert stats.notification_skipped == 1
    assert summarizer.calls[0][2].language_code == "pt"
    assert stored is not None
    assert stored.status == "summarized"
    assert analysis is not None
    assert analysis.transcript_language_code == "pt"


def test_lower_threshold_can_notify_previously_skipped_summary(tmp_path) -> None:
    config = make_config(tmp_path, first_run_mode="process_all", min_score=80)
    database = Database(config.database_path)
    transcript = make_transcript(language="English", code="en")
    notifier = FakeNotifier()
    workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=FakeRssClient([make_feed_video()]),
        transcript_service=FakeTranscriptService({"video-1": transcript}),
        summarizer=FakeSummarizer(
            SummaryResult(
                score=75,
                priority="high",
                confidence=90,
                why_it_matters="Useful",
                summary_bullets=("a", "b", "c"),
            )
        ),
        notifier=notifier,
    )

    first = workflow.run()
    stored = database.get_video_by_youtube_id("video-1")
    assert first.notification_sent == 0
    assert first.notification_skipped == 1
    assert stored is not None
    assert stored.status == "summarized"

    lower_threshold_config = AppConfig(
        youtube_channel_ids=config.youtube_channel_ids,
        gemini_api_key=config.gemini_api_key,
        gemini_model=config.gemini_model,
        telegram_bot_token=config.telegram_bot_token,
        telegram_chat_id=config.telegram_chat_id,
        database_path=config.database_path,
        summary_language_mode=config.summary_language_mode,
        min_notification_score=70,
        first_run_mode=config.first_run_mode,
        http_timeout_seconds=config.http_timeout_seconds,
        telegram_message_limit=config.telegram_message_limit,
        gemini_min_request_interval_seconds=config.gemini_min_request_interval_seconds,
        gemini_cooldown_after_rate_limit_seconds=config.gemini_cooldown_after_rate_limit_seconds,
        gemini_cooldown_after_timeout_seconds=config.gemini_cooldown_after_timeout_seconds,
        gemini_max_input_tokens_per_minute=config.gemini_max_input_tokens_per_minute,
        max_transcript_video_age_days=config.max_transcript_video_age_days,
        max_transcript_candidates_per_channel_per_poll=config.max_transcript_candidates_per_channel_per_poll,
        telegram_message_interval_seconds=config.telegram_message_interval_seconds,
        transcript_retry_delays_minutes=config.transcript_retry_delays_minutes,
        summary_retry_delays_minutes=config.summary_retry_delays_minutes,
    )
    second_workflow = PollWorkflow(
        config=lower_threshold_config,
        database=database,
        rss_client=FakeRssClient([make_feed_video()]),
        transcript_service=FakeTranscriptService({"video-1": transcript}),
        summarizer=FakeSummarizer(
            SummaryResult(
                score=75,
                priority="high",
                confidence=90,
                why_it_matters="Useful",
                summary_bullets=("a", "b", "c"),
            )
        ),
        notifier=notifier,
    )

    second = second_workflow.run()
    stored = database.get_video_by_youtube_id("video-1")
    database.close()

    assert second.notification_sent == 1
    assert stored is not None
    assert stored.status == "notification_sent"
    assert len(notifier.messages) == 1


def test_missing_transcript_retries_then_marks_unavailable(tmp_path) -> None:
    config = make_config(tmp_path, first_run_mode="process_all")
    database = Database(config.database_path)
    transcript_service = FakeTranscriptService(
        {"video-1": TranscriptUnavailableError("No transcript")}
    )
    workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=FakeRssClient([make_feed_video()]),
        transcript_service=transcript_service,
        summarizer=FakeSummarizer(
            SummaryResult(
                score=90,
                priority="high",
                confidence=90,
                why_it_matters="Useful",
                summary_bullets=("a", "b", "c"),
            )
        ),
        notifier=FakeNotifier(),
    )

    first = workflow.run()
    stored = database.get_video_by_youtube_id("video-1")
    assert first.transcript_unavailable == 0
    assert stored is not None
    assert stored.status == "pending_transcript"
    assert stored.retry_count == 1

    for _ in range(3):
        database.schedule_retry(
            stored.id,
            status="pending_transcript",
            retry_count=stored.retry_count,
            next_retry_at=datetime.now(tz=UTC) - timedelta(minutes=1),
            last_error="No transcript",
            stage="transcript",
        )
        workflow.run()
        stored = database.get_video_by_youtube_id("video-1")
        assert stored is not None
        if stored.status == "transcript_unavailable":
            break

    database.close()
    assert stored.status == "transcript_unavailable"


def test_transcript_block_pauses_all_remaining_transcript_requests(tmp_path, caplog) -> None:
    config = AppConfig(
        youtube_channel_ids=("channel-1", "channel-2"),
        gemini_api_key="gemini-secret",
        gemini_model="gemini-2.0-flash",
        telegram_bot_token="telegram-secret",
        telegram_chat_id="@feed",
        database_path=tmp_path / "youtube_feed.db",
        summary_language_mode="transcript",
        min_notification_score=75,
        first_run_mode="process_all",
        http_timeout_seconds=30.0,
        telegram_message_limit=3800,
        gemini_min_request_interval_seconds=0.0,
        gemini_cooldown_after_rate_limit_seconds=0.0,
        gemini_cooldown_after_timeout_seconds=0.0,
        gemini_max_input_tokens_per_minute=225000,
        max_transcript_video_age_days=7,
        max_transcript_candidates_per_channel_per_poll=1,
        telegram_message_interval_seconds=0.0,
    )
    database = Database(config.database_path)
    transcript_service = FakeTranscriptService(
        {
            "video-1": TranscriptBlockedError(
                "YouTube is blocking transcript requests from this IP.",
                kind="ip_blocked",
            ),
            "video-2": make_transcript(language="English", code="en"),
        }
    )
    workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=FakeRssClient(
            [
                make_feed_video("video-1", channel_id="channel-1"),
                make_feed_video("video-2", channel_id="channel-2"),
            ]
        ),
        transcript_service=transcript_service,
        summarizer=FakeSummarizer(
            SummaryResult(
                score=90,
                priority="high",
                confidence=90,
                why_it_matters="Useful",
                summary_bullets=("a", "b", "c"),
            )
        ),
        notifier=FakeNotifier(),
    )

    caplog.set_level("INFO")
    workflow.run()
    first = database.get_video_by_youtube_id("video-1")
    second = database.get_video_by_youtube_id("video-2")
    blocked_until = database.get_transcript_blocked_until()
    block_kind = database.get_transcript_block_kind()
    workflow.run()
    database.close()

    assert transcript_service.calls == ["video-1"]
    assert first is not None
    assert first.status == "pending_transcript"
    assert first.retry_count == 0
    assert second is not None
    assert second.status == "pending_transcript"
    assert blocked_until is not None
    assert block_kind == "ip_blocked"
    assert "Transcript fetching remains globally paused until" in caplog.text


def test_gemini_http_errors_warn_and_retry_or_fail(tmp_path, caplog) -> None:
    config = make_config(tmp_path, first_run_mode="process_all")
    database = Database(config.database_path)
    transcript = make_transcript(language="English", code="en")
    transient_workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=FakeRssClient([make_feed_video("video-retry")]),
        transcript_service=FakeTranscriptService({"video-retry": transcript}),
        summarizer=FakeSummarizer(
            GeminiHTTPError(429, "Gemini rate limit or quota was exceeded.", retryable=True)
        ),
        notifier=FakeNotifier(),
    )

    caplog.set_level("WARNING")
    transient_workflow.run()
    stored_retry = database.get_video_by_youtube_id("video-retry")
    assert stored_retry is not None
    assert stored_retry.status == "ready_for_summary"
    assert stored_retry.retry_count == 1
    assert "Gemini returned HTTP 429" in caplog.text

    permanent_workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=FakeRssClient([make_feed_video("video-fail")]),
        transcript_service=FakeTranscriptService({"video-fail": transcript}),
        summarizer=FakeSummarizer(
            GeminiHTTPError(400, "Gemini rejected the request as invalid.", retryable=False)
        ),
        notifier=FakeNotifier(),
    )
    permanent_workflow.run()
    stored_fail = database.get_video_by_youtube_id("video-fail")
    database.close()

    assert stored_fail is not None
    assert stored_fail.status == "error"


def test_gemini_auth_error_stops_remaining_summary_batch(tmp_path, caplog) -> None:
    config = AppConfig(
        youtube_channel_ids=("channel-1", "channel-2"),
        gemini_api_key="gemini-secret",
        gemini_model="gemini-2.0-flash",
        telegram_bot_token="telegram-secret",
        telegram_chat_id="@feed",
        database_path=tmp_path / "youtube_feed.db",
        summary_language_mode="transcript",
        min_notification_score=75,
        first_run_mode="process_all",
        http_timeout_seconds=30.0,
        telegram_message_limit=3800,
        gemini_min_request_interval_seconds=0.0,
        gemini_cooldown_after_rate_limit_seconds=0.0,
        gemini_cooldown_after_timeout_seconds=0.0,
        gemini_max_input_tokens_per_minute=225000,
        max_transcript_video_age_days=7,
        max_transcript_candidates_per_channel_per_poll=1,
        telegram_message_interval_seconds=0.0,
    )
    database = Database(config.database_path)
    transcript = make_transcript(language="English", code="en")
    workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=FakeRssClient(
            [
                make_feed_video("video-1", channel_id="channel-1"),
                make_feed_video("video-2", channel_id="channel-2"),
            ]
        ),
        transcript_service=FakeTranscriptService({"video-1": transcript, "video-2": transcript}),
        summarizer=FakeSummarizer(
            GeminiHTTPError(
                401,
                "Gemini credentials or project access are invalid.",
                retryable=False,
            )
        ),
        notifier=FakeNotifier(),
    )

    caplog.set_level("ERROR")
    workflow.run()
    first = database.get_video_by_youtube_id("video-1")
    second = database.get_video_by_youtube_id("video-2")
    database.close()

    assert first is not None
    assert first.status == "error"
    assert second is not None
    assert second.status == "ready_for_summary"
    assert "Stopping Gemini summary processing for this poll" in caplog.text


def test_retryable_summary_errors_remain_retryable_after_short_budget_is_exhausted(
    tmp_path,
) -> None:
    config = make_config(tmp_path, first_run_mode="process_all")
    database = Database(config.database_path)
    transcript = make_transcript(language="English", code="en")
    workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=FakeRssClient([make_feed_video("video-retry-forever")]),
        transcript_service=FakeTranscriptService({"video-retry-forever": transcript}),
        summarizer=FakeSummarizer(
            GeminiHTTPError(503, "Gemini returned a transient server error.", retryable=True)
        ),
        notifier=FakeNotifier(),
    )

    workflow.run()
    stored = database.get_video_by_youtube_id("video-retry-forever")
    assert stored is not None
    assert stored.status == "ready_for_summary"
    assert stored.retry_count == 1

    database.schedule_retry(
        stored.id,
        status="ready_for_summary",
        retry_count=len(config.summary_retry_delays_minutes),
        next_retry_at=datetime.now(tz=UTC) - timedelta(minutes=1),
        last_error="previous transient error",
        stage="summary",
    )

    workflow.run()
    stored = database.get_video_by_youtube_id("video-retry-forever")
    database.close()

    assert stored is not None
    assert stored.status == "ready_for_summary"
    assert stored.next_retry_at is not None
    assert stored.last_error_stage == "summary"


def test_gemini_rate_limit_stops_remaining_batch_and_sleeps(tmp_path, caplog) -> None:
    config = AppConfig(
        youtube_channel_ids=("channel-1", "channel-2"),
        gemini_api_key="gemini-secret",
        gemini_model="gemini-2.0-flash",
        telegram_bot_token="telegram-secret",
        telegram_chat_id="@feed",
        database_path=tmp_path / "youtube_feed.db",
        summary_language_mode="transcript",
        min_notification_score=75,
        first_run_mode="process_all",
        http_timeout_seconds=30.0,
        telegram_message_limit=3800,
        gemini_min_request_interval_seconds=0.0,
        gemini_cooldown_after_rate_limit_seconds=60.0,
        gemini_cooldown_after_timeout_seconds=20.0,
        gemini_max_input_tokens_per_minute=225000,
        max_transcript_video_age_days=7,
        max_transcript_candidates_per_channel_per_poll=1,
        telegram_message_interval_seconds=0.0,
    )
    database = Database(config.database_path)
    entries = [
        make_feed_video("video-1", channel_id="channel-1"),
        make_feed_video("video-2", channel_id="channel-2"),
    ]
    transcript = make_transcript(language="English", code="en")
    clock = FakeClock()
    summarizer = FakeSummarizer(
        GeminiHTTPError(429, "Gemini rate limit or quota was exceeded.", retryable=True)
    )
    workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=FakeRssClient(entries),
        transcript_service=FakeTranscriptService(
            {
                "video-1": transcript,
                "video-2": transcript,
            }
        ),
        summarizer=summarizer,
        notifier=FakeNotifier(),
        monotonic_func=clock.monotonic,
        sleep_func=clock.sleep,
    )

    caplog.set_level("WARNING")
    workflow.run()
    first = database.get_video_by_youtube_id("video-1")
    second = database.get_video_by_youtube_id("video-2")
    database.close()

    assert len(summarizer.calls) == 1
    assert first is not None
    assert first.status == "ready_for_summary"
    assert second is not None
    assert second.status == "ready_for_summary"
    assert clock.sleeps == [60.0]
    assert "Stopping Gemini summary processing for this poll after HTTP 429" in caplog.text


def test_gemini_rate_limit_prefers_retry_after_header_for_cooldown(tmp_path) -> None:
    config = AppConfig(
        youtube_channel_ids=("channel-1", "channel-2"),
        gemini_api_key="gemini-secret",
        gemini_model="gemini-2.0-flash",
        telegram_bot_token="telegram-secret",
        telegram_chat_id="@feed",
        database_path=tmp_path / "youtube_feed.db",
        summary_language_mode="transcript",
        min_notification_score=75,
        first_run_mode="process_all",
        http_timeout_seconds=30.0,
        telegram_message_limit=3800,
        gemini_min_request_interval_seconds=0.0,
        gemini_cooldown_after_rate_limit_seconds=60.0,
        gemini_cooldown_after_timeout_seconds=20.0,
        gemini_max_input_tokens_per_minute=225000,
        max_transcript_video_age_days=7,
        max_transcript_candidates_per_channel_per_poll=1,
        telegram_message_interval_seconds=0.0,
    )
    database = Database(config.database_path)
    entries = [
        make_feed_video("video-1", channel_id="channel-1"),
        make_feed_video("video-2", channel_id="channel-2"),
    ]
    transcript = make_transcript(language="English", code="en")
    clock = FakeClock()
    summarizer = FakeSummarizer(
        GeminiHTTPError(
            429,
            "Gemini rate limit or quota was exceeded.",
            retryable=True,
            retry_after_seconds=7.0,
        )
    )
    workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=FakeRssClient(entries),
        transcript_service=FakeTranscriptService(
            {
                "video-1": transcript,
                "video-2": transcript,
            }
        ),
        summarizer=summarizer,
        notifier=FakeNotifier(),
        monotonic_func=clock.monotonic,
        sleep_func=clock.sleep,
    )

    workflow.run()
    database.close()

    assert clock.sleeps == [7.0]


def test_gemini_transport_error_retries_inline_before_persistent_retry(tmp_path) -> None:
    config = AppConfig(
        youtube_channel_ids=("channel-1",),
        gemini_api_key="gemini-secret",
        gemini_model="gemini-2.0-flash",
        telegram_bot_token="telegram-secret",
        telegram_chat_id="@feed",
        database_path=tmp_path / "youtube_feed.db",
        summary_language_mode="transcript",
        min_notification_score=75,
        first_run_mode="process_all",
        http_timeout_seconds=30.0,
        telegram_message_limit=3800,
        gemini_min_request_interval_seconds=0.0,
        gemini_cooldown_after_rate_limit_seconds=0.0,
        gemini_cooldown_after_timeout_seconds=20.0,
        gemini_cooldown_after_service_unavailable_seconds=0.0,
        gemini_transport_inline_retry_delays_seconds=(5, 15),
        gemini_max_input_tokens_per_minute=225000,
        max_transcript_video_age_days=7,
        max_transcript_candidates_per_channel_per_poll=1,
        telegram_message_interval_seconds=0.0,
    )
    database = Database(config.database_path)
    clock = FakeClock()
    summarizer = SequenceSummarizer(
        [
            GeminiTransportError("read timed out"),
            SummaryResult(
                score=90,
                priority="high",
                confidence=90,
                why_it_matters="Useful",
                summary_bullets=("a", "b", "c"),
            ),
        ]
    )
    notifier = FakeNotifier()
    workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=FakeRssClient([make_feed_video("video-1")]),
        transcript_service=FakeTranscriptService({"video-1": make_transcript()}),
        summarizer=summarizer,
        notifier=notifier,
        monotonic_func=clock.monotonic,
        sleep_func=clock.sleep,
    )

    stats = workflow.run()
    stored = database.get_video_by_youtube_id("video-1")
    database.close()

    assert len(summarizer.calls) == 2
    assert clock.sleeps == [5]
    assert stats.notification_sent == 1
    assert stored is not None
    assert stored.status == "notification_sent"
    assert notifier.messages


def test_gemini_503_applies_short_cooldown_without_stopping_batch(tmp_path) -> None:
    config = AppConfig(
        youtube_channel_ids=("channel-1", "channel-2"),
        gemini_api_key="gemini-secret",
        gemini_model="gemini-2.0-flash",
        telegram_bot_token="telegram-secret",
        telegram_chat_id="@feed",
        database_path=tmp_path / "youtube_feed.db",
        summary_language_mode="transcript",
        min_notification_score=75,
        first_run_mode="process_all",
        http_timeout_seconds=30.0,
        telegram_message_limit=3800,
        gemini_min_request_interval_seconds=0.0,
        gemini_cooldown_after_rate_limit_seconds=0.0,
        gemini_cooldown_after_timeout_seconds=0.0,
        gemini_cooldown_after_service_unavailable_seconds=11.0,
        gemini_max_input_tokens_per_minute=225000,
        max_transcript_video_age_days=7,
        max_transcript_candidates_per_channel_per_poll=1,
        telegram_message_interval_seconds=0.0,
    )
    database = Database(config.database_path)
    transcript = make_transcript(language="English", code="en")
    clock = FakeClock()
    workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=FakeRssClient([make_feed_video("video-1")]),
        transcript_service=FakeTranscriptService({"video-1": transcript}),
        summarizer=FakeSummarizer(
            GeminiHTTPError(503, "Gemini returned a transient server error.", retryable=True)
        ),
        notifier=FakeNotifier(),
        monotonic_func=clock.monotonic,
        sleep_func=clock.sleep,
    )

    workflow.run()
    stored = database.get_video_by_youtube_id("video-1")
    database.close()

    assert stored is not None
    assert stored.status == "ready_for_summary"
    assert clock.sleeps == [11.0]


def test_gemini_invalid_response_is_retried_before_becoming_terminal(tmp_path) -> None:
    config = make_config(tmp_path, first_run_mode="process_all")
    database = Database(config.database_path)
    transcript = make_transcript(language="English", code="en")
    workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=FakeRssClient([make_feed_video("video-invalid")]),
        transcript_service=FakeTranscriptService({"video-invalid": transcript}),
        summarizer=FakeSummarizer(GeminiResponseError("bad json")),
        notifier=FakeNotifier(),
    )

    workflow.run()
    stored = database.get_video_by_youtube_id("video-invalid")
    assert stored is not None
    assert stored.status == "ready_for_summary"
    assert stored.retry_count == 1

    database.schedule_retry(
        stored.id,
        status="ready_for_summary",
        retry_count=len(config.summary_retry_delays_minutes),
        next_retry_at=datetime.now(tz=UTC) - timedelta(minutes=1),
        last_error="previous invalid response",
        stage="summary",
    )

    workflow.run()
    stored = database.get_video_by_youtube_id("video-invalid")
    database.close()

    assert stored is not None
    assert stored.status == "error"


def test_gemini_token_budget_sleeps_before_next_request(tmp_path) -> None:
    transcript = TranscriptData(
        video_id="video-1",
        language="English",
        language_code="en",
        is_generated=False,
        snippets=(
            TranscriptSnippet(text="x" * 3000, start=0.0, duration=2.0),
            TranscriptSnippet(text="y" * 3000, start=2.0, duration=2.0),
        ),
    )
    config = AppConfig(
        youtube_channel_ids=("channel-1", "channel-2"),
        gemini_api_key="gemini-secret",
        gemini_model="gemini-2.0-flash",
        telegram_bot_token="telegram-secret",
        telegram_chat_id="@feed",
        database_path=tmp_path / "youtube_feed.db",
        summary_language_mode="transcript",
        min_notification_score=75,
        first_run_mode="process_all",
        http_timeout_seconds=30.0,
        telegram_message_limit=3800,
        gemini_min_request_interval_seconds=0.0,
        gemini_cooldown_after_rate_limit_seconds=0.0,
        gemini_cooldown_after_timeout_seconds=0.0,
        gemini_max_input_tokens_per_minute=2000,
        max_transcript_video_age_days=7,
        max_transcript_candidates_per_channel_per_poll=1,
        min_video_duration_seconds=0,
        telegram_message_interval_seconds=0.0,
    )
    database = Database(config.database_path)
    entries = [
        make_feed_video("video-1", channel_id="channel-1"),
        make_feed_video("video-2", channel_id="channel-2"),
    ]
    clock = FakeClock()
    workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=FakeRssClient(entries),
        transcript_service=FakeTranscriptService(
            {
                "video-1": transcript,
                "video-2": transcript,
            }
        ),
        summarizer=FakeSummarizer(
            SummaryResult(
                score=80,
                priority="high",
                confidence=90,
                why_it_matters="Useful",
                summary_bullets=("a", "b", "c"),
            )
        ),
        notifier=FakeNotifier(),
        monotonic_func=clock.monotonic,
        sleep_func=clock.sleep,
    )

    workflow.run()
    database.close()

    assert clock.sleeps == [60.0]


def test_only_latest_recent_video_per_channel_requests_transcript(tmp_path) -> None:
    config = make_config(tmp_path, first_run_mode="process_all")
    database = Database(config.database_path)
    newer = FeedVideo(
        youtube_video_id="video-new",
        channel_id="channel-1",
        channel_title="Channel One",
        title="New video",
        published_at=datetime.now(tz=UTC) - timedelta(days=1),
        url="https://www.youtube.com/watch?v=video-new",
    )
    older = FeedVideo(
        youtube_video_id="video-old",
        channel_id="channel-1",
        channel_title="Channel One",
        title="Old video",
        published_at=datetime.now(tz=UTC) - timedelta(days=2),
        url="https://www.youtube.com/watch?v=video-old",
    )
    transcript_service = FakeTranscriptService({"video-new": make_transcript()})
    workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=FakeRssClient([older, newer]),
        transcript_service=transcript_service,
        summarizer=FakeSummarizer(
            SummaryResult(
                score=90,
                priority="high",
                confidence=90,
                why_it_matters="Useful",
                summary_bullets=("a", "b", "c"),
            )
        ),
        notifier=FakeNotifier(),
    )

    workflow.run()
    stored_new = database.get_video_by_youtube_id("video-new")
    stored_old = database.get_video_by_youtube_id("video-old")
    database.close()

    assert transcript_service.calls == ["video-new"]
    assert stored_new is not None
    assert stored_new.status in {"ready_for_summary", "notification_sent", "summarized"}
    assert stored_old is not None
    assert stored_old.status == "transcript_skipped"


def test_short_video_is_skipped_without_skipping_older_channel_candidates(tmp_path) -> None:
    config = make_config(
        tmp_path,
        first_run_mode="process_all",
        min_video_duration_seconds=180,
    )
    database = Database(config.database_path)
    short_video = FeedVideo(
        youtube_video_id="video-short",
        channel_id="channel-1",
        channel_title="Channel One",
        title="Short clip",
        published_at=datetime.now(tz=UTC) - timedelta(hours=1),
        url="https://www.youtube.com/watch?v=video-short",
    )
    older_video = FeedVideo(
        youtube_video_id="video-long",
        channel_id="channel-1",
        channel_title="Channel One",
        title="Long interview",
        published_at=datetime.now(tz=UTC) - timedelta(hours=2),
        url="https://www.youtube.com/watch?v=video-long",
    )
    short_transcript = TranscriptData(
        video_id="video-short",
        language="English",
        language_code="en",
        is_generated=False,
        snippets=(TranscriptSnippet(text="short text", start=0.0, duration=77.0),),
    )
    transcript_service = FakeTranscriptService({"video-short": short_transcript})
    workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=FakeRssClient([older_video, short_video]),
        transcript_service=transcript_service,
        summarizer=FakeSummarizer(
            SummaryResult(
                score=90,
                priority="high",
                confidence=90,
                why_it_matters="Useful",
                summary_bullets=("a", "b", "c"),
            )
        ),
        notifier=FakeNotifier(),
    )

    workflow.run()
    stored_short = database.get_video_by_youtube_id("video-short")
    stored_long = database.get_video_by_youtube_id("video-long")
    database.close()

    assert transcript_service.calls == ["video-short"]
    assert stored_short is not None
    assert stored_short.status == "transcript_skipped"
    assert stored_short.duration_seconds == 77
    assert stored_long is not None
    assert stored_long.status == "pending_transcript"


def test_rss_short_url_is_skipped_and_next_channel_candidate_is_processed(tmp_path) -> None:
    config = make_config(tmp_path, first_run_mode="process_all")
    database = Database(config.database_path)
    short_video = FeedVideo(
        youtube_video_id="video-short",
        channel_id="channel-1",
        channel_title="Channel One",
        title="Short clip",
        published_at=datetime.now(tz=UTC) - timedelta(hours=1),
        url="https://www.youtube.com/shorts/video-short",
    )
    older_video = FeedVideo(
        youtube_video_id="video-long",
        channel_id="channel-1",
        channel_title="Channel One",
        title="Long interview",
        published_at=datetime.now(tz=UTC) - timedelta(hours=2),
        url="https://www.youtube.com/watch?v=video-long",
    )
    transcript_service = FakeTranscriptService({"video-long": make_transcript()})
    workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=FakeRssClient([older_video, short_video]),
        transcript_service=transcript_service,
        summarizer=FakeSummarizer(
            SummaryResult(
                score=90,
                priority="high",
                confidence=90,
                why_it_matters="Useful",
                summary_bullets=("a", "b", "c"),
            )
        ),
        notifier=FakeNotifier(),
    )

    workflow.run()
    stored_short = database.get_video_by_youtube_id("video-short")
    stored_long = database.get_video_by_youtube_id("video-long")
    database.close()

    assert transcript_service.calls == ["video-long"]
    assert stored_short is not None
    assert stored_short.status == "transcript_skipped"
    assert stored_long is not None
    assert stored_long.status in {"ready_for_summary", "summarized", "notification_sent"}


def test_videos_older_than_threshold_are_marked_transcript_skipped(tmp_path) -> None:
    config = make_config(tmp_path, first_run_mode="process_all")
    config = AppConfig(
        youtube_channel_ids=config.youtube_channel_ids,
        gemini_api_key=config.gemini_api_key,
        gemini_model=config.gemini_model,
        telegram_bot_token=config.telegram_bot_token,
        telegram_chat_id=config.telegram_chat_id,
        database_path=config.database_path,
        summary_language_mode=config.summary_language_mode,
        min_notification_score=config.min_notification_score,
        first_run_mode=config.first_run_mode,
        http_timeout_seconds=config.http_timeout_seconds,
        telegram_message_limit=config.telegram_message_limit,
        gemini_min_request_interval_seconds=config.gemini_min_request_interval_seconds,
        gemini_cooldown_after_rate_limit_seconds=config.gemini_cooldown_after_rate_limit_seconds,
        gemini_cooldown_after_timeout_seconds=config.gemini_cooldown_after_timeout_seconds,
        gemini_max_input_tokens_per_minute=config.gemini_max_input_tokens_per_minute,
        max_transcript_video_age_days=0,
        max_transcript_candidates_per_channel_per_poll=1,
        telegram_message_interval_seconds=config.telegram_message_interval_seconds,
    )
    database = Database(config.database_path)
    old_video = FeedVideo(
        youtube_video_id="video-old",
        channel_id="channel-1",
        channel_title="Channel One",
        title="Old video",
        published_at=datetime.now(tz=UTC) - timedelta(days=2),
        url="https://www.youtube.com/watch?v=video-old",
    )
    transcript_service = FakeTranscriptService({})
    workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=FakeRssClient([old_video]),
        transcript_service=transcript_service,
        summarizer=FakeSummarizer(
            SummaryResult(
                score=90,
                priority="high",
                confidence=90,
                why_it_matters="Useful",
                summary_bullets=("a", "b", "c"),
            )
        ),
        notifier=FakeNotifier(),
    )

    workflow.run()
    stored_old = database.get_video_by_youtube_id("video-old")
    database.close()

    assert transcript_service.calls == []
    assert stored_old is not None
    assert stored_old.status == "transcript_skipped"


def test_telegram_messages_sleep_between_deliveries(tmp_path) -> None:
    config = AppConfig(
        youtube_channel_ids=("channel-1", "channel-2"),
        gemini_api_key="gemini-secret",
        gemini_model="gemini-2.0-flash",
        telegram_bot_token="telegram-secret",
        telegram_chat_id="@feed",
        database_path=tmp_path / "youtube_feed.db",
        summary_language_mode="transcript",
        min_notification_score=75,
        first_run_mode="process_all",
        http_timeout_seconds=30.0,
        telegram_message_limit=3800,
        gemini_min_request_interval_seconds=0.0,
        gemini_cooldown_after_rate_limit_seconds=0.0,
        gemini_cooldown_after_timeout_seconds=0.0,
        gemini_max_input_tokens_per_minute=225000,
        max_transcript_video_age_days=7,
        max_transcript_candidates_per_channel_per_poll=1,
        telegram_message_interval_seconds=0.5,
    )
    database = Database(config.database_path)
    clock = FakeClock()
    workflow = PollWorkflow(
        config=config,
        database=database,
        rss_client=FakeRssClient(
            [
                make_feed_video("video-1", channel_id="channel-1"),
                make_feed_video("video-2", channel_id="channel-2"),
            ]
        ),
        transcript_service=FakeTranscriptService(
            {
                "video-1": make_transcript(language="English", code="en"),
                "video-2": make_transcript(language="English", code="en"),
            }
        ),
        summarizer=FakeSummarizer(
            SummaryResult(
                score=90,
                priority="high",
                confidence=90,
                why_it_matters="Useful",
                summary_bullets=("a", "b", "c"),
            )
        ),
        notifier=FakeNotifier(),
        monotonic_func=clock.monotonic,
        sleep_func=clock.sleep,
    )

    workflow.run()
    database.close()

    assert clock.sleeps == [0.5]


def test_gemini_summarizer_maps_known_http_status_codes() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, json={"error": {"message": "quota"}})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    summarizer = GeminiSummarizer("secret", "gemini-2.0-flash", client)

    with pytest.raises(GeminiHTTPError) as exc_info:
        summarizer.summarize(
            video_title="Video",
            channel_title="Channel",
            transcript=make_transcript(language="English", code="en"),
        )

    client.close()
    assert exc_info.value.status_code == 429
    assert exc_info.value.retryable is True


class ParseErrorApi:
    def list(self, video_id: str):
        raise ParseError("no element found: line 1, column 0")


class TimeoutApi:
    def list(self, video_id: str):
        raise Timeout("read timed out")


def test_transcript_service_maps_parse_error_to_temporary() -> None:
    service = TranscriptService(api=ParseErrorApi())

    with pytest.raises(TranscriptTemporaryError):
        service.fetch("video-1")


def test_transcript_service_maps_requests_timeout_to_temporary() -> None:
    service = TranscriptService(api=TimeoutApi())

    with pytest.raises(TranscriptTemporaryError):
        service.fetch("video-1")
