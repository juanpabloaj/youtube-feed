from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from youtube_feed.exceptions import TelegramHTTPError
from youtube_feed.models import StoredAnalysis, StoredVideo
from youtube_feed.telegram import TelegramNotifier, render_message


def test_render_message_respects_limit_and_keeps_watch_link() -> None:
    published_at = datetime(2026, 4, 17, 12, 0, tzinfo=UTC)
    video = StoredVideo(
        id=1,
        youtube_video_id="abc123",
        channel_id="channel",
        channel_title="Channel",
        title="Very Long Video Title",
        published_at=published_at,
        url="https://www.youtube.com/watch?v=abc123",
        status="summarized",
        duration_seconds=3723,
        transcript_text="transcript",
        transcript_language="English",
        transcript_language_code="en",
        retry_count=0,
        next_retry_at=None,
        last_error=None,
        last_error_stage=None,
    )
    analysis = StoredAnalysis(
        video_id=1,
        model="gemini-2.0-flash",
        prompt_version="v1",
        transcript_text="transcript",
        transcript_hash="hash",
        transcript_language="English",
        transcript_language_code="en",
        score=88,
        priority="high",
        confidence=90,
        why_it_matters=" ".join(["important"] * 200),
        summary_bullets=tuple(" ".join(["bullet"] * 120) for _ in range(5)),
    )

    message = render_message(video, analysis, limit=500)

    assert len(message) <= 500
    assert "https://www.youtube.com/watch?v=abc123" in message
    assert "Title:" not in message
    assert "Watch:" not in message
    assert "Very Long Video Title" in message
    assert "2026-04-17 • 1:02:03 • High 88/100" in message


def test_telegram_notifier_survives_non_json_error_body() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(502, text="<html>upstream failure</html>")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    notifier = TelegramNotifier("secret", "@feed", client)

    with pytest.raises(TelegramHTTPError) as exc_info:
        notifier.send_message("hello")

    client.close()
    assert exc_info.value.status_code == 502
    assert "upstream failure" in str(exc_info.value)
