from __future__ import annotations

import httpx
import pytest

from youtube_feed.exceptions import GeminiHTTPError
from youtube_feed.models import TranscriptData, TranscriptSnippet
from youtube_feed.summarizer import GeminiSummarizer


def make_transcript() -> TranscriptData:
    return TranscriptData(
        video_id="video-1",
        language="English",
        language_code="en",
        is_generated=False,
        snippets=(TranscriptSnippet(text="Useful transcript text.", start=0.0, duration=2.0),),
    )


def test_gemini_summarizer_extracts_retry_after_seconds_from_429() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            headers={"Retry-After": "12"},
            json={"error": {"message": "quota exhausted"}},
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    summarizer = GeminiSummarizer("secret", "gemini-2.0-flash", client)

    with pytest.raises(GeminiHTTPError) as exc_info:
        summarizer.summarize(
            video_title="Video",
            channel_title="Channel",
            transcript=make_transcript(),
        )

    client.close()
    assert exc_info.value.status_code == 429
    assert exc_info.value.retryable is True
    assert exc_info.value.retry_after_seconds == 12.0
