from __future__ import annotations

from datetime import UTC, datetime

from youtube_feed.db import LATEST_SCHEMA_VERSION, Database
from youtube_feed.models import FeedVideo, SummaryResult, TranscriptData, TranscriptSnippet


def test_init_schema_sets_user_version(tmp_path) -> None:
    database = Database(tmp_path / "youtube_feed.db")
    database.init_schema()
    row = database._connection.execute("PRAGMA user_version").fetchone()
    database.close()

    assert row is not None
    assert int(row[0]) == LATEST_SCHEMA_VERSION


def test_upsert_feed_videos_inserts_and_updates_metadata(tmp_path) -> None:
    database = Database(tmp_path / "youtube_feed.db")
    database.init_schema()

    inserted = database.upsert_feed_videos(
        [
            FeedVideo(
                youtube_video_id="video-1",
                channel_id="channel-1",
                channel_title="Channel A",
                title="First title",
                published_at=datetime(2026, 4, 18, 12, 0, tzinfo=UTC),
                url="https://example.com/video-1",
            ),
            FeedVideo(
                youtube_video_id="video-2",
                channel_id="channel-2",
                channel_title="Channel B",
                title="Second title",
                published_at=datetime(2026, 4, 18, 12, 5, tzinfo=UTC),
                url="https://example.com/video-2",
            ),
        ],
        initial_status="pending_transcript",
    )
    updated = database.upsert_feed_videos(
        [
            FeedVideo(
                youtube_video_id="video-1",
                channel_id="channel-1",
                channel_title="Channel A updated",
                title="First title updated",
                published_at=datetime(2026, 4, 18, 12, 10, tzinfo=UTC),
                url="https://example.com/video-1-new",
            )
        ],
        initial_status="seen",
    )
    stored = database.get_video_by_youtube_id("video-1")
    database.close()

    assert inserted == 2
    assert updated == 0
    assert stored is not None
    assert stored.channel_title == "Channel A updated"
    assert stored.title == "First title updated"
    assert stored.url == "https://example.com/video-1-new"
    assert stored.status == "pending_transcript"


def test_list_pending_notifications_filters_by_score_and_delivery_status(tmp_path) -> None:
    database = Database(tmp_path / "youtube_feed.db")
    database.init_schema()
    database.upsert_feed_videos(
        [
            FeedVideo(
                youtube_video_id="video-1",
                channel_id="channel-1",
                channel_title="Channel A",
                title="Title 1",
                published_at=datetime(2026, 4, 18, 12, 0, tzinfo=UTC),
                url="https://example.com/video-1",
            ),
            FeedVideo(
                youtube_video_id="video-2",
                channel_id="channel-2",
                channel_title="Channel B",
                title="Title 2",
                published_at=datetime(2026, 4, 18, 12, 5, tzinfo=UTC),
                url="https://example.com/video-2",
            ),
        ],
        initial_status="pending_transcript",
    )
    transcript = TranscriptData(
        video_id="video-1",
        language="English",
        language_code="en",
        is_generated=False,
        snippets=(TranscriptSnippet(text="Useful text", start=0.0, duration=5.0),),
    )
    for youtube_id in ("video-1", "video-2"):
        video = database.get_video_by_youtube_id(youtube_id)
        assert video is not None
        database.save_transcript(video.id, transcript)

    video_1 = database.get_video_by_youtube_id("video-1")
    video_2 = database.get_video_by_youtube_id("video-2")
    assert video_1 is not None
    assert video_2 is not None

    database.store_analysis(
        video_id=video_1.id,
        model="gemini",
        prompt_version="v1",
        transcript=transcript,
        summary=SummaryResult(
            score=90,
            priority="high",
            confidence=88,
            why_it_matters="Useful",
            summary_bullets=("a", "b", "c"),
        ),
        transcript_hash="hash-1",
    )
    database.store_analysis(
        video_id=video_2.id,
        model="gemini",
        prompt_version="v1",
        transcript=transcript,
        summary=SummaryResult(
            score=65,
            priority="high",
            confidence=88,
            why_it_matters="Useful",
            summary_bullets=("a", "b", "c"),
        ),
        transcript_hash="hash-2",
    )
    database.record_notification(
        video_id=video_1.id,
        target_chat_id="@feed",
        message_text="sent",
        delivery_status="delivered",
        telegram_message_id=1,
        error=None,
    )

    pending = database.list_pending_notifications(min_score=75)
    database.close()

    assert pending == []
