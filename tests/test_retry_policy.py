from __future__ import annotations

from datetime import UTC, datetime

from youtube_feed.retry_policy import RetryPolicy


class StubDatabase:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def mark_transcript_unavailable(self, video_id: int, error: str) -> None:
        self.calls.append(("mark_transcript_unavailable", video_id, error))

    def mark_summary_error(self, video_id: int, error: str) -> None:
        self.calls.append(("mark_summary_error", video_id, error))

    def schedule_retry(
        self,
        video_id: int,
        *,
        status: str,
        retry_count: int,
        next_retry_at: datetime,
        last_error: str,
        stage: str,
    ) -> None:
        self.calls.append(
            (
                "schedule_retry",
                video_id,
                status,
                retry_count,
                next_retry_at,
                last_error,
                stage,
            )
        )


def test_schedule_or_finalize_marks_summary_error_when_budget_is_exhausted() -> None:
    database = StubDatabase()
    now = datetime(2026, 4, 18, 12, 0, tzinfo=UTC)
    policy = RetryPolicy(
        database=database,
        transcript_retry_delays_minutes=(15, 60, 180),
        summary_retry_delays_minutes=(5, 15, 45),
        now_func=lambda: now,
    )

    next_retry_at, finalized = policy.schedule_or_finalize(
        video_id=10,
        retry_count=3,
        error="bad response",
        stage="summary",
    )

    assert next_retry_at is None
    assert finalized is False
    assert database.calls == [("mark_summary_error", 10, "bad response")]


def test_schedule_retry_keeps_summary_retryable_after_short_budget_is_exhausted() -> None:
    database = StubDatabase()
    now = datetime(2026, 4, 18, 12, 0, tzinfo=UTC)
    policy = RetryPolicy(
        database=database,
        transcript_retry_delays_minutes=(15, 60, 180),
        summary_retry_delays_minutes=(5, 15, 45),
        now_func=lambda: now,
    )

    next_retry_at, finalized = policy.schedule_retry(
        video_id=11,
        retry_count=3,
        error="timeout",
        stage="summary",
    )

    assert finalized is False
    assert next_retry_at == datetime(2026, 4, 18, 12, 45, tzinfo=UTC)
    assert database.calls == [
        (
            "schedule_retry",
            11,
            "ready_for_summary",
            4,
            datetime(2026, 4, 18, 12, 45, tzinfo=UTC),
            "timeout",
            "summary",
        )
    ]
