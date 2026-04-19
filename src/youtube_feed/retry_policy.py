from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Literal

from youtube_feed.db import Database

RetryStage = Literal["transcript", "summary"]


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


class RetryPolicy:
    def __init__(
        self,
        *,
        database: Database,
        transcript_retry_delays_minutes: tuple[int, ...],
        summary_retry_delays_minutes: tuple[int, ...],
        now_func=utc_now,
    ) -> None:
        self._database = database
        self._transcript_retry_delays_minutes = transcript_retry_delays_minutes
        self._summary_retry_delays_minutes = summary_retry_delays_minutes
        self._now = now_func

    def schedule_or_finalize(
        self,
        *,
        video_id: int,
        retry_count: int,
        error: str,
        stage: RetryStage,
    ) -> tuple[datetime | None, bool]:
        next_retry = self._next_retry_time(retry_count=retry_count, stage=stage)
        if next_retry is None:
            if stage == "transcript":
                self._database.mark_transcript_unavailable(video_id, error)
                return None, True
            self._database.mark_summary_error(video_id, error)
            return None, False

        self._database.schedule_retry(
            video_id,
            status="pending_transcript" if stage == "transcript" else "ready_for_summary",
            retry_count=retry_count + 1,
            next_retry_at=next_retry,
            last_error=error,
            stage=stage,
        )
        return next_retry, False

    def schedule_retry(
        self,
        *,
        video_id: int,
        retry_count: int,
        error: str,
        stage: RetryStage,
    ) -> tuple[datetime | None, bool]:
        next_retry = self._next_retry_time(retry_count=retry_count, stage=stage)
        if next_retry is None:
            if stage == "transcript":
                self._database.mark_transcript_unavailable(video_id, error)
                return None, True
            next_retry = self._now() + timedelta(minutes=self._summary_retry_delays_minutes[-1])

        self._database.schedule_retry(
            video_id,
            status="pending_transcript" if stage == "transcript" else "ready_for_summary",
            retry_count=retry_count + 1,
            next_retry_at=next_retry,
            last_error=error,
            stage=stage,
        )
        return next_retry, False

    def _next_retry_time(self, *, retry_count: int, stage: RetryStage) -> datetime | None:
        delays_minutes = (
            self._transcript_retry_delays_minutes
            if stage == "transcript"
            else self._summary_retry_delays_minutes
        )
        if retry_count >= len(delays_minutes):
            return None
        return self._now() + timedelta(minutes=delays_minutes[retry_count])
