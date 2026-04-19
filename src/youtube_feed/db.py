from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from youtube_feed.models import StoredAnalysis, StoredVideo, SummaryResult, TranscriptData

LATEST_SCHEMA_VERSION = 2


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


class Database:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self._path)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.execute("PRAGMA synchronous = NORMAL")
        self._connection.execute("PRAGMA busy_timeout = 5000")

    def close(self) -> None:
        self._connection.close()

    def init_schema(self) -> None:
        current_version = self._get_user_version()
        if current_version < 1:
            self._migrate_to_v1()
            current_version = 1
        if current_version < 2:
            self._migrate_to_v2()
            current_version = 2
        if current_version != LATEST_SCHEMA_VERSION:
            self._set_user_version(LATEST_SCHEMA_VERSION)

    def _migrate_to_v1(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                youtube_video_id TEXT NOT NULL UNIQUE,
                channel_id TEXT NOT NULL,
                channel_title TEXT NOT NULL,
                title TEXT NOT NULL,
                published_at TEXT NOT NULL,
                url TEXT NOT NULL,
                status TEXT NOT NULL,
                duration_seconds INTEGER,
                transcript_text TEXT,
                transcript_language TEXT,
                transcript_language_code TEXT,
                retry_count INTEGER NOT NULL DEFAULT 0,
                next_retry_at TEXT,
                last_error TEXT,
                last_error_stage TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_videos_status_due
            ON videos (status, next_retry_at);

            CREATE TABLE IF NOT EXISTS analyses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id INTEGER NOT NULL UNIQUE REFERENCES videos(id) ON DELETE CASCADE,
                model TEXT NOT NULL,
                prompt_version TEXT NOT NULL,
                transcript_text TEXT NOT NULL,
                transcript_hash TEXT NOT NULL,
                transcript_language TEXT NOT NULL,
                transcript_language_code TEXT NOT NULL,
                score INTEGER NOT NULL,
                priority TEXT NOT NULL,
                confidence INTEGER NOT NULL,
                why_it_matters TEXT NOT NULL,
                summary_bullets_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id INTEGER NOT NULL UNIQUE REFERENCES videos(id) ON DELETE CASCADE,
                target_chat_id TEXT NOT NULL,
                message_text TEXT NOT NULL,
                telegram_message_id INTEGER,
                delivery_status TEXT NOT NULL,
                error TEXT,
                delivered_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        self._set_user_version(1)

    def _migrate_to_v2(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS service_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        self._set_user_version(2)

    def _get_user_version(self) -> int:
        row = self._connection.execute("PRAGMA user_version").fetchone()
        if row is None:
            return 0
        return int(row[0])

    def _set_user_version(self, version: int) -> None:
        self._connection.execute(f"PRAGMA user_version = {version}")
        self._connection.commit()

    def count_videos(self) -> int:
        row = self._connection.execute("SELECT COUNT(*) AS count FROM videos").fetchone()
        return int(row["count"])

    def upsert_feed_video(self, video, *, initial_status: str) -> bool:
        return self.upsert_feed_videos([video], initial_status=initial_status) > 0

    def upsert_feed_videos(self, videos: list, *, initial_status: str) -> int:
        if not videos:
            return 0
        youtube_video_ids = [video.youtube_video_id for video in videos]
        placeholders = ",".join("?" for _ in youtube_video_ids)
        existing_rows = self._connection.execute(
            f"SELECT youtube_video_id FROM videos WHERE youtube_video_id IN ({placeholders})",
            youtube_video_ids,
        ).fetchall()
        existing_ids = {str(row["youtube_video_id"]) for row in existing_rows}
        inserted_count = 0
        now = _isoformat(utc_now())
        with self._connection:
            for video in videos:
                self._connection.execute(
                    """
                    INSERT INTO videos (
                        youtube_video_id,
                        channel_id,
                        channel_title,
                        title,
                        published_at,
                        url,
                        status,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(youtube_video_id) DO UPDATE SET
                        channel_title = excluded.channel_title,
                        title = excluded.title,
                        published_at = excluded.published_at,
                        url = excluded.url,
                        updated_at = excluded.updated_at
                    """,
                    (
                        video.youtube_video_id,
                        video.channel_id,
                        video.channel_title,
                        video.title,
                        _isoformat(video.published_at),
                        video.url,
                        initial_status,
                        now,
                        now,
                    ),
                )
                if video.youtube_video_id not in existing_ids:
                    inserted_count += 1
        return inserted_count

    def list_due_videos(self, statuses: tuple[str, ...], *, now: datetime) -> list[StoredVideo]:
        placeholders = ",".join("?" for _ in statuses)
        rows = self._connection.execute(
            f"""
            SELECT *
            FROM videos
            WHERE status IN ({placeholders})
              AND (next_retry_at IS NULL OR next_retry_at <= ?)
            ORDER BY published_at ASC
            """,
            (*statuses, _isoformat(now)),
        ).fetchall()
        return [_row_to_video(row) for row in rows]

    def count_deferred_videos(self, statuses: tuple[str, ...], *, now: datetime) -> int:
        placeholders = ",".join("?" for _ in statuses)
        row = self._connection.execute(
            f"""
            SELECT COUNT(*) AS count
            FROM videos
            WHERE status IN ({placeholders})
              AND next_retry_at IS NOT NULL
              AND next_retry_at > ?
            """,
            (*statuses, _isoformat(now)),
        ).fetchone()
        return int(row["count"])

    def get_video_by_youtube_id(self, youtube_video_id: str) -> StoredVideo | None:
        row = self._connection.execute(
            "SELECT * FROM videos WHERE youtube_video_id = ?",
            (youtube_video_id,),
        ).fetchone()
        return _row_to_video(row) if row else None

    def get_video(self, video_id: int) -> StoredVideo | None:
        row = self._connection.execute(
            "SELECT * FROM videos WHERE id = ?",
            (video_id,),
        ).fetchone()
        return _row_to_video(row) if row else None

    def get_transcript_blocked_until(self) -> datetime | None:
        row = self._connection.execute(
            "SELECT value FROM service_state WHERE key = 'transcript_blocked_until'",
        ).fetchone()
        if row is None:
            return None
        return _parse_datetime(str(row["value"]))

    def get_transcript_block_reason(self) -> str | None:
        row = self._connection.execute(
            "SELECT value FROM service_state WHERE key = 'transcript_block_reason'",
        ).fetchone()
        if row is None:
            return None
        return str(row["value"])

    def get_transcript_block_kind(self) -> str | None:
        row = self._connection.execute(
            "SELECT value FROM service_state WHERE key = 'transcript_block_kind'",
        ).fetchone()
        if row is None:
            return None
        return str(row["value"])

    def set_transcript_block(self, *, blocked_until: datetime, reason: str, kind: str) -> None:
        now = _isoformat(utc_now())
        blocked_value = _isoformat(blocked_until)
        self._connection.execute(
            """
            INSERT INTO service_state (key, value, updated_at)
            VALUES ('transcript_blocked_until', ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (blocked_value, now),
        )
        self._connection.execute(
            """
            INSERT INTO service_state (key, value, updated_at)
            VALUES ('transcript_block_reason', ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (reason, now),
        )
        self._connection.execute(
            """
            INSERT INTO service_state (key, value, updated_at)
            VALUES ('transcript_block_kind', ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (kind, now),
        )
        self._connection.commit()

    def clear_transcript_block(self) -> None:
        self._connection.execute(
            "DELETE FROM service_state "
            "WHERE key IN ("
            "'transcript_blocked_until', "
            "'transcript_block_reason', "
            "'transcript_block_kind'"
            ")"
        )
        self._connection.commit()

    def defer_transcript_until(
        self,
        video_id: int,
        *,
        retry_count: int,
        next_retry_at: datetime,
        last_error: str,
    ) -> None:
        self._connection.execute(
            """
            UPDATE videos
            SET status = 'pending_transcript',
                retry_count = ?,
                next_retry_at = ?,
                last_error = ?,
                last_error_stage = 'transcript',
                updated_at = ?
            WHERE id = ?
            """,
            (
                retry_count,
                _isoformat(next_retry_at),
                last_error,
                _isoformat(utc_now()),
                video_id,
            ),
        )
        self._connection.commit()

    def get_analysis_for_video(self, video_id: int) -> StoredAnalysis | None:
        row = self._connection.execute(
            "SELECT * FROM analyses WHERE video_id = ?",
            (video_id,),
        ).fetchone()
        return _row_to_analysis(row) if row else None

    def save_transcript(self, video_id: int, transcript: TranscriptData) -> None:
        now = _isoformat(utc_now())
        self._connection.execute(
            """
            UPDATE videos
            SET status = 'ready_for_summary',
                duration_seconds = ?,
                transcript_text = ?,
                transcript_language = ?,
                transcript_language_code = ?,
                retry_count = 0,
                next_retry_at = NULL,
                last_error = NULL,
                last_error_stage = NULL,
                updated_at = ?
            WHERE id = ?
            """,
            (
                transcript.duration_seconds,
                transcript.text,
                transcript.language,
                transcript.language_code,
                now,
                video_id,
            ),
        )
        self._connection.commit()

    def mark_video_too_short(
        self,
        video_id: int,
        *,
        transcript: TranscriptData,
        min_duration_seconds: int,
    ) -> None:
        now = _isoformat(utc_now())
        self._connection.execute(
            """
            UPDATE videos
            SET status = 'transcript_skipped',
                duration_seconds = ?,
                transcript_language = ?,
                transcript_language_code = ?,
                retry_count = 0,
                next_retry_at = NULL,
                last_error = ?,
                last_error_stage = 'transcript',
                updated_at = ?
            WHERE id = ?
            """,
            (
                transcript.duration_seconds,
                transcript.language,
                transcript.language_code,
                (
                    "Skipped transcript fetch because video duration "
                    f"{transcript.duration_seconds}s is shorter than configured minimum "
                    f"{min_duration_seconds}s."
                ),
                now,
                video_id,
            ),
        )
        self._connection.commit()

    def mark_transcript_skipped(self, video_id: int, reason: str) -> None:
        self._update_video_status(
            video_id,
            status="transcript_skipped",
            last_error=reason,
            last_error_stage="transcript",
            retry_count=0,
            next_retry_at=None,
        )

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
        self._connection.execute(
            """
            UPDATE videos
            SET status = ?,
                retry_count = ?,
                next_retry_at = ?,
                last_error = ?,
                last_error_stage = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                status,
                retry_count,
                _isoformat(next_retry_at),
                last_error,
                stage,
                _isoformat(utc_now()),
                video_id,
            ),
        )
        self._connection.commit()

    def mark_transcript_unavailable(self, video_id: int, last_error: str) -> None:
        self._update_video_status(
            video_id,
            status="transcript_unavailable",
            last_error=last_error,
            last_error_stage="transcript",
            retry_count=0,
            next_retry_at=None,
        )

    def mark_summary_error(self, video_id: int, last_error: str) -> None:
        self._update_video_status(
            video_id,
            status="error",
            last_error=last_error,
            last_error_stage="summary",
            retry_count=0,
            next_retry_at=None,
        )

    def store_analysis(
        self,
        *,
        video_id: int,
        model: str,
        prompt_version: str,
        transcript: TranscriptData,
        summary: SummaryResult,
        transcript_hash: str,
    ) -> None:
        now = _isoformat(utc_now())
        self._connection.execute(
            """
            INSERT INTO analyses (
                video_id,
                model,
                prompt_version,
                transcript_text,
                transcript_hash,
                transcript_language,
                transcript_language_code,
                score,
                priority,
                confidence,
                why_it_matters,
                summary_bullets_json,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(video_id) DO UPDATE SET
                model = excluded.model,
                prompt_version = excluded.prompt_version,
                transcript_text = excluded.transcript_text,
                transcript_hash = excluded.transcript_hash,
                transcript_language = excluded.transcript_language,
                transcript_language_code = excluded.transcript_language_code,
                score = excluded.score,
                priority = excluded.priority,
                confidence = excluded.confidence,
                why_it_matters = excluded.why_it_matters,
                summary_bullets_json = excluded.summary_bullets_json,
                updated_at = excluded.updated_at
            """,
            (
                video_id,
                model,
                prompt_version,
                transcript.text,
                transcript_hash,
                transcript.language,
                transcript.language_code,
                summary.score,
                summary.priority,
                summary.confidence,
                summary.why_it_matters,
                json.dumps(summary.summary_bullets),
                now,
                now,
            ),
        )
        self._connection.execute(
            """
            UPDATE videos
            SET status = 'summarized',
                retry_count = 0,
                next_retry_at = NULL,
                last_error = NULL,
                last_error_stage = NULL,
                updated_at = ?
            WHERE id = ?
            """,
            (now, video_id),
        )
        self._connection.commit()

    def mark_notification_skipped(self, video_id: int) -> None:
        self._update_video_status(
            video_id,
            status="summarized",
            last_error=None,
            last_error_stage=None,
            retry_count=0,
            next_retry_at=None,
        )

    def mark_notification_sent(self, video_id: int) -> None:
        self._update_video_status(
            video_id,
            status="notification_sent",
            last_error=None,
            last_error_stage=None,
            retry_count=0,
            next_retry_at=None,
        )

    def record_notification(
        self,
        *,
        video_id: int,
        target_chat_id: str,
        message_text: str,
        delivery_status: str,
        telegram_message_id: int | None,
        error: str | None,
    ) -> None:
        now = _isoformat(utc_now())
        delivered_at = now if delivery_status == "delivered" else None
        self._connection.execute(
            """
            INSERT INTO notifications (
                video_id,
                target_chat_id,
                message_text,
                telegram_message_id,
                delivery_status,
                error,
                delivered_at,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(video_id) DO UPDATE SET
                target_chat_id = excluded.target_chat_id,
                message_text = excluded.message_text,
                telegram_message_id = excluded.telegram_message_id,
                delivery_status = excluded.delivery_status,
                error = excluded.error,
                delivered_at = excluded.delivered_at,
                updated_at = excluded.updated_at
            """,
            (
                video_id,
                target_chat_id,
                message_text,
                telegram_message_id,
                delivery_status,
                error,
                delivered_at,
                now,
                now,
            ),
        )
        self._connection.commit()

    def list_pending_notifications(
        self,
        *,
        min_score: int,
    ) -> list[tuple[StoredVideo, StoredAnalysis]]:
        rows = self._connection.execute(
            """
            SELECT
                v.*,
                a.model AS analysis_model,
                a.prompt_version,
                a.transcript_text,
                a.transcript_hash,
                a.transcript_language AS analysis_transcript_language,
                a.transcript_language_code AS analysis_transcript_language_code,
                a.score,
                a.priority,
                a.confidence,
                a.why_it_matters,
                a.summary_bullets_json
            FROM videos v
            JOIN analyses a ON a.video_id = v.id
            LEFT JOIN notifications n ON n.video_id = v.id
            WHERE v.status IN ('summarized', 'notification_skipped')
              AND lower(a.priority) = 'high'
              AND a.score >= ?
              AND (n.delivery_status IS NULL OR n.delivery_status != 'delivered')
            ORDER BY v.published_at ASC
            """,
            (min_score,),
        ).fetchall()

        pairs: list[tuple[StoredVideo, StoredAnalysis]] = []
        for row in rows:
            pairs.append((_row_to_video(row), _row_to_joined_analysis(row)))
        return pairs

    def requeue_video(self, video_id: int, *, status: str) -> None:
        self._update_video_status(
            video_id,
            status=status,
            last_error=None,
            last_error_stage=None,
            retry_count=0,
            next_retry_at=None,
        )

    def _update_video_status(
        self,
        video_id: int,
        *,
        status: str,
        last_error: str | None,
        last_error_stage: str | None,
        retry_count: int,
        next_retry_at: datetime | None,
    ) -> None:
        self._connection.execute(
            """
            UPDATE videos
            SET status = ?,
                last_error = ?,
                last_error_stage = ?,
                retry_count = ?,
                next_retry_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                status,
                last_error,
                last_error_stage,
                retry_count,
                _isoformat(next_retry_at) if next_retry_at else None,
                _isoformat(utc_now()),
                video_id,
            ),
        )
        self._connection.commit()


def _isoformat(value: datetime | None) -> str:
    if value is None:
        raise ValueError("Cannot serialize a null datetime to ISO format.")
    return value.astimezone(UTC).isoformat()


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _row_to_video(row: sqlite3.Row) -> StoredVideo:
    return StoredVideo(
        id=int(row["id"]),
        youtube_video_id=str(row["youtube_video_id"]),
        channel_id=str(row["channel_id"]),
        channel_title=str(row["channel_title"]),
        title=str(row["title"]),
        published_at=_parse_datetime(row["published_at"]) or utc_now(),
        url=str(row["url"]),
        status=str(row["status"]),
        duration_seconds=(
            int(row["duration_seconds"]) if row["duration_seconds"] is not None else None
        ),
        transcript_text=str(row["transcript_text"]) if row["transcript_text"] else None,
        transcript_language=str(row["transcript_language"]) if row["transcript_language"] else None,
        transcript_language_code=(
            str(row["transcript_language_code"]) if row["transcript_language_code"] else None
        ),
        retry_count=int(row["retry_count"]),
        next_retry_at=_parse_datetime(row["next_retry_at"]),
        last_error=str(row["last_error"]) if row["last_error"] else None,
        last_error_stage=str(row["last_error_stage"]) if row["last_error_stage"] else None,
    )


def _row_to_analysis(row: sqlite3.Row) -> StoredAnalysis:
    return StoredAnalysis(
        video_id=int(row["video_id"]),
        model=str(row["model"]),
        prompt_version=str(row["prompt_version"]),
        transcript_text=str(row["transcript_text"]),
        transcript_hash=str(row["transcript_hash"]),
        transcript_language=str(row["transcript_language"]),
        transcript_language_code=str(row["transcript_language_code"]),
        score=int(row["score"]),
        priority=str(row["priority"]),
        confidence=int(row["confidence"]),
        why_it_matters=str(row["why_it_matters"]),
        summary_bullets=tuple(json.loads(row["summary_bullets_json"])),
    )


def _row_to_joined_analysis(row: sqlite3.Row) -> StoredAnalysis:
    return StoredAnalysis(
        video_id=int(row["id"]),
        model=str(row["analysis_model"]),
        prompt_version=str(row["prompt_version"]),
        transcript_text=str(row["transcript_text"]),
        transcript_hash=str(row["transcript_hash"]),
        transcript_language=str(row["analysis_transcript_language"]),
        transcript_language_code=str(row["analysis_transcript_language_code"]),
        score=int(row["score"]),
        priority=str(row["priority"]),
        confidence=int(row["confidence"]),
        why_it_matters=str(row["why_it_matters"]),
        summary_bullets=tuple(json.loads(row["summary_bullets_json"])),
    )
