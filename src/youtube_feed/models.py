from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class FeedVideo:
    youtube_video_id: str
    channel_id: str
    channel_title: str
    title: str
    published_at: datetime
    url: str


@dataclass(frozen=True)
class TranscriptSnippet:
    text: str
    start: float
    duration: float


@dataclass(frozen=True)
class TranscriptData:
    video_id: str
    language: str
    language_code: str
    is_generated: bool
    snippets: tuple[TranscriptSnippet, ...]

    @property
    def text(self) -> str:
        return "\n".join(snippet.text for snippet in self.snippets if snippet.text).strip()

    @property
    def duration_seconds(self) -> int:
        if not self.snippets:
            return 0
        end_time = max(snippet.start + snippet.duration for snippet in self.snippets)
        return int(round(end_time))


@dataclass(frozen=True)
class SummaryResult:
    score: int
    priority: str
    confidence: int
    why_it_matters: str
    summary_bullets: tuple[str, ...]


@dataclass(frozen=True)
class StoredVideo:
    id: int
    youtube_video_id: str
    channel_id: str
    channel_title: str
    title: str
    published_at: datetime
    url: str
    status: str
    duration_seconds: int | None
    transcript_text: str | None
    transcript_language: str | None
    transcript_language_code: str | None
    retry_count: int
    next_retry_at: datetime | None
    last_error: str | None
    last_error_stage: str | None


@dataclass(frozen=True)
class StoredAnalysis:
    video_id: int
    model: str
    prompt_version: str
    transcript_text: str
    transcript_hash: str
    transcript_language: str
    transcript_language_code: str
    score: int
    priority: str
    confidence: int
    why_it_matters: str
    summary_bullets: tuple[str, ...]
