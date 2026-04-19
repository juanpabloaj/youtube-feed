from __future__ import annotations

from collections import Counter
from xml.etree.ElementTree import ParseError

from requests import RequestException
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import (
    IpBlocked,
    NoTranscriptFound,
    RequestBlocked,
    TranscriptsDisabled,
    VideoUnavailable,
)

from youtube_feed.exceptions import (
    TranscriptBlockedError,
    TranscriptTemporaryError,
    TranscriptUnavailableError,
)
from youtube_feed.models import TranscriptData, TranscriptSnippet

STOPWORDS = {
    "en": {"the", "and", "that", "with", "this", "from", "you", "your"},
    "es": {"que", "con", "para", "una", "las", "los", "como", "por"},
    "pt": {"que", "com", "para", "uma", "como", "por", "você", "não"},
    "fr": {"que", "avec", "pour", "une", "dans", "pas", "vous", "est"},
    "de": {"und", "mit", "das", "die", "ist", "nicht", "eine", "für"},
    "it": {"che", "con", "per", "una", "non", "come", "sono", "della"},
}


class TranscriptService:
    def __init__(self, api: YouTubeTranscriptApi | None = None) -> None:
        self._api = api or YouTubeTranscriptApi()

    def fetch(self, video_id: str) -> TranscriptData:
        try:
            transcript_list = list(self._api.list(video_id))
            selected_transcript = _select_transcript(transcript_list)
            fetched = selected_transcript.fetch()
        except (NoTranscriptFound, TranscriptsDisabled, VideoUnavailable) as exc:
            raise TranscriptUnavailableError(str(exc)) from exc
        except RequestBlocked as exc:
            raise TranscriptBlockedError(str(exc), kind="request_blocked") from exc
        except IpBlocked as exc:
            raise TranscriptBlockedError(str(exc), kind="ip_blocked") from exc
        except (RequestException, ParseError) as exc:
            raise TranscriptTemporaryError(str(exc)) from exc

        snippets = tuple(
            TranscriptSnippet(
                text=_normalize_text(snippet.text),
                start=float(snippet.start),
                duration=float(snippet.duration),
            )
            for snippet in fetched
            if _normalize_text(snippet.text)
        )
        language = (
            getattr(fetched, "language", "")
            or getattr(selected_transcript, "language", "")
            or "Unknown"
        )
        language_code = (
            getattr(fetched, "language_code", "")
            or getattr(selected_transcript, "language_code", "")
            or infer_language_code("\n".join(snippet.text for snippet in snippets))
        )
        return TranscriptData(
            video_id=video_id,
            language=language,
            language_code=language_code,
            is_generated=bool(
                getattr(
                    fetched,
                    "is_generated",
                    getattr(selected_transcript, "is_generated", False),
                )
            ),
            snippets=snippets,
        )


def infer_language_code(text: str) -> str:
    lowered_tokens = [token.strip(".,!?;:\"'()[]{}").lower() for token in text.split()]
    scores = Counter()
    for token in lowered_tokens:
        for language_code, stopwords in STOPWORDS.items():
            if token in stopwords:
                scores[language_code] += 1
    if not scores:
        return "und"
    top_language, top_score = scores.most_common(1)[0]
    if top_score < 3:
        return "und"
    if len(scores) > 1 and top_score - scores.most_common(2)[1][1] < 2:
        return "und"
    return top_language


def _select_transcript(transcripts: list[object]) -> object:
    if not transcripts:
        raise TranscriptUnavailableError("No transcript candidates were returned.")
    manual = [
        transcript for transcript in transcripts if not getattr(transcript, "is_generated", False)
    ]
    return manual[0] if manual else transcripts[0]


def _normalize_text(text: str) -> str:
    return " ".join(text.split())
