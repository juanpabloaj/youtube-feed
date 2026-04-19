from __future__ import annotations

import json
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from json import JSONDecodeError
from typing import Any

import httpx

from youtube_feed.exceptions import GeminiHTTPError, GeminiResponseError, GeminiTransportError
from youtube_feed.models import SummaryResult, TranscriptData

PROMPT_VERSION = "v1"
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
SUMMARY_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "priority": {"type": "string", "enum": ["low", "medium", "high"]},
        "score": {"type": "integer"},
        "confidence": {"type": "integer"},
        "why_it_matters": {"type": "string"},
        "summary_bullets": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 3,
            "maxItems": 5,
        },
    },
    "required": [
        "priority",
        "score",
        "confidence",
        "why_it_matters",
        "summary_bullets",
    ],
}


class GeminiSummarizer:
    def __init__(self, api_key: str, model: str, http_client: httpx.Client) -> None:
        self._api_key = api_key
        self._model = model
        self._http_client = http_client

    @property
    def model(self) -> str:
        return self._model

    def summarize(
        self,
        *,
        video_title: str,
        channel_title: str,
        transcript: TranscriptData,
    ) -> SummaryResult:
        payload = {
            "systemInstruction": {
                "parts": [
                    {
                        "text": (
                            "You evaluate whether YouTube videos are worth the user's attention. "
                            "Prioritize durable insight, actionable advice, depth, and novelty. "
                            "Avoid hype, repetition, and shallow commentary."
                        )
                    }
                ]
            },
            "contents": [
                {
                    "parts": [
                        {
                            "text": _build_prompt(
                                video_title=video_title,
                                channel_title=channel_title,
                                transcript=transcript,
                            )
                        }
                    ]
                }
            ],
            "generationConfig": {
                "temperature": 0.2,
                "responseMimeType": "application/json",
                "responseJsonSchema": SUMMARY_SCHEMA,
            },
        }

        try:
            response = self._http_client.post(
                GEMINI_API_URL.format(model=self._model),
                headers={
                    "Content-Type": "application/json",
                    "x-goog-api-key": self._api_key,
                },
                json=payload,
            )
        except (
            httpx.ConnectError,
            httpx.ReadTimeout,
            httpx.WriteTimeout,
            httpx.PoolTimeout,
        ) as exc:
            raise GeminiTransportError(str(exc)) from exc

        self._raise_for_recognized_status(response)
        response.raise_for_status()
        return _parse_summary_response(response.json())

    def healthcheck(self) -> str:
        try:
            response = self._http_client.post(
                GEMINI_API_URL.format(model=self._model),
                headers={
                    "Content-Type": "application/json",
                    "x-goog-api-key": self._api_key,
                },
                json={
                    "contents": [{"parts": [{"text": "Reply with the single word OK."}]}],
                    "generationConfig": {"temperature": 0},
                },
            )
        except (
            httpx.ConnectError,
            httpx.ReadTimeout,
            httpx.WriteTimeout,
            httpx.PoolTimeout,
        ) as exc:
            raise GeminiTransportError(str(exc)) from exc

        self._raise_for_recognized_status(response)
        response.raise_for_status()
        data = response.json()
        parts = data.get("candidates", [{}])[0].get("content", {}).get("parts", [])
        text = "".join(part.get("text", "") for part in parts).strip()
        if not text:
            raise GeminiResponseError("Gemini healthcheck response did not contain text.")
        return text

    def _raise_for_recognized_status(self, response: httpx.Response) -> None:
        details = _extract_error_details(response)
        if response.status_code == 400:
            raise GeminiHTTPError(
                400,
                "Gemini rejected the request as invalid.",
                retryable=False,
                details=details,
            )
        if response.status_code in {401, 403}:
            raise GeminiHTTPError(
                response.status_code,
                "Gemini credentials or project access are invalid.",
                retryable=False,
                details=details,
            )
        if response.status_code == 404:
            raise GeminiHTTPError(
                404,
                "Gemini model or endpoint was not found.",
                retryable=False,
                details=details,
            )
        if response.status_code == 429:
            raise GeminiHTTPError(
                429,
                "Gemini rate limit or quota was exceeded.",
                retryable=True,
                details=details,
                retry_after_seconds=_extract_retry_after_seconds(response),
            )
        if response.status_code in {500, 502, 503, 504}:
            raise GeminiHTTPError(
                response.status_code,
                "Gemini returned a transient server error.",
                retryable=True,
                details=details,
            )
        if 500 <= response.status_code <= 599:
            raise GeminiHTTPError(
                response.status_code,
                "Gemini returned an unexpected server error.",
                retryable=True,
                details=details,
            )
        if 400 <= response.status_code <= 499:
            raise GeminiHTTPError(
                response.status_code,
                "Gemini rejected the request with an unexpected client error.",
                retryable=False,
                details=details,
            )


def _build_prompt(*, video_title: str, channel_title: str, transcript: TranscriptData) -> str:
    transcript_language = (
        transcript.language or transcript.language_code or "the transcript language"
    )
    return f"""
Analyze this YouTube video transcript and decide whether it is worth the user's attention.

Output requirements:
- Respond in the same language as the transcript.
- Do not translate to English unless the transcript itself is in English.
- Return only JSON matching the provided schema.
- Produce 3 to 5 concise summary bullets.
- Score from 0 to 100.

Evaluation criteria:
- Reward durable insight, clear takeaways, novelty, and practical usefulness.
- Penalize shallow reactions, recycled commentary, clickbait framing, and sponsor-heavy filler.

Metadata:
- Video title: {video_title}
- Channel title: {channel_title}
- Transcript language: {transcript_language} ({transcript.language_code})

Transcript:
{transcript.text}
""".strip()


def estimate_input_tokens(
    *,
    video_title: str,
    channel_title: str,
    transcript: TranscriptData,
) -> int:
    prompt = _build_prompt(
        video_title=video_title,
        channel_title=channel_title,
        transcript=transcript,
    )
    # Rough heuristic for Gemini input budgeting: ~4 chars/token plus schema/system overhead.
    return max(1, (len(prompt) // 4) + 800)


def _parse_summary_response(payload: dict[str, Any]) -> SummaryResult:
    parts = payload.get("candidates", [{}])[0].get("content", {}).get("parts", [])
    text = "".join(part.get("text", "") for part in parts).strip()
    if not text:
        raise GeminiResponseError("Gemini response did not contain structured text.")
    try:
        data = json.loads(text)
        bullets = tuple(str(item).strip() for item in data["summary_bullets"] if str(item).strip())
        if len(bullets) < 3:
            raise GeminiResponseError("Gemini returned fewer than 3 summary bullets.")
        return SummaryResult(
            score=int(data["score"]),
            priority=str(data["priority"]).lower(),
            confidence=int(data["confidence"]),
            why_it_matters=str(data["why_it_matters"]).strip(),
            summary_bullets=bullets[:5],
        )
    except JSONDecodeError as exc:
        raise GeminiResponseError("Gemini returned invalid JSON.") from exc
    except KeyError as exc:
        raise GeminiResponseError(f"Gemini response was missing required field: {exc}") from exc
    except TypeError as exc:
        raise GeminiResponseError("Gemini response had invalid field types.") from exc


def _extract_error_details(response: httpx.Response, limit: int = 280) -> str | None:
    try:
        payload = response.json()
    except JSONDecodeError:
        text = response.text.strip()
        return _clip_text(text, limit) if text else None

    error = payload.get("error")
    if isinstance(error, dict):
        message = error.get("message")
        if isinstance(message, str) and message.strip():
            return _clip_text(message.strip(), limit)

        details = error.get("details")
        if isinstance(details, list):
            for item in details:
                if isinstance(item, dict):
                    detail_message = item.get("message")
                    if isinstance(detail_message, str) and detail_message.strip():
                        return _clip_text(detail_message.strip(), limit)

    message = payload.get("message")
    if isinstance(message, str) and message.strip():
        return _clip_text(message.strip(), limit)
    return None


def _extract_retry_after_seconds(response: httpx.Response) -> float | None:
    header_value = response.headers.get("Retry-After")
    if not header_value:
        return None

    try:
        seconds = float(header_value)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(header_value)
        except (TypeError, ValueError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        seconds = (retry_at - datetime.now(tz=UTC)).total_seconds()

    return max(0.0, seconds)


def _clip_text(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3].rstrip()}..."
