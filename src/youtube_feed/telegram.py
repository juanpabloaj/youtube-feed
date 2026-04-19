from __future__ import annotations

from collections.abc import Iterable
from json import JSONDecodeError

import httpx

from youtube_feed.exceptions import TelegramHTTPError
from youtube_feed.models import StoredAnalysis, StoredVideo

TELEGRAM_API_BASE = "https://api.telegram.org"
TELEGRAM_MAX_TEXT_LENGTH = 4096


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str, http_client: httpx.Client) -> None:
        self._bot_token = bot_token
        self._chat_id = chat_id
        self._http_client = http_client

    def send_message(self, message_text: str) -> int:
        response = self._http_client.post(
            f"{TELEGRAM_API_BASE}/bot{self._bot_token}/sendMessage",
            json={
                "chat_id": self._chat_id,
                "text": message_text,
                "disable_web_page_preview": True,
            },
        )
        if response.status_code >= 400:
            description = _extract_telegram_error(
                response,
                default="Telegram rejected the request.",
            )
            raise TelegramHTTPError(response.status_code, description)
        payload = _parse_json_response(
            response,
            default_error="Telegram returned an invalid JSON payload.",
        )
        return int(payload["result"]["message_id"])

    def healthcheck(self) -> dict[str, str]:
        me_response = self._http_client.get(f"{TELEGRAM_API_BASE}/bot{self._bot_token}/getMe")
        if me_response.status_code >= 400:
            description = _extract_telegram_error(
                me_response,
                default="Telegram bot authentication failed.",
            )
            raise TelegramHTTPError(me_response.status_code, description)

        chat_response = self._http_client.get(
            f"{TELEGRAM_API_BASE}/bot{self._bot_token}/getChat",
            params={"chat_id": self._chat_id},
        )
        if chat_response.status_code >= 400:
            description = _extract_telegram_error(
                chat_response,
                default="Telegram chat lookup failed.",
            )
            raise TelegramHTTPError(chat_response.status_code, description)

        me_payload = _parse_json_response(
            me_response,
            default_error="Telegram getMe returned invalid JSON.",
        )["result"]
        chat_payload = _parse_json_response(
            chat_response,
            default_error="Telegram getChat returned invalid JSON.",
        )["result"]
        return {
            "bot_username": me_payload.get("username", ""),
            "chat_title": chat_payload.get("title", chat_payload.get("username", "")),
        }


def render_message(video: StoredVideo, analysis: StoredAnalysis, *, limit: int) -> str:
    safe_limit = min(limit, TELEGRAM_MAX_TEXT_LENGTH)
    bullet_variants = [
        tuple(_clip_text(bullet, 280) for bullet in analysis.summary_bullets[:5]),
        tuple(_clip_text(bullet, 220) for bullet in analysis.summary_bullets[:5]),
        tuple(_clip_text(bullet, 160) for bullet in analysis.summary_bullets[:5]),
        tuple(_clip_text(bullet, 120) for bullet in analysis.summary_bullets[:4]),
        tuple(_clip_text(bullet, 90) for bullet in analysis.summary_bullets[:3]),
    ]
    why_variants = [
        _clip_text(analysis.why_it_matters, 240),
        _clip_text(analysis.why_it_matters, 160),
        "",
    ]

    for bullets in bullet_variants:
        for why_text in why_variants:
            message = _build_message(video, analysis, bullets, why_text)
            if len(message) <= safe_limit:
                return message

    final_message = _build_message(
        video,
        analysis,
        tuple(_clip_text(bullet, 70) for bullet in analysis.summary_bullets[:3]),
        "",
    )
    if len(final_message) <= safe_limit:
        return final_message

    watch_line = video.url
    reserve = len(watch_line) + 2
    clipped = final_message[: max(safe_limit - reserve, 0)].rstrip()
    return f"{clipped}\n\n{watch_line}"


def _build_message(
    video: StoredVideo,
    analysis: StoredAnalysis,
    bullets: Iterable[str],
    why_it_matters: str,
) -> str:
    metadata_line = (
        f"{video.published_at.date().isoformat()} "
        f"• {_format_duration(video.duration_seconds)} "
        f"• {analysis.priority.title()} {analysis.score}/100"
    )
    lines = [
        video.title,
        video.channel_title,
        metadata_line,
        "",
    ]
    lines.extend(f"- {bullet}" for bullet in bullets if bullet)
    if why_it_matters:
        lines.extend(["", why_it_matters])
    lines.extend(["", video.url])
    return "\n".join(lines)


def _format_duration(duration_seconds: int | None) -> str:
    if not duration_seconds:
        return "unknown"
    hours, remainder = divmod(duration_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def _clip_text(text: str, limit: int) -> str:
    normalized = " ".join(text.split())
    if len(normalized) <= limit:
        return normalized
    if limit <= 1:
        return normalized[:limit]
    return normalized[: limit - 1].rstrip() + "…"


def _parse_json_response(response: httpx.Response, *, default_error: str) -> dict:
    try:
        return response.json()
    except JSONDecodeError as exc:
        raise TelegramHTTPError(response.status_code, default_error) from exc


def _extract_telegram_error(response: httpx.Response, *, default: str) -> str:
    try:
        payload = response.json()
    except JSONDecodeError:
        text = " ".join(response.text.split())
        return text[:200] if text else default
    description = payload.get("description")
    if isinstance(description, str) and description.strip():
        return description.strip()
    return default
