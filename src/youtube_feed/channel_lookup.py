from __future__ import annotations

import json
import re
from dataclasses import dataclass
from html import unescape
from typing import Any
from urllib.parse import quote_plus

import httpx

YOUTUBE_SEARCH_URL = "https://www.youtube.com/results"
YOUTUBE_CHANNEL_FILTER = "EgIQAg%253D%253D"
INITIAL_DATA_PATTERN = re.compile(r"var ytInitialData = (\{.*?\});", re.DOTALL)
CHANNEL_ID_PATTERN = re.compile(r"^UC[a-zA-Z0-9_-]{20,}$")


@dataclass(frozen=True)
class ChannelSearchResult:
    channel_id: str
    title: str
    handle: str | None
    description: str | None
    url: str
    score: tuple[int, int, int]


class YouTubeChannelLookupClient:
    def __init__(self, http_client: httpx.Client) -> None:
        self._http_client = http_client

    def search(self, query: str, *, limit: int = 5) -> list[ChannelSearchResult]:
        response = self._http_client.get(
            YOUTUBE_SEARCH_URL,
            params={
                "search_query": query,
                "sp": YOUTUBE_CHANNEL_FILTER,
            },
            headers={
                "Accept-Language": "en-US,en;q=0.9",
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
            },
        )
        response.raise_for_status()
        return parse_channel_search_results(query, response.text, limit=limit)


def parse_channel_search_results(
    query: str,
    html_text: str,
    *,
    limit: int = 5,
) -> list[ChannelSearchResult]:
    payload = _extract_initial_data(html_text)
    results: dict[str, ChannelSearchResult] = {}
    normalized_query = _normalize_text(query)

    for channel_renderer in _walk_for_channel_renderers(payload):
        channel_id = channel_renderer.get("channelId", "").strip()
        if not CHANNEL_ID_PATTERN.match(channel_id):
            continue
        title = _extract_text(channel_renderer.get("title", {}))
        handle = _extract_handle(channel_renderer)
        description = _extract_text(channel_renderer.get("descriptionSnippet", {})) or None
        if not title:
            continue
        candidate = ChannelSearchResult(
            channel_id=channel_id,
            title=title,
            handle=handle,
            description=description,
            url=f"https://www.youtube.com/channel/{channel_id}",
            score=_score_candidate(normalized_query, title, handle, description),
        )
        previous = results.get(channel_id)
        if previous is None or candidate.score > previous.score:
            results[channel_id] = candidate

    ordered = sorted(results.values(), key=lambda item: item.score, reverse=True)
    return ordered[:limit]


def build_search_url(query: str) -> str:
    return f"{YOUTUBE_SEARCH_URL}?search_query={quote_plus(query)}&sp={YOUTUBE_CHANNEL_FILTER}"


def _extract_initial_data(html_text: str) -> dict[str, Any]:
    match = INITIAL_DATA_PATTERN.search(html_text)
    if match is None:
        raise ValueError("Could not find ytInitialData in the YouTube search response.")
    return json.loads(unescape(match.group(1)))


def _walk_for_channel_renderers(node: Any) -> list[dict[str, Any]]:
    renderers: list[dict[str, Any]] = []
    if isinstance(node, dict):
        channel_renderer = node.get("channelRenderer")
        if isinstance(channel_renderer, dict):
            renderers.append(channel_renderer)
        for value in node.values():
            renderers.extend(_walk_for_channel_renderers(value))
    elif isinstance(node, list):
        for item in node:
            renderers.extend(_walk_for_channel_renderers(item))
    return renderers


def _extract_text(node: dict[str, Any]) -> str:
    simple_text = node.get("simpleText")
    if isinstance(simple_text, str):
        return " ".join(simple_text.split())
    runs = node.get("runs", [])
    parts = [run.get("text", "") for run in runs if isinstance(run, dict)]
    return " ".join(" ".join(parts).split())


def _extract_handle(channel_renderer: dict[str, Any]) -> str | None:
    navigation_endpoint = channel_renderer.get("navigationEndpoint", {})
    browse_endpoint = navigation_endpoint.get("browseEndpoint", {})
    canonical_base_url = browse_endpoint.get("canonicalBaseUrl", "")
    if isinstance(canonical_base_url, str) and canonical_base_url.startswith("/@"):
        return canonical_base_url.removeprefix("/")
    return None


def _score_candidate(
    normalized_query: str,
    title: str,
    handle: str | None,
    description: str | None,
) -> tuple[int, int, int]:
    normalized_title = _normalize_text(title)
    normalized_handle = _normalize_text(handle or "")
    normalized_description = _normalize_text(description or "")
    return (
        int(normalized_title == normalized_query),
        int(normalized_title.startswith(normalized_query))
        + int(normalized_query in normalized_title or normalized_query in normalized_handle),
        int(normalized_query in normalized_description),
    )


def _normalize_text(value: str) -> str:
    return " ".join(value.casefold().split())
