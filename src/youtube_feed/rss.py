from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from xml.etree import ElementTree

import httpx

from youtube_feed.models import FeedVideo

YOUTUBE_RSS_URL = "https://www.youtube.com/feeds/videos.xml"
YOUTUBE_CHANNEL_VIDEOS_URL = "https://www.youtube.com/channel/{channel_id}/videos"
ATOM_NAMESPACE = {
    "atom": "http://www.w3.org/2005/Atom",
    "yt": "http://www.youtube.com/xml/schemas/2015",
}
INITIAL_DATA_PATTERN = re.compile(r"var ytInitialData = (\{.*?\});", re.DOTALL)
RELATIVE_TIME_PATTERN = re.compile(r"^(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago$")
YOUTUBE_HEADERS = {
    "Accept-Language": "en-US,en;q=0.9",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
}


class YouTubeRssClient:
    def __init__(self, http_client: httpx.Client) -> None:
        self._http_client = http_client

    def fetch_channel(self, channel_id: str) -> list[FeedVideo]:
        response = self._http_client.get(
            YOUTUBE_RSS_URL,
            params={"channel_id": channel_id},
            headers=YOUTUBE_HEADERS,
        )
        if response.status_code < 400:
            return parse_feed(channel_id, response.text)
        if response.status_code in {404, 500, 502, 503, 504}:
            return self._fetch_channel_videos_fallback(channel_id)
        response.raise_for_status()
        return []

    def _fetch_channel_videos_fallback(self, channel_id: str) -> list[FeedVideo]:
        response = self._http_client.get(
            YOUTUBE_CHANNEL_VIDEOS_URL.format(channel_id=channel_id),
        )
        if _is_youtube_consent_redirect(response):
            raise httpx.HTTPError("YouTube channel videos fallback redirected to consent page.")
        response.raise_for_status()
        try:
            return parse_channel_videos_page(
                channel_id,
                response.text,
                fetched_at=datetime.now(tz=UTC),
            )
        except (ValueError, KeyError, TypeError) as exc:
            raise httpx.HTTPError("YouTube channel videos fallback parsing failed.") from exc


def parse_feed(channel_id: str, xml_text: str) -> list[FeedVideo]:
    root = ElementTree.fromstring(xml_text)
    feed_title = _clean_text(root.findtext("atom:title", default="", namespaces=ATOM_NAMESPACE))
    entries: list[FeedVideo] = []
    for entry in root.findall("atom:entry", ATOM_NAMESPACE):
        video_id = _clean_text(entry.findtext("yt:videoId", default="", namespaces=ATOM_NAMESPACE))
        title = _clean_text(entry.findtext("atom:title", default="", namespaces=ATOM_NAMESPACE))
        published_at_raw = _clean_text(
            entry.findtext("atom:published", default="", namespaces=ATOM_NAMESPACE)
        )
        channel_title = _clean_text(
            entry.findtext("atom:author/atom:name", default=feed_title, namespaces=ATOM_NAMESPACE)
        )
        link_url = _extract_entry_link(entry) or f"https://www.youtube.com/watch?v={video_id}"
        if not video_id or not title or not published_at_raw:
            continue
        entries.append(
            FeedVideo(
                youtube_video_id=video_id,
                channel_id=channel_id,
                channel_title=channel_title or feed_title,
                title=title,
                published_at=_parse_rfc3339(published_at_raw),
                url=link_url,
            )
        )
    return entries


def parse_channel_videos_page(
    channel_id: str,
    html_text: str,
    *,
    fetched_at: datetime,
) -> list[FeedVideo]:
    initial_data = _extract_initial_data(html_text)
    renderers = _walk_video_renderers(initial_data)
    entries: list[FeedVideo] = []
    seen_video_ids: set[str] = set()

    for renderer in renderers:
        video_id = _clean_text(renderer.get("videoId", ""))
        title = _extract_text(renderer.get("title", {}))
        channel_title = _extract_text(renderer.get("ownerText", {})) or _extract_text(
            renderer.get("longBylineText", {})
        )
        published_label = _extract_text(renderer.get("publishedTimeText", {}))
        url = _extract_renderer_url(renderer) or f"https://www.youtube.com/watch?v={video_id}"
        if not video_id or not title or video_id in seen_video_ids:
            continue
        entries.append(
            FeedVideo(
                youtube_video_id=video_id,
                channel_id=channel_id,
                channel_title=channel_title or channel_id,
                title=title,
                published_at=_parse_relative_time(published_label, reference=fetched_at),
                url=url,
            )
        )
        seen_video_ids.add(video_id)

    return entries


def _parse_rfc3339(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _clean_text(value: str) -> str:
    return " ".join(value.split())


def _is_youtube_consent_redirect(response: httpx.Response) -> bool:
    location = response.headers.get("location", "")
    return 300 <= response.status_code < 400 and "consent.youtube.com" in location


def _extract_entry_link(entry: ElementTree.Element) -> str:
    for link in entry.findall("atom:link", ATOM_NAMESPACE):
        if link.get("rel") == "alternate":
            href = _clean_text(link.get("href", ""))
            if href:
                return href
    return ""


def _extract_renderer_url(renderer: dict) -> str:
    command_metadata = (
        renderer.get("navigationEndpoint", {})
        .get("commandMetadata", {})
        .get("webCommandMetadata", {})
    )
    url = command_metadata.get("url")
    if not isinstance(url, str) or not url:
        return ""
    if url.startswith("http"):
        return url
    return f"https://www.youtube.com{url}"


def _extract_initial_data(html_text: str) -> dict:
    match = INITIAL_DATA_PATTERN.search(html_text)
    if match is None:
        raise ValueError("Could not find ytInitialData in the channel videos page.")
    return json.loads(match.group(1))


def _walk_video_renderers(node: object) -> list[dict]:
    renderers: list[dict] = []
    if isinstance(node, dict):
        video_renderer = node.get("videoRenderer")
        if isinstance(video_renderer, dict):
            renderers.append(video_renderer)
        rich_item_renderer = node.get("richItemRenderer")
        if isinstance(rich_item_renderer, dict):
            content = rich_item_renderer.get("content", {})
            if isinstance(content, dict):
                renderers.extend(_walk_video_renderers(content))
        for value in node.values():
            renderers.extend(_walk_video_renderers(value))
    elif isinstance(node, list):
        for item in node:
            renderers.extend(_walk_video_renderers(item))
    return renderers


def _extract_text(node: dict) -> str:
    simple_text = node.get("simpleText")
    if isinstance(simple_text, str):
        return _clean_text(simple_text)
    runs = node.get("runs", [])
    parts = [run.get("text", "") for run in runs if isinstance(run, dict)]
    return _clean_text(" ".join(parts))


def _parse_relative_time(value: str, *, reference: datetime) -> datetime:
    normalized = _clean_text(value).casefold()
    match = RELATIVE_TIME_PATTERN.match(normalized)
    if match is None:
        return reference
    amount = int(match.group(1))
    unit = match.group(2)
    if unit == "second":
        return (reference - timedelta(seconds=amount)).replace(microsecond=0)
    if unit == "minute":
        return (reference - timedelta(minutes=amount)).replace(second=0, microsecond=0)
    if unit == "hour":
        return (reference - timedelta(hours=amount)).replace(minute=0, second=0, microsecond=0)
    if unit == "day":
        return (reference - timedelta(days=amount)).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
    if unit == "week":
        return (reference - timedelta(weeks=amount)).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
    if unit == "month":
        return (reference - timedelta(days=30 * amount)).replace(
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
    if unit == "year":
        return (reference - timedelta(days=365 * amount)).replace(
            month=1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
    return reference
