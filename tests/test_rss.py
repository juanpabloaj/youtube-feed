from datetime import UTC, datetime

import httpx
import pytest

from youtube_feed.rss import YouTubeRssClient, parse_channel_videos_page, parse_feed

HTML_FIXTURE = """
<html>
  <body>
    <script>
      var ytInitialData = {
        "contents": {
          "twoColumnBrowseResultsRenderer": {
            "tabs": [
              {
                "tabRenderer": {
                  "content": {
                    "richGridRenderer": {
                      "contents": [
                        {
                          "richItemRenderer": {
                            "content": {
                              "videoRenderer": {
                                "videoId": "abc123xyz01",
                                "title": {"runs": [{"text": "Episode One"}]},
                                "ownerText": {"runs": [{"text": "Lex Fridman"}]},
                                "publishedTimeText": {"simpleText": "2 days ago"}
                              }
                            }
                          }
                        },
                        {
                          "richItemRenderer": {
                            "content": {
                              "videoRenderer": {
                                "videoId": "abc123xyz02",
                                "title": {"runs": [{"text": "Episode Two"}]},
                                "ownerText": {"runs": [{"text": "Lex Fridman"}]},
                                "publishedTimeText": {"simpleText": "1 week ago"}
                              }
                            }
                          }
                        }
                      ]
                    }
                  }
                }
              }
            ]
          }
        }
      };
    </script>
  </body>
</html>
"""


def test_parse_channel_videos_page_extracts_recent_entries() -> None:
    fetched_at = datetime(2026, 4, 17, 18, 0, tzinfo=UTC)

    results = parse_channel_videos_page(
        "UCSHZKyawb77ixDdsGog4iWA",
        HTML_FIXTURE,
        fetched_at=fetched_at,
    )

    assert len(results) == 2
    assert results[0].youtube_video_id == "abc123xyz01"
    assert results[0].title == "Episode One"
    assert results[0].channel_title == "Lex Fridman"
    assert results[0].url.endswith("abc123xyz01")


def test_parse_feed_preserves_shorts_link_from_alternate_url() -> None:
    xml_text = """<?xml version="1.0" encoding="UTF-8"?>
    <feed xmlns:yt="http://www.youtube.com/xml/schemas/2015"
          xmlns="http://www.w3.org/2005/Atom">
      <title>Example Channel</title>
      <entry>
        <id>yt:video:short123</id>
        <yt:videoId>short123</yt:videoId>
        <yt:channelId>channel-1</yt:channelId>
        <title>Short clip</title>
        <link rel="alternate" href="https://www.youtube.com/shorts/short123"/>
        <author><name>Example Channel</name></author>
        <published>2026-04-16T17:15:00+00:00</published>
      </entry>
      <entry>
        <id>yt:video:video123</id>
        <yt:videoId>video123</yt:videoId>
        <yt:channelId>channel-1</yt:channelId>
        <title>Full episode</title>
        <link rel="alternate" href="https://www.youtube.com/watch?v=video123"/>
        <author><name>Example Channel</name></author>
        <published>2026-04-15T17:15:00+00:00</published>
      </entry>
    </feed>
    """

    results = parse_feed("channel-1", xml_text)

    assert results[0].url == "https://www.youtube.com/shorts/short123"
    assert results[1].url == "https://www.youtube.com/watch?v=video123"


def test_fallback_request_uses_curl_user_agent_without_browser_headers() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/feeds/videos.xml":
            return httpx.Response(404)
        return httpx.Response(200, text=HTML_FIXTURE)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    rss_client = YouTubeRssClient(client)

    rss_client.fetch_channel("channel-1")
    client.close()

    assert len(requests) == 2
    assert requests[1].url.path == "/channel/channel-1/videos"
    assert requests[1].headers.get("accept-language") is None
    assert requests[1].headers.get("user-agent") == "curl/8.5.0"


def test_fallback_reports_youtube_consent_redirect() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/feeds/videos.xml":
            return httpx.Response(404)
        return httpx.Response(
            302,
            headers={"location": "https://consent.youtube.com/m?continue=..."},
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    rss_client = YouTubeRssClient(client)

    with pytest.raises(httpx.HTTPError, match="consent page"):
        rss_client.fetch_channel("channel-1")

    client.close()
