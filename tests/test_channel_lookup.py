from youtube_feed.channel_lookup import build_search_url, parse_channel_search_results

HTML_FIXTURE = """
<html>
  <body>
    <script>
      var ytInitialData = {
        "contents": {
          "twoColumnSearchResultsRenderer": {
            "primaryContents": {
              "sectionListRenderer": {
                "contents": [
                  {
                    "itemSectionRenderer": {
                      "contents": [
                        {
                          "channelRenderer": {
                            "channelId": "UC1111111111111111111111",
                            "title": {"simpleText": "YouTube Feed"},
                            "descriptionSnippet": {
                              "runs": [{"text": "Smart summaries for YouTube"}]
                            },
                            "navigationEndpoint": {
                              "browseEndpoint": {
                                "canonicalBaseUrl": "/@youtube_feed"
                              }
                            }
                          }
                        },
                        {
                          "channelRenderer": {
                            "channelId": "UC2222222222222222222222",
                            "title": {"simpleText": "YouTube Feed Clips"},
                            "descriptionSnippet": {"runs": [{"text": "Short clips"}]}
                          }
                        }
                      ]
                    }
                  }
                ]
              }
            }
          }
        }
      };
    </script>
  </body>
</html>
"""


def test_parse_channel_search_results_orders_best_match_first() -> None:
    results = parse_channel_search_results("YouTube Feed", HTML_FIXTURE, limit=5)

    assert len(results) == 2
    assert results[0].channel_id == "UC1111111111111111111111"
    assert results[0].title == "YouTube Feed"
    assert results[0].handle == "@youtube_feed"
    assert results[1].channel_id == "UC2222222222222222222222"


def test_build_search_url_includes_channel_filter() -> None:
    url = build_search_url("YouTube Feed")

    assert "search_query=YouTube+Feed" in url
    assert "sp=EgIQAg%253D%253D" in url
