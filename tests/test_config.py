from __future__ import annotations

from pathlib import Path

from youtube_feed.config import AppConfig


def test_config_summary_masks_secrets_and_resolves_alias(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    env_path = Path(tmp_path) / ".env"
    env_path.write_text("", encoding="utf-8")
    monkeypatch.setenv("YOUTUBE_CHANNELS_IDS", "abc123,def456,")
    monkeypatch.setenv("GEMINI_API_KEY", "secret-key")
    monkeypatch.setenv("GEMINI_MODEL", "")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "secret-bot")
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    monkeypatch.setenv("TELEGRAM_CHANNEL_ID", "@feed")
    monkeypatch.setenv("DATABASE_PATH", str(tmp_path / "feed.db"))
    monkeypatch.setenv("HTTP_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("RSS_HTTP_TIMEOUT_SECONDS", "21")
    monkeypatch.setenv("GEMINI_HTTP_TIMEOUT_SECONDS", "61")
    monkeypatch.setenv("TELEGRAM_HTTP_TIMEOUT_SECONDS", "16")
    monkeypatch.setenv("CHANNEL_LOOKUP_HTTP_TIMEOUT_SECONDS", "22")
    monkeypatch.setenv("TELEGRAM_MESSAGE_LIMIT", "3500")
    monkeypatch.setenv("GEMINI_COOLDOWN_AFTER_SERVICE_UNAVAILABLE_SECONDS", "17")
    monkeypatch.setenv("GEMINI_TRANSPORT_INLINE_RETRY_DELAYS_SECONDS", "3,9")
    monkeypatch.setenv("MIN_VIDEO_DURATION_SECONDS", "240")
    monkeypatch.setenv("TRANSCRIPT_GLOBAL_BLOCK_COOLDOWN_MINUTES", "180")

    config = AppConfig.from_env()

    assert config.telegram_chat_id == "@feed"
    assert config.gemini_model == "gemini-2.0-flash"
    assert config.http_timeout_seconds == 45.0
    assert config.rss_http_timeout_seconds == 21.0
    assert config.gemini_http_timeout_seconds == 61.0
    assert config.telegram_http_timeout_seconds == 16.0
    assert config.channel_lookup_http_timeout_seconds == 22.0
    assert config.telegram_message_limit == 3500
    assert config.gemini_cooldown_after_service_unavailable_seconds == 17.0
    assert config.gemini_transport_inline_retry_delays_seconds == (3, 9)
    assert config.min_video_duration_seconds == 240
    assert config.transcript_global_block_cooldown_minutes == 180

    rendered = config.render_summary()
    assert "GEMINI_API_KEY=configured" in rendered
    assert "TELEGRAM_BOT_TOKEN=configured" in rendered
    assert "secret-key" not in rendered
    assert "secret-bot" not in rendered
    assert "TELEGRAM_CHAT_ID=@feed" in rendered
    assert "HTTP_TIMEOUT_SECONDS=45.0" in rendered
    assert "RSS_HTTP_TIMEOUT_SECONDS=21.0" in rendered
    assert "GEMINI_HTTP_TIMEOUT_SECONDS=61.0" in rendered
    assert "TELEGRAM_HTTP_TIMEOUT_SECONDS=16.0" in rendered
    assert "CHANNEL_LOOKUP_HTTP_TIMEOUT_SECONDS=22.0" in rendered
    assert "TELEGRAM_MESSAGE_LIMIT=3500" in rendered
    assert "GEMINI_COOLDOWN_AFTER_SERVICE_UNAVAILABLE_SECONDS=17.0" in rendered
    assert "GEMINI_TRANSPORT_INLINE_RETRY_DELAYS_SECONDS=3,9" in rendered
    assert "MIN_VIDEO_DURATION_SECONDS=240" in rendered
    assert "TRANSCRIPT_GLOBAL_BLOCK_COOLDOWN_MINUTES=180" in rendered
