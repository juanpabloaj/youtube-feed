from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from youtube_feed.exceptions import ConfigurationError

DEFAULT_GEMINI_MODEL = "gemini-2.0-flash"
DEFAULT_DATABASE_PATH = "data/youtube_feed.db"
DEFAULT_SUMMARY_LANGUAGE_MODE = "transcript"
DEFAULT_MIN_NOTIFICATION_SCORE = 75
DEFAULT_FIRST_RUN_MODE = "mark_seen"
DEFAULT_HTTP_TIMEOUT_SECONDS = 30.0
DEFAULT_RSS_HTTP_TIMEOUT_SECONDS = 20.0
DEFAULT_GEMINI_HTTP_TIMEOUT_SECONDS = 60.0
DEFAULT_TELEGRAM_HTTP_TIMEOUT_SECONDS = 15.0
DEFAULT_CHANNEL_LOOKUP_HTTP_TIMEOUT_SECONDS = 20.0
DEFAULT_TELEGRAM_MESSAGE_LIMIT = 3800
DEFAULT_GEMINI_MIN_REQUEST_INTERVAL_SECONDS = 3.0
DEFAULT_GEMINI_COOLDOWN_AFTER_RATE_LIMIT_SECONDS = 60.0
DEFAULT_GEMINI_COOLDOWN_AFTER_TIMEOUT_SECONDS = 20.0
DEFAULT_GEMINI_COOLDOWN_AFTER_SERVICE_UNAVAILABLE_SECONDS = 15.0
DEFAULT_GEMINI_TRANSPORT_INLINE_RETRY_DELAYS_SECONDS = (5, 15)
DEFAULT_GEMINI_MAX_INPUT_TOKENS_PER_MINUTE = 225000
DEFAULT_MAX_TRANSCRIPT_VIDEO_AGE_DAYS = 7
DEFAULT_MAX_TRANSCRIPT_CANDIDATES_PER_CHANNEL_PER_POLL = 1
DEFAULT_MIN_VIDEO_DURATION_SECONDS = 180
DEFAULT_TELEGRAM_MESSAGE_INTERVAL_SECONDS = 0.5
DEFAULT_TRANSCRIPT_RETRY_DELAYS_MINUTES = (15, 60, 180)
DEFAULT_SUMMARY_RETRY_DELAYS_MINUTES = (5, 15, 45)
DEFAULT_TRANSCRIPT_GLOBAL_BLOCK_COOLDOWN_MINUTES = 120


@dataclass(frozen=True)
class AppConfig:
    youtube_channel_ids: tuple[str, ...]
    gemini_api_key: str
    gemini_model: str
    telegram_bot_token: str
    telegram_chat_id: str
    database_path: Path
    summary_language_mode: str
    min_notification_score: int
    first_run_mode: str
    http_timeout_seconds: float = DEFAULT_HTTP_TIMEOUT_SECONDS
    rss_http_timeout_seconds: float = DEFAULT_RSS_HTTP_TIMEOUT_SECONDS
    gemini_http_timeout_seconds: float = DEFAULT_GEMINI_HTTP_TIMEOUT_SECONDS
    telegram_http_timeout_seconds: float = DEFAULT_TELEGRAM_HTTP_TIMEOUT_SECONDS
    channel_lookup_http_timeout_seconds: float = DEFAULT_CHANNEL_LOOKUP_HTTP_TIMEOUT_SECONDS
    telegram_message_limit: int = DEFAULT_TELEGRAM_MESSAGE_LIMIT
    gemini_min_request_interval_seconds: float = DEFAULT_GEMINI_MIN_REQUEST_INTERVAL_SECONDS
    gemini_cooldown_after_rate_limit_seconds: float = (
        DEFAULT_GEMINI_COOLDOWN_AFTER_RATE_LIMIT_SECONDS
    )
    gemini_cooldown_after_timeout_seconds: float = DEFAULT_GEMINI_COOLDOWN_AFTER_TIMEOUT_SECONDS
    gemini_cooldown_after_service_unavailable_seconds: float = (
        DEFAULT_GEMINI_COOLDOWN_AFTER_SERVICE_UNAVAILABLE_SECONDS
    )
    gemini_transport_inline_retry_delays_seconds: tuple[int, ...] = (
        DEFAULT_GEMINI_TRANSPORT_INLINE_RETRY_DELAYS_SECONDS
    )
    gemini_max_input_tokens_per_minute: int = DEFAULT_GEMINI_MAX_INPUT_TOKENS_PER_MINUTE
    max_transcript_video_age_days: int = DEFAULT_MAX_TRANSCRIPT_VIDEO_AGE_DAYS
    max_transcript_candidates_per_channel_per_poll: int = (
        DEFAULT_MAX_TRANSCRIPT_CANDIDATES_PER_CHANNEL_PER_POLL
    )
    min_video_duration_seconds: int = DEFAULT_MIN_VIDEO_DURATION_SECONDS
    telegram_message_interval_seconds: float = DEFAULT_TELEGRAM_MESSAGE_INTERVAL_SECONDS
    transcript_retry_delays_minutes: tuple[int, ...] = DEFAULT_TRANSCRIPT_RETRY_DELAYS_MINUTES
    summary_retry_delays_minutes: tuple[int, ...] = DEFAULT_SUMMARY_RETRY_DELAYS_MINUTES
    transcript_global_block_cooldown_minutes: int = DEFAULT_TRANSCRIPT_GLOBAL_BLOCK_COOLDOWN_MINUTES

    @classmethod
    def from_env(cls) -> AppConfig:
        load_dotenv(dotenv_path=Path.cwd() / ".env", override=True)
        channel_ids = _parse_csv_env("YOUTUBE_CHANNELS_IDS")
        telegram_chat_id = (
            os.getenv("TELEGRAM_CHAT_ID", "").strip()
            or os.getenv("TELEGRAM_CHANNEL_ID", "").strip()
        )

        return cls(
            youtube_channel_ids=channel_ids,
            gemini_api_key=os.getenv("GEMINI_API_KEY", "").strip(),
            gemini_model=os.getenv("GEMINI_MODEL", DEFAULT_GEMINI_MODEL).strip()
            or DEFAULT_GEMINI_MODEL,
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            telegram_chat_id=telegram_chat_id,
            database_path=Path(os.getenv("DATABASE_PATH", DEFAULT_DATABASE_PATH)).expanduser(),
            summary_language_mode=os.getenv(
                "SUMMARY_LANGUAGE_MODE",
                DEFAULT_SUMMARY_LANGUAGE_MODE,
            ).strip()
            or DEFAULT_SUMMARY_LANGUAGE_MODE,
            min_notification_score=_parse_int_env(
                "MIN_NOTIFICATION_SCORE",
                DEFAULT_MIN_NOTIFICATION_SCORE,
            ),
            first_run_mode=os.getenv("FIRST_RUN_MODE", DEFAULT_FIRST_RUN_MODE).strip()
            or DEFAULT_FIRST_RUN_MODE,
            http_timeout_seconds=_parse_float_env(
                "HTTP_TIMEOUT_SECONDS",
                DEFAULT_HTTP_TIMEOUT_SECONDS,
            ),
            rss_http_timeout_seconds=_parse_float_env(
                "RSS_HTTP_TIMEOUT_SECONDS",
                DEFAULT_RSS_HTTP_TIMEOUT_SECONDS,
            ),
            gemini_http_timeout_seconds=_parse_float_env(
                "GEMINI_HTTP_TIMEOUT_SECONDS",
                DEFAULT_GEMINI_HTTP_TIMEOUT_SECONDS,
            ),
            telegram_http_timeout_seconds=_parse_float_env(
                "TELEGRAM_HTTP_TIMEOUT_SECONDS",
                DEFAULT_TELEGRAM_HTTP_TIMEOUT_SECONDS,
            ),
            channel_lookup_http_timeout_seconds=_parse_float_env(
                "CHANNEL_LOOKUP_HTTP_TIMEOUT_SECONDS",
                DEFAULT_CHANNEL_LOOKUP_HTTP_TIMEOUT_SECONDS,
            ),
            telegram_message_limit=_parse_int_env(
                "TELEGRAM_MESSAGE_LIMIT",
                DEFAULT_TELEGRAM_MESSAGE_LIMIT,
            ),
            gemini_min_request_interval_seconds=_parse_float_env(
                "GEMINI_MIN_REQUEST_INTERVAL_SECONDS",
                DEFAULT_GEMINI_MIN_REQUEST_INTERVAL_SECONDS,
            ),
            gemini_cooldown_after_rate_limit_seconds=_parse_float_env(
                "GEMINI_COOLDOWN_AFTER_RATE_LIMIT_SECONDS",
                DEFAULT_GEMINI_COOLDOWN_AFTER_RATE_LIMIT_SECONDS,
            ),
            gemini_cooldown_after_timeout_seconds=_parse_float_env(
                "GEMINI_COOLDOWN_AFTER_TIMEOUT_SECONDS",
                DEFAULT_GEMINI_COOLDOWN_AFTER_TIMEOUT_SECONDS,
            ),
            gemini_cooldown_after_service_unavailable_seconds=_parse_float_env(
                "GEMINI_COOLDOWN_AFTER_SERVICE_UNAVAILABLE_SECONDS",
                DEFAULT_GEMINI_COOLDOWN_AFTER_SERVICE_UNAVAILABLE_SECONDS,
            ),
            gemini_transport_inline_retry_delays_seconds=_parse_int_csv_env(
                "GEMINI_TRANSPORT_INLINE_RETRY_DELAYS_SECONDS",
                DEFAULT_GEMINI_TRANSPORT_INLINE_RETRY_DELAYS_SECONDS,
            ),
            gemini_max_input_tokens_per_minute=_parse_int_env(
                "GEMINI_MAX_INPUT_TOKENS_PER_MINUTE",
                DEFAULT_GEMINI_MAX_INPUT_TOKENS_PER_MINUTE,
            ),
            max_transcript_video_age_days=_parse_int_env(
                "MAX_TRANSCRIPT_VIDEO_AGE_DAYS",
                DEFAULT_MAX_TRANSCRIPT_VIDEO_AGE_DAYS,
            ),
            max_transcript_candidates_per_channel_per_poll=_parse_int_env(
                "MAX_TRANSCRIPT_CANDIDATES_PER_CHANNEL_PER_POLL",
                DEFAULT_MAX_TRANSCRIPT_CANDIDATES_PER_CHANNEL_PER_POLL,
            ),
            min_video_duration_seconds=_parse_int_env(
                "MIN_VIDEO_DURATION_SECONDS",
                DEFAULT_MIN_VIDEO_DURATION_SECONDS,
            ),
            telegram_message_interval_seconds=_parse_float_env(
                "TELEGRAM_MESSAGE_INTERVAL_SECONDS",
                DEFAULT_TELEGRAM_MESSAGE_INTERVAL_SECONDS,
            ),
            transcript_retry_delays_minutes=_parse_int_csv_env(
                "TRANSCRIPT_RETRY_DELAYS_MINUTES",
                DEFAULT_TRANSCRIPT_RETRY_DELAYS_MINUTES,
            ),
            summary_retry_delays_minutes=_parse_int_csv_env(
                "SUMMARY_RETRY_DELAYS_MINUTES",
                DEFAULT_SUMMARY_RETRY_DELAYS_MINUTES,
            ),
            transcript_global_block_cooldown_minutes=_parse_int_env(
                "TRANSCRIPT_GLOBAL_BLOCK_COOLDOWN_MINUTES",
                DEFAULT_TRANSCRIPT_GLOBAL_BLOCK_COOLDOWN_MINUTES,
            ),
        )

    def ensure_data_directory(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)

    def validate_for_poll(self) -> None:
        missing = self.missing_required_for_runtime()
        if missing:
            joined = ", ".join(missing)
            raise ConfigurationError(f"Missing required configuration: {joined}")
        if not self.youtube_channel_ids:
            raise ConfigurationError("YOUTUBE_CHANNELS_IDS must contain at least one channel ID.")
        self._validate_common()

    def validate_for_doctor(self) -> None:
        self.validate_for_poll()

    def validate_for_init_db(self) -> None:
        self._validate_common()

    def missing_required_for_runtime(self) -> list[str]:
        missing: list[str] = []
        if not self.gemini_api_key:
            missing.append("GEMINI_API_KEY")
        if not self.telegram_bot_token:
            missing.append("TELEGRAM_BOT_TOKEN")
        if not self.telegram_chat_id:
            missing.append("TELEGRAM_CHAT_ID")
        return missing

    def render_summary(self) -> str:
        lines = [
            "Effective configuration:",
            f"- YOUTUBE_CHANNELS_IDS={','.join(self.youtube_channel_ids)}",
            f"- GEMINI_API_KEY={_secret_status(self.gemini_api_key)}",
            f"- GEMINI_MODEL={self.gemini_model}",
            f"- TELEGRAM_BOT_TOKEN={_secret_status(self.telegram_bot_token)}",
            f"- TELEGRAM_CHAT_ID={self.telegram_chat_id}",
            f"- DATABASE_PATH={self.database_path.as_posix()}",
            f"- SUMMARY_LANGUAGE_MODE={self.summary_language_mode}",
            f"- MIN_NOTIFICATION_SCORE={self.min_notification_score}",
            f"- FIRST_RUN_MODE={self.first_run_mode}",
            f"- HTTP_TIMEOUT_SECONDS={self.http_timeout_seconds}",
            f"- RSS_HTTP_TIMEOUT_SECONDS={self.rss_http_timeout_seconds}",
            f"- GEMINI_HTTP_TIMEOUT_SECONDS={self.gemini_http_timeout_seconds}",
            f"- TELEGRAM_HTTP_TIMEOUT_SECONDS={self.telegram_http_timeout_seconds}",
            f"- CHANNEL_LOOKUP_HTTP_TIMEOUT_SECONDS={self.channel_lookup_http_timeout_seconds}",
            f"- TELEGRAM_MESSAGE_LIMIT={self.telegram_message_limit}",
            (f"- GEMINI_MIN_REQUEST_INTERVAL_SECONDS={self.gemini_min_request_interval_seconds}"),
            (
                "- GEMINI_COOLDOWN_AFTER_RATE_LIMIT_SECONDS="
                f"{self.gemini_cooldown_after_rate_limit_seconds}"
            ),
            (
                "- GEMINI_COOLDOWN_AFTER_TIMEOUT_SECONDS="
                f"{self.gemini_cooldown_after_timeout_seconds}"
            ),
            (
                "- GEMINI_COOLDOWN_AFTER_SERVICE_UNAVAILABLE_SECONDS="
                f"{self.gemini_cooldown_after_service_unavailable_seconds}"
            ),
            (
                "- GEMINI_TRANSPORT_INLINE_RETRY_DELAYS_SECONDS="
                f"{self._gemini_transport_inline_retry_delays_summary()}"
            ),
            (f"- GEMINI_MAX_INPUT_TOKENS_PER_MINUTE={self.gemini_max_input_tokens_per_minute}"),
            f"- MAX_TRANSCRIPT_VIDEO_AGE_DAYS={self.max_transcript_video_age_days}",
            (
                "- MAX_TRANSCRIPT_CANDIDATES_PER_CHANNEL_PER_POLL="
                f"{self.max_transcript_candidates_per_channel_per_poll}"
            ),
            f"- MIN_VIDEO_DURATION_SECONDS={self.min_video_duration_seconds}",
            (f"- TELEGRAM_MESSAGE_INTERVAL_SECONDS={self.telegram_message_interval_seconds}"),
            (
                "- TRANSCRIPT_RETRY_DELAYS_MINUTES="
                f"{','.join(str(value) for value in self.transcript_retry_delays_minutes)}"
            ),
            (
                "- SUMMARY_RETRY_DELAYS_MINUTES="
                f"{','.join(str(value) for value in self.summary_retry_delays_minutes)}"
            ),
            (
                "- TRANSCRIPT_GLOBAL_BLOCK_COOLDOWN_MINUTES="
                f"{self.transcript_global_block_cooldown_minutes}"
            ),
        ]
        return "\n".join(lines)

    def _gemini_transport_inline_retry_delays_summary(self) -> str:
        return ",".join(str(value) for value in self.gemini_transport_inline_retry_delays_seconds)

    def _validate_common(self) -> None:
        if self.summary_language_mode != "transcript":
            raise ConfigurationError("SUMMARY_LANGUAGE_MODE must be 'transcript'.")
        if self.first_run_mode not in {"mark_seen", "process_all"}:
            raise ConfigurationError("FIRST_RUN_MODE must be 'mark_seen' or 'process_all'.")
        if not 0 <= self.min_notification_score <= 100:
            raise ConfigurationError("MIN_NOTIFICATION_SCORE must be between 0 and 100.")
        if self.http_timeout_seconds <= 0:
            raise ConfigurationError("HTTP_TIMEOUT_SECONDS must be > 0.")
        if self.rss_http_timeout_seconds <= 0:
            raise ConfigurationError("RSS_HTTP_TIMEOUT_SECONDS must be > 0.")
        if self.gemini_http_timeout_seconds <= 0:
            raise ConfigurationError("GEMINI_HTTP_TIMEOUT_SECONDS must be > 0.")
        if self.telegram_http_timeout_seconds <= 0:
            raise ConfigurationError("TELEGRAM_HTTP_TIMEOUT_SECONDS must be > 0.")
        if self.channel_lookup_http_timeout_seconds <= 0:
            raise ConfigurationError("CHANNEL_LOOKUP_HTTP_TIMEOUT_SECONDS must be > 0.")
        if self.telegram_message_limit <= 0:
            raise ConfigurationError("TELEGRAM_MESSAGE_LIMIT must be > 0.")
        if self.gemini_min_request_interval_seconds < 0:
            raise ConfigurationError("GEMINI_MIN_REQUEST_INTERVAL_SECONDS must be >= 0.")
        if self.gemini_cooldown_after_rate_limit_seconds < 0:
            raise ConfigurationError("GEMINI_COOLDOWN_AFTER_RATE_LIMIT_SECONDS must be >= 0.")
        if self.gemini_cooldown_after_timeout_seconds < 0:
            raise ConfigurationError("GEMINI_COOLDOWN_AFTER_TIMEOUT_SECONDS must be >= 0.")
        if self.gemini_cooldown_after_service_unavailable_seconds < 0:
            raise ConfigurationError(
                "GEMINI_COOLDOWN_AFTER_SERVICE_UNAVAILABLE_SECONDS must be >= 0."
            )
        if any(value < 0 for value in self.gemini_transport_inline_retry_delays_seconds):
            raise ConfigurationError(
                "GEMINI_TRANSPORT_INLINE_RETRY_DELAYS_SECONDS must contain non-negative integers."
            )
        if self.gemini_max_input_tokens_per_minute <= 0:
            raise ConfigurationError("GEMINI_MAX_INPUT_TOKENS_PER_MINUTE must be > 0.")
        if self.max_transcript_video_age_days < 0:
            raise ConfigurationError("MAX_TRANSCRIPT_VIDEO_AGE_DAYS must be >= 0.")
        if self.max_transcript_candidates_per_channel_per_poll <= 0:
            raise ConfigurationError("MAX_TRANSCRIPT_CANDIDATES_PER_CHANNEL_PER_POLL must be > 0.")
        if self.min_video_duration_seconds < 0:
            raise ConfigurationError("MIN_VIDEO_DURATION_SECONDS must be >= 0.")
        if self.telegram_message_interval_seconds < 0:
            raise ConfigurationError("TELEGRAM_MESSAGE_INTERVAL_SECONDS must be >= 0.")
        if not self.transcript_retry_delays_minutes:
            raise ConfigurationError("TRANSCRIPT_RETRY_DELAYS_MINUTES must not be empty.")
        if any(value <= 0 for value in self.transcript_retry_delays_minutes):
            raise ConfigurationError(
                "TRANSCRIPT_RETRY_DELAYS_MINUTES must contain positive integers."
            )
        if not self.summary_retry_delays_minutes:
            raise ConfigurationError("SUMMARY_RETRY_DELAYS_MINUTES must not be empty.")
        if any(value <= 0 for value in self.summary_retry_delays_minutes):
            raise ConfigurationError("SUMMARY_RETRY_DELAYS_MINUTES must contain positive integers.")
        if self.transcript_global_block_cooldown_minutes <= 0:
            raise ConfigurationError("TRANSCRIPT_GLOBAL_BLOCK_COOLDOWN_MINUTES must be > 0.")


def _secret_status(value: str) -> str:
    return "configured" if value else "missing"


def _parse_csv_env(name: str) -> tuple[str, ...]:
    raw = os.getenv(name, "")
    parts = [item.strip() for item in raw.split(",")]
    return tuple(item for item in parts if item)


def _parse_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return int(raw)


def _parse_float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return float(raw)


def _parse_int_csv_env(name: str, default: tuple[int, ...]) -> tuple[int, ...]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    values = [item.strip() for item in raw.split(",")]
    return tuple(int(value) for value in values if value)
