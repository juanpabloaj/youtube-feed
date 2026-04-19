from __future__ import annotations

import logging

from youtube_feed.gemini_rate_limiter import GeminiRateLimiter


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def test_reserve_slot_waits_for_minimum_request_interval() -> None:
    clock = FakeClock()
    limiter = GeminiRateLimiter(
        min_request_interval_seconds=3.0,
        max_input_tokens_per_minute=10_000,
        logger=logging.getLogger("test"),
        monotonic_func=clock.monotonic,
        sleep_func=clock.sleep,
    )

    limiter.reserve_slot(estimated_tokens=500)
    limiter.mark_request_completed()
    limiter.reserve_slot(estimated_tokens=500)

    assert clock.sleeps == [3.0]


def test_reserve_slot_waits_for_tpm_budget_window_to_open() -> None:
    clock = FakeClock()
    limiter = GeminiRateLimiter(
        min_request_interval_seconds=0.0,
        max_input_tokens_per_minute=2_000,
        logger=logging.getLogger("test"),
        monotonic_func=clock.monotonic,
        sleep_func=clock.sleep,
    )

    limiter.reserve_slot(estimated_tokens=1_500)
    limiter.reserve_slot(estimated_tokens=1_500)

    assert clock.sleeps == [60.0]


def test_cooldown_uses_configured_sleep_function() -> None:
    clock = FakeClock()
    limiter = GeminiRateLimiter(
        min_request_interval_seconds=0.0,
        max_input_tokens_per_minute=2_000,
        logger=logging.getLogger("test"),
        monotonic_func=clock.monotonic,
        sleep_func=clock.sleep,
    )

    limiter.cooldown(seconds=12.5, reason="HTTP 429 with Retry-After")

    assert clock.sleeps == [12.5]
