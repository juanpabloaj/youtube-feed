from __future__ import annotations

import logging
import time


class GeminiRateLimiter:
    def __init__(
        self,
        *,
        min_request_interval_seconds: float,
        max_input_tokens_per_minute: int,
        logger: logging.Logger,
        monotonic_func=None,
        sleep_func=None,
    ) -> None:
        self._min_request_interval_seconds = min_request_interval_seconds
        self._max_input_tokens_per_minute = max_input_tokens_per_minute
        self._logger = logger
        self._monotonic = monotonic_func or time.monotonic
        self._sleep = sleep_func or time.sleep
        self._last_request_at: float | None = None
        self._token_events: list[tuple[float, int]] = []

    def reserve_slot(self, estimated_tokens: int) -> None:
        while True:
            now = self._monotonic()
            self._prune_token_events(now)

            interval_wait = 0.0
            if self._last_request_at is not None:
                elapsed = now - self._last_request_at
                interval_wait = max(0.0, self._min_request_interval_seconds - elapsed)

            token_wait = self._calculate_token_wait(now, estimated_tokens)
            wait_seconds = max(interval_wait, token_wait)
            if wait_seconds <= 0:
                reserved_tokens = min(estimated_tokens, self._max_input_tokens_per_minute)
                self._token_events.append((now, reserved_tokens))
                return

            if wait_seconds >= 5.0:
                self._logger.info(
                    (
                        "Sleeping %.2fs before the next Gemini request "
                        "to respect request pacing and input TPM budget."
                    ),
                    wait_seconds,
                )
            else:
                self._logger.debug(
                    (
                        "Sleeping %.2fs before the next Gemini request "
                        "to respect request pacing and input TPM budget."
                    ),
                    wait_seconds,
                )
            self._sleep(wait_seconds)

    def mark_request_completed(self) -> None:
        self._last_request_at = self._monotonic()

    def cooldown(self, *, seconds: float, reason: str) -> None:
        if seconds <= 0:
            return
        self._logger.warning(
            "Cooling down Gemini requests after %s. Sleeping %.2fs.",
            reason,
            seconds,
        )
        self._sleep(seconds)

    def _prune_token_events(self, now: float) -> None:
        self._token_events = [event for event in self._token_events if now - event[0] < 60.0]

    def _calculate_token_wait(self, now: float, estimated_tokens: int) -> float:
        reserved_tokens = min(estimated_tokens, self._max_input_tokens_per_minute)
        used = sum(token_count for _, token_count in self._token_events)
        if used + reserved_tokens <= self._max_input_tokens_per_minute:
            return 0.0

        for timestamp, token_count in self._token_events:
            used -= token_count
            if used + reserved_tokens <= self._max_input_tokens_per_minute:
                return max(0.0, 60.0 - (now - timestamp))
        return 60.0
