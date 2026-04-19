class ConfigurationError(ValueError):
    """Raised when runtime configuration is invalid."""


class TranscriptError(Exception):
    """Base transcript exception."""


class TranscriptUnavailableError(TranscriptError):
    """Raised when a transcript is not available for a video."""


class TranscriptTemporaryError(TranscriptError):
    """Raised when a transcript fetch should be retried."""


class TranscriptBlockedError(TranscriptTemporaryError):
    """Raised when YouTube blocks transcript fetching for the current IP/session."""

    def __init__(self, message: str, *, kind: str) -> None:
        super().__init__(message)
        self.kind = kind


class GeminiError(Exception):
    """Base Gemini exception."""


class GeminiHTTPError(GeminiError):
    """Raised for recognized Gemini HTTP errors."""

    def __init__(
        self,
        status_code: int,
        message: str,
        *,
        retryable: bool,
        details: str | None = None,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable
        self.message = message
        self.details = details
        self.retry_after_seconds = retry_after_seconds

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} Details: {self.details}"
        return self.message


class GeminiTransportError(GeminiError):
    """Raised for retryable Gemini transport failures."""


class GeminiResponseError(GeminiError):
    """Raised when Gemini returns an unexpected payload."""


class TelegramError(Exception):
    """Base Telegram exception."""


class TelegramHTTPError(TelegramError):
    """Raised when Telegram rejects a request."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
