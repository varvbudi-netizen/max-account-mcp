"""Безопасные ошибки на границе инструментов, без сырых ответов и секретов."""


# Компоненты возвращают пользователю только заранее сформулированное объяснение.
class BridgeError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        retryable: bool = False,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds

    def to_dict(self) -> dict:
        result = {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
        }
        if self.retry_after_seconds is not None:
            result["retry_after_seconds"] = self.retry_after_seconds
        return result
