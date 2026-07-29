class TrishulaError(Exception):
    """Base class for user-actionable application errors."""


class DatasetValidationError(TrishulaError, ValueError):
    def __init__(self, message: str, hint: str = ""):
        super().__init__(message)
        self.message = message
        self.hint = hint

    def __str__(self) -> str:
        return f"{self.message}{f' Hint: {self.hint}' if self.hint else ''}"
