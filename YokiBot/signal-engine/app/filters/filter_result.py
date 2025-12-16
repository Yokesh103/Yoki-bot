from dataclasses import dataclass


@dataclass
class FilterResult:
    allowed: bool
    reason: str | None = None

    @staticmethod
    def ok():
        return FilterResult(allowed=True)

    @staticmethod
    def fail(reason: str):
        return FilterResult(allowed=False, reason=reason)
