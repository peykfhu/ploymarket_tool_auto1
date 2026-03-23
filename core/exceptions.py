"""
core/exceptions.py
Custom exception hierarchy for the trading system.
"""


class PolymarketBaseError(Exception):
    """Base exception for all system errors."""


class ConfigurationError(PolymarketBaseError):
    """Invalid or missing configuration."""


class CollectorError(PolymarketBaseError):
    """Data ingestion collector error."""


class CollectorRateLimitError(CollectorError):
    """API rate limit hit."""


class CollectorAuthError(CollectorError):
    """Authentication failure."""


class SignalProcessingError(PolymarketBaseError):
    """Signal processing failure."""


class StrategyError(PolymarketBaseError):
    """Strategy engine error."""


class RiskRejectionError(PolymarketBaseError):
    """Order rejected by the risk layer."""
    def __init__(self, message: str, violations: list[str]) -> None:
        super().__init__(message)
        self.violations = violations


class ExecutionError(PolymarketBaseError):
    """Order execution failure."""


class InsufficientBalanceError(ExecutionError):
    """Not enough balance to place the order."""


class SlippageExceededError(ExecutionError):
    """Fill price exceeded slippage tolerance."""


class CircuitBreakerError(PolymarketBaseError):
    """Circuit breaker is active; trading halted."""


class DatabaseError(PolymarketBaseError):
    """Database operation failure."""


class NotificationError(PolymarketBaseError):
    """Notification delivery failure."""
