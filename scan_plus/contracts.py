"""Stable contracts between orchestration and market adapters."""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class ScanRequest:
    market: str
    symbol: Optional[str] = None


class MarketAdapter(ABC):
    """A market boundary. Adapter failures must remain isolated to this market."""

    market: str

    @abstractmethod
    def prescan(self, **kwargs) -> Mapping[str, Any]:
        raise NotImplementedError

    def default_symbols(self, limit: int = 30):
        """Return market-native symbols for an unqualified multi-scan, if supported."""
        return []

    @abstractmethod
    def scan(self, symbol: str) -> Mapping[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def diagnostics(self, symbol: str) -> Mapping[str, Any]:
        raise NotImplementedError
