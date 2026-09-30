"""Shared analytical-core boundary.

The core owns *what* evidence is requested, not *where* market data comes from.
The legacy Crypto implementation remains the reference backend during extraction.
New market adapters can replace the backend without changing orchestration.
"""
from typing import Any, Mapping, Protocol


class AnalyticalBackend(Protocol):
    def structure(self, rows): ...
    def fibonacci(self, rows, structure): ...
    def elliott(self, rows, structure): ...
    def harmonics(self, structure): ...


class LegacyCryptoBackend:
    """Compatibility backend wrapping the existing canonical Scan+ calculations."""
    def __init__(self):
        from dynamic_collector import MarketStream
        self._stream_cls=MarketStream
        self._instance=object.__new__(MarketStream)

    def structure(self, rows):
        return self._stream_cls._structure_metrics(self._instance, rows)

    def fibonacci(self, rows, structure):
        return self._stream_cls._fib_metrics(self._instance, rows, structure)

    def elliott(self, rows, structure):
        return self._stream_cls._elliott_metrics(self._instance, rows, structure)

    def harmonics(self, structure):
        return self._stream_cls._harmonic_metrics(self._instance, structure)


class AnalyticalEngine:
    def __init__(self, backend: AnalyticalBackend):
        self.backend=backend

    def analyze(self, rows, *, requested=None):
        requested=set(requested or ("structure","fibonacci","elliott","harmonics"))
        result={}
        structure=self.backend.structure(rows)
        result["structure"]=structure
        if "fibonacci" in requested:
            result["fibonacci"]=self.backend.fibonacci(rows,structure)
        if "elliott" in requested:
            result["elliott"]=self.backend.elliott(rows,structure)
        if "harmonics" in requested:
            result["harmonics"]=self.backend.harmonics(structure)
        return result
