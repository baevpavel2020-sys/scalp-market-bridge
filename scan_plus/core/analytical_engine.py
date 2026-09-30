"""Shared analytical-core boundary.

The core owns *what* evidence is requested, not *where* market data comes from.
The legacy Crypto implementation remains the reference backend during extraction.
New market adapters can replace the backend without changing orchestration.
"""
from typing import Protocol


SHARED_BLOCKS = frozenset({"structure_mtf", "structure", "fibonacci", "elliott", "harmonics"})


class AnalyticalBackend(Protocol):
    def structure(self, rows): ...
    def fibonacci(self, rows, structure): ...
    def elliott(self, rows, structure): ...
    def harmonics(self, structure): ...


class CanonicalPricePatternBackend:
    """Market-neutral compatibility backend for canonical price-pattern calculations.

    The implementation source is still the legacy reference during extraction, but
    the analytical contract itself contains no Crypto-specific market assumptions.
    """
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


class LegacyCryptoBackend(CanonicalPricePatternBackend):
    """Backward-compatible name for tests/integration during migration."""


class AnalyticalEngine:
    def __init__(self, backend: AnalyticalBackend):
        self.backend=backend

    def analyze(self, rows, *, requested=None):
        if requested is None:
            requested={"structure","fibonacci","elliott","harmonics"}
        else:
            requested=set(requested)
        supported={"structure","fibonacci","elliott","harmonics"}
        requested &= supported
        result={}
        # Structure is the dependency root for the reusable price-pattern blocks.
        structure=self.backend.structure(rows)
        result["structure"]=structure
        if "fibonacci" in requested:
            result["fibonacci"]=self.backend.fibonacci(rows,structure)
        if "elliott" in requested:
            result["elliott"]=self.backend.elliott(rows,structure)
        if "harmonics" in requested:
            result["harmonics"]=self.backend.harmonics(structure)
        return result

    @staticmethod
    def split_priorities(priorities):
        ordered=list(priorities or [])
        aliases={
            "structure_mtf":"structure",
            "structure":"structure",
            "fibonacci":"fibonacci",
            "elliott":"elliott",
            "harmonics":"harmonics",
        }
        shared=[]
        context=[]
        for name in ordered:
            if name in aliases:
                if aliases[name] not in shared:
                    shared.append(aliases[name])
            else:
                context.append(name)
        return shared, context

    def analyze_profiled(self, rows, priorities):
        """Run shared blocks only and explicitly return market-context requests."""
        shared, context=self.split_priorities(priorities)
        result=self.analyze(rows,requested=shared)
        result["_context_blocks_requested"]=context
        return result

    @staticmethod
    def compare(reference, candidate):
        """Structural differential comparison; returns exact and top-level diffs."""
        if reference == candidate:
            return {"equal":True,"differences":[]}
        differences=[]
        if isinstance(reference,dict) and isinstance(candidate,dict):
            keys=sorted(set(reference)|set(candidate))
            for key in keys:
                if reference.get(key)!=candidate.get(key):
                    differences.append(key)
        else:
            differences.append("$")
        return {"equal":False,"differences":differences}
