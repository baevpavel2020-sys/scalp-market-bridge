"""Shared analytical-core boundary.

The core owns *what* evidence is requested, not *where* market data comes from.
The legacy Crypto implementation remains the reference backend during extraction.
New market adapters can replace the backend without changing orchestration.
"""
from typing import Protocol


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

    def analyze_profiled(self, rows, priorities):
        """Run only the requested blocks, preserving priority order."""
        ordered=list(priorities or [])
        aliases={
            "structure_mtf":"structure",
            "fibonacci":"fibonacci",
            "elliott":"elliott",
            "harmonics":"harmonics",
        }
        requested=[aliases[name] for name in ordered if name in aliases]
        return self.analyze(rows,requested=requested)

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
