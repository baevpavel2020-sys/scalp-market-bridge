"""Underlying-equity provider contract for tokenized-stock analysis."""
from typing import Callable, Mapping

class UnderlyingProvider:
    def __init__(self, fetcher: Callable):
        if not callable(fetcher): raise TypeError("underlying fetcher must be callable")
        self.fetcher=fetcher

    def quote(self, symbol) -> Mapping:
        result=self.fetcher(str(symbol).upper())
        if not isinstance(result,Mapping): raise TypeError("underlying provider must return a mapping")
        return dict(result)
