"""Provider-isolated commodity loader contract.

Commodity symbols and feeds differ materially; this adapter intentionally requires
an injected provider rather than pretending a generic FX/crypto endpoint is valid.
"""
from typing import Callable

class CommodityLoader:
    def __init__(self, fetcher: Callable):
        if not callable(fetcher):
            raise TypeError("commodity fetcher must be callable")
        self.fetcher=fetcher

    def candles(self,symbol,**kwargs):
        data=self.fetcher(str(symbol).upper(),**kwargs)
        if not isinstance(data,dict):
            raise TypeError("commodity provider must return a mapping")
        data.setdefault("symbol",str(symbol).upper())
        data.setdefault("product","commodity")
        return data
