"""Provider-neutral normalization for stock underlying context."""
from typing import Mapping


REQUIRED_KEYS=("symbol","price")


class UnderlyingProvider:
    def __init__(self, fetcher):
        if not callable(fetcher):
            raise TypeError("underlying fetcher must be callable")
        self.fetcher=fetcher

    def quote(self, symbol) -> Mapping:
        raw=self.fetcher(str(symbol).upper())
        if not isinstance(raw, Mapping):
            raise TypeError("underlying provider must return a mapping")
        data={str(k):v for k,v in raw.items()}
        data.setdefault("symbol",str(symbol).upper())
        return data

    @staticmethod
    def normalize(symbol, raw):
        if not isinstance(raw, Mapping):
            return {"ready":False,"reason":"invalid_provider_payload"}
        data=dict(raw)
        price=data.get("price",data.get("last_price"))
        if price is None:
            return {"ready":False,"reason":"underlying_price_missing"}
        out={"ready":True,"symbol":str(symbol).upper(),"price":float(price)}
        for key in ("session_open","previous_close","open","high","low","volume"):
            if key in data and data[key] is not None:
                try: out[key]=float(data[key])
                except (TypeError,ValueError): pass
        if "session_open" not in out and "open" in out:
            out["session_open"]=out["open"]
        if "previous_close" not in out and "prev_close" in data:
            try: out["previous_close"]=float(data["prev_close"])
            except (TypeError,ValueError): pass
        return out
