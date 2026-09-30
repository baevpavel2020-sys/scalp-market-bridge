"""Provider-isolated FX loader.

The provider URL is injectable so Scan+ does not hard-code an analytical assumption
about the FX feed. Default uses Stooq's public CSV endpoint for read-only candles.
"""
import csv, io, requests

class StooqFXLoader:
    def __init__(self, base_url="https://stooq.com/q/d/l/", timeout=8, session=None):
        self.base_url=base_url
        self.timeout=float(timeout)
        self.session=session or requests.Session()

    SUPPORTED_INTERVALS={"d","w","m"}

    def candles(self,symbol="EURUSD",interval="d"):
        interval=str(interval).lower()
        if interval not in self.SUPPORTED_INTERVALS:
            raise NotImplementedError(
                f"StooqFXLoader does not provide validated intraday interval: {interval}"
            )
        pair=str(symbol).lower().replace("/","")
        response=self.session.get(self.base_url,params={"s":pair,"i":interval},timeout=self.timeout)
        response.raise_for_status()
        text=response.text
        if text.lstrip().lower().startswith("no data"):
            raise LookupError(f"FX data unavailable: {symbol}")
        rows=[]
        for row in csv.DictReader(io.StringIO(text)):
            rows.append({"date":row.get("Date"),"open":float(row["Open"]),"high":float(row["High"]),
                         "low":float(row["Low"]),"close":float(row["Close"]),
                         "volume":float(row.get("Volume") or 0)})
        return {"source":"stooq","product":"fx","symbol":str(symbol).upper(),"interval":interval,"candles":rows}
