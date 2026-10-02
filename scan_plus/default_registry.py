"""Default wiring for the independent Scan+ market adapters.

No provider credentials are embedded. Commodity data requires an injected fetcher;
all other adapters use their existing public/default providers.
"""
from scan_plus.market_registry import MarketRegistry
from scan_plus.markets.crypto.adapter import CryptoMarketAdapter
from scan_plus.markets.stocks.adapter import StocksMarketAdapter
from scan_plus.markets.forex.adapter import ForexMarketAdapter
from scan_plus.markets.commodities.adapter import CommoditiesMarketAdapter
from scan_plus.markets.commodities.yahoo import YahooCommodityLoader



def build_default_registry(*, commodity_fetcher=None, crypto_manager=None,
                           stocks_loader=None, forex_loader=None):
    registry=MarketRegistry()
    registry.register(CryptoMarketAdapter(manager=crypto_manager))
    registry.register(StocksMarketAdapter(loader=stocks_loader))
    registry.register(ForexMarketAdapter(loader=forex_loader))
    registry.register(CommoditiesMarketAdapter(commodity_fetcher or YahooCommodityLoader()))
    return registry
