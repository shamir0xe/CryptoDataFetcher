from typing import Optional


class CoinexAdapter:
    def __init__(
        self, 
        market: Optional[str], 
        interval: Optional[int], 
        past_candles: Optional[int]
    ) -> None:
        self.name = 'coinex'
        self.market = market
        self.interval = interval
        self.past_candles = past_candles

    def convert(self):
        res = {}
        if self.market is not None:
            res['market'] = self.market.upper()
        if self.interval is not None:
            res['type'] = self.interval
        if self.past_candles is not None:
            res['limit'] = self.past_candles
        return res

