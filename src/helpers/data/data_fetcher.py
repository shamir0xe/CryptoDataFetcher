from typing import Dict, List
from libs.PythonLibrary.Helpers.json_helper import JsonHelper
from src.models.candle import Candle
import src.helpers.data.coinex.market_data_fetcher as coinex


class DataFetcher:
    @staticmethod
    def fetch_document(obj: Dict) -> List[Dict]:
        res = []
        if obj["provider"] == "coinex":
            data = coinex.MarketDataFetcher.fetch(
                market = obj["market"],
                interval = obj["interval"],
                past_candles = obj["count"]
            )
            base = DataFetcher.build_basement(obj)
            for candle_data in data:
                res.append(JsonHelper.merge(base, {
                    "time": candle_data[0],
                    "candle": candle_data,
                }))
        return res

    @staticmethod
    def build_basement(obj: Dict) -> Dict:
        base = {}
        base = JsonHelper.selector_set_value(base, "market", JsonHelper.selector_get_value(obj, "market"))
        base = JsonHelper.selector_set_value(base, "interval", JsonHelper.selector_get_value(obj, "interval"))
        base = JsonHelper.selector_set_value(base, "provider", JsonHelper.selector_get_value(obj, "provider"))
        return base
