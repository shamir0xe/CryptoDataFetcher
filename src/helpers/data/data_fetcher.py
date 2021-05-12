from libs.PythonLibrary.utils import debug_text
from typing import Dict, List, Optional
from src.facades.kucoin import MarketClient
from src.helpers.data.database_helper import DatabaseHelper
from src.adapters.interval_adapter import IntervalAdapter
from src.helpers.time.time_helper import TimeHelper
from libs.PythonLibrary.Helpers.json_helper import JsonHelper
from src.models.candle import Candle
import src.helpers.data.coinex.market_data_fetcher as coinex
import src.helpers.data.kucoin.market_data_fetcher as kucoin


class DataFetcher:
    @staticmethod
    def fetch_document(obj: Dict, market_client: Optional[MarketClient] = None) -> List[Dict]:
        data = res = []
        if obj["provider"] == "coinex":
            data = coinex.MarketDataFetcher.fetch(
                market = obj["market"],
                interval = obj["interval"],
                past_candles = obj["count"]
            )
        if obj["provider"] == "kucoin":
            data = kucoin.MarketDataFetcher.fetch(
                market_client=market_client,
                market=obj['market'],
                interval=obj['interval'],
                past_candles=obj['count'],
            )
        base = DataFetcher.build_basement(obj)
        interval_seconds = IntervalAdapter.plug(obj["interval"])
        for candle_data in data:
            # debug_text('candle data: %', candle_data)
            time_diff = TimeHelper.current_utc_timestamp() - int(candle_data[0])
            if time_diff >= interval_seconds:
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
