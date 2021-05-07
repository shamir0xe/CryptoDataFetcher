from __future__ import annotations
from libs.PythonLibrary.Helpers.json_helper import JsonHelper
from typing import Dict
from src.helpers.config.config_reader import ConfigReader
import math
from src.adapters.interval_adapter import IntervalAdapter
from src.finders.candle_finder import CandleFinder
from src.helpers.data.data_fetcher import DataFetcher
from src.helpers.data.database_helper import DatabaseHelper
from libs.PythonLibrary.utils import TerminalProcess, debug_text
from src.helpers.time.time_helper import TimeHelper


class App:
    def __init__(self) -> None:
        self.config = self.__read_config()

    def __read_config(self) -> Dict:
        return ConfigReader()

    def get_markets(self) -> App:
        debug_text('getting markets')
        self.markets = [obj['name'] for obj in DatabaseHelper.find_markets()]
        self.intervals = self.config.get("intervals")
        self.providers = self.config.get("providers")
        self.data = []
        return self
    
    def upsert_data(self) -> App:
        debug_text('updating data')
        tp = TerminalProcess(len(self.providers) * len(self.markets) * len(self.intervals))
        for provider in self.providers:
            provider = provider.lower()
            for market in self.markets:
                market = market.lower()
                for interval in self.intervals:
                    tp.hit()
                    data = CandleFinder.find_last_candle({
                        "provider": provider,
                        "market": market,
                        "interval": interval
                    })
                    candle_count = 0
                    if data is None:
                        candle_count = self.config.get('max-fetch-candle')
                    else:
                        time_diff = TimeHelper.current_utc_timestamp() - JsonHelper.selector_get_value(data, "time")
                        if time_diff > IntervalAdapter.plug(interval):
                            candle_count = math.ceil(time_diff / IntervalAdapter.plug(interval))
                    if candle_count > 0:
                        DatabaseHelper.insert_many(DataFetcher.fetch_document({
                            "provider": provider,
                            "market": market,
                            "interval": interval,
                            "count": candle_count,
                        }))
        return self
    