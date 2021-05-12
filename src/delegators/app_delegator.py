from __future__ import annotations

from pymongo import DESCENDING
from src.facades.kucoin import Kucoin
from src.helpers.config.environment_reader import EnvironmentReader
from src.helpers.data.kucoin.market_list_fetcher import MarketListFetcher
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
import json


class App:
    def __init__(self) -> None:
        self.config = self.__read_config()
        self.env = EnvironmentReader()
        self.market_client = Kucoin({
            'key': self.env.get('key'),
            'secret': self.env.get('secret'),
            'passphrase': self.env.get('passphrase')
        }).market_client()

    def __read_config(self) -> Dict:
        return ConfigReader()

    def fetch_markets(self) -> App:
        markets = MarketListFetcher.do(market_client=self.market_client)
        print(json.dumps(markets))
        return self

    def get_markets(self) -> App:
        debug_text('getting markets')
        self.markets = [obj['name'] for obj in DatabaseHelper.find_markets()]
        # self.markets = self.markets[:2]
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
                        cur_time = int(TimeHelper.current_utc_timestamp())
                        cur_time -= cur_time % IntervalAdapter.plug(interval=interval)
                        time_diff = cur_time - int(JsonHelper.selector_get_value(data, "time"))
                        if time_diff > IntervalAdapter.plug(interval):
                            candle_count = round(time_diff / IntervalAdapter.plug(interval))
                    if candle_count > 1:
                        # debug_text('current corrected time: %', cur_time)
                        # debug_text('last candle time: %', JsonHelper.selector_get_value(data, 'time'))
                        # debug_text('candle count : %', candle_count)
                        try:
                            count = DatabaseHelper.insert_many(DataFetcher.fetch_document({
                                "provider": provider,
                                "market": market,
                                "interval": interval,
                                "count": candle_count,
                            }, self.market_client))
                        except:
                            debug_text('error occured while fetching this pair: %', market)
                            
        return self
    