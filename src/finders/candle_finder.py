from typing import Dict, Optional
from src.helpers.data.database_helper import DatabaseHelper


class CandleFinder:
    @staticmethod
    def find_last_candle(obj: Dict) -> Optional[Dict]:
        return DatabaseHelper.find_one(obj, {
            "time": -1
        })

