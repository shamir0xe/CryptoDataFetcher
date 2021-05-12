from typing import Any, Dict, List, Optional
from libs.PythonLibrary.mongo_db import Mongo
from src.facades.database import CryptoDB


class DatabaseHelper:
    @staticmethod
    def find_one(obj: Dict, sort_obj: Optional[Dict] = None) -> Optional[Dict]:
        candles = CryptoDB.db().candles
        cursor = candles.find(obj)
        if sort_obj is not None:
            key = [*sort_obj][0]
            cursor.sort(key, sort_obj[key])
        for obj in cursor:
            return obj
        return None
    
    @staticmethod
    def insert_many(data: List[Dict]) -> List[int]:
        if len(data) > 0:
            candles = CryptoDB.db().candles
            return candles.insert_many(data).inserted_ids
        return []

    @staticmethod
    def find_markets() -> Any:
        return CryptoDB.db().markets.find({})
