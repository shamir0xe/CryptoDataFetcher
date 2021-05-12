from typing import List
from src.helpers.config.config_reader import ConfigReader
from src.facades.database import CryptoDB

class Migration:
    def __init__(self):
        self.config = ConfigReader('migration')
        self.db = CryptoDB.db()
    
    def do(self) -> None:
        methods = self.__get_methods()
        for method in methods:
            getattr(self, method)()

    def __get_methods(self) -> List[str]:
        methods = [func for func in dir(self) if callable(getattr(self, func))]
        string = 'migration'
        sz = len(string)
        return [method for method in methods if method[-sz:] == string]

    def make_compound_indices_migration(self):
        self.db.candles.create_index([('market', +1), ('interval', +1), ('time', -1)], unique=True)
