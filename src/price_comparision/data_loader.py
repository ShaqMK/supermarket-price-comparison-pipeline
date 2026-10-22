from abc import ABC, abstractmethod
import pandas as pd
from sqlalchemy import create_engine

# DATABASE CONFIGURATION
class DatabaseConfig:
    def __init__(self, host: str, port: int, db_name: str, user: str, password: str):
        self.host = host
        self.port = port
        self.db_name = db_name
        self._user = user
        self.__password = password

    def get_connection_url(self):
        return f'postgresql+psycopg://{self._user}:{self.__password}@{self.host}:{self.port}/{self.db_name}'



# PIPELINE 
class SupermarketPipeline(ABC):
    def __init__(self, filepath: str, source_name: str, db_config: DatabaseConfig):
        self.filepath = filepath
        self.source_name = source_name
        self.__db_config = db_config
        self.df = None

    @abstractmethod
    def extract(self):
        pass

    def transform(self):
        print(f'[{self.source_name}] Transforming data...')
        for col in ['retailer', 'category', 'product_name', 'pack_size', 'price_kes', 'date_collected']:
            self.df[col] = self.df[col].astype(str).str.strip()

        self.df['pipeline_source'] = self.source_name

    def load(self):
        print(f'[{self.source_name}] Connecting to Postgres and loading {len(self.df)} records...')

        try:
            connection_url = self.__db_config.get_connection_url()
            engine = create_engine(connection_url)

            self.df.to_sql(
                name = 'supermarket_prices',
                con = engine,
                if_exists = 'append',
                index = False
            )

            print(f'[{self.source_name}] Successfully committed records to PostgreSQL')

        except Exception as e:
            print('Database Error!')


# PIPELINES
# CSV Pipleine
class CSVPipeline(SupermarketPipeline):
    def extract(self):
        print(f'[{self.source_name}] Extracting CSV from {self.filepath}')
        self.df = pd.read_csv(self.filepath)


# JSON Pipeline
class JSON(SupermarketPipeline):
    def extract(self):
        print(f'[{self.source_name}] Extracting JSON from {self.filepath}')
        self.df = pd.read_json(self.filepath)