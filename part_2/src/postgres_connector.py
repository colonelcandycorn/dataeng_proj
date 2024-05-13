import pandas as pd
import psycopg as pg
import os
from sqlalchemy import create_engine, inspect, text
from urllib.parse import quote_plus


class PostgresConnector:
    def __init__(self):
        user = os.environ.get("USER")
        password = os.environ.get("PASS")
        host = os.environ.get("HOST")
        port = '5432'
        db = os.environ.get("DB")
        self.raw_table = os.environ.get("RAW_TABLE")
        self.breadcrumb_table = os.environ.get("BREADCRUMB_TABLE")
        self.trip_table = os.environ.get("TRIP_TABLE")
        self.engine = create_engine(f'postgresql://{user}:{quote_plus(password)}@{host}:{port}/{db}')
        self.connection = self.engine.connect()

    def append_to_raw(self, df):
        df.to_sql(self.raw_table, self.engine, if_exists='append', index=False)

    def set_is_in_final_table(self):
        # set every row in raw table to is_in_final_table = True
        query = f"UPDATE {self.raw_table} SET is_in_final_table = 't' WHERE is_in_final_table = 'f'"
        self.connection.execute(text(query))
        self.connection.commit()

    def append_to_breadcrumb(self, df):
        df.to_sql(self.breadcrumb_table, self.engine, if_exists='append', index=False)
        
    def append_to_trip(self, df):
        df.to_sql(self.trip_table, self.engine, if_exists='append', index=False)

    def get_raw(self):
        return pd.read_sql(f"SELECT * FROM {self.raw_table}", self.engine)

    def get_breadcrumb(self):
        return pd.read_sql(f"SELECT * FROM {self.breadcrumb_table}", self.engine)

    def get_trip(self):
        return pd.read_sql(f"SELECT * FROM {self.trip_table}", self.engine)

    def empty_raw(self):
        query = f"DELETE FROM {self.raw_table}"
        self.connection.execute(text(query))
        self.connection.commit()

