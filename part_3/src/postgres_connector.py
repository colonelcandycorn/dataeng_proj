import pandas as pd
import psycopg as pg
import os
from sqlalchemy import create_engine, inspect, text, BigInteger, Text
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
        self.part3_table = os.environ.get("PART3_TABLE")
        self.engine = create_engine(f'postgresql://{user}:{quote_plus(password)}@{host}:{port}/{db}')
        self.connection = self.engine.connect()

    def upsert_to_trip(self, df):
        temp_table_name = "temp_trip_table"
        engine = self.engine

        with engine.connect() as connection:
            connection.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
            connection.commit()

            connection.execute(text(f"""
                    CREATE TEMPORARY TABLE {temp_table_name} (
                        trip_id bigint,
                        route_id text,
                        vehicle_id bigint,
                        service_key text,
                        direction text
                    );
                """))
    
            connection.commit()
    
            df = df.drop_duplicates(subset=['trip_id'])
    
            df.to_sql(temp_table_name, con=connection, if_exists='append', index=False, dtype={
                          'trip_id': BigInteger,
                          'route_id': Text,
                          'vehicle_id': BigInteger,
                          'service_key': Text,
                          'direction': Text
            })
            connection.execute(text(f"""
                        INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction)
                        SELECT 
                            trip_id::bigint, 
                            route_id, 
                            vehicle_id::bigint, 
                            service_key, 
                            direction
                        FROM {temp_table_name}
                        ON CONFLICT (trip_id)
                        DO UPDATE SET
                            route_id = EXCLUDED.route_id,
                            vehicle_id = EXCLUDED.vehicle_id,
                            service_key = EXCLUDED.service_key,
                            direction = EXCLUDED.direction;
                    """))
    
            connection.commit()
        
            connection.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
            connection.commit()

    def append_to_part3(self, df):
        df.to_sql(self.part3_table, self.engine, if_exists='append', index=False)
        
    def get_part3(self):
        return pd.read_sql(f"SELECT * FROM {self.part3_table}", self.engine)

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
