import duckdb
import pandas as pd
from query_db.utils import validate_memory_limit


class DatabaseManager:
    def __init__(self, db_file, memory_limit="8GB", read_only=False):
        validated_memory_limit = validate_memory_limit(memory_limit)
        
        self.db_file = db_file
        self.con = duckdb.connect(database=db_file, read_only=read_only)
        self.con.execute(f"SET memory_limit='{validated_memory_limit}';")
        self.con.execute("PRAGMA threads=1;")
    
    def query_df(self, sql_query, params=None):
        if params:
            return self.con.execute(sql_query, params).fetch_df()
        return self.con.execute(sql_query).fetch_df()
    
    def query(self, sql_query, params=None):
        if params:
            return self.con.execute(sql_query, params).fetchall()
        return self.con.execute(sql_query).fetchall()
    
    def query_one(self, sql_query, params=None):
        if params:
            return self.con.execute(sql_query, params).fetchone()
        return self.con.execute(sql_query).fetchone()
    
    def execute(self, sql_query, params=None):
        if params:
            self.con.execute(sql_query, params)
        else:
            self.con.execute(sql_query)
    
    def register_df(self, name, df):
        self.con.register(name, df)
    
    def create_function(self, name, func, arg_types, return_type):
        self.con.create_function(name, func, arg_types, return_type)
    
    def close(self):
        if self.con:
            self.con.close()