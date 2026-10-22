# pip install snowflake-snowpark-python
# pip install snowflake-connector-python

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import *
import snowflake.connector
from snowflake.snowpark import Session
import os
from config import connection_parameters

def main():
    session = Session.builder.configs(connection_parameters).create()

    try:
        # session.sql("CREATE DATABASE IF EXISTS ORDERS_SALES").collect()
        # print("Database ORDERS_SALES verified/created successfully.")
        session.sql("DROP SCHEMA IF EXISTS ORDERS_SALES.BRONZE").collect()
        print("Schema BRONZE dropped successfully.")

        

    finally:
        session.close()

if __name__ == "__main__":
    main()