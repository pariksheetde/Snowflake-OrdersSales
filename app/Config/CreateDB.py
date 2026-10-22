# pip install snowflake-snowpark-python
# pip install snowflake-connector-python

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import *
import snowflake.connector
from snowflake.snowpark import Session
import os
from configs import connection_parameters
from snowflake.snowpark import Session

def main():
    session = Session.builder.configs(connection_parameters).create()

    try:
        print("Connected successfully")

        session.sql("CREATE DATABASE IF NOT EXISTS ORDERS_SALES").collect()
        session.sql("CREATE SCHEMA IF NOT EXISTS ORDERS_SALES.BRONZE").collect()

        print("Database and Schema created")

    finally:
        session.close()

if __name__ == "__main__":
    main()