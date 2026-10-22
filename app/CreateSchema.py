from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import *

def main():
    session = get_active_session
    session.sql("CREATE DATABASE IF NOT EXISTS ORDERS_SALES").collect()
    session.sql("CREATE SCHEMA IF NOT EXISTS ORDERS_SALES.BRONZE").collect()

if __name__ == "__main__":
    main()