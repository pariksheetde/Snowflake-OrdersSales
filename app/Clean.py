from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import *

def main():
    session = get_active_session
    session.sql("DROP SCHEMA IF EXISTS ORDERS_SALES.BRONZE").collect()
    session.sql("CREATE DATABASE IF NOT EXISTS ORDERS_SALES").collect()
    print("DATABASE DROPPED...")
    
if __name__ == "__main__":
    main()