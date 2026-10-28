# pip install snowflake-snowpark-python
# pip install snowflake-connector-python

'''
#### CREATE WAREHOUSE ORDERS_SALES_WAREHOUSE
#### WITH BELOW PARAMETERS LIKE
#### WAREHOUSE_SIZE, AUTO_SUSPEND, AUTO_RESUME,
#### INITIALLY_SUSPENDED, MIN_CLUSTER_COUNT,
#### MAX_CLUSTER_COUNT, SCALING_POLICY, COMMENT
'''

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import *
import snowflake.connector
from snowflake.snowpark import Session
import os
from config import connection_parameters
from snowflake.snowpark import Session

def main():
    session = Session.builder.configs(connection_parameters).create()

    try:
        print("Connected successfully")

        session.sql("USE ROLE ACCOUNTADMIN").collect()
        session.sql('''CREATE WAREHOUSE IF NOT EXISTS ORDERS_SALES_WAREHOUSE
                        WITH
                        WAREHOUSE_SIZE = 'XSMALL'
                        AUTO_SUSPEND = 300
                        AUTO_RESUME = TRUE
                        INITIALLY_SUSPENDED = TRUE
                        MIN_CLUSTER_COUNT = 1
                        MAX_CLUSTER_COUNT = 3
                        SCALING_POLICY = 'STANDARD'
                        COMMENT = "Warehouse for Orders and Sales processing"
    ''').collect()

        print("Warehouse created")

    finally:
        session.close()

if __name__ == "__main__":
    main()