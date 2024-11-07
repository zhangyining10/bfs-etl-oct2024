from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_ROLE = 'BF_DEVELOPER1007'
SNOWFLAKE_WAREHOUSE = 'BF_ETL1007'
# @TODO: our own stage name
SNOWFLAKE_STAGE = 'beaconfire_stage'

SNOWFLAKE_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA = 'BF_DEV'
# @TODO: our own table name
SNOWFLAKE_TARGET_TABLE = 'airflow_testing'

DAG_ID = "project2_snowflake_to_snowflake"
SQL_FILE = "sql_op_str.sql"
# SQL commands
# @TODO: our own SQL statement
CREATE_TABLE_SQL_STRING = (
    f'''CREATE OR REPLACE TABLE AIRFLOW1007.BF_DEV.FACT_STOCK_HISTORY_TEAM6 (
            SYMBOL VARCHAR(16) NOT NULL,
            TRADEDATE DATE NOT NULL,
            OPEN FLOAT,
            HIGH FLOAT,
            LOW FLOAT,
            CLOSE FLOAT,
            VOLUME NUMBER(38,0),
            ADJCLOSE FLOAT,
            primary key (SYMBOL, TRADEDATE),
            foreign key (SYMBOL) references AIRFLOW1007.BF_DEV.DIM_SYMBOLS_TEAM6(SYMBOL)
        );
        create or replace TABLE AIRFLOW1007.BF_DEV.DIM_COMPANY_PROFILE_TEAM6 (
            ID NUMBER(38,0) NOT NULL,
            SYMBOL VARCHAR(16),
            PRICE FLOAT,
            BETA FLOAT,
            VOLAVG FLOAT,
            INDUSTRY VARCHAR(16777216),
            SECTOR VARCHAR(16777216),
            WEBSITE VARCHAR(16777216),
            NAME VARCHAR(16777216),
            primary key (ID)
        );
        create or replace TABLE AIRFLOW1007.BF_DEV.DIM_SYMBOLS_TEAM6 (
            SYMBOL VARCHAR(16) NOT NULL,
            NAME VARCHAR(16777216),
            EXCHANGE VARCHAR(16777216),
            primary key (SYMBOL)
        );
        '''
)
# DAG operator
with DAG(
    DAG_ID,
    start_date=datetime(2024, 11, 7),
    schedule_interval='*/5 * * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['team6project2'],
    catchup=False,
) as dag:
    
    snowflake_op_sql_create_table = SnowflakeOperator(
        task_id='snowflake_op_sql_create_table',
        sql=CREATE_TABLE_SQL_STRING,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_op_sql_merge = SnowflakeOperator(
        task_id='snowflake_op_sql_merge',
        sql=SQL_FILE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    (
        snowflake_op_sql_create_table
        >> [
            snowflake_op_sql_merge,
            ]

    )
