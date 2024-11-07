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
UPDATE_TABLE_SQL_STRING = (
    f"UPDATE {SNOWFLAKE_TARGET_TABLE} SET name = 'new_name' WHERE id = 1;"
)
# DAG operator
with DAG(
    DAG_ID,
    start_date=datetime(2024, 11, 7),
    schedule_interval='0 0 * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['team6project2'],
    catchup=False,
) as dag:
    
    snowflake_op_sql_str = SnowflakeOperator(
        task_id='snowflake_op_sql_str',
        sql=UPDATE_TABLE_SQL_STRING,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_op_sql_file = SnowflakeOperator(
        task_id='snowflake_op_sql_file',
        sql=SQL_FILE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    (
        snowflake_op_sql_str
        >> snowflake_op_sql_file

    )
