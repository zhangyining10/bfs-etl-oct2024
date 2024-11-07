from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_ROLE = 'BF_DEVELOPER1007'
SNOWFLAKE_WAREHOUSE = 'BF_ETL1007'

SNOWFLAKE_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA = 'BF_DEV'

DAG_ID = "project2_snowflake_to_snowflake"
SQL_FILE = "sql_op_str.sql"

# DAG operator
with DAG(
    DAG_ID,
    start_date=datetime(2024, 11, 7),
    schedule_interval='*/5 * * * *',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['team6project2'],
    catchup=False,
) as dag:

    snowflake_op_sql_merge = SnowflakeOperator(
        task_id='snowflake_op_sql_merge',
        sql=SQL_FILE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    (
        snowflake_op_sql_merge

    )
