import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'AIRFLOW1007'
SNOWFLAKE_SCHEMA = 'BF_DEV'

SNOWFLAKE_ROLE = 'AW_developer'
SNOWFLAKE_WAREHOUSE = 'aw_etl'
SNOWFLAKE_STAGE = 's3_stage_trans_order'


with DAG(
    "project1_s3_to_snowflake",
    start_date = datetime(2024, 11, 6),
    end_date = datetime(2024, 11, 9),
    schedule_interval='0 0 * * *', # send the data to snowflake every day at midnight
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['beaconfire'],
    catchup=True,
) as dag:
    
    copy_into_prestg = CopyFromExternalStageToSnowflakeOperator(
        task_id='prestg_sales_data',
        files=['SalesData_Group6_{{ ds }}.csv'],
        # @TODO change to our table name if not already
        table='prestage_TableName_Group6',
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format='''(type = 'CSV', field_delimiter = ',', SKIP_HEADER = 1 \
            NULL_IF =('NULL','null',''), empty_field_as_null = true, FIELD_OPTIONALLY_ENCLOSED_BY = '\"' \
            ESCAPE_UNENCLOSED_FIELD = NONE RECORD_DELIMITER = '\n')''',

    )
    copy_into_prestg
