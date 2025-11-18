import logging
import pendulum

from airflow import DAG
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# from duckdb_extensions import import_extension

# import_extension("httpfs")
# import_extension("postgres_scanner")
# import_extension("sqlite")
# import_extension("sqlite3")

import duckdb




#S3

ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

#Duckdb

PASSWORD = Variable.get('pg_password')


SCHEMA = 'ods'
TARGET_TABLE = 'fct_sumki'



def get_dates(**context) -> tuple[str, str]:
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    
    return start_date, end_date

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 8, 17, tz="Europe/Moscow"),
    'retries': 3,
    'retray_daily': pendulum.duration(hours=1)
    
}


def transfer_data_to_postgres(**context):
    
    start_date, end_date = get_dates(**context)
    logging.info(f'Start load for dates:{start_date}/{end_date}')
    
    conn = duckdb.connect()
    
    
    conn.sql(
        f"""
        SET TIMEZONE = 'UTC';
        INSTALL httpfs;
        LOAD httpfs;
        INSTALL sqlite_scanner FROM core_nightly;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;
        
        CREATE SECRET dwh_postgres(
            TYPE postgres,
            HOST 'postgres_dwh',
            PORT 5432,
            DATABASE postgres,
            USER 'postres',
            PASSWORD '{PASSWORD}'
        );
        
        ATTACH '' AS dwh_postgres_db (TYPE postges, SECRET dwh_postgres);
        
        INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
        (
            title,
            price,
            old_price,
            http
        )
        SELECT
            title,
            price,
            old_price,
            http
        FROM 's3://prod/raw/websraping/sumkimarket.gzip.parquet';
        """,
    )
    
    conn.close()
    logging.info(f"Download for date success: {start_date}")
    
    

dag=DAG(
    dag_id='transfer_to_postgres',
    default_args=default_args,
    start_date=pendulum.datetime(2025, 8, 17, tz="Europe/Moscow"),
    schedule_interval=None,
)
    
start = EmptyOperator(
    task_id='start',
)

trigger_on_raw_layer = TriggerDagRunOperator(
    task_id = 'trigger_on_raw_layer',
    trigger_dag_id='transfer_to_s3',
    dag=dag,
    
)

get_and_transfer_raw_data_to_ods_pg = PythonOperator(
    task_id = 'get_and_transfer_raw_data_to_ods_pg',
    python_callable=transfer_data_to_postgres,
    dag=dag,
)

end = EmptyOperator(
    task_id = 'end',
)

start >> trigger_on_raw_layer >> get_and_transfer_raw_data_to_ods_pg >> end