import duckdb
import pendulum
import logging
from airflow import DAG
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator



duckdb.query("install 'httpfs';")


ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

LAYER = "raw"
SOURCE = "websraping"

def get_dates(**context) -> tuple[str, str]:
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYY-MM-DD")
    
    return start_date, end_date

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 8, 17, tz="Europe/Moscow"),
    'retries': 3,
    'retray_daily': pendulum.duration(hours=1)
}

    
dag = DAG(
    dag_id='transfer_to_s3',
    default_args=default_args,
    start_date= pendulum.datetime(2025, 8, 17),
    schedule_interval='0 5 * * *',
)

def transfer_data_to_s3(**context):
    
    start_date, end_date= get_dates(**context)
    logging.info(f"Start load for dates:{start_date}/{end_date}")
    conn = duckdb.connect()
    
    conn.sql(
        f"""
        SET TIMEZONE = 'UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;
        
        COPY
        (
            SELECT
                *
            FROM
                read_csv_auto('C:\Project\webscrapping-airflow\sumkimarket.csv') AS res
        ) TO 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
        """,
    )
    
    conn.close()
    logging.info(f"Download for date success:{start_date}")
    

start = EmptyOperator(
    task_id="start",
)

    
transfer_to_s3_task = PythonOperator(
    task_id = 'transfer_data',
    python_callable=transfer_data_to_s3,
    dag=dag,
)


end = EmptyOperator(
    task_id="end",
)


start >> transfer_to_s3_task >> end