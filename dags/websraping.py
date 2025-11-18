import requests
from bs4 import BeautifulSoup
import csv
import duckdb
import pendulum
import logging
from airflow import DAG
from airflow.decorators import dag
# from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator






# ACCESS_KEY = Variable.get("access_key")
# SECRET_KEY = Variable.get("secret_key")


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
    dag_id='webcraping',
    default_args=default_args,
    start_date= pendulum.datetime(2025, 8, 7),
    schedule_interval='0 5 * * *',
)

def refined_price(s):
    r = s.strip()
    no_space_string = r.replace(" ", "")
    delete_element =  no_space_string[:11]
    delete_element_end = delete_element.replace("./ш", "")
    delete_element_end_rub = delete_element_end.replace("руб", "")
    return delete_element_end_rub

def refined_old_price(o):
    r = o.replace(" ", "")
    delete_element = r.strip()
    delete_element_end = delete_element.replace("руб.", "")
    return delete_element_end

def write_csv(data):
    with open("C:\Project\webscrapping-airflow\sumkimarket.csv", "a", encoding="utf-8") as f:
        writer =  csv.writer(f)
        
        writer.writerow([data['title'],
                         data['price'],
                         data['old_price'],
                         data['http']])
    

# def connect_to_db():
#     conn = psycopg2.connect(
#     database = "postgres",
#     user = "postgres",
#     password = "postgres",
#     host = "localhost",
#     port = "5432"
# )

#     conn.close()
#     print("DB connected successfully")


    

def extract_html(url):
    r = requests.get(url)
    return r.text

def get_data(html):
    soup = BeautifulSoup(html, 'lxml')
    data = soup.find("body").find("div", {"id": "main"}).find("div", {"id": "catalogSection"}).find_all("div", {"class": "item product sku"})
   
    
    
    for da in data:
        try:
            title = da.find("div", {"class": "productTable"}).find("div", {"class":"productColText"}).find("span", {"class": "middle"}).text
        except:
            title = ''
            
        try:
            r = da.find("div", {"class": "productTable"}).find("div", {"class":"productColText"}).find("a", {"class": "price"}).text
            price = refined_price(r)
        except:
            price = ''
            
        try:
            o = da.find("div", {"class": "productTable"}).find("div", {"class":"productColText"}).find("s", class_="discount").text
            old_price = refined_old_price(o)
        except:
            old_price = ''
            
        try:
            http = 'https://sakvoyazh-online.ru' + da.find("div", {"class": "productTable"}).find("div", {"class":"productColText"}).find("a").get("href")
        except:
            http = ''
        
        
        
        data = {
            "title": title,
            "price": price,
            "old_price": old_price,
            "http": http
        }
        
        write_csv(data)
        
       

        
        
        
    

def get_sumki_data(**context):
    
    start_date, end_date = get_dates(**context)
    logging.info(f"Start parsing for dates:{start_date}/{end_date}")
    pattern = "https://sakvoyazh-online.ru/catalog/zhenskoe/sumki_zhenskie/?PAGEN_1={}"
    
    for i in range(0, 10):
        url = pattern.format(str(i))
        get_data(extract_html(url))
        
    logging.info(f"Parsing for date success:{start_date}")


# def transfer_data_to_postgres(**context):
    
#     start_date, end_date= get_dates(**context)
#     logging.info(f"Start load for dates:{start_date}/{end_date}")
#     conn = duckdb.connect()
    
#     conn.sql(
#         f"""
#         SET TIMEZONE = 'UTC';
#         INSTALL httpfs;
#         LOAD httpfs;
#         SET s3_url_style = 'path';
#         SET s3_endpoint = 'minio:9000';
#         SET s3_access_key_id = '[ACCESS_KEY]';
#         SET s3_secret_key = '[SECRET_KEY]';
#         SET s3_use_ssl = FALSE;
        
#         COPY
#         (
#             SELECT
#                 *
#             FROM
#                 read_csv_auto('C:\Project\Forresume\sumkimarket.csv') AS res
#         ) TO 's3://prod/prod_00_00_00.gz.parquet';
#         """,
#     )
    
#     conn.close()
#     logging.info(f"Download for date success:{start_date}")

start = EmptyOperator(
    task_id="start",
)

fetch_sumki_data_task = PythonOperator(
    task_id = 'get_sumki_data',
    python_callable=get_sumki_data,
    dag=dag,
)

end = EmptyOperator(
    task_id="end",
)

# transfer_to_postgres_task = PythonOperator(
#     task_id = 'transfer_data',
#     python_callable=transfer_data_to_postgres,
#     dag=dag,
# )



start >> fetch_sumki_data_task >> end 