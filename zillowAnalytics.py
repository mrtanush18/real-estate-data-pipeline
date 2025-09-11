from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
import json
from datetime import timedelta, datetime
import requests
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
import time

with open('/home/ubuntu/airflow/api_key.json', 'r') as config_file:
    api_host_key = json.load(config_file)

now = datetime.now()
dt_now_string = now.strftime("%d%m%Y")

locations = [
   "seattle, wa",
    "new york, ny",
    "los angeles, ca",
    "san francisco, ca",
    "miami, fl"
]

res = []

def extract_data():
    for i in range(0, len(locations)):
        url = "https://zillow56.p.rapidapi.com/search"
        querystring = {"location": locations[i]}
        headers = api_host_key
        response = requests.get(url, headers=headers, params=querystring)
        response_data = response.json()
        res.append(response_data)
        time.sleep(2)

    output_file_path = f"/home/ubuntu/response_data_{dt_now_string}.json"
    file_name = f"response_data_{dt_now_string}.csv"

    with open(output_file_path, "w") as output_file:
        json.dump(res, output_file, indent=4)  

    print(res)

    list = [output_file_path, file_name]

    return list

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'email': ['tanushshetty47@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=15)
}

with DAG('zillow_analytics_dag',
         default_args = default_args,
         schedule_interval = '@daily',
         catchup = False) as dag:

        extract_zillow_data_var = PythonOperator(
            task_id = 'task_extract_zillow_data',
            python_callable = extract_data
        )

        load_to_s3_bucket = BashOperator(
            task_id = 'task_move_data_to_s3',
            bash_command = 'aws s3 mv {{ ti.xcom_pull("task_extract_zillow_data")[0] }} s3://zillowprojectdatabucket/',
        )

        trsfm_file_check_in_s3 = S3KeySensor(
             task_id = "trsfm_file_check_in_s3",
             bucket_key = '{{ti.xcom_pull("task_extract_zillow_data")[1]}}',
             bucket_name = "zillowtransformdata",
             aws_conn_id='aws_s3_conn',
             wildcard_match=False,  
             timeout=60,
             poke_interval=5,
        )

        load_to_redshift_from_s3 = S3ToRedshiftOperator(
            task_id = "load_to_redshift",
            aws_conn_id='aws_s3_conn',
            redshift_conn_id='conn_id_redshift',
            s3_bucket = "zillowtransformdata",
            s3_key = '{{ti.xcom_pull("task_extract_zillow_data")[1]}}',
            schema = "PUBLIC",
            table = 'zillowdata',
            copy_options=['csv IGNOREHEADER 1'],
            
        )

        
        extract_zillow_data_var >> load_to_s3_bucket >> trsfm_file_check_in_s3 >> load_to_redshift_from_s3
             
