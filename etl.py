import os
from datetime import timedelta
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
def task_1():
    path = r'C:\****\****\****\***'
    files = os.listdir(path)
    engine = create_engine('mysql+pymysql://root:*********@localhost:3306/cars_database')
    table_name = ['Sales_dim','dates_dim','cars_dim']
    for i in files:
    for j in table_name:
        data = pd.read_csv(os.path.join(path, i),header=0)
        data.to_sql(name=j, con=engine, if_exists='replace', index=False)
dag_path = os.getcwd()
default_args = {
        'owner': 'Sunny', 
        'start_date': days_ago(5), 
        'retry_delay': timedelta(minutes=5)
}

dag = DAG(
        'Simple_dag',
        default_args=default_args,
        description='Reading csv files',
        schedule_interval=timedelta(days=1),
)

task = PythonOperator(
        task_id='Extract and Load',
        python_callable=task_1,
        dag=dag,
)

task

