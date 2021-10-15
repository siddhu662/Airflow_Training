from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from datacleaner import data_cleaner
from airflow.operators.mysql_operator import MySqlOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 9, 29),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("store_DAG", default_args=default_args, schedule_interval=timedelta(1),template_searchpath=['/usr/local/airflow/sql_files'],catchup=False)


t1 = BashOperator(task_id="Check_file_exists", bash_command='shasum ~/store_files_airflow/raw_store_transactions.csv',
                    retries=1,retry_delay=timedelta(seconds=15), dag=dag)

t2=PythonOperator(task_id='clean_file',python_callable=data_cleaner,dag=dag)

t3=MySqlOperator(task_id="create_table_mysql",mysql_conn_id='mysql_conn',sql="create_table.sql",dag=dag)

t4=MySqlOperator(task_id="insert_table_mysql",mysql_conn_id='mysql_conn',sql="insert_table.sql",dag=dag)

t1 >> t2 >>t3 >>t4
