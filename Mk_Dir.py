"""
Code that create a directoy in DAGS folder

"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


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

dag = DAG("Mk_Dir", default_args=default_args, schedule_interval=timedelta(1))


t1 = BashOperator(task_id="Mkdir", bash_command="mkdir /usr/local/airflow/store_files_airflow", retries=1, dag=dag)
